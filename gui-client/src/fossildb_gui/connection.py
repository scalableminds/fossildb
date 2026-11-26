"""Blocking access to a FossilDB server. All calls are meant to run in background threads (see tasks.py)."""

import socket
import threading

import grpc

from fossildb_gui.api import pb, pb_grpc
from fossildb_gui.tunnel import SshTunnel

MAX_MESSAGE_LENGTH = 1 << 30
DEFAULT_TIMEOUT_SECONDS = 30
CONNECT_TIMEOUT_SECONDS = 10


class FossilDBError(Exception):
    pass


class NotSupportedError(FossilDBError):
    """The server does not know this call, it is older than the client."""


class Connection:
    def __init__(self, target: str, label: str, tunnel: SshTunnel | None = None):
        self.target = target  # host:port that the channel connects to
        self.label = label  # shown to the user, e.g. the remote address for a tunnel
        self.tunnel = tunnel
        self._channel = grpc.insecure_channel(
            target,
            options=[
                ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
                ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
            ],
        )
        self._stub = pb_grpc.FossilDBStub(self._channel)

    def check_health(self, timeout: float = CONNECT_TIMEOUT_SECONDS) -> None:
        state = self._wait_for_channel(timeout)
        if state == grpc.ChannelConnectivity.TRANSIENT_FAILURE:
            if self.tunnel is not None:
                reason = self.tunnel.last_error() or "the connection failed"
                raise FossilDBError(f"{self.label}: not reachable from {self.tunnel.ssh_host} ({reason})")
            raise FossilDBError(
                f"{self.label} accepted the connection, but did not answer like FossilDB. Is the port right?"
            )
        if state != grpc.ChannelConnectivity.READY:
            raise FossilDBError(f"No answer from FossilDB at {self.label} within {timeout:g} seconds")
        self._call("Health", pb.HealthRequest(), timeout)

    def _wait_for_channel(self, timeout: float) -> grpc.ChannelConnectivity | None:
        """Waits until the channel is ready or its first connection attempt failed. gRPC itself would keep retrying
        until the timeout, even when the connection was refused right away."""
        result: list[grpc.ChannelConnectivity] = []
        done = threading.Event()

        def watch(state: grpc.ChannelConnectivity) -> None:
            if state in (grpc.ChannelConnectivity.READY, grpc.ChannelConnectivity.TRANSIENT_FAILURE):
                result.append(state)
                done.set()

        self._channel.subscribe(watch, try_to_connect=True)
        try:
            done.wait(timeout)
        finally:
            self._channel.unsubscribe(watch)
        return result[0] if result else None

    def close(self) -> None:
        self._channel.close()
        if self.tunnel is not None:
            self.tunnel.stop()

    def _call(self, method: str, request, timeout: float | None = DEFAULT_TIMEOUT_SECONDS):
        try:
            reply = getattr(self._stub, method)(request, timeout=timeout)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                raise NotSupportedError(f"This FossilDB server does not support {method}, it is too old") from None
            raise FossilDBError(f"{method} failed: {e.details() or e.code().name}") from None
        if not reply.success:
            raise FossilDBError(reply.errorMessage or f"{method} failed")
        return reply

    def database_info(self):
        """The DatabaseInfoProto, or None if the server is too old to provide it."""
        try:
            return self._call("GetDatabaseInfo", pb.GetDatabaseInfoRequest()).databaseInfo
        except NotSupportedError:
            return None

    def list_keys(self, collection: str, limit: int, start_after: str | None = None, prefix: str | None = None):
        request = pb.ListKeysRequest(collection=collection, limit=limit)
        if start_after is not None:
            request.startAfterKey = start_after
        if prefix:
            request.prefix = prefix
        return list(self._call("ListKeys", request).keys)

    def list_versions(self, collection: str, key: str, limit: int | None = None, offset: int = 0) -> list[int]:
        """The versions of the key, newest first; with limit and offset a part of them, counted from the newest."""
        request = pb.ListVersionsRequest(collection=collection, key=key)
        if limit is not None:
            request.limit = limit
        if offset:
            request.offset = offset
        return sorted(self._call("ListVersions", request).versions, reverse=True)

    def get(self, collection: str, key: str, version: int) -> bytes:
        return self.get_version(collection, key, version)[0]

    def get_version(self, collection: str, key: str, version: int) -> tuple[bytes, int]:
        """The value and its actual version: if the version doesn't exist, FossilDB returns the next older one."""
        reply = self._call("Get", pb.GetRequest(collection=collection, key=key, version=version))
        return reply.value, reply.actualVersion

    def version_sizes(self, collection: str, key: str, newest: int, oldest: int) -> list[tuple[int, int]]:
        """(version, value size) for the versions from newest to oldest. FossilDB can't report sizes, so this
        downloads the values."""
        request = pb.GetMultipleVersionsRequest(
            collection=collection, key=key, newestVersion=newest, oldestVersion=oldest
        )
        reply = self._call("GetMultipleVersions", request)
        return [(version, len(value)) for version, value in zip(reply.versions, reply.values, strict=True)]

    def delete_version(self, collection: str, key: str, version: int) -> None:
        self._call("Delete", pb.DeleteRequest(collection=collection, key=key, version=version))

    def delete_key(self, collection: str, key: str) -> None:
        """Deletes all versions of the key."""
        self._call("DeleteMultipleVersions", pb.DeleteMultipleVersionsRequest(collection=collection, key=key))

    def delete_all_by_prefix(self, collection: str, prefix: str) -> None:
        # Can take long on large collections, so no timeout
        self._call("DeleteAllByPrefix", pb.DeleteAllByPrefixRequest(collection=collection, prefix=prefix), None)

    def backup(self):
        """Creates a backup in the server's backup directory. Returns the BackupReply (id, timestamp, size)."""
        # Blocks until the backup is done, which can take long
        return self._call("Backup", pb.BackupRequest(), None)

    def export_db(self, new_data_dir: str, options_file: str | None = None) -> None:
        """Copies the database into a new directory on the server, optionally with other RocksDB options."""
        request = pb.ExportDBRequest(newDataDir=new_data_dir)
        if options_file:
            request.optionsFile = options_file
        self._call("ExportDB", request, None)

    def compact_all_data(self) -> None:
        # Rewrites the whole database, which can take hours
        self._call("CompactAllData", pb.CompactAllDataRequest(), None)


def connect(address: str, ssh_host: str | None = None) -> Connection:
    """Connects to address (host:port), through an SSH tunnel to ssh_host if given. Blocks until the server answers."""
    tunnel = None
    if ssh_host:
        remote_host, _, remote_port = address.rpartition(":")
        if not remote_host or not remote_port.isdigit():
            raise FossilDBError(f"Expected the FossilDB address as host:port, got {address}")
        tunnel = SshTunnel(ssh_host, remote_host, int(remote_port))
        local_port = tunnel.start()
        connection = Connection(f"127.0.0.1:{local_port}", f"{address} via {ssh_host}", tunnel)
    else:
        _check_reachable(address)
        connection = Connection(address, address)
    try:
        connection.check_health()
    except Exception:
        connection.close()
        raise
    return connection


def _check_reachable(address: str, timeout: float = CONNECT_TIMEOUT_SECONDS) -> None:
    """Opens and closes a plain network connection, which tells right away why a server can't be reached."""
    host, _, port = address.rpartition(":")
    host = host.strip("[]")  # IPv6, e.g. [::1]:7155
    if not host or not port.isdigit():
        raise FossilDBError(f"Expected the FossilDB address as host:port, got {address}")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            pass
    except socket.gaierror:
        raise FossilDBError(f"Unknown host {host}") from None
    except ConnectionRefusedError:
        raise FossilDBError(
            f"Nothing is listening at {address} (connection refused). Is FossilDB running there?"
        ) from None
    except TimeoutError:
        raise FossilDBError(f"No answer from {address} within {timeout:g} seconds") from None
    except OSError as e:
        raise FossilDBError(f"Could not reach {address}: {e.strerror or e}") from None


def probe(address: str, timeout: float) -> None:
    """Raises FossilDBError unless a FossilDB server answers at address (host:port) within the timeout."""
    _check_reachable(address, timeout)
    connection = Connection(address, address)
    try:
        connection.check_health(timeout)
    except NotSupportedError:
        raise FossilDBError(f"{address} is a gRPC server, but not FossilDB") from None
    finally:
        connection.close()
