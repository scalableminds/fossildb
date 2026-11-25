"""Blocking access to a FossilDB server. All calls are meant to run in background threads (see tasks.py)."""

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

    def check_health(self) -> None:
        try:
            grpc.channel_ready_future(self._channel).result(timeout=CONNECT_TIMEOUT_SECONDS)
        except grpc.FutureTimeoutError:
            raise FossilDBError(f"Could not reach FossilDB at {self.label}") from None
        self._call("Health", pb.HealthRequest(), CONNECT_TIMEOUT_SECONDS)

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

    def list_versions(self, collection: str, key: str) -> list[int]:
        """All versions of the key, newest first."""
        versions = self._call("ListVersions", pb.ListVersionsRequest(collection=collection, key=key)).versions
        return sorted(versions, reverse=True)

    def get(self, collection: str, key: str, version: int) -> bytes:
        return self._call("Get", pb.GetRequest(collection=collection, key=key, version=version)).value

    def delete_version(self, collection: str, key: str, version: int) -> None:
        self._call("Delete", pb.DeleteRequest(collection=collection, key=key, version=version))

    def delete_key(self, collection: str, key: str) -> None:
        """Deletes all versions of the key."""
        self._call("DeleteMultipleVersions", pb.DeleteMultipleVersionsRequest(collection=collection, key=key))

    def delete_all_by_prefix(self, collection: str, prefix: str) -> None:
        # Can take long on large collections, so no timeout
        self._call("DeleteAllByPrefix", pb.DeleteAllByPrefixRequest(collection=collection, prefix=prefix), None)

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
        connection = Connection(address, address)
    try:
        connection.check_health()
    except Exception:
        connection.close()
        raise
    return connection
