"""SSH port forwarding with the system's ssh, so the user's ~/.ssh/config and ssh agent apply."""

import atexit
import shutil
import socket
import subprocess
import threading
import time

# Tunnels run in their own session and would outlive the app, so stop them on exit
_running: set["SshTunnel"] = set()
atexit.register(lambda: [tunnel.stop() for tunnel in list(_running)])


class TunnelError(Exception):
    pass


def _free_local_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def local_ports_in_use() -> set[int]:
    """The local ports of the tunnels that this app runs."""
    return {tunnel.local_port for tunnel in list(_running) if tunnel.local_port is not None}


class SshTunnel:
    def __init__(self, ssh_host: str, remote_host: str, remote_port: int):
        self.ssh_host = ssh_host
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.local_port: int | None = None
        self._process: subprocess.Popen | None = None
        # ssh's error output, collected while it runs, e.g. "channel 2: open failed: connect failed: ..."
        self._errors: list[str] = []

    def start(self, timeout_seconds: float = 20) -> int:
        """Starts the tunnel and returns the local port once it accepts connections."""
        ssh = shutil.which("ssh") or "/usr/bin/ssh"
        self.local_port = _free_local_port()
        command = [
            ssh,
            "-N",
            # There is no terminal to ask for passwords, so authentication must work without (keys, ssh agent)
            "-o", "BatchMode=yes",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "ConnectTimeout=10",
            "-o", "ServerAliveInterval=30",
            "-L", f"127.0.0.1:{self.local_port}:{self.remote_host}:{self.remote_port}",
            self.ssh_host,
        ]  # fmt: skip
        self._process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            start_new_session=True,  # don't receive the terminal's Ctrl+C
        )
        _running.add(self)
        threading.Thread(target=self._collect_errors, args=(self._process.stderr,), daemon=True).start()
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise TunnelError(f"SSH to {self.ssh_host} failed: {self._read_error() or 'ssh exited'}")
            try:
                # ssh only listens on the local port once the connection is established
                with socket.create_connection(("127.0.0.1", self.local_port), timeout=0.2):
                    return self.local_port
            except OSError:
                time.sleep(0.1)
        self.stop()
        raise TunnelError(f"SSH tunnel to {self.ssh_host} was not ready after {timeout_seconds:.0f} seconds")

    def is_alive(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def exit_message(self) -> str:
        return self._read_error() or "ssh exited"

    def _collect_errors(self, stream) -> None:
        for line in stream:
            self._errors.append(line.decode(errors="replace").strip())

    def _read_error(self) -> str:
        if self._process is not None and self._process.poll() is not None:
            time.sleep(0.05)  # the last lines may still be on their way
        return "\n".join(line for line in self._errors if line)

    def last_error(self, wait_seconds: float = 0.5) -> str:
        """The latest message from ssh, e.g. why a forwarded connection failed. Waits a little for it to arrive."""
        deadline = time.monotonic() + wait_seconds
        while not self._errors and time.monotonic() < deadline:
            time.sleep(0.02)
        return self._errors[-1] if self._errors else ""

    def stop(self) -> None:
        _running.discard(self)
        if self._process is None or self._process.poll() is not None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self._process.kill()
