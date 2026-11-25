"""SshTunnel with a fake ssh on PATH, which forwards the port locally instead of over SSH."""

import os
import socket
import stat
import sys
import threading

import pytest

from fossildb_gui.tunnel import SshTunnel, TunnelError

FAKE_SSH = """#!{python}
import socket, sys, threading
args = sys.argv[1:]
if args[-1] == "unreachable":
    sys.stderr.write("ssh: Could not resolve hostname unreachable\\n")
    sys.exit(255)
_, local_port, remote_host, remote_port = args[args.index("-L") + 1].split(":")
listener = socket.create_server(("127.0.0.1", int(local_port)))
def pipe(a, b):
    while data := a.recv(65536):
        b.sendall(data)
    b.close()
while True:
    client, _ = listener.accept()
    upstream = socket.create_connection((remote_host, int(remote_port)))
    threading.Thread(target=pipe, args=(client, upstream), daemon=True).start()
    threading.Thread(target=pipe, args=(upstream, client), daemon=True).start()
"""


@pytest.fixture
def fake_ssh(tmp_path, monkeypatch):
    ssh = tmp_path / "ssh"
    ssh.write_text(FAKE_SSH.format(python=sys.executable))
    ssh.chmod(ssh.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")


@pytest.fixture
def echo_server():
    server = socket.create_server(("127.0.0.1", 0))

    def serve():
        while True:
            try:
                connection, _ = server.accept()
            except OSError:
                return
            connection.sendall(connection.recv(100).upper())
            connection.close()

    threading.Thread(target=serve, daemon=True).start()
    yield server.getsockname()[1]
    server.close()


def test_tunnel_forwards_and_stops(fake_ssh, echo_server):
    tunnel = SshTunnel("somehost", "127.0.0.1", echo_server)
    port = tunnel.start()
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.sendall(b"hello")
        assert s.recv(100) == b"HELLO"
    assert tunnel.is_alive()
    tunnel.stop()
    assert not tunnel.is_alive()


def test_tunnel_reports_ssh_errors(fake_ssh):
    with pytest.raises(TunnelError, match="Could not resolve hostname"):
        SshTunnel("unreachable", "localhost", 7155).start()
