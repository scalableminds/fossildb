import socket

from fossildb_gui import discovery

PROC_TCP = """  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:1BF3 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 1 1 0000000000000000 100 0 0 10 0
   1: 00000000:1C0B 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 2 1 0000000000000000 100 0 0 10 0
   2: 0A00A8C0:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 3 1 0000000000000000 100 0 0 10 0
   3: 0100007F:9C40 0100007F:1BF3 01 00000000:00000000 00:00000000 00000000  1000        0 4 1 0000000000000000 100 0 0 10 0
"""
PROC_TCP6 = """  sl  local_address                         remote_address                        st ...
   0: 00000000000000000000000001000000:1C1C 00000000000000000000000000000000:0000 0A 0
   1: 0000000000000000FFFF00000A00A8C0:1C1D 00000000000000000000000000000000:0000 0A 0
"""


def test_parse_proc_net():
    # 127.0.0.1:7155 and 0.0.0.0:7179 listen; 192.168.0.10 is not localhost; the last line is a connection
    assert discovery._parse_proc_net(PROC_TCP, ipv6=False) == {7155, 7179}
    assert discovery._parse_proc_net(PROC_TCP6, ipv6=True) == {7196}


def test_find_local(current_server, monkeypatch):
    _, address = current_server
    fossildb_port = int(address.rpartition(":")[2])
    closed = socket.create_server(("127.0.0.1", 0))
    closed_port = closed.getsockname()[1]
    closed.close()
    other = socket.create_server(("127.0.0.1", 0))  # listens, but never answers
    other_port = other.getsockname()[1]
    monkeypatch.setattr(discovery, "listening_local_ports", lambda: {fossildb_port, closed_port, other_port})
    assert discovery.find_local(set()) == [fossildb_port]
    assert discovery.find_local({fossildb_port}) == []
    other.close()


def test_check(current_server):
    _, address = current_server
    assert discovery.check(address) is None
    assert "Unknown host" in discovery.check("doesnotexist.invalid:7155")
