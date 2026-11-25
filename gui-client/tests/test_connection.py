import pytest

from fossildb_gui.connection import FossilDBError, connect


def test_database_info(fake_server):
    fake, address = fake_server
    fake.put("collectionA", "k", 1, b"v")
    connection = connect(address)
    info = connection.database_info()
    if fake.legacy:
        assert info is None
    else:
        assert [c.name for c in info.collections] == ["collectionA", "collectionB"]
        assert info.collections[0].firstKey == "k"
    connection.close()


def test_keys_versions_and_values(current_server):
    fake, address = current_server
    for key in ["a1", "a2", "b1"]:
        fake.put("collectionA", key, 0, key.encode())
    fake.put("collectionA", "a1", 3, b"newer")
    connection = connect(address)
    assert connection.list_keys("collectionA", 10) == ["a1", "a2", "b1"]
    assert connection.list_keys("collectionA", 10, prefix="a") == ["a1", "a2"]
    assert connection.list_keys("collectionA", 1, start_after="a1") == ["a2"]
    assert connection.list_versions("collectionA", "a1") == [3, 0]
    assert connection.get("collectionA", "a1", 3) == b"newer"
    connection.close()


def test_deletes(current_server):
    fake, address = current_server
    for key in ["a1", "a2", "b1"]:
        fake.put("collectionA", key, 0, b"")
        fake.put("collectionA", key, 1, b"")
    connection = connect(address)
    connection.delete_version("collectionA", "b1", 1)
    assert connection.list_versions("collectionA", "b1") == [0]
    connection.delete_key("collectionA", "b1")
    assert connection.list_versions("collectionA", "b1") == []
    connection.delete_all_by_prefix("collectionA", "a")
    assert connection.list_keys("collectionA", 10) == []
    connection.compact_all_data()
    assert fake.compactions == 1
    connection.close()


def test_errors_are_raised(current_server):
    _, address = current_server
    connection = connect(address)
    with pytest.raises(FossilDBError, match="No store for column family"):
        connection.list_keys("missing", 10)
    connection.close()


def test_unreachable_server():
    import socket
    import threading
    import time

    closed = socket.create_server(("127.0.0.1", 0))
    port = closed.getsockname()[1]
    closed.close()
    start = time.monotonic()
    with pytest.raises(FossilDBError, match="Nothing is listening"):
        connect(f"127.0.0.1:{port}")
    with pytest.raises(FossilDBError, match="Unknown host"):
        connect("doesnotexist.invalid:7155")
    with pytest.raises(FossilDBError, match="host:port"):
        connect("no-port")

    # A server that accepts connections, but is not FossilDB
    other = socket.create_server(("127.0.0.1", 0))

    def serve():
        while True:
            try:
                client, _ = other.accept()
            except OSError:
                return
            client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            client.close()

    threading.Thread(target=serve, daemon=True).start()
    with pytest.raises(FossilDBError, match="did not answer like FossilDB"):
        connect(f"127.0.0.1:{other.getsockname()[1]}")
    other.close()
    assert time.monotonic() - start < 5
