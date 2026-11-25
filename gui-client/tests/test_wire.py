import struct

import pytest

from fossildb_gui import decoding, wire


def varint(n: int) -> bytes:
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


def field(number: int, wire_type: int, payload: bytes) -> bytes:
    return varint(number << 3 | wire_type) + payload


def length_delimited(number: int, data: bytes) -> bytes:
    return field(number, wire.LENGTH_DELIMITED, varint(len(data)) + data)


NESTED = field(1, wire.VARINT, varint(150))
MESSAGE = (
    field(1, wire.VARINT, varint(2**64 - 1))  # int64 -1
    + field(2, wire.FIXED64, struct.pack("<d", 1.5))
    + field(3, wire.FIXED32, struct.pack("<f", 2.5))
    + length_delimited(4, b"hello world")
    + length_delimited(5, NESTED)
    + length_delimited(6, b"\xff\x00\xfe")
    + length_delimited(7, b"")
)


def test_parse_message_reads_all_wire_types():
    fields, truncated = wire.parse_message(MESSAGE)
    assert not truncated
    assert [(f.number, f.wire_type) for f in fields] == [(1, 0), (2, 1), (3, 5), (4, 2), (5, 2), (6, 2), (7, 2)]
    assert fields[0].value == 2**64 - 1
    assert bytes(fields[3].value) == b"hello world"


def test_parse_message_stops_at_max_fields():
    fields, truncated = wire.parse_message(MESSAGE, max_fields=2)
    assert len(fields) == 2
    assert truncated


@pytest.mark.parametrize(
    "data",
    [
        b"\x08",  # truncated varint
        b"\x00\x01",  # field number 0
        b"\x0b\x01",  # group (wire type 3)
        b"\x22\x05ab",  # length beyond the data
        b"\x09\x01\x02",  # truncated fixed64
    ],
)
def test_parse_message_rejects_invalid_data(data):
    with pytest.raises(wire.WireError):
        wire.parse_message(data)


def test_classify_length_delimited():
    assert wire.classify_length_delimited(memoryview(b"hello\nworld")).kind == "string"
    assert wire.classify_length_delimited(memoryview(NESTED)).kind == "message"
    assert wire.classify_length_delimited(memoryview(b"\xff\x00\xfe")).kind == "bytes"
    assert wire.classify_length_delimited(memoryview(b"")).kind == "empty"
    # "hi" is also a valid message (field 13 = 105), but text is the likelier meaning
    kind = wire.classify_length_delimited(memoryview(b"hi"))
    assert kind.kind == "string" and kind.also_message
    # \x01 is a control character, so this is not text
    assert wire.classify_length_delimited(memoryview(b"x\x01")).kind == "message"


def test_interpretations():
    assert wire.varint_interpretations(2**64 - 1) == ["int64 -1", "sint -9223372036854775808"]
    assert wire.varint_interpretations(3) == ["sint -2"]
    assert wire.fixed64_interpretations(struct.unpack("<Q", struct.pack("<d", 1.5))[0])[0] == "double 1.5"
    assert wire.fixed32_interpretations(struct.unpack("<I", struct.pack("<f", 2.5))[0])[0] == "float 2.5"
    assert wire.packed_varints(memoryview(varint(1) + varint(300)), 10) == [1, 300]


def test_hex_dump():
    dump = wire.hex_dump(bytes(range(20)) + b"AB")
    lines = dump.splitlines()
    assert lines[0] == "00000000  00 01 02 03 04 05 06 07  08 09 0a 0b 0c 0d 0e 0f  |................|"
    assert lines[1] == "00000010  10 11 12 13 41 42" + " " * 33 + "|....AB|"
    assert wire.hex_dump(bytes(100), max_bytes=16).count("\n") == 0


def test_wire_nodes():
    nodes = decoding.wire_nodes(MESSAGE)
    columns = [node.columns for node in nodes]
    assert columns[0][:3] == ("1", "varint", str(2**64 - 1))
    assert "int64 -1" in columns[0][3]
    assert columns[3][1].startswith("string") and columns[3][2] == "hello world"
    assert columns[4][1].startswith("message")
    nested = nodes[4].children
    assert nested[0].columns[:3] == ("1", "varint", "150")
    assert columns[5][1].startswith("bytes")
    assert columns[6][1] == "empty"


def test_wire_nodes_rejects_non_protobuf():
    with pytest.raises(wire.WireError):
        decoding.wire_nodes(b"\xff\xff\xff")


def test_values_as_text():
    from fossildb_gui import record_view

    assert record_view._as_text(b'[{"a": 1}]') == '[\n  {\n    "a": 1\n  }\n]'
    assert record_view._as_text(b"plain text") == "plain text"
    assert record_view._as_text(b"\xff\x00") is None
    assert record_view._as_text(b"") is None
    # A multi-byte character cut at the limit
    long_text = "ä" * record_view.TEXT_LIMIT
    assert record_view._as_text(long_text.encode()) == "ä" * (record_view.TEXT_LIMIT // 2)
