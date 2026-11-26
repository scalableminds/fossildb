"""Decoding values for display, and JSON trees."""

from test_wire import MESSAGE

from fossildb_gui import json_tree, values
from fossildb_gui.values import TEXT_LIMIT, decode_value


def test_as_text():
    assert values.as_text(b'[{"a": 1}]') == '[{"a": 1}]'
    assert values.as_text(b"plain text") == "plain text"
    assert values.as_text(b"\xff\x00") is None
    assert values.as_text(b"") is None
    # A multi-byte character cut at the limit
    long_text = "ä" * TEXT_LIMIT
    assert values.as_text(long_text.encode()) == "ä" * (TEXT_LIMIT // 2)


def test_decode_json():
    value = decode_value('[{"a": "ä"}]'.encode())
    assert value.json_text == '[\n  {\n    "a": "ä"\n  }\n]' and value.not_json is None
    assert value.json_nodes[0].columns == ("0", '{"a": "ä"}')
    assert value.json_nodes[0].children[0].columns == ("a", '"ä"')
    value = decode_value(b"plain text")
    assert value.is_text and value.json_text is None and value.not_json.startswith("Not valid JSON")
    value = decode_value(b"[" + b" " * TEXT_LIMIT + b"]")
    assert value.text_truncated and value.json_text is None and "too large" in value.not_json


def test_decode_protobuf_and_binary():
    value = decode_value(MESSAGE)
    assert value.is_protobuf and len(value.protobuf_nodes) == 7 and value.typed_nodes is None
    value = decode_value(b"\xff\xfe")
    assert not value.is_protobuf and value.not_protobuf.startswith("Not a protobuf message")
    assert not value.is_text and value.hex_dump.startswith("00000000  ff fe")
    assert decode_value(b"").not_protobuf == "The value is empty"


def test_json_nodes():
    nodes = json_tree.json_nodes({"list": [1, None], "empty": {}, "long": "x" * 300, "flag": True})
    assert [node.columns for node in nodes[1:]] == [("empty", "{}"), ("long", '"' + "x" * 199 + " …"), ("flag", "true")]
    assert nodes[2].tooltip == '"' + "x" * 300 + '"'
    assert [child.columns for child in nodes[0].children] == [("0", "1"), ("1", "null")]
    assert json_tree.json_nodes(5)[0].columns == ("", "5")
    # Only short containers of plain values are shown inline
    nested = json_tree.json_nodes({"a": [[1]], "b": list(range(30)), "c": {"x": 1, "y": None}})
    assert [node.columns[1] for node in nested] == ["[…] 1 item", "[…] 30 items", '{"x": 1, "y": null}']
    assert json_tree.json_nodes({"a": list(range(1500))})[0].columns[1] == "[…] 1\u202f500 items"


def test_json_timestamps():
    nodes = json_tree.json_nodes(
        {"actionTimestamp": 1732540000000, "timestamps": [1732540000], "count": 1732540000, "lastTimestamp": "x"}
    )
    assert nodes[0].columns[1] == "1732540000000 (2024-11-25T13:06:40.000Z)"
    assert nodes[1].children[0].columns[1] == "1732540000 (2024-11-25T13:06:40Z)"
    assert nodes[2].columns[1] == "1732540000"  # not named like a timestamp
    assert nodes[3].columns[1] == '"x"'
