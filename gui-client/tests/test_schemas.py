import pytest

from fossildb_gui import decoding
from fossildb_gui.schemas import SchemaError, load_schemas

PROTO = """
syntax = "proto2";
package test;
import "google/protobuf/timestamp.proto";
import "sub/other.proto";

message Skeleton {
  required int64 id = 1;
  repeated Node nodes = 2;
  map<string, int32> counts = 3;
  optional Kind kind = 4;
  optional bytes payload = 5;
  optional google.protobuf.Timestamp created = 6;
  optional test.other.Other other = 7;
  optional int64 modifiedTimestamp = 8;
  message Node { optional double x = 1; }
  enum Kind { SKELETON = 0; VOLUME = 1; }
}
"""

OTHER = """
syntax = "proto2";
package test.other;
message Other { optional string name = 1; }
"""


@pytest.fixture
def schema_folder(tmp_path):
    (tmp_path / "skeleton.proto").write_text(PROTO)
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "other.proto").write_text(OTHER)
    return tmp_path


def test_load_schemas_lists_message_types(schema_folder):
    schemas = load_schemas(schema_folder)
    assert schemas.message_names == ["test.Skeleton", "test.Skeleton.Node", "test.other.Other"]


def test_typed_nodes(schema_folder):
    schemas = load_schemas(schema_folder)
    cls = schemas.message_class("test.Skeleton")
    message = cls(id=7, kind=1, payload=b"\x01\x02")
    message.nodes.add(x=1.5)
    message.counts["a"] = 3
    message.other.name = "o"
    message.modifiedTimestamp = 1732540000000
    nodes = decoding.message_nodes(cls.FromString(message.SerializeToString()))
    by_name = {node.columns[0]: node for node in nodes}
    assert by_name["id"].columns == ("id", "int64", "2 B", "7")
    assert by_name["kind"].columns == ("kind", "Kind", "2 B", "VOLUME (1)")
    assert by_name["payload"].columns[1:3] == ("bytes", "4 B")
    assert by_name["nodes"].columns[1] == "repeated Node [1]"
    assert by_name["nodes"].children[0].children[0].columns == ("x", "double", "9 B", "1.5")
    assert by_name["counts"].children[0].columns == ("a", "int32", "7 B", "3")
    assert by_name["other"].children[0].columns == ("name", "string", "3 B", "o")
    assert by_name["modifiedTimestamp"].columns[3] == "1732540000000 (2024-11-25T13:06:40.000Z)"


def test_unknown_fields_are_shown(schema_folder):
    cls = load_schemas(schema_folder).message_class("test.other.Other")
    message = cls.FromString(b"\x10\x05")  # field 2 is not in the schema
    assert [node.columns[0] for node in decoding.message_nodes(message)] == ["[unknown 2]"]


def test_compile_errors_are_reported(tmp_path):
    (tmp_path / "broken.proto").write_text('syntax = "proto2"; message X { optional Missing m = 1; }')
    with pytest.raises(SchemaError, match="Missing"):
        load_schemas(tmp_path)


def test_empty_folder(tmp_path):
    with pytest.raises(SchemaError, match="No .proto files"):
        load_schemas(tmp_path)


SIZES = """
syntax = "proto3";
package sizes;
message Sizes {
  repeated int64 packed = 1;
  repeated string names = 2;
  int32 negative = 3;
  sint64 zigzag = 4;
  fixed32 fixed = 5;
  double real = 6;
  bool flag = 7;
  string text = 8;
  map<string, Sizes> children = 9;
  Sizes child = 2000;
  repeated sint32 small = 10;
}
"""


def test_typed_sizes_add_up(tmp_path):
    from fossildb_gui.decoding import _field_size

    (tmp_path / "sizes.proto").write_text(SIZES)
    cls = load_schemas(tmp_path).message_class("sizes.Sizes")
    message = cls(packed=[1, 300, 2**40], names=["a", "bé"], negative=-5, zigzag=-3, fixed=7, real=0.5, flag=True)
    message.text = "ünïcode"
    message.small.extend([-1, 1, -64])
    message.children["x"].text = "child"
    message.children["y"].negative = -1
    message.child.packed.extend([5])
    assert sum(_field_size(f, v) for f, v in message.ListFields()) == message.ByteSize()

    # The items add up to their field, which for packed fields also has one tag and length prefix
    by_name = {node.columns[0]: node for node in decoding.message_nodes(message)}

    def total(node):
        return sum(int(child.columns[2].removesuffix(" B")) for child in node.children)

    assert total(by_name["names"]) == int(by_name["names"].columns[2].removesuffix(" B"))
    assert total(by_name["packed"]) + 2 == int(by_name["packed"].columns[2].removesuffix(" B"))
    assert total(by_name["children"]) == int(by_name["children"].columns[2].removesuffix(" B"))


def _tooltip_total(node) -> int:
    """The total of a size tooltip, e.g. from "Tag: 1 byte\nValue: 2 bytes\nTotal: 3 bytes"."""
    last = node.column_tooltips[decoding.SIZE_COLUMN].splitlines()[-1]
    return int(last.split(": ")[1].split()[0].replace(" ", ""))


def test_size_tooltips_add_up(tmp_path):
    from fossildb_gui import wire
    from fossildb_gui.decoding import _field_size

    (tmp_path / "sizes.proto").write_text(SIZES)
    cls = load_schemas(tmp_path).message_class("sizes.Sizes")
    message = cls(packed=[1, 300], names=["a", "bé"], negative=-5, flag=True, text="x" * 200)
    message.children["x"].text = "child"
    fields = dict(message.ListFields())
    nodes = {node.columns[0]: node for node in decoding.message_nodes(message)}
    for field, value in fields.items():
        assert _tooltip_total(nodes[field.name]) == _field_size(field, value)
    # Items and entries
    assert sum(_tooltip_total(item) for item in nodes["names"].children) == _field_size(*[(f, v) for f, v in fields.items() if f.name == "names"][0])
    assert nodes["negative"].column_tooltips[decoding.SIZE_COLUMN] == "Tag: 1 byte\nValue: 10 bytes\nTotal: 11 bytes"
    assert nodes["text"].column_tooltips[decoding.SIZE_COLUMN] == "Tag: 1 byte\nLength prefix: 2 bytes\nValue: 200 bytes\nTotal: 203 bytes"
    assert nodes["packed"].column_tooltips[decoding.SIZE_COLUMN].startswith("Tag: 1 byte\nLength prefix: 1 byte\n2 packed values: 3 bytes")
    assert nodes["packed"].children[1].column_tooltips[decoding.SIZE_COLUMN].startswith("Value (the tag and length prefix")
    assert nodes["children"].children[0].column_tooltips[decoding.SIZE_COLUMN].startswith("Tag: 1 byte\nLength prefix: 1 byte\n")

    # The schema-less view
    data = message.SerializeToString()
    raw = decoding.wire_nodes(data)
    assert sum(_tooltip_total(node) for node in raw) == len(data)
    fields, _ = wire.parse_message(data)
    assert [_tooltip_total(node) for node in raw] == [field.size for field in fields]


def test_guess_message_types(schema_folder):
    schemas = load_schemas(schema_folder)
    cls = schemas.message_class("test.Skeleton")
    message = cls(id=7, kind=1)
    message.nodes.add(x=1.5)
    message.other.name = "o"
    guesses = decoding.guess_message_types(message.SerializeToString(), schemas)
    assert guesses[0].name == "test.Skeleton" and guesses[0].fits and guesses[0].known == 6
    # Other types see unknown fields, or miss the required id
    assert all(not g.fits for g in guesses[1:])
    other = schemas.message_class("test.other.Other")(name="just a name").SerializeToString()
    assert decoding.guess_message_types(other, schemas)[0].name == "test.other.Other"

