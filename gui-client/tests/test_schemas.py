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
    assert by_name["id"].columns == ("id", "int64", "7")
    assert by_name["kind"].columns == ("kind", "Kind", "VOLUME (1)")
    assert by_name["payload"].columns[1] == "bytes (2 bytes)"
    assert by_name["nodes"].columns[1] == "repeated Node [1]"
    assert by_name["nodes"].children[0].children[0].columns == ("x", "double", "1.5")
    assert by_name["counts"].children[0].columns == ("a", "int32", "3")
    assert by_name["other"].children[0].columns == ("name", "string", "o")
    assert by_name["modifiedTimestamp"].columns[2] == "1732540000000 (2024-11-25T13:06:40.000Z)"


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
