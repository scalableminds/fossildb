"""Turning values into trees of Nodes: schema-less (wire format), typed (with a message class), and JSON."""

import json

from google.protobuf.descriptor import FieldDescriptor

from fossildb_gui import fmt, wire
from fossildb_gui.tree import MAX_CHILDREN, Node, limited

WIRE_HEADERS = ["Field", "Type", "Value", "Other interpretations"]
TYPED_HEADERS = ["Field", "Type", "Value"]
JSON_HEADERS = ["Key", "Value"]

_MAX_TEXT = 200
_MAX_PACKED_LENGTH = 4096


def _shorten(text: str) -> str:
    text = text.replace("\n", "⏎")
    return text if len(text) <= _MAX_TEXT else text[:_MAX_TEXT] + " …"


# Schema-less


def wire_nodes(data: bytes | memoryview) -> list[Node]:
    """Nodes for the top-level fields. Raises wire.WireError if the data is not a protobuf message."""
    fields, truncated = wire.parse_message(data, MAX_CHILDREN)
    return list(_wire_field_nodes(fields, truncated))


def _wire_field_nodes(fields: list[wire.WireField], truncated: bool):
    for field in fields:
        yield _wire_field_node(field)
    if truncated:
        yield Node(("… more", "", "not shown"))


def _wire_field_node(field: wire.WireField) -> Node:
    name = str(field.number)
    if field.wire_type == wire.VARINT:
        return Node((name, "varint", str(field.value), ", ".join(wire.varint_interpretations(field.value))))
    if field.wire_type == wire.FIXED64:
        return Node((name, "fixed64", str(field.value), ", ".join(wire.fixed64_interpretations(field.value))))
    if field.wire_type == wire.FIXED32:
        return Node((name, "fixed32", str(field.value), ", ".join(wire.fixed32_interpretations(field.value))))

    data = field.value
    kind = wire.classify_length_delimited(data, MAX_CHILDREN)
    size = f"{len(data):,} bytes"
    if kind.kind == "empty":
        return Node((name, "empty", '"" / empty message', ""))
    if kind.kind == "message":
        others = f"bytes {wire.hex_preview(data, 16)}" if len(data) <= 16 else ""
        return Node(
            (name, f"message ({size})", "", others),
            loader=lambda: _wire_field_nodes(kind.fields, kind.truncated),
        )
    if kind.kind == "string":
        others = "also parses as a message" if kind.also_message else ""
        return Node((name, f"string ({size})", _shorten(kind.text), others), tooltip=kind.text[:10_000])
    others = ""
    if len(data) <= _MAX_PACKED_LENGTH:
        packed = wire.packed_varints(data, 64)
        if packed is not None:
            others = "packed varints " + ", ".join(map(str, packed))
    return Node((name, f"bytes ({size})", wire.hex_preview(data), others))


# Typed


_TYPE_NAMES = {
    getattr(FieldDescriptor, name): name.removeprefix("TYPE_").lower()
    for name in dir(FieldDescriptor)
    if name.startswith("TYPE_")
}


def _is_repeated(field: FieldDescriptor) -> bool:
    is_repeated = getattr(field, "is_repeated", None)  # protobuf >= 6
    return is_repeated if is_repeated is not None else field.label == FieldDescriptor.LABEL_REPEATED


def _type_name(field: FieldDescriptor) -> str:
    if field.type in (FieldDescriptor.TYPE_MESSAGE, FieldDescriptor.TYPE_GROUP):
        return field.message_type.name
    if field.type == FieldDescriptor.TYPE_ENUM:
        return field.enum_type.name
    return _TYPE_NAMES.get(field.type, "?")


def message_nodes(message) -> list[Node]:
    nodes = []
    for field, value in message.ListFields():
        if field.message_type is not None and field.message_type.GetOptions().map_entry:
            nodes.append(_map_node(field, value))
        elif _is_repeated(field):
            nodes.append(_repeated_node(field, value))
        else:
            nodes.append(_value_node(field.name, field, value))
    nodes.extend(_unknown_field_nodes(message))
    return nodes


def _value_node(name: str, field: FieldDescriptor, value) -> Node:
    type_name = _type_name(field)
    if field.type in (FieldDescriptor.TYPE_MESSAGE, FieldDescriptor.TYPE_GROUP):
        return Node((name, type_name, ""), loader=lambda: message_nodes(value))
    if field.type == FieldDescriptor.TYPE_ENUM:
        enum_value = field.enum_type.values_by_number.get(value)
        return Node((name, type_name, f"{enum_value.name} ({value})" if enum_value else str(value)))
    if field.type == FieldDescriptor.TYPE_BYTES:
        return Node((name, f"bytes ({len(value):,} bytes)", wire.hex_preview(value)))
    if field.type == FieldDescriptor.TYPE_STRING:
        return Node((name, type_name, _shorten(value)), tooltip=value[:10_000])
    if field.type == FieldDescriptor.TYPE_BOOL:
        return Node((name, type_name, "true" if value else "false"))
    text = repr(value) if isinstance(value, float) else str(value)
    return Node((name, type_name, fmt.with_timestamp(field.name, value, text)))


def _repeated_node(field: FieldDescriptor, values) -> Node:
    count = len(values)
    return Node(
        (field.name, f"repeated {_type_name(field)} [{count:,}]", ""),
        loader=lambda: limited((_value_node(f"[{i}]", field, v) for i, v in enumerate(values)), count),
    )


def _map_node(field: FieldDescriptor, mapping) -> Node:
    value_field = field.message_type.fields_by_name["value"]
    key_type = _type_name(field.message_type.fields_by_name["key"])
    count = len(mapping)
    return Node(
        (field.name, f"map<{key_type}, {_type_name(value_field)}> [{count:,}]", ""),
        loader=lambda: limited((_value_node(str(k), value_field, mapping[k]) for k in mapping), count),
    )


def _unknown_field_nodes(message) -> list[Node]:
    try:
        from google.protobuf import unknown_fields

        fields = list(unknown_fields.UnknownFieldSet(message))
    except Exception:  # not available in this protobuf version
        return []
    nodes = []
    for field in fields:
        data = field.data
        if isinstance(data, bytes):
            value = wire.hex_preview(data)
        elif isinstance(data, int):
            value = str(data)
        else:
            value = "group"
        type_name = wire.WIRE_TYPE_NAMES.get(field.wire_type, "group")
        nodes.append(
            Node(
                (f"[unknown {field.field_number}]", type_name, value),
                tooltip="This field is not in the schema. Is the right message type selected?",
            )
        )
    return nodes


# JSON


def json_nodes(value) -> list[Node]:
    """Nodes for a parsed JSON document: the members of an object or the items of an array."""
    if isinstance(value, (dict, list)):
        return list(_json_children(value, ""))
    return [_json_node("", value, "")]


def _json_children(value: dict | list, key: str):
    if isinstance(value, dict):
        return limited((_json_node(str(k), item, str(k)) for k, item in value.items()), len(value))
    # Array items take the array's key for recognizing timestamps, e.g. "timestamps": [...]
    return limited((_json_node(str(i), item, key) for i, item in enumerate(value)), len(value))


_INLINE_JSON_LENGTH = 60


def _json_summary(value: dict | list) -> str:
    """Short arrays and objects of plain values are shown inline, e.g. [1024, 2048, 512]."""
    items = value.values() if isinstance(value, dict) else value
    if len(value) <= 10 and not any(isinstance(item, (dict, list)) for item in items):
        inline = json.dumps(value, ensure_ascii=False)
        if len(inline) <= _INLINE_JSON_LENGTH:
            return inline
    if isinstance(value, dict):
        return f"{{…}} {len(value):,} key{'s' if len(value) != 1 else ''}"
    return f"[…] {len(value):,} item{'s' if len(value) != 1 else ''}"


def _json_node(key: str, value, name: str) -> Node:
    if isinstance(value, (dict, list)) and value:
        return Node((key, _json_summary(value)), loader=lambda: _json_children(value, name))
    text = json.dumps(value, ensure_ascii=False)
    shortened = _shorten(text)
    return Node((key, fmt.with_timestamp(name, value, shortened)), tooltip=text if shortened != text else None)
