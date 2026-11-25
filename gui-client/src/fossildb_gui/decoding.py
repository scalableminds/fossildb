"""Turning values into trees of Nodes: schema-less (wire format), typed (with a message class), and JSON."""

import json

from google.protobuf.descriptor import FieldDescriptor

from fossildb_gui import fmt, wire
from fossildb_gui.tree import MAX_CHILDREN, Node, limited

WIRE_HEADERS = ["Field", "Type", "Size", "Value", "Other interpretations"]
TYPED_HEADERS = ["Field", "Type", "Size", "Value"]
SIZE_COLUMN = 2  # in both, right-aligned
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
        yield Node(("… more not shown",))


def _wire_field_node(field: wire.WireField) -> Node:
    node = _wire_field_node_columns(field)
    tag = _varint_size(field.number << 3 | field.wire_type)
    if field.wire_type == wire.LENGTH_DELIMITED:
        length = len(field.value)
        parts = [("Tag", tag), ("Length prefix", _varint_size(length)), ("Value", length)]
    else:
        parts = [("Tag", tag), ("Value", field.size - tag)]
    node.column_tooltips = {SIZE_COLUMN: _size_tooltip(parts)}
    return node


def _wire_field_node_columns(field: wire.WireField) -> Node:
    name = str(field.number)
    size = fmt.size(field.size)
    if field.wire_type == wire.VARINT:
        return Node((name, "varint", size, str(field.value), ", ".join(wire.varint_interpretations(field.value))))
    if field.wire_type == wire.FIXED64:
        return Node((name, "fixed64", size, str(field.value), ", ".join(wire.fixed64_interpretations(field.value))))
    if field.wire_type == wire.FIXED32:
        return Node((name, "fixed32", size, str(field.value), ", ".join(wire.fixed32_interpretations(field.value))))

    data = field.value
    kind = wire.classify_length_delimited(data, MAX_CHILDREN)
    if kind.kind == "empty":
        return Node((name, "empty", size, '"" / empty message', ""))
    if kind.kind == "message":
        others = f"bytes {wire.hex_preview(data, 16)}" if len(data) <= 16 else ""
        return Node(
            (name, "message", size, "", others),
            loader=lambda: _wire_field_nodes(kind.fields, kind.truncated),
        )
    if kind.kind == "string":
        others = "also parses as a message" if kind.also_message else ""
        return Node((name, "string", size, _shorten(kind.text), others), tooltip=kind.text[:10_000])
    others = ""
    if len(data) <= _MAX_PACKED_LENGTH:
        packed = wire.packed_varints(data, 64)
        if packed is not None:
            others = "packed varints " + ", ".join(map(str, packed))
    return Node((name, "bytes", size, wire.hex_preview(data), others))


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


# Encoded sizes. The protobuf runtime only reports the size of whole messages, so fields are measured here.

_T = FieldDescriptor
_VARINT_TYPES = {_T.TYPE_INT32, _T.TYPE_INT64, _T.TYPE_UINT32, _T.TYPE_UINT64, _T.TYPE_ENUM, _T.TYPE_BOOL}
_FIXED64_TYPES = {_T.TYPE_FIXED64, _T.TYPE_SFIXED64, _T.TYPE_DOUBLE}
_FIXED32_TYPES = {_T.TYPE_FIXED32, _T.TYPE_SFIXED32, _T.TYPE_FLOAT}
_LENGTH_DELIMITED_TYPES = {_T.TYPE_STRING, _T.TYPE_BYTES, _T.TYPE_MESSAGE}


def _varint_size(value: int) -> int:
    if value < 0:
        return 10  # negative int32 and int64 are sign-extended to 64 bits
    size = 1
    while value >= 0x80:
        value >>= 7
        size += 1
    return size


def _tag_size(field: FieldDescriptor) -> int:
    return _varint_size(field.number << 3)


def _value_size(field: FieldDescriptor, value) -> int:
    """Encoded size of one value, without its tag."""
    t = field.type
    if t in _VARINT_TYPES:
        return _varint_size(int(value))
    if t in (_T.TYPE_SINT32, _T.TYPE_SINT64):
        return _varint_size((value << 1) ^ (value >> 63))
    if t in _FIXED64_TYPES:
        return 8
    if t in _FIXED32_TYPES:
        return 4
    if t == _T.TYPE_GROUP:
        return value.ByteSize() + _tag_size(field)  # plus the end-group tag
    length = _length(field, value)
    return _varint_size(length) + length


def _length(field: FieldDescriptor, value) -> int:
    """The length of a string, bytes or message value, without its length prefix."""
    if field.type == _T.TYPE_STRING:
        return len(value.encode())
    return len(value) if field.type == _T.TYPE_BYTES else value.ByteSize()


def _is_packed(field: FieldDescriptor) -> bool:
    packed = getattr(field, "is_packed", None)  # protobuf >= 6
    if packed is not None:
        return packed
    if field.type in _LENGTH_DELIMITED_TYPES or field.type == _T.TYPE_GROUP:
        return False
    options = field.GetOptions()
    if options.HasField("packed"):
        return options.packed
    return getattr(field.file, "syntax", "proto2") == "proto3"


def _map_entry_length(field: FieldDescriptor, key, value) -> int:
    """The size of a map entry's key and value, with their tags."""
    entry = field.message_type
    length = _tag_size(entry.fields_by_name["key"]) + _value_size(entry.fields_by_name["key"], key)
    return length + _tag_size(entry.fields_by_name["value"]) + _value_size(entry.fields_by_name["value"], value)


def _map_entry_size(field: FieldDescriptor, key, value) -> int:
    length = _map_entry_length(field, key, value)
    return _tag_size(field) + _varint_size(length) + length


def _field_size(field: FieldDescriptor, value) -> int:
    """Encoded size of a field as returned by ListFields: all its tags, length prefixes and values."""
    if field.message_type is not None and field.message_type.GetOptions().map_entry:
        return sum(_map_entry_size(field, k, value[k]) for k in value)
    if _is_repeated(field):
        if _is_packed(field):
            length = sum(_value_size(field, v) for v in value)
            return _tag_size(field) + _varint_size(length) + length
        return sum(_tag_size(field) + _value_size(field, v) for v in value)
    return _tag_size(field) + _value_size(field, value)


def _bytes(n: int) -> str:
    return f"{fmt.count(n)} byte{'s' if n != 1 else ''}"


def _size_tooltip(parts: list[tuple[str, int]]) -> str:
    """E.g. "Tag: 1 byte", "Length prefix: 1 byte", "Value: 11 bytes", "Total: 13 bytes"."""
    lines = [f"{label}: {_bytes(n)}" for label, n in parts]
    if len(parts) > 1:
        lines.append(f"Total: {_bytes(sum(n for _, n in parts))}")
    return "\n".join(lines)


def _value_size_parts(field: FieldDescriptor, value) -> list[tuple[str, int]]:
    """The parts of one value with its tag."""
    tag = _tag_size(field)
    if field.type == _T.TYPE_GROUP:
        return [("Start and end tags", 2 * tag), ("Fields", value.ByteSize())]
    if field.type in _LENGTH_DELIMITED_TYPES:
        length = _length(field, value)
        return [("Tag", tag), ("Length prefix", _varint_size(length)), ("Value", length)]
    return [("Tag", tag), ("Value", _value_size(field, value))]


def _map_entry_parts(field: FieldDescriptor, key, value) -> list[tuple[str, int]]:
    length = _map_entry_length(field, key, value)
    return [("Tag", _tag_size(field)), ("Length prefix", _varint_size(length)), ("Key and value, with their tags", length)]


def _field_size_parts(field: FieldDescriptor, value) -> list[tuple[str, int]]:
    """The parts that _field_size adds up."""
    tag = _tag_size(field)
    if field.message_type is not None and field.message_type.GetOptions().map_entry:
        entries = [_map_entry_parts(field, k, value[k]) for k in value]
        return [
            (f"{fmt.count(len(entries))} tags", sum(parts[0][1] for parts in entries)),
            (f"{fmt.count(len(entries))} length prefixes", sum(parts[1][1] for parts in entries)),
            ("Keys and values, with their tags", sum(parts[2][1] for parts in entries)),
        ]
    if _is_repeated(field):
        values = sum(_value_size(field, v) for v in value)
        if _is_packed(field):
            return [("Tag", tag), ("Length prefix", _varint_size(values)), (f"{fmt.count(len(value))} packed values", values)]
        with_prefixes = " with their length prefixes" if field.type in _LENGTH_DELIMITED_TYPES else ""
        return [(f"{fmt.count(len(value))} tags", len(value) * tag), (f"{fmt.count(len(value))} values{with_prefixes}", values)]
    return _value_size_parts(field, value)


def message_nodes(message) -> list[Node]:
    nodes = []
    for field, value in message.ListFields():
        size = fmt.size(_field_size(field, value))
        if field.message_type is not None and field.message_type.GetOptions().map_entry:
            node = _map_node(field, value, size)
        elif _is_repeated(field):
            node = _repeated_node(field, value, size)
        else:
            node = _value_node(field.name, field, value, size)
        node.column_tooltips = {SIZE_COLUMN: _size_tooltip(_field_size_parts(field, value))}
        nodes.append(node)
    nodes.extend(_unknown_field_nodes(message))
    return nodes


def _value_node(name: str, field: FieldDescriptor, value, size: str) -> Node:
    type_name = _type_name(field)
    if field.type in (FieldDescriptor.TYPE_MESSAGE, FieldDescriptor.TYPE_GROUP):
        return Node((name, type_name, size, ""), loader=lambda: message_nodes(value))
    if field.type == FieldDescriptor.TYPE_ENUM:
        enum_value = field.enum_type.values_by_number.get(value)
        return Node((name, type_name, size, f"{enum_value.name} ({value})" if enum_value else str(value)))
    if field.type == FieldDescriptor.TYPE_BYTES:
        return Node((name, "bytes", size, wire.hex_preview(value)))
    if field.type == FieldDescriptor.TYPE_STRING:
        return Node((name, type_name, size, _shorten(value)), tooltip=value[:10_000])
    if field.type == FieldDescriptor.TYPE_BOOL:
        return Node((name, type_name, size, "true" if value else "false"))
    text = repr(value) if isinstance(value, float) else str(value)
    return Node((name, type_name, size, fmt.with_timestamp(field.name, value, text)))


def _repeated_node(field: FieldDescriptor, values, size: str) -> Node:
    count = len(values)
    # Items of packed fields share one tag and length prefix, other items have their own tag
    tag = 0 if _is_packed(field) else _tag_size(field)

    def items():
        for i, v in enumerate(values):
            node = _value_node(f"[{i}]", field, v, fmt.size(tag + _value_size(field, v)))
            if tag:
                parts = _value_size_parts(field, v)
            else:
                parts = [("Value (the tag and length prefix are shared by all values of the field)", _value_size(field, v))]
            node.column_tooltips = {SIZE_COLUMN: _size_tooltip(parts)}
            yield node

    return Node((field.name, f"repeated {_type_name(field)} [{fmt.count(count)}]", size, ""), loader=lambda: limited(items(), count))


def _map_node(field: FieldDescriptor, mapping, size: str) -> Node:
    value_field = field.message_type.fields_by_name["value"]
    key_type = _type_name(field.message_type.fields_by_name["key"])
    count = len(mapping)

    def entries():
        for k in mapping:
            node = _value_node(str(k), value_field, mapping[k], fmt.size(_map_entry_size(field, k, mapping[k])))
            node.column_tooltips = {SIZE_COLUMN: _size_tooltip(_map_entry_parts(field, k, mapping[k]))}
            yield node

    return Node(
        (field.name, f"map<{key_type}, {_type_name(value_field)}> [{fmt.count(count)}]", size, ""),
        loader=lambda: limited(entries(), count),
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
        tag = _varint_size(field.field_number << 3)
        size = ""
        parts = []
        if isinstance(data, bytes):
            value = wire.hex_preview(data)
            parts = [("Tag", tag), ("Length prefix", _varint_size(len(data))), ("Value", len(data))]
        elif isinstance(data, int):
            value = str(data)
            parts = [("Tag", tag), ("Value", {wire.FIXED64: 8, wire.FIXED32: 4}.get(field.wire_type) or _varint_size(data))]
        else:
            value = "group"
        if parts:
            size = fmt.size(sum(n for _, n in parts))
        type_name = wire.WIRE_TYPE_NAMES.get(field.wire_type, "group")
        nodes.append(
            Node(
                (f"[unknown {field.field_number}]", type_name, size, value),
                tooltip="This field is not in the schema. Is the right message type selected?",
                column_tooltips={SIZE_COLUMN: _size_tooltip(parts)} if parts else None,
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
        return f"{{…}} {fmt.count(len(value))} key{'s' if len(value) != 1 else ''}"
    return f"[…] {fmt.count(len(value))} item{'s' if len(value) != 1 else ''}"


def _json_node(key: str, value, name: str) -> Node:
    if isinstance(value, (dict, list)) and value:
        return Node((key, _json_summary(value)), loader=lambda: _json_children(value, name))
    text = json.dumps(value, ensure_ascii=False)
    shortened = _shorten(text)
    return Node((key, fmt.with_timestamp(name, value, shortened)), tooltip=text if shortened != text else None)
