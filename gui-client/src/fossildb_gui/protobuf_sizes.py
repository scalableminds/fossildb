"""Encoded sizes of protobuf fields, and what they consist of (tags, length prefixes, values).

The protobuf runtime only reports the size of whole messages, so fields are measured here from their types.
"""

from google.protobuf.descriptor import FieldDescriptor

from fossildb_gui import fmt, wire

_T = FieldDescriptor
_VARINT_TYPES = {_T.TYPE_INT32, _T.TYPE_INT64, _T.TYPE_UINT32, _T.TYPE_UINT64, _T.TYPE_ENUM, _T.TYPE_BOOL}
_FIXED64_TYPES = {_T.TYPE_FIXED64, _T.TYPE_SFIXED64, _T.TYPE_DOUBLE}
_FIXED32_TYPES = {_T.TYPE_FIXED32, _T.TYPE_SFIXED32, _T.TYPE_FLOAT}
_LENGTH_DELIMITED_TYPES = {_T.TYPE_STRING, _T.TYPE_BYTES, _T.TYPE_MESSAGE}

# The parts of an encoded field, e.g. [("Tag", 1), ("Length prefix", 1), ("Value", 11)]
Parts = list[tuple[str, int]]


def is_map(field: FieldDescriptor) -> bool:
    return field.message_type is not None and field.message_type.GetOptions().map_entry


def is_repeated(field: FieldDescriptor) -> bool:
    repeated = getattr(field, "is_repeated", None)  # protobuf >= 6
    return repeated if repeated is not None else field.label == FieldDescriptor.LABEL_REPEATED


def is_packed(field: FieldDescriptor) -> bool:
    """Whether the values of a repeated field share one tag and length prefix."""
    packed = getattr(field, "is_packed", None)  # protobuf >= 6
    if packed is not None:
        return packed
    if field.type in _LENGTH_DELIMITED_TYPES or field.type == _T.TYPE_GROUP:
        return False
    options = field.GetOptions()
    if options.HasField("packed"):
        return options.packed
    return getattr(field.file, "syntax", "proto2") == "proto3"


def field_size(field: FieldDescriptor, value) -> int:
    """Encoded size of a field as returned by ListFields: all its tags, length prefixes and values."""
    return sum(n for _, n in field_parts(field, value))


def field_parts(field: FieldDescriptor, value) -> Parts:
    tag = _tag_size(field)
    if is_map(field):
        entries = [map_entry_parts(field, k, value[k]) for k in value]
        return [
            (f"{fmt.count(len(entries))} tags", sum(parts[0][1] for parts in entries)),
            (f"{fmt.count(len(entries))} length prefixes", sum(parts[1][1] for parts in entries)),
            ("Keys and values, with their tags", sum(parts[2][1] for parts in entries)),
        ]
    if is_repeated(field):
        values = sum(_value_size(field, v) for v in value)
        count = fmt.count(len(value))
        if is_packed(field):
            return [("Tag", tag), ("Length prefix", wire.varint_size(values)), (f"{count} packed values", values)]
        with_prefixes = " with their length prefixes" if field.type in _LENGTH_DELIMITED_TYPES else ""
        return [(f"{count} tags", len(value) * tag), (f"{count} values{with_prefixes}", values)]
    return _single_value_parts(field, value)


def item_parts(field: FieldDescriptor, value) -> Parts:
    """The parts of one value of a repeated field."""
    if is_packed(field):
        return [("Value (the tag and length prefix are shared by all values of the field)", _value_size(field, value))]
    return _single_value_parts(field, value)


def map_entry_parts(field: FieldDescriptor, key, value) -> Parts:
    entry = field.message_type
    length = sum(
        _tag_size(entry.fields_by_name[name]) + _value_size(entry.fields_by_name[name], item)
        for name, item in (("key", key), ("value", value))
    )
    return [
        ("Tag", _tag_size(field)),
        ("Length prefix", wire.varint_size(length)),
        ("Key and value, with their tags", length),
    ]


def wire_field_parts(field: wire.WireField) -> Parts:
    """The parts of a field parsed without a schema."""
    tag = wire.varint_size(field.number << 3 | field.wire_type)
    if field.wire_type == wire.LENGTH_DELIMITED:
        return [("Tag", tag), ("Length prefix", wire.varint_size(len(field.value))), ("Value", len(field.value))]
    return [("Tag", tag), ("Value", field.size - tag)]


def unknown_field_parts(field) -> Parts | None:
    """The parts of a field that the schema doesn't know (from UnknownFieldSet). None for groups."""
    tag = wire.varint_size(field.field_number << 3)
    if isinstance(field.data, bytes):
        return [("Tag", tag), ("Length prefix", wire.varint_size(len(field.data))), ("Value", len(field.data))]
    if isinstance(field.data, int):
        value = {wire.FIXED64: 8, wire.FIXED32: 4}.get(field.wire_type) or wire.varint_size(field.data)
        return [("Tag", tag), ("Value", value)]
    return None


def total(parts: Parts) -> int:
    return sum(n for _, n in parts)


def tooltip(parts: Parts) -> str:
    """E.g. "Tag: 1 byte", "Length prefix: 1 byte", "Value: 11 bytes", "Total: 13 bytes"."""
    lines = [f"{label}: {fmt.plural(n, 'byte')}" for label, n in parts]
    if len(parts) > 1:
        lines.append(f"Total: {fmt.plural(total(parts), 'byte')}")
    return "\n".join(lines)


def _tag_size(field: FieldDescriptor) -> int:
    return wire.varint_size(field.number << 3)


def _single_value_parts(field: FieldDescriptor, value) -> Parts:
    """The parts of one value with its tag."""
    tag = _tag_size(field)
    if field.type == _T.TYPE_GROUP:
        return [("Start and end tags", 2 * tag), ("Fields", value.ByteSize())]
    if field.type in _LENGTH_DELIMITED_TYPES:
        length = _length(field, value)
        return [("Tag", tag), ("Length prefix", wire.varint_size(length)), ("Value", length)]
    return [("Tag", tag), ("Value", _value_size(field, value))]


def _value_size(field: FieldDescriptor, value) -> int:
    """Encoded size of one value, without its tag."""
    t = field.type
    if t in _VARINT_TYPES:
        return wire.varint_size(int(value))
    if t in (_T.TYPE_SINT32, _T.TYPE_SINT64):
        return wire.varint_size((value << 1) ^ (value >> 63))  # zigzag
    if t in _FIXED64_TYPES:
        return 8
    if t in _FIXED32_TYPES:
        return 4
    if t == _T.TYPE_GROUP:
        return value.ByteSize() + _tag_size(field)  # plus the end-group tag
    length = _length(field, value)
    return wire.varint_size(length) + length


def _length(field: FieldDescriptor, value) -> int:
    """The length of a string, bytes or message value, without its length prefix."""
    if field.type == _T.TYPE_STRING:
        return len(value.encode())
    return len(value) if field.type == _T.TYPE_BYTES else value.ByteSize()
