"""Protobuf values as trees of Nodes: schema-less (wire format), and typed (with a message class)."""

from google.protobuf.descriptor import FieldDescriptor

from fossildb_gui import fmt, protobuf_sizes, wire
from fossildb_gui.protobuf_sizes import is_map, is_repeated
from fossildb_gui.tree import MAX_CHILDREN, Node, limited

WIRE_HEADERS = ["Field", "Type", "Size", "Value", "Other interpretations"]
TYPED_HEADERS = ["Field", "Type", "Size", "Value"]
SIZE_COLUMN = 2  # in both

_MAX_PACKED_LENGTH = 4096
_MAX_TOOLTIP_TEXT = 10_000


def _size_columns(parts: protobuf_sizes.Parts) -> tuple[str, dict[int, str]]:
    """The size, and its tooltip with the parts."""
    return fmt.size(protobuf_sizes.total(parts)), {SIZE_COLUMN: protobuf_sizes.tooltip(parts)}


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
    name = str(field.number)
    size, size_tooltip = _size_columns(protobuf_sizes.wire_field_parts(field))

    def node(type_name: str, value: str, others: str, **kwargs) -> Node:
        return Node((name, type_name, size, value, others), column_tooltips=size_tooltip, **kwargs)

    if field.wire_type == wire.VARINT:
        return node("varint", str(field.value), ", ".join(wire.varint_interpretations(field.value)))
    if field.wire_type == wire.FIXED64:
        return node("fixed64", str(field.value), ", ".join(wire.fixed64_interpretations(field.value)))
    if field.wire_type == wire.FIXED32:
        return node("fixed32", str(field.value), ", ".join(wire.fixed32_interpretations(field.value)))

    data = field.value
    kind = wire.classify_length_delimited(data, MAX_CHILDREN)
    if kind.kind == "empty":
        return node("empty", '"" / empty message', "")
    if kind.kind == "message":
        others = f"bytes {wire.hex_preview(data, 16)}" if len(data) <= 16 else ""
        return node("message", "", others, loader=lambda: _wire_field_nodes(kind.fields, kind.truncated))
    if kind.kind == "string":
        others = "also parses as a message" if kind.also_message else ""
        return node("string", fmt.one_line(kind.text), others, tooltip=kind.text[:_MAX_TOOLTIP_TEXT])
    others = ""
    if len(data) <= _MAX_PACKED_LENGTH:
        packed = wire.packed_varints(data, 64)
        if packed is not None:
            others = "packed varints " + ", ".join(map(str, packed))
    return node("bytes", wire.hex_preview(data), others)


# Typed

_TYPE_NAMES = {
    getattr(FieldDescriptor, name): name.removeprefix("TYPE_").lower()
    for name in dir(FieldDescriptor)
    if name.startswith("TYPE_")
}


def _type_name(field: FieldDescriptor) -> str:
    if field.type in (FieldDescriptor.TYPE_MESSAGE, FieldDescriptor.TYPE_GROUP):
        return field.message_type.name
    if field.type == FieldDescriptor.TYPE_ENUM:
        return field.enum_type.name
    return _TYPE_NAMES.get(field.type, "?")


def message_nodes(message) -> list[Node]:
    """Nodes for the fields of a parsed message, including those that its schema doesn't know."""
    nodes = []
    for field, value in message.ListFields():
        size = _size_columns(protobuf_sizes.field_parts(field, value))
        if is_map(field):
            nodes.append(_map_node(field, value, size))
        elif is_repeated(field):
            nodes.append(_repeated_node(field, value, size))
        else:
            nodes.append(_value_node(field.name, field, value, size))
    nodes.extend(_unknown_field_nodes(message))
    return nodes


def _value_node(name: str, field: FieldDescriptor, value, size: tuple[str, dict]) -> Node:
    size_text, size_tooltip = size

    def node(type_name: str, text: str, **kwargs) -> Node:
        return Node((name, type_name, size_text, text), column_tooltips=size_tooltip, **kwargs)

    type_name = _type_name(field)
    if field.type in (FieldDescriptor.TYPE_MESSAGE, FieldDescriptor.TYPE_GROUP):
        return node(type_name, "", loader=lambda: message_nodes(value))
    if field.type == FieldDescriptor.TYPE_ENUM:
        enum_value = field.enum_type.values_by_number.get(value)
        return node(type_name, f"{enum_value.name} ({value})" if enum_value else str(value))
    if field.type == FieldDescriptor.TYPE_BYTES:
        return node("bytes", wire.hex_preview(value))
    if field.type == FieldDescriptor.TYPE_STRING:
        return node(type_name, fmt.one_line(value), tooltip=value[:_MAX_TOOLTIP_TEXT])
    if field.type == FieldDescriptor.TYPE_BOOL:
        return node(type_name, "true" if value else "false")
    text = repr(value) if isinstance(value, float) else str(value)
    return node(type_name, fmt.with_timestamp(field.name, value, text))


def _repeated_node(field: FieldDescriptor, values, size: tuple[str, dict]) -> Node:
    def items():
        for i, value in enumerate(values):
            yield _value_node(f"[{i}]", field, value, _size_columns(protobuf_sizes.item_parts(field, value)))

    type_name = f"repeated {_type_name(field)} [{fmt.count(len(values))}]"
    return Node(
        (field.name, type_name, size[0], ""), column_tooltips=size[1], loader=lambda: limited(items(), len(values))
    )


def _map_node(field: FieldDescriptor, mapping, size: tuple[str, dict]) -> Node:
    value_field = field.message_type.fields_by_name["value"]
    key_type = _type_name(field.message_type.fields_by_name["key"])

    def entries():
        for key in mapping:
            entry_size = _size_columns(protobuf_sizes.map_entry_parts(field, key, mapping[key]))
            yield _value_node(str(key), value_field, mapping[key], entry_size)

    type_name = f"map<{key_type}, {_type_name(value_field)}> [{fmt.count(len(mapping))}]"
    return Node(
        (field.name, type_name, size[0], ""), column_tooltips=size[1], loader=lambda: limited(entries(), len(mapping))
    )


def _unknown_field_nodes(message) -> list[Node]:
    try:
        from google.protobuf import unknown_fields

        fields = list(unknown_fields.UnknownFieldSet(message))
    except Exception:  # not available in this protobuf version
        return []
    nodes = []
    for field in fields:
        if isinstance(field.data, bytes):
            value = wire.hex_preview(field.data)
        else:
            value = str(field.data) if isinstance(field.data, int) else "group"
        parts = protobuf_sizes.unknown_field_parts(field)
        size, size_tooltip = _size_columns(parts) if parts else ("", None)
        nodes.append(
            Node(
                (f"[unknown {field.field_number}]", wire.WIRE_TYPE_NAMES.get(field.wire_type, "group"), size, value),
                tooltip="This field is not in the schema. Is the right message type selected?",
                column_tooltips=size_tooltip,
            )
        )
    return nodes
