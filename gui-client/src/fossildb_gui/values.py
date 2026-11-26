"""Decoding a value for display: as protobuf (with or without schema), text, JSON and hex dump.

Runs in background threads, so nothing here touches widgets.
"""

import json
from dataclasses import dataclass

from fossildb_gui import fmt, json_tree, protobuf_tree, wire
from fossildb_gui.tree import Node

HEX_DUMP_LIMIT = 256 * 1024
TEXT_LIMIT = 1024 * 1024


@dataclass
class DecodedValue:
    size: int
    # Protobuf without schema; if it is not protobuf, why not
    protobuf_nodes: list[Node] | None
    not_protobuf: str | None
    # With the chosen message type (None if none is chosen); a note if decoding failed or fields are missing
    typed_nodes: list[Node] | None
    typed_note: str | None
    message: object | None  # the parsed message, e.g. for converting it to JSON
    # As text, at most TEXT_LIMIT bytes of it; None if it is not text
    text: str | None
    # Pretty-printed, and as tree; if it is not JSON, why not
    json_text: str | None
    json_nodes: list[Node] | None
    not_json: str | None
    hex_dump: str

    @property
    def is_protobuf(self) -> bool:
        return self.protobuf_nodes is not None

    @property
    def is_text(self) -> bool:
        return self.text is not None

    @property
    def text_truncated(self) -> bool:
        return self.is_text and self.size > TEXT_LIMIT

    @property
    def hex_truncated(self) -> bool:
        return self.size > HEX_DUMP_LIMIT


def decode_value(data: bytes, message_class=None) -> DecodedValue:
    if not data:
        protobuf_nodes, not_protobuf = None, "The value is empty"
    else:
        try:
            protobuf_nodes, not_protobuf = protobuf_tree.wire_nodes(data), None
        except wire.WireError as e:
            protobuf_nodes, not_protobuf = None, f"Not a protobuf message ({e})"
    typed_nodes, typed_note, message = _decode_typed(data, message_class) if message_class else (None, None, None)
    text = as_text(data)
    json_text, json_nodes, not_json = _as_json(text, len(data)) if text is not None else (None, None, None)
    return DecodedValue(
        size=len(data),
        protobuf_nodes=protobuf_nodes,
        not_protobuf=not_protobuf,
        typed_nodes=typed_nodes,
        typed_note=typed_note,
        message=message,
        text=text,
        json_text=json_text,
        json_nodes=json_nodes,
        not_json=not_json,
        hex_dump=wire.hex_dump(data, HEX_DUMP_LIMIT),
    )


def as_text(data: bytes) -> str | None:
    """The value as text (at most TEXT_LIMIT bytes of it). None if it is not text."""
    if not data:
        return None
    # Cutting at the limit can split a multi-byte character, so try a few bytes shorter
    for end in range(TEXT_LIMIT, TEXT_LIMIT - 4, -1) if len(data) > TEXT_LIMIT else [len(data)]:
        text = wire.as_text(memoryview(data)[:end])
        if text is not None:
            return text
    return None


def _as_json(text: str, size: int) -> tuple[str | None, list[Node] | None, str | None]:
    if size > TEXT_LIMIT:
        return None, None, f"The value is larger than {fmt.size(TEXT_LIMIT)}, too large to format. Download it."
    try:
        value = json.loads(text)
    except ValueError as e:
        return None, None, f"Not valid JSON: {e}"
    return json.dumps(value, indent=2, ensure_ascii=False), json_tree.json_nodes(value), None


def _decode_typed(data: bytes, message_class):
    message = message_class()
    try:
        message.ParseFromString(data)
    except Exception as e:
        return None, f"Could not decode as {message_class.DESCRIPTOR.full_name}: {e}", None
    note = None
    if not message.IsInitialized():
        missing = ", ".join(message.FindInitializationErrors())
        note = f"Required fields are missing ({missing}). Is the right message type selected?"
    return protobuf_tree.message_nodes(message), note, message


def message_to_json(message) -> str:
    from google.protobuf import json_format

    # Field names as in the .proto files; fields that the schema doesn't know are left out
    return json_format.MessageToJson(message, preserving_proto_field_name=True, indent=2, ensure_ascii=False)
