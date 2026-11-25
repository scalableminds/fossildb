"""Decoding the protobuf wire format without a schema.

Without a schema, a length-delimited field can be a string, raw bytes or a nested message (or packed repeated
numbers). classify_length_delimited guesses which one, and the other interpretations are offered alongside.
"""

import re
import struct
from dataclasses import dataclass

VARINT = 0
FIXED64 = 1
LENGTH_DELIMITED = 2
FIXED32 = 5

WIRE_TYPE_NAMES = {VARINT: "varint", FIXED64: "fixed64", LENGTH_DELIMITED: "length-delimited", FIXED32: "fixed32"}

_MAX_FIELD_NUMBER = (1 << 29) - 1
# Control characters other than tab, newline and carriage return make a string unlikely to be text
_NON_TEXT = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


class WireError(ValueError):
    def __init__(self, message: str, offset: int):
        super().__init__(f"{message} at byte {offset}")
        self.offset = offset


@dataclass(frozen=True, slots=True)
class WireField:
    number: int
    wire_type: int
    offset: int  # of the tag, relative to the start of the parsed data
    value: int | memoryview  # memoryview for length-delimited fields, int otherwise
    size: int  # encoded size of the whole field: tag, length prefix and value


def read_varint(buf: memoryview, pos: int, end: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if pos >= end:
            raise WireError("truncated varint", pos)
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            if result >= 1 << 64:
                raise WireError("varint larger than 64 bits", pos)
            return result, pos
        shift += 7
        if shift >= 70:
            raise WireError("varint longer than 10 bytes", pos)


def parse_message(data: bytes | memoryview, max_fields: int | None = None) -> tuple[list[WireField], bool]:
    """Parses all fields of one message level. Returns the fields, and whether parsing stopped at max_fields.

    Raises WireError if the data is not a valid message. Groups (deprecated) are treated as invalid.
    """
    buf = memoryview(data)
    pos = 0
    end = len(buf)
    fields: list[WireField] = []
    while pos < end:
        if max_fields is not None and len(fields) >= max_fields:
            return fields, True
        tag_offset = pos
        tag, pos = read_varint(buf, pos, end)
        number, wire_type = tag >> 3, tag & 7
        if number == 0 or number > _MAX_FIELD_NUMBER:
            raise WireError(f"invalid field number {number}", tag_offset)
        if wire_type == VARINT:
            value, pos = read_varint(buf, pos, end)
        elif wire_type == FIXED64:
            if pos + 8 > end:
                raise WireError("truncated fixed64", pos)
            value = int.from_bytes(buf[pos : pos + 8], "little")
            pos += 8
        elif wire_type == FIXED32:
            if pos + 4 > end:
                raise WireError("truncated fixed32", pos)
            value = int.from_bytes(buf[pos : pos + 4], "little")
            pos += 4
        elif wire_type == LENGTH_DELIMITED:
            length, pos = read_varint(buf, pos, end)
            if pos + length > end:
                raise WireError(f"length {length} exceeds the data", pos)
            value = buf[pos : pos + length]
            pos += length
        else:
            raise WireError(f"unsupported wire type {wire_type}", tag_offset)
        fields.append(WireField(number, wire_type, tag_offset, value, pos - tag_offset))
    return fields, False


def as_text(data: memoryview) -> str | None:
    """The data as text, if it is valid UTF-8 without unusual control characters."""
    try:
        text = bytes(data).decode("utf-8")
    except UnicodeDecodeError:
        return None
    return None if _NON_TEXT.search(text) else text


@dataclass(slots=True)
class LengthDelimitedKind:
    kind: str  # "message", "string", "bytes" or "empty"
    fields: list[WireField] | None = None  # for "message"
    truncated: bool = False  # for "message": more fields exist than were parsed
    text: str | None = None  # for "string"
    also_message: bool = False  # for "string": the data would also parse as a message


def classify_length_delimited(data: memoryview, max_fields: int | None = None) -> LengthDelimitedKind:
    if len(data) == 0:
        return LengthDelimitedKind("empty")
    text = as_text(data)
    try:
        fields, truncated = parse_message(data, max_fields)
    except WireError:
        fields, truncated = None, False
    if text is not None:
        # Short texts often happen to be valid messages too, so prefer text
        return LengthDelimitedKind("string", text=text, also_message=fields is not None)
    if fields is not None:
        return LengthDelimitedKind("message", fields=fields, truncated=truncated)
    return LengthDelimitedKind("bytes")


def varint_interpretations(value: int) -> list[str]:
    result = []
    if value >= 1 << 63:
        result.append(f"int64 {value - (1 << 64)}")
    zigzag = (value >> 1) ^ -(value & 1)
    if zigzag != value:
        result.append(f"sint {zigzag}")
    return result


def fixed64_interpretations(value: int) -> list[str]:
    result = [f"double {struct.unpack('<d', value.to_bytes(8, 'little'))[0]!r}"]
    if value >= 1 << 63:
        result.append(f"int64 {value - (1 << 64)}")
    return result


def fixed32_interpretations(value: int) -> list[str]:
    result = [f"float {struct.unpack('<f', value.to_bytes(4, 'little'))[0]!r}"]
    if value >= 1 << 31:
        result.append(f"int32 {value - (1 << 32)}")
    return result


def packed_varints(data: memoryview, max_count: int) -> list[int] | None:
    """The data as packed repeated varints, if it parses as such."""
    values = []
    pos = 0
    try:
        while pos < len(data):
            if len(values) >= max_count:
                return None
            value, pos = read_varint(data, pos, len(data))
            values.append(value)
    except WireError:
        return None
    return values


def hex_preview(data: bytes | memoryview, max_bytes: int = 32) -> str:
    shown = bytes(data[:max_bytes]).hex(" ")
    return shown + (" …" if len(data) > max_bytes else "")


def hex_dump(data: bytes | memoryview, max_bytes: int | None = None) -> str:
    """Classic hex dump: offset, 16 bytes in hex, and their printable ASCII characters."""
    view = memoryview(data)
    if max_bytes is not None:
        view = view[:max_bytes]
    lines = []
    printable = bytes(c if 32 <= c < 127 else ord(".") for c in range(256))
    for offset in range(0, len(view), 16):
        chunk = bytes(view[offset : offset + 16])
        hex_part = chunk[:8].hex(" ") + "  " + chunk[8:].hex(" ")
        lines.append(f"{offset:08x}  {hex_part:<48}  |{chunk.translate(printable).decode('ascii')}|")
    return "\n".join(lines)
