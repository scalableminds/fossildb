"""Guessing the message type of a value: the type that knows all of its fields."""

from dataclasses import dataclass

from google.protobuf.descriptor import FieldDescriptor

from fossildb_gui.protobuf_sizes import is_map, is_repeated

_BUDGET = 20_000  # fields counted per type, so that huge values don't take forever
_MESSAGE_TYPES = (FieldDescriptor.TYPE_MESSAGE, FieldDescriptor.TYPE_GROUP)


@dataclass(frozen=True)
class TypeGuess:
    name: str
    known: int  # fields that the type knows, in the whole message
    unknown: int  # fields that it doesn't know
    complete: bool  # all required fields are set

    @property
    def fits(self) -> bool:
        return self.unknown == 0 and self.complete and self.known > 0


def guess_message_types(data: bytes, schemas) -> list[TypeGuess]:
    """All message types that can parse the value, best first: those that know all its fields, with the most known."""
    guesses = []
    for name in schemas.message_names:
        try:
            message = schemas.message_class(name).FromString(data)
        except Exception:  # not parseable as this type, e.g. invalid UTF-8 in a string field
            continue
        known, unknown = _count_fields(message, [_BUDGET])
        guesses.append(TypeGuess(name, known, unknown, message.IsInitialized()))
    return sorted(guesses, key=lambda g: (not g.fits, g.unknown, not g.complete, -g.known, g.name))


def _count_fields(message, budget: list[int]) -> tuple[int, int]:
    """Known and unknown fields in the message and its sub-messages, until the budget is used up."""
    try:
        from google.protobuf import unknown_fields

        unknown = len(unknown_fields.UnknownFieldSet(message))
    except Exception:  # not available in this protobuf version
        unknown = 0
    known = 0
    for field, value in message.ListFields():
        if budget[0] <= 0:
            break
        if is_map(field):
            values = list(value.values())
            is_message = field.message_type.fields_by_name["value"].type in _MESSAGE_TYPES
        else:
            values = list(value) if is_repeated(field) else [value]
            is_message = field.type in _MESSAGE_TYPES
        known += len(values)
        budget[0] -= len(values)
        if is_message:
            for item in values:
                sub_known, sub_unknown = _count_fields(item, budget)
                known += sub_known
                unknown += sub_unknown
    return known, unknown
