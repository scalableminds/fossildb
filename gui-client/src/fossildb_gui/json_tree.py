"""JSON documents as trees of Nodes, with collapsed arrays and objects summarized."""

import json

from fossildb_gui import fmt
from fossildb_gui.tree import Node, limited

JSON_HEADERS = ["Key", "Value"]
VALUE_COLUMN = 1

# Short arrays and objects of plain values are shown inline up to this length, e.g. [1024, 2048, 512]
_INLINE_LENGTH = 60
_INLINE_ITEMS = 10


def json_nodes(value) -> list[Node]:
    """Nodes for a parsed JSON document: the members of an object or the items of an array."""
    if isinstance(value, (dict, list)):
        return list(_children(value, ""))
    return [_node("", value, "")]


def _children(value: dict | list, key: str):
    if isinstance(value, dict):
        return limited((_node(str(k), item, str(k)) for k, item in value.items()), len(value))
    # Array items take the array's key for recognizing timestamps, e.g. "timestamps": [...]
    return limited((_node(str(i), item, key) for i, item in enumerate(value)), len(value))


def _summary(value: dict | list) -> str:
    items = value.values() if isinstance(value, dict) else value
    if len(value) <= _INLINE_ITEMS and not any(isinstance(item, (dict, list)) for item in items):
        inline = json.dumps(value, ensure_ascii=False)
        if len(inline) <= _INLINE_LENGTH:
            return inline
    if isinstance(value, dict):
        return f"{{…}} {fmt.plural(len(value), 'key')}"
    return f"[…] {fmt.plural(len(value), 'item')}"


def _node(key: str, value, name: str) -> Node:
    """name: the key that tells whether numbers are timestamps; for array items, the array's key."""
    if isinstance(value, (dict, list)) and value:
        return Node((key, _summary(value)), loader=lambda: _children(value, name))
    text = json.dumps(value, ensure_ascii=False)
    shortened = fmt.one_line(text)
    return Node((key, fmt.with_timestamp(name, value, shortened)), tooltip=text if shortened != text else None)
