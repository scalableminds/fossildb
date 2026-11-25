"""Persistent settings: recent connections, the schema folder, and message types per collection."""

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field, replace

from PySide6.QtCore import QSettings

MAX_RECENT = 15

# Collections used by WEBKNOSSOS, suggested for servers that can't list their collections
KNOWN_COLLECTIONS = [
    "annotations",
    "annotationUpdates",
    "editableMappingsAgglomerateToGraph",
    "editableMappingsInfo",
    "editableMappingsSegmentToAgglomerate",
    "skeletons",
    "skeletonTreeBodies",
    "volumes",
    "volumeData",
    "volumeSegmentIndex",
]


@dataclass(frozen=True)
class ConnectionSpec:
    address: str  # host:port of FossilDB, as seen from the SSH host when tunneling
    ssh_host: str = ""
    # Optional name chosen by the user. Not part of the identity, so renaming keeps e.g. the recent collections.
    name: str = field(default="", compare=False)

    @property
    def label(self) -> str:
        """The technical description, which also identifies the connection in the settings."""
        return f"{self.address} via {self.ssh_host}" if self.ssh_host else self.address

    @property
    def title(self) -> str:
        return f"{self.name} ({self.label})" if self.name else self.label


# Saving the settings file is slow (Qt writes a new copy and calls fsync, which took up to a second), so writes
# go to a cache right away and are saved in the background, one after the other.
_cache: dict[str, str | None] = {}  # None: removed
_cache_lock = threading.Lock()
_writer = ThreadPoolExecutor(1, thread_name_prefix="settings")


def _get(key: str, default: str) -> str:
    with _cache_lock:
        if key in _cache:
            value = _cache[key]
            return default if value is None else value
    return QSettings().value(key, default)


def _set(key: str, value: str | None) -> None:
    with _cache_lock:
        _cache[key] = value
    _writer.submit(_save, key, value)


def _save(key: str, value: str | None) -> None:
    # QSettings objects can be used in any thread, as long as each is used by one thread only
    stored = QSettings()
    if value is None:
        stored.remove(key)
    else:
        stored.setValue(key, value)
    stored.sync()


def flush() -> None:
    """Waits until all changes are saved."""
    _writer.submit(lambda: None).result()


def recent_connections() -> list[ConnectionSpec]:
    try:
        entries = json.loads(_get("recentConnections", "[]"))
        return [ConnectionSpec(**entry) for entry in entries]
    except (ValueError, TypeError):
        return []


def _save_recent_connections(entries: list[ConnectionSpec]) -> None:
    _set("recentConnections", json.dumps([asdict(s) for s in entries[:MAX_RECENT]]))


def add_recent_connection(spec: ConnectionSpec) -> ConnectionSpec:
    """Moves the connection to the top. Without a name, it keeps the name it had. Returns the stored spec."""
    recent = recent_connections()
    if not spec.name:
        spec = next((s for s in recent if s == spec), spec)
    _save_recent_connections([spec] + [s for s in recent if s != spec])
    return spec


def rename_recent_connection(spec: ConnectionSpec, name: str) -> ConnectionSpec:
    renamed = replace(spec, name=name.strip())
    _save_recent_connections([renamed if s == spec else s for s in recent_connections()])
    return renamed


def remove_recent_connection(spec: ConnectionSpec) -> None:
    _save_recent_connections([s for s in recent_connections() if s != spec])


def recent_collections(spec_label: str) -> list[str]:
    try:
        return json.loads(_get(f"recentCollections/{spec_label}", "[]"))
    except (ValueError, TypeError):
        return []


def add_recent_collection(spec_label: str, collection: str) -> None:
    entries = [collection] + [c for c in recent_collections(spec_label) if c != collection]
    _set(f"recentCollections/{spec_label}", json.dumps(entries[:MAX_RECENT]))


def schema_folder() -> str:
    return _get("schemaFolder", "")


def set_schema_folder(folder: str) -> None:
    _set("schemaFolder", folder)


def message_type_for(collection: str) -> str:
    return _get(f"messageTypes/{collection}", "")


def set_message_type_for(collection: str, message_type: str) -> None:
    if message_type:
        _set(f"messageTypes/{collection}", message_type)
    else:
        _set(f"messageTypes/{collection}", None)
