"""Persistent settings: recent connections, the schema folder, and message types per collection."""

import json
from dataclasses import asdict, dataclass

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

    @property
    def label(self) -> str:
        return f"{self.address} via {self.ssh_host}" if self.ssh_host else self.address


def _settings() -> QSettings:
    return QSettings()


def recent_connections() -> list[ConnectionSpec]:
    try:
        entries = json.loads(_settings().value("recentConnections", "[]"))
        return [ConnectionSpec(**entry) for entry in entries]
    except (ValueError, TypeError):
        return []


def add_recent_connection(spec: ConnectionSpec) -> None:
    entries = [spec] + [s for s in recent_connections() if s != spec]
    _settings().setValue("recentConnections", json.dumps([asdict(s) for s in entries[:MAX_RECENT]]))


def recent_collections(spec_label: str) -> list[str]:
    try:
        return json.loads(_settings().value(f"recentCollections/{spec_label}", "[]"))
    except (ValueError, TypeError):
        return []


def add_recent_collection(spec_label: str, collection: str) -> None:
    entries = [collection] + [c for c in recent_collections(spec_label) if c != collection]
    _settings().setValue(f"recentCollections/{spec_label}", json.dumps(entries[:MAX_RECENT]))


def schema_folder() -> str:
    return _settings().value("schemaFolder", "")


def set_schema_folder(folder: str) -> None:
    _settings().setValue("schemaFolder", folder)


def message_type_for(collection: str) -> str:
    return _settings().value(f"messageTypes/{collection}", "")


def set_message_type_for(collection: str, message_type: str) -> None:
    if message_type:
        _settings().setValue(f"messageTypes/{collection}", message_type)
    else:
        _settings().remove(f"messageTypes/{collection}")
