"""One key: its versions next to the selected version's value, with actions such as download and delete."""

from collections import OrderedDict
from pathlib import Path

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QFileDialog, QHBoxLayout, QLabel, QMessageBox, QPushButton, QVBoxLayout, QWidget

from fossildb_gui import fmt
from fossildb_gui.host import Host
from fossildb_gui.schema_manager import SchemaManager
from fossildb_gui.tasks import Latest, TaskRunner
from fossildb_gui.value_tabs import ValueTabs, file_name
from fossildb_gui.values import decode_value
from fossildb_gui.versions_panel import VersionsPanel
from fossildb_gui.widgets import splitter

# Recently shown values are kept, so that going back to them, or decoding them again, needs no request
_CACHED_VALUES = 32


class RecordView(QWidget):
    key_deleted = Signal(str)  # all versions of the key were deleted

    def __init__(
        self, connection, collection: str, runner: TaskRunner, schemas: SchemaManager, host: Host, parent=None
    ):
        super().__init__(parent)
        self._connection = connection
        self._collection = collection
        self._runner = runner
        self._host = host
        self._key: str | None = None
        self._version: int | None = None  # shown, or loading
        self._values: OrderedDict[tuple[str, int], bytes] = OrderedDict()
        self._latest_value = Latest()

        self._title = QLabel()
        self._title.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._title.setTextFormat(Qt.TextFormat.PlainText)  # keys may contain "<"
        self._title.setWordWrap(True)
        font = self._title.font()
        font.setBold(True)
        self._title.setFont(font)

        self.versions = VersionsPanel(connection, collection, runner)
        self.versions.version_selected.connect(self._show_version)
        self.versions.version_requested.connect(self._show_version)
        self.versions.versions_changed.connect(self.update_write_actions)

        self.value_label = QLabel()
        self.download_button = QPushButton("Download…")
        self.download_button.clicked.connect(self._download)
        self.delete_version_button = QPushButton("Delete version…")
        self.delete_version_button.clicked.connect(self._delete_version)
        self.delete_key_button = QPushButton("Delete key…")
        self.delete_key_button.clicked.connect(self.delete_key)
        actions = QHBoxLayout()
        actions.addWidget(self.value_label, stretch=1)
        actions.addWidget(self.download_button)
        actions.addWidget(self.delete_version_button)
        actions.addWidget(self.delete_key_button)

        self.value = ValueTabs(collection, schemas, runner, host)
        self.value.message_type_changed.connect(self._decode_again)
        value_panel = QWidget()
        value_layout = QVBoxLayout(value_panel)
        value_layout.setContentsMargins(0, 0, 0, 0)
        value_layout.addLayout(actions)
        value_layout.addWidget(self.value)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._title)
        layout.addWidget(splitter(self.versions, value_panel, 160, "versions"), stretch=1)
        self.show_key(None)

    @property
    def key(self) -> str | None:
        return self._key

    @property
    def version(self) -> int | None:
        return self._version

    def show_key(self, key: str | None, select: int | None = None, version_count: int | None = None) -> None:
        """Shows the key's versions, and the given version, or else the newest. The number of versions can be given
        if it is known already."""
        self._key = key
        self._version = None
        self._latest_value.invalidate()
        self._clear_value()
        created = fmt.object_id_date(key) if key is not None else None
        title = "Select a key" if key is None else f"{self._collection} / {key}"
        self._title.setText(title + (f"  (ObjectId created {created})" if created else ""))
        self.versions.show_key(key, select, version_count)
        self.update_write_actions()

    def reload(self) -> None:
        """Loads the versions and the shown value again from the server."""
        self._values.clear()
        self.show_key(self._key, select=self._version)

    def redisplay(self) -> None:
        """Shows everything again, e.g. with other size units."""
        self.versions.redisplay()
        self._decode_again()

    def update_write_actions(self) -> None:
        writable = self._host.writes_enabled
        tip = "" if writable else " (enable writes in the Database menu first)"
        self.delete_version_button.setEnabled(writable and self._version is not None)
        self.delete_key_button.setEnabled(writable and self._key is not None and bool(self.versions.versions))
        self.delete_version_button.setToolTip("Delete the shown version" + tip)
        self.delete_key_button.setToolTip("Delete all versions of this key" + tip)

    # Values

    def _clear_value(self) -> None:
        self.value_label.setText("")
        self.download_button.setEnabled(False)
        self.value.clear()

    def _show_version(self, version: int) -> None:
        self._version = version
        self.update_write_actions()
        self._load_value()

    def _decode_again(self) -> None:
        if self._version is not None:
            self._load_value()  # from the cache

    def _load_value(self) -> None:
        key, version = self._key, self._version
        generation = self._latest_value.next()
        self.value_label.setText(f"Version {version} · loading…")
        self.download_button.setEnabled(False)
        message_class = self.value.message_class()

        def load():
            cached = self._values.get((key, version))
            if cached is not None:
                data, actual = cached, version
            else:
                # For a version that doesn't exist, FossilDB returns the next older one
                data, actual = self._connection.get_version(self._collection, key, version)
            return actual, data, decode_value(data, message_class)

        self._runner.submit(
            load,
            self._latest_value.wrap(generation, lambda result: self._value_loaded(key, version, *result)),
            self._latest_value.wrap(generation, lambda e: self.value_label.setText(f"Version {version}: {e}")),
            context=self,
        )

    def _value_loaded(self, key: str, requested: int, version: int, data: bytes, value) -> None:
        self._version = version
        self.update_write_actions()
        self._values[(key, version)] = data
        self._values.move_to_end((key, version))
        while len(self._values) > _CACHED_VALUES:
            self._values.popitem(last=False)
        self.versions.set_size(version, len(data))
        self.versions.select(version)
        size = f"{fmt.size(len(data))} ({fmt.plural(len(data), 'byte')})"
        older = f" (there is no version {requested}, this is the next older one)" if version != requested else ""
        self.value_label.setText(f"Version {version} · {size}{older}")
        self.download_button.setEnabled(True)
        self.value.show_value(value, data, f"{self._collection}_{key}_{version}")

    # Actions

    def _download(self) -> None:
        data = self._values.get((self._key, self._version))
        if data is None:
            return
        name = file_name(self._collection, self._key, self._version) + ".bin"
        path, _ = QFileDialog.getSaveFileName(self, "Save value", str(Path.home() / name))
        if path:
            Path(path).write_bytes(data)
            self._host.show_status(f"Saved {fmt.size(len(data))} to {path}", 8000)

    def _delete_version(self) -> None:
        key, version = self._key, self._version
        if self._host.confirm_write(
            "Delete version",
            f"Delete version {version} of key\n\n{key}\n\nin {self._collection}? This cannot be undone.",
        ):
            self._run_delete(
                lambda: self._connection.delete_version(self._collection, key, version),
                f"Deleted version {version} of {key}",
            )

    def delete_key(self) -> None:
        key = self._key
        if self._host.confirm_write(
            "Delete key",
            f"Delete all {self.versions.versions_text()} of key\n\n{key}\n\nin {self._collection}? "
            "This cannot be undone.",
        ):
            self._run_delete(
                lambda: self._connection.delete_key(self._collection, key), f"Deleted all versions of {key}"
            )

    def _run_delete(self, delete, message: str) -> None:
        key = self._key

        def deleted(_) -> None:
            self._host.show_status(message, 8000)
            self._values = OrderedDict((k, v) for k, v in self._values.items() if k[0] != key)
            # Whether versions are left decides between showing the key again and removing it from the list
            self._runner.submit(
                lambda: self._connection.list_versions(self._collection, key, 1),
                lambda versions: self.key_deleted.emit(key) if not versions else self.show_key(key),
                context=self,
            )

        self._runner.submit(
            delete, deleted, lambda e: QMessageBox.critical(self, "Delete failed", str(e)), context=self
        )
