"""A collection tab: the list of keys, loaded page by page while scrolling, next to the selected key's record."""

from collections import deque

from PySide6.QtCore import QEvent, QModelIndex, Qt, QTimer
from PySide6.QtGui import QGuiApplication
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QStyle,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from fossildb_gui import fmt, versions_panel
from fossildb_gui.host import Host
from fossildb_gui.paged_model import PagedModel
from fossildb_gui.record_view import RecordView
from fossildb_gui.tasks import TaskRunner
from fossildb_gui.widgets import fetch_early, splitter, take_list_key

# Small pages show quickly even when the server reads from a slow disk; more are loaded while scrolling
PAGE_SIZE = 100
# Version counts need one ListVersions call per key. They are loaded for visible rows only, a few at a time.
MAX_VERSION_COUNT_REQUESTS = 3
# The next page is loaded once fewer rows than this are left below the visible ones
PREFETCH_ROWS = 50
# Counting the keys that a prefix delete affects stops here
MAX_COUNTED_KEYS = 10_000
# Separates key and version in the entries that FossilDB stores, so keys cannot contain it
# (VersionedKey.versionSeparator on the server)
VERSION_SEPARATOR = "@"


class KeyListModel(PagedModel):
    """The keys of a collection with a prefix, and their number of versions (loaded for visible rows only)."""

    def __init__(self, connection, collection: str, runner: TaskRunner, parent=None):
        super().__init__(runner, PAGE_SIZE, parent)
        self._connection = connection
        self._collection = collection
        self.prefix = ""
        self._version_counts: dict[str, int | str] = {}
        self._count_queue: deque[str] = deque()
        self._counts_requested: set[str] = set()
        self._counts_in_flight = 0

    @property
    def keys(self) -> list[str]:
        return self.items

    def reset(self, prefix: str | None = None, load: bool = True) -> None:
        if prefix is not None:
            self.prefix = prefix
        self._version_counts.clear()
        self._count_queue.clear()
        self._counts_requested.clear()
        super().reset(load)

    def page_loader(self, limit: int):
        start_after = self.keys[-1] if self.keys else None
        collection, prefix = self._collection, self.prefix
        return lambda: self._connection.list_keys(collection, limit, start_after, prefix)

    def version_count(self, key: str) -> int | None:
        """The number of versions, if counted already; more than versions_panel.MAX_VERSION_COUNT means more."""
        count = self._version_counts.get(key)
        return count if isinstance(count, int) else None

    def remove(self, key: str) -> None:
        self._version_counts.pop(key, None)
        super().remove(key)

    # Version counts, loaded when rows become visible

    def _request_version_count(self, key: str) -> None:
        if key in self._counts_requested:
            return
        self._counts_requested.add(key)
        self._count_queue.appendleft(key)  # most recently shown first
        self._pump_version_counts()

    def _pump_version_counts(self) -> None:
        while self._counts_in_flight < MAX_VERSION_COUNT_REQUESTS and self._count_queue:
            key = self._count_queue.popleft()
            self._counts_in_flight += 1
            self._runner.submit(
                lambda key=key: versions_panel.count_versions(self._connection, self._collection, key),
                lambda n, key=key: self._version_count_loaded(key, n),
                lambda e, key=key: self._version_count_loaded(key, "?"),
                context=self,
            )

    def _version_count_loaded(self, key: str, count: int | str) -> None:
        self._counts_in_flight -= 1
        if key in self._counts_requested and self.keys:  # not reset in the meantime
            self._version_counts[key] = count
            # Finding the row would be slow for long lists, and views only repaint visible rows anyway
            self.dataChanged.emit(self.index(0, 1), self.index(len(self.keys) - 1, 1))
        self._pump_version_counts()

    # Model

    def columnCount(self, parent=QModelIndex()):
        return 2

    def item_data(self, key: str, column: int, role):
        if column == 0:
            return key if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.ToolTipRole) else None
        if role == Qt.ItemDataRole.DisplayRole:
            count = self._version_counts.get(key)
            if count is None:
                self._request_version_count(key)
                return ""
            if isinstance(count, int):
                limit = versions_panel.MAX_VERSION_COUNT
                return f"{fmt.count(limit)}+" if count > limit else fmt.count(count)
            return count
        if role == Qt.ItemDataRole.TextAlignmentRole:
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return ("Key", "Versions")[section]
        return None


class KeyBrowser(QWidget):
    def __init__(self, connection, collection: str, runner: TaskRunner, schemas, host: Host, parent=None):
        super().__init__(parent)
        self.collection = collection
        self._connection = connection
        self._runner = runner
        self._host = host

        self._prefix = QLineEdit()
        self._prefix.setPlaceholderText("Filter by key prefix")
        self._prefix.setClearButtonEnabled(True)
        self._prefix_timer = QTimer(self, singleShot=True, interval=300)
        self._prefix_timer.timeout.connect(lambda: self._model.reset(self._prefix.text()))
        self._prefix.textChanged.connect(lambda: self._prefix_timer.start())
        self._prefix.returnPressed.connect(lambda: (self._prefix_timer.stop(), self._model.reset(self._prefix.text())))

        self._open_key = QLineEdit()
        self._open_key.setPlaceholderText("Open exact key (Enter)")
        self._open_key.returnPressed.connect(self._open_exact_key)

        self._delete_prefix_button = QPushButton("Delete keys with prefix…")
        self._delete_prefix_button.clicked.connect(self._delete_prefix)

        self._model = KeyListModel(connection, collection, runner, self)
        self._model.status_changed.connect(self._update_status)
        self._table = QTableView()
        self._table.setWordWrap(False)  # otherwise long keys are cut at hyphens instead of at the end
        self._table.setTabKeyNavigation(False)  # Tab moves to the next control, not to the next cell
        self._table.setModel(self._model)
        self._table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().hide()
        self._table.verticalHeader().setDefaultSectionSize(self._table.fontMetrics().height() + 6)
        header = self._table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        self._table.setColumnWidth(1, 80)
        self._table.selectionModel().currentRowChanged.connect(self._row_changed)
        # Qt only loads more when scrolled to the very end, so load the next page earlier
        fetch_early(self._table, PREFETCH_ROWS)
        self._table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._table.customContextMenuRequested.connect(self._context_menu)

        self._status = QLabel()
        self._status.setWordWrap(True)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.addWidget(self._prefix)
        left_layout.addWidget(self._open_key)
        left_layout.addWidget(self._table)
        bottom = QHBoxLayout()
        bottom.addWidget(self._status, stretch=1)
        bottom.addWidget(self._delete_prefix_button)
        left_layout.addLayout(bottom)

        self.record = RecordView(connection, collection, runner, schemas, host)
        self.record.key_deleted.connect(self._key_deleted)

        panels = splitter(left, self.record, self._key_list_width(), "keys")
        # Arrow keys move between the key list and the version list
        self._table.installEventFilter(self)
        self.record.versions.view.installEventFilter(self)

        layout = QVBoxLayout(self)
        layout.addWidget(panels)

        self.update_write_actions()
        self._model.reset()

    def focus_prefix(self) -> None:
        self._prefix.setFocus()
        self._prefix.selectAll()

    def refresh(self) -> None:
        self._model.reset()
        self.record.reload()

    def update_write_actions(self) -> None:
        writable = self._host.writes_enabled
        self._delete_prefix_button.setEnabled(writable)
        self._delete_prefix_button.setToolTip(
            "Delete all versions of all keys starting with the prefix in the filter"
            + ("" if writable else " (enable writes in the Database menu first)")
        )
        self.record.update_write_actions()

    def _update_status(self) -> None:
        model = self._model
        if model.error:
            text = f"Could not load keys: {model.error}"
            if "No store for column family" in model.error:
                text = f"The collection {self.collection} does not exist on this server."
        elif model.loading and not model.keys:
            text = "Loading keys…"
        else:
            matching = f" starting with “{model.prefix}”" if model.prefix else ""
            noun = "key" if len(model.keys) == 1 and not model.has_more else "keys"
            text = f"{fmt.count(len(model.keys))}{'+' if model.has_more else ''} {noun}{matching}"
            if model.has_more:
                text += " (scroll for more)"
            if model.loading:
                text += ", loading more…"
        self._status.setText(text)

    def _row_changed(self, current, _previous) -> None:
        if current.isValid() and self._model.is_item_row(current.row()):
            key = self._model.keys[current.row()]
            self.record.show_key(key, version_count=self._model.version_count(key))

    def _open_exact_key(self) -> None:
        key = self._open_key.text().strip()
        if key:
            self._table.clearSelection()
            self.record.show_key(key)

    # Typical keys, e.g. of WEBKNOSSOS, are UUIDs, which should fit without being shortened
    TYPICAL_KEY = "00000000-5813-8649-5500-00a6004814b0"

    def _key_list_width(self) -> int:
        table = self._table
        key = table.fontMetrics().horizontalAdvance(self.TYPICAL_KEY) + 16  # plus the cell's margins
        scroll_bar = table.style().pixelMetric(QStyle.PixelMetric.PM_ScrollBarExtent)
        return key + table.columnWidth(1) + scroll_bar + 2 * table.frameWidth() + 8

    def eventFilter(self, watched, event) -> bool:
        if event.type() == QEvent.Type.KeyPress and not event.modifiers():
            if watched is self._table and event.key() == Qt.Key.Key_Right and self.record.versions.focus():
                return True
            if watched is self.record.versions.view and event.key() == Qt.Key.Key_Left:
                self._table.setFocus()
                return True
        return super().eventFilter(watched, event)

    def take_list_key(self, event) -> bool:
        """Handles an arrow key that no other widget used: moves the focus to the list, and selects its first row if
        none is selected yet, or else moves the selection. False if there is no list."""
        if not self._model.keys:
            return False
        return take_list_key(self._table, event)

    def _context_menu(self, position) -> None:
        index = self._table.indexAt(position)
        if not index.isValid() or not self._model.is_item_row(index.row()):
            return
        key = self._model.keys[index.row()]
        menu = QMenu(self)
        menu.addAction("Copy key", lambda: QGuiApplication.clipboard().setText(key))
        menu.exec(self._table.viewport().mapToGlobal(position))

    def _key_deleted(self, key: str) -> None:
        self._model.remove(key)
        self.record.show_key(None)

    # Deleting by prefix

    def _delete_prefix(self) -> None:
        prefix = self._prefix.text()
        if not prefix:
            QMessageBox.information(
                self, "Delete keys with prefix", "Enter the prefix of the keys to delete in the filter field first."
            )
            return
        if VERSION_SEPARATOR in prefix:
            # FossilDB stores entries as key@…@version and matches the prefix against those, so a prefix with @ would
            # delete versions of keys that don't start with it, e.g. "abc@" all versions of the key "abc"
            QMessageBox.warning(
                self,
                "Delete keys with prefix",
                f"The prefix contains “{VERSION_SEPARATOR}”, which keys cannot contain. Deleting by such a prefix "
                "would delete other data than the listed keys, so it is not possible.",
            )
            return
        self._host.show_status(f"Counting keys starting with “{prefix}”…")

        def count_keys():
            total, start_after = 0, None
            while total < MAX_COUNTED_KEYS:
                keys = self._connection.list_keys(self.collection, 1000, start_after, prefix)
                total += len(keys)
                if len(keys) < 1000:
                    return total, False
                start_after = keys[-1]
            return total, True

        self._runner.submit(
            count_keys,
            lambda result: self._confirm_delete_prefix(prefix, *result),
            lambda e: QMessageBox.critical(self, "Counting keys failed", str(e)),
            context=self,
        )

    def _confirm_delete_prefix(self, prefix: str, total: int, more: bool) -> None:
        self._host.show_status("")
        if total == 0:
            QMessageBox.information(self, "Delete keys with prefix", f"No keys start with “{prefix}”.")
            return
        amount = f"at least {fmt.count(total)}" if more else fmt.count(total)
        if not self._host.confirm_write(
            "Delete keys with prefix",
            f"Delete all versions of {amount} keys starting with\n\n{prefix}\n\nin {self.collection}? "
            "This cannot be undone.",
            require_text=prefix,
        ):
            return
        self._host.show_status(f"Deleting keys starting with “{prefix}”…")

        def deleted(_):
            self._host.show_status(f"Deleted {amount} keys starting with “{prefix}”", 8000)
            self.record.show_key(None)
            self._model.reset()

        self._runner.submit(
            lambda: self._connection.delete_all_by_prefix(self.collection, prefix),
            deleted,
            lambda e: QMessageBox.critical(self, "Delete failed", str(e)),
            context=self,
        )
