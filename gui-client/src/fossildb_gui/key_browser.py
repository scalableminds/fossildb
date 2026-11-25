"""A collection tab: the list of keys, loaded page by page while scrolling, next to the selected key's record."""

from collections import deque

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt, QTimer, Signal
from PySide6.QtGui import QGuiApplication, QPalette
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from fossildb_gui import fmt
from fossildb_gui.record_view import RecordView
from fossildb_gui.tasks import Latest, TaskRunner

# Small pages show quickly even when the server reads from a slow disk; more are loaded while scrolling
PAGE_SIZE = 100
# Version counts need one ListVersions call per key. They are loaded for visible rows only, a few at a time.
MAX_VERSION_COUNT_REQUESTS = 3
# The next page is loaded once fewer rows than this are left below the visible ones
PREFETCH_ROWS = 50
# Counting the keys that a prefix delete affects stops here
MAX_COUNTED_KEYS = 10_000


class KeyListModel(QAbstractTableModel):
    status_changed = Signal()

    def __init__(self, connection, collection: str, runner: TaskRunner, parent=None):
        super().__init__(parent)
        self._connection = connection
        self._collection = collection
        self._runner = runner
        self._latest = Latest()
        self.prefix = ""
        self.keys: list[str] = []
        self.has_more = False
        self.loading = False
        self.error: str | None = None
        self._version_counts: dict[str, int | str] = {}
        self._count_queue: deque[str] = deque()
        self._counts_requested: set[str] = set()
        self._counts_in_flight = 0

    def reset(self, prefix: str | None = None) -> None:
        if prefix is not None:
            self.prefix = prefix
        self.beginResetModel()
        self.keys = []
        self.has_more = True
        self.error = None
        self.loading = False
        self._version_counts.clear()
        self._count_queue.clear()
        self._counts_requested.clear()
        self._latest.invalidate()
        self.endResetModel()
        self.fetchMore(QModelIndex())

    # Paging

    def canFetchMore(self, parent=QModelIndex()):
        return not parent.isValid() and self.has_more and not self.loading and self.error is None

    def fetchMore(self, parent=QModelIndex()):
        if not self.canFetchMore(parent):
            return
        if self.keys:
            # A last row "Loading more…", for when the user reaches the end before the page arrives
            self.beginInsertRows(QModelIndex(), len(self.keys), len(self.keys))
            self.loading = True
            self.endInsertRows()
        else:
            self.loading = True
        self.status_changed.emit()
        start_after = self.keys[-1] if self.keys else None
        prefix = self.prefix
        generation = self._latest.next()
        self._runner.submit(
            # One more than a page, to know whether there are more
            lambda: self._connection.list_keys(self._collection, PAGE_SIZE + 1, start_after, prefix),
            self._latest.wrap(generation, self._page_loaded),
            self._latest.wrap(generation, self._page_failed),
            context=self,
        )

    def _stop_loading(self) -> None:
        if self.loading and self.keys:
            self.beginRemoveRows(QModelIndex(), len(self.keys), len(self.keys))
            self.loading = False
            self.endRemoveRows()
        else:
            self.loading = False

    def _page_loaded(self, keys: list[str]) -> None:
        self._stop_loading()
        self.has_more = len(keys) > PAGE_SIZE
        keys = keys[:PAGE_SIZE]
        if keys:
            self.beginInsertRows(QModelIndex(), len(self.keys), len(self.keys) + len(keys) - 1)
            self.keys.extend(keys)
            self.endInsertRows()
        self.status_changed.emit()

    def _page_failed(self, error: Exception) -> None:
        self._stop_loading()
        self.error = str(error)
        self.status_changed.emit()

    def remove_key(self, key: str) -> None:
        try:
            row = self.keys.index(key)
        except ValueError:
            return
        self.beginRemoveRows(QModelIndex(), row, row)
        del self.keys[row]
        self.endRemoveRows()
        self._version_counts.pop(key, None)
        self.status_changed.emit()

    def forget_version_count(self, key: str) -> None:
        self._version_counts.pop(key, None)
        self._counts_requested.discard(key)
        if key in self.keys:
            row = self.keys.index(key)
            self.dataChanged.emit(self.index(row, 1), self.index(row, 1))

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
                lambda key=key: len(self._connection.list_versions(self._collection, key)),
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

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self.keys) + (1 if self.loading and self.keys else 0)

    def is_key_row(self, row: int) -> bool:
        return 0 <= row < len(self.keys)

    def flags(self, index):
        if not self.is_key_row(index.row()):
            return Qt.ItemFlag.NoItemFlags
        return super().flags(index)

    def columnCount(self, parent=QModelIndex()):
        return 2

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not self.is_key_row(index.row()):
            return self._loading_row_data(index, role)
        key = self.keys[index.row()]
        if index.column() == 0:
            if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.ToolTipRole):
                return key
            return None
        if role == Qt.ItemDataRole.DisplayRole:
            count = self._version_counts.get(key)
            if count is None:
                self._request_version_count(key)
                return ""
            return fmt.count(count) if isinstance(count, int) else count
        if role == Qt.ItemDataRole.TextAlignmentRole:
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        return None

    @staticmethod
    def _loading_row_data(index, role):
        if index.column() != 0:
            return None
        if role == Qt.ItemDataRole.DisplayRole:
            return "Loading more…"
        if role == Qt.ItemDataRole.ForegroundRole:
            return QGuiApplication.palette().color(QPalette.ColorRole.PlaceholderText)
        if role == Qt.ItemDataRole.FontRole:
            font = QGuiApplication.font()
            font.setItalic(True)
            return font
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return ("Key", "Versions")[section]
        return None


class KeyBrowser(QWidget):
    def __init__(self, connection, collection: str, runner: TaskRunner, schemas, window, parent=None):
        super().__init__(parent)
        self.collection = collection
        self._connection = connection
        self._runner = runner
        self._window = window

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
        scroll_bar = self._table.verticalScrollBar()
        scroll_bar.valueChanged.connect(self._prefetch)
        scroll_bar.rangeChanged.connect(self._prefetch)
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

        self.record = RecordView(connection, collection, runner, schemas, window)
        self.record.key_deleted.connect(self._key_deleted)

        splitter = QSplitter()
        splitter.addWidget(left)
        splitter.addWidget(self.record)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([360, 900])

        layout = QVBoxLayout(self)
        layout.addWidget(splitter)

        self.update_write_actions()
        self._model.reset()

    def focus_prefix(self) -> None:
        self._prefix.setFocus()
        self._prefix.selectAll()

    def refresh(self) -> None:
        self._model.reset()
        self.record.reload()

    def update_write_actions(self) -> None:
        writable = self._window.writes_enabled
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
        if current.isValid() and self._model.is_key_row(current.row()):
            self.record.show_key(self._model.keys[current.row()])

    def _open_exact_key(self) -> None:
        key = self._open_key.text().strip()
        if key:
            self._table.clearSelection()
            self.record.show_key(key)

    def _prefetch(self) -> None:
        last_visible = self._table.rowAt(self._table.viewport().height() - 1)
        if last_visible == -1:  # below the last row, which Qt handles itself
            return
        if self._model.rowCount() - 1 - last_visible < PREFETCH_ROWS and self._model.canFetchMore():
            self._model.fetchMore()

    def _context_menu(self, position) -> None:
        index = self._table.indexAt(position)
        if not index.isValid() or not self._model.is_key_row(index.row()):
            return
        key = self._model.keys[index.row()]
        menu = QMenu(self)
        menu.addAction("Copy key", lambda: QGuiApplication.clipboard().setText(key))
        menu.exec(self._table.viewport().mapToGlobal(position))

    def _key_deleted(self, key: str) -> None:
        self._model.remove_key(key)
        self.record.show_key(None)

    # Deleting by prefix

    def _delete_prefix(self) -> None:
        prefix = self._prefix.text()
        if not prefix:
            QMessageBox.information(
                self, "Delete keys with prefix", "Enter the prefix of the keys to delete in the filter field first."
            )
            return
        self._window.statusBar().showMessage(f"Counting keys starting with “{prefix}”…")

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
        self._window.statusBar().clearMessage()
        if total == 0:
            QMessageBox.information(self, "Delete keys with prefix", f"No keys start with “{prefix}”.")
            return
        amount = f"at least {fmt.count(total)}" if more else fmt.count(total)
        if not self._window.confirm_write(
            "Delete keys with prefix",
            f"Delete all versions of {amount} keys starting with\n\n{prefix}\n\nin {self.collection}? "
            "This cannot be undone.",
            require_text=prefix,
        ):
            return
        self._window.statusBar().showMessage(f"Deleting keys starting with “{prefix}”…")

        def deleted(_):
            self._window.statusBar().showMessage(f"Deleted {amount} keys starting with “{prefix}”", 8000)
            self.record.show_key(None)
            self._model.reset()

        self._runner.submit(
            lambda: self._connection.delete_all_by_prefix(self.collection, prefix),
            deleted,
            lambda e: QMessageBox.critical(self, "Delete failed", str(e)),
            context=self,
        )
