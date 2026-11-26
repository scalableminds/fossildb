"""The first tab: information about the server, and a table of its collections."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from PySide6.QtCore import QAbstractTableModel, QModelIndex, QRect, QSortFilterProxyModel, Qt, QTimer, Signal
from PySide6.QtGui import QGuiApplication, QPainter, QPalette
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMenu,
    QPushButton,
    QStyle,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from fossildb_gui import collection_stats as stats
from fossildb_gui import fmt, settings
from fossildb_gui.settings import ConnectionSpec
from fossildb_gui.widgets import PADDING, ElidedLabel, PaddedDelegate, dimmed, pad_header, take_list_key

AUTO_REFRESH_SECONDS = 1

_SORT_ROLE = Qt.ItemDataRole.UserRole
_BAR_ROLE = Qt.ItemDataRole.UserRole + 1  # size as share of the largest collection, 0 to 1


@dataclass
class _Column:
    title: str
    sort_value: Callable[[Any], Any]
    text: Callable[[Any], str]
    tooltip: str
    numeric: bool  # right-aligned


COLUMNS = [
    _Column("Collection", lambda c: c.name, lambda c: c.name, "Hover over a collection for all its statistics", False),
    _Column(
        "Column family",
        lambda c: c.columnFamily,
        lambda c: c.columnFamily,
        "The RocksDB column family holding this collection. Differs from the name when a RocksDB options file is used.",
        False,
    ),
    _Column(
        "On disk",
        lambda c: c.liveSstFilesBytes,
        lambda c: fmt.size(c.liveSstFilesBytes),
        "Size of the data files (compressed). The bar compares it to the largest collection.",
        True,
    ),
    _Column(
        "Versions",
        lambda c: c.estimatedVersionCount,
        lambda c: fmt.compact_count(c.estimatedVersionCount),
        "Estimated number of stored versions (of all keys). Deleted or overwritten entries count until a compaction "
        "removes them.",
        True,
    ),
    _Column(
        "Compression",
        lambda c: stats.ratio(c) or 0,
        stats.compression,
        "Compression ratio (uncompressed size / size on disk) and compression types of the data files. "
        "CompactAllData converts old files to the current compression.",
        False,
    ),
    _Column(
        "Status",
        lambda c: " ".join(stats.warnings(c)),
        lambda c: "   ".join(stats.warnings(c)),
        f"Pending compactions, and deletion markers that make up at least {stats.DELETIONS_WARNING_SHARE:.0%} of "
        "the entries",
        False,
    ),
]
_COLUMN_FAMILY_COLUMN = 1
_SIZE_COLUMN = 2


class CollectionsModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._collections: list = []
        self._largest = 0

    def set_collections(self, collections) -> bool:
        """Returns whether the rows changed. Otherwise only the values are updated, which keeps selection and scroll
        position, e.g. when refreshing automatically."""
        collections = list(collections)
        same_rows = [c.name for c in collections] == [c.name for c in self._collections]
        if not same_rows:
            self.beginResetModel()
        self._collections = collections
        self._largest = max((c.liveSstFilesBytes for c in collections), default=0)
        if not same_rows:
            self.endResetModel()
        elif collections:
            self.dataChanged.emit(self.index(0, 0), self.index(len(collections) - 1, len(COLUMNS) - 1))
        return not same_rows

    def collection(self, row: int):
        return self._collections[row]

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._collections)

    def columnCount(self, parent=QModelIndex()):
        return len(COLUMNS)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        collection = self._collections[index.row()]
        column = COLUMNS[index.column()]
        if role == Qt.ItemDataRole.DisplayRole:
            return column.text(collection)
        if role == _SORT_ROLE:
            return column.sort_value(collection)
        if role == _BAR_ROLE:
            return collection.liveSstFilesBytes / self._largest if self._largest else 0
        if role == Qt.ItemDataRole.TextAlignmentRole and column.numeric:
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        if role == Qt.ItemDataRole.ToolTipRole:
            return stats.details_html(collection)
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation != Qt.Orientation.Horizontal:
            return None
        column = COLUMNS[section]
        if role == Qt.ItemDataRole.DisplayRole:
            return column.title
        if role == Qt.ItemDataRole.ToolTipRole:
            return column.tooltip
        if role == Qt.ItemDataRole.TextAlignmentRole:
            horizontal = Qt.AlignmentFlag.AlignRight if column.numeric else Qt.AlignmentFlag.AlignLeft
            return horizontal | Qt.AlignmentFlag.AlignVCenter
        return None


class _CollectionsView(QTableView):
    """Only the left mouse button opens collections. In single-click mode, Qt activates items with any button."""

    def __init__(self):
        super().__init__()
        self.other_button = False

    def mouseReleaseEvent(self, event) -> None:
        self._with_button(event, super().mouseReleaseEvent)

    def mouseDoubleClickEvent(self, event) -> None:
        self._with_button(event, super().mouseDoubleClickEvent)

    def _with_button(self, event, handler) -> None:
        self.other_button = event.button() != Qt.MouseButton.LeftButton
        try:
            handler(event)
        finally:
            self.other_button = False


class _SizeBarDelegate(PaddedDelegate):
    """Draws a bar for the size, left of the right-aligned number."""

    _TEXT_SPACE = 70  # room for e.g. "1023.9 GiB"

    def paint(self, painter, option, index) -> None:
        super().paint(painter, option, index)
        share = index.data(_BAR_ROLE) or 0
        bar_space = option.rect.width() - self._TEXT_SPACE - 2 * PADDING
        if bar_space <= 0 or share <= 0:
            return
        color = option.palette.color(QPalette.ColorRole.Highlight)
        color.setAlphaF(0.55)
        height = max(4, option.rect.height() // 3)
        top = option.rect.center().y() - height // 2
        bar = QRect(option.rect.left() + PADDING, top, max(2, round(bar_space * share)), height)
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(color)
        painter.drawRoundedRect(bar, 2, 2)
        painter.restore()


# The server information, as (label, shortened in the middle if too long) per grid column
_SERVER_FIELDS = [
    [("Address", False), ("Version", False), ("Running since", False), ("Status", False)],
    [("Data directory", True), ("Backup directory", True), ("Options file", True), ("Write-ahead log", False)],
    [("Updated", False), ("Disk", False), ("Collections", False), ("Size", False)],
]
# What servers without GetDatabaseInfo show
_LEGACY_FIELDS = {"Address", "Version", "Updated"}


class OverviewWidget(QWidget):
    open_collection = Signal(str)
    refresh_requested = Signal()
    auto_refresh_requested = Signal()  # only sent while the overview is visible

    def __init__(self, spec: ConnectionSpec, parent=None):
        super().__init__(parent)
        self._spec = spec
        self._info = None  # the DatabaseInfoProto shown

        refresh = QPushButton("Refresh")
        refresh.clicked.connect(self.refresh_requested)
        self._auto_refresh = QPushButton("Auto-refresh")
        self._auto_refresh.setCheckable(True)
        self._auto_refresh.setToolTip(f"Refresh every {AUTO_REFRESH_SECONDS} s while the overview is shown")
        self._auto_refresh_timer = QTimer(self, interval=AUTO_REFRESH_SECONDS * 1000)
        self._auto_refresh_timer.timeout.connect(self._auto_refresh_tick)
        self._auto_refresh.toggled.connect(self._auto_refresh_toggled)

        self._table = self._collections_table()
        self._manual_box = self._manual_entry()

        layout = QVBoxLayout(self)
        layout.addLayout(self._server_grid(refresh))
        layout.addWidget(self._table, stretch=1)
        layout.addWidget(self._manual_box)
        layout.addStretch(0)

    def _server_grid(self, refresh: QPushButton) -> QGridLayout:
        """The server information, compact, so that the collections table gets the space."""
        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(8)
        grid.setVerticalSpacing(4)
        self._labels: dict[str, QLabel] = {}
        self._captions: dict[str, QLabel] = {}
        for group, fields in enumerate(_SERVER_FIELDS):
            for row, (name, elide) in enumerate(fields):
                caption = dimmed(QLabel(f"{name}:"))
                if group:
                    caption.setContentsMargins(16, 0, 0, 0)  # space between the column groups
                value = ElidedLabel() if elide else QLabel()
                value.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                self._captions[name], self._labels[name] = caption, value
                grid.addWidget(caption, row, 2 * group)
                if name == "Updated":
                    updated = QHBoxLayout()
                    updated.addWidget(value)
                    updated.addSpacing(12)
                    updated.addStretch()
                    updated.addWidget(refresh)
                    updated.addWidget(self._auto_refresh)
                    grid.addLayout(updated, row, 2 * group + 1)
                else:
                    grid.addWidget(value, row, 2 * group + 1)
            grid.setColumnStretch(2 * group + 1, 1)
        grid.setColumnStretch(3, 3)  # the paths get most of the room
        self._labels["Address"].setText(self._spec.title)
        self._labels["Status"].setTextFormat(Qt.TextFormat.RichText)
        self._labels["Size"].setToolTip(
            "Data files of all collections. Recent writes that are still in memory and the write-ahead log are not "
            "included."
        )
        return grid

    def _collections_table(self) -> _CollectionsView:
        """The collections of servers with GetDatabaseInfo."""
        self._model = CollectionsModel(self)
        self._proxy = QSortFilterProxyModel(self)
        self._proxy.setSourceModel(self._model)
        self._proxy.setSortRole(_SORT_ROLE)
        table = _CollectionsView()
        table.setModel(self._proxy)
        pad_header(table)
        table.setItemDelegate(PaddedDelegate(table))
        table.setItemDelegateForColumn(_SIZE_COLUMN, _SizeBarDelegate(table))
        table.setWordWrap(False)  # otherwise long names are cut at hyphens instead of at the end
        table.setTabKeyNavigation(False)  # Tab moves to the next control, not to the next cell
        table.setSortingEnabled(True)
        table.sortByColumn(_SIZE_COLUMN, Qt.SortOrder.DescendingOrder)
        table.setShowGrid(False)
        table.setAlternatingRowColors(True)
        table.verticalHeader().setDefaultSectionSize(table.fontMetrics().height() + 10)
        table.verticalHeader().hide()
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        table.horizontalHeader().setStretchLastSection(True)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        # Activation follows the desktop setting, e.g. KDE can open items with a single click
        single_click = table.style().styleHint(QStyle.StyleHint.SH_ItemView_ActivateItemOnSingleClick)
        table.setToolTip(f"{'Click' if single_click else 'Double-click'} a collection to open it")
        table.activated.connect(self._activated)
        table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        table.customContextMenuRequested.connect(self._context_menu)
        return table

    def _manual_entry(self) -> QGroupBox:
        """Entering the name of a collection, for servers without GetDatabaseInfo."""
        box = QGroupBox("Open a collection")
        self._manual_combo = QComboBox()
        self._manual_combo.setEditable(True)
        self._manual_combo.setMinimumWidth(320)
        self._manual_combo.lineEdit().setPlaceholderText("collection name")
        self._manual_combo.lineEdit().returnPressed.connect(self._open_manual)
        open_button = QPushButton("Open")
        open_button.clicked.connect(self._open_manual)
        row = QHBoxLayout()
        row.addWidget(self._manual_combo)
        row.addWidget(open_button)
        row.addStretch()
        layout = QVBoxLayout(box)
        layout.addWidget(
            QLabel(
                "This server is older and cannot list its collections or report statistics.\n"
                "Enter the name of the collection to open."
            )
        )
        layout.addLayout(row)
        return box

    def set_info(self, info) -> None:
        """Shows the DatabaseInfoProto, or the manual collection entry if info is None."""
        self._labels["Updated"].setText(time.strftime("%H:%M:%S"))
        self._info = info
        self._show(info)

    def redisplay(self) -> None:
        """Shows the same information again, e.g. with other size units."""
        if self._info is not None:
            self._show(self._info)
            self._table.viewport().update()

    def take_list_key(self, event) -> bool:
        """Moves an arrow key that no other widget used to the collections table. False if there is none."""
        if not self._table.isVisible() or not self._model.rowCount():
            return False
        return take_list_key(self._table, event)

    def _show(self, info) -> None:
        legacy = info is None
        self._manual_box.setVisible(legacy)
        self._table.setVisible(not legacy)
        self._auto_refresh.setEnabled(not legacy)  # nothing changes without GetDatabaseInfo
        if legacy:
            self._auto_refresh.setChecked(False)
        for name, label in self._labels.items():
            visible = not legacy or name in _LEGACY_FIELDS
            label.setVisible(visible)
            self._captions[name].setVisible(visible)
        labels = self._labels
        if legacy:
            labels["Version"].setText("older FossilDB without GetDatabaseInfo")
            self._fill_manual_suggestions()
            return

        labels["Version"].setText(f"FossilDB {info.fossildbVersion}, RocksDB {info.rocksdbVersion}")
        started = info.startTimeMillis / 1000
        labels["Running since"].setText(f"{fmt.timestamp(started)} ({fmt.duration(time.time() - started)})")
        labels["Status"].setText(stats.server_status_html(info))
        labels["Data directory"].setText(info.dataDir)
        labels["Backup directory"].setText(info.backupDir)
        labels["Options file"].setText(info.rocksOptionsFile if info.HasField("rocksOptionsFile") else "none")
        labels["Write-ahead log"].setText(fmt.size(info.walSizeBytes))
        used = info.diskTotalBytes - info.diskUsableBytes
        labels["Disk"].setText(
            f"{fmt.size(info.diskUsableBytes)} free of {fmt.size(info.diskTotalBytes)} ({fmt.size(used)} used)"
        )
        labels["Collections"].setText(stats.collections_total(info))
        labels["Size"].setText(stats.size_total(info))

        if self._model.set_collections(info.collections):
            # Only for new rows, so that refreshing keeps the column widths the user chose
            self._table.setColumnHidden(_COLUMN_FAMILY_COLUMN, all(c.columnFamily == c.name for c in info.collections))
            self._table.resizeColumnsToContents()
            self._table.setColumnWidth(0, min(self._table.columnWidth(0), 320))
            self._table.setColumnWidth(_SIZE_COLUMN, 240)

    def _auto_refresh_toggled(self, on: bool) -> None:
        if on:
            self._auto_refresh_timer.start()
            self._auto_refresh_tick()
        else:
            self._auto_refresh_timer.stop()

    def _auto_refresh_tick(self) -> None:
        # Hidden behind a collection tab or minimized, nobody looks at the numbers
        if self.isVisible() and not self.window().isMinimized():
            self.auto_refresh_requested.emit()

    def _fill_manual_suggestions(self) -> None:
        current = self._manual_combo.currentText()
        recent = settings.recent_collections(self._spec.label)
        self._manual_combo.clear()
        self._manual_combo.addItems(recent + [c for c in settings.KNOWN_COLLECTIONS if c not in recent])
        self._manual_combo.setCurrentText(current)

    def _open_manual(self) -> None:
        name = self._manual_combo.currentText().strip()
        if name:
            self.open_collection.emit(name)

    def _collection_at(self, index):
        return self._model.collection(self._proxy.mapToSource(index).row())

    def _activated(self, index) -> None:
        if not self._table.other_button:
            self.open_collection.emit(self._collection_at(index).name)

    def _context_menu(self, position) -> None:
        index = self._table.indexAt(position)
        if not index.isValid():
            return
        collection = self._collection_at(index)
        value = index.data() or ""
        clipboard = QGuiApplication.clipboard()
        menu = QMenu(self)
        menu.addAction("Copy collection name", lambda: clipboard.setText(collection.name))
        if index.column() > 0 and value:
            menu.addAction(f"Copy “{value}”", lambda: clipboard.setText(value))
        menu.addAction("Copy info summary", lambda: clipboard.setText(stats.summary_text(collection, self._spec.title)))
        menu.addSeparator()
        menu.addAction("Open", lambda: self.open_collection.emit(collection.name))
        menu.exec(self._table.viewport().mapToGlobal(position))
