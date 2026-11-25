"""The first tab: information about the server and its collections."""

import time
from dataclasses import dataclass
from typing import Any, Callable

from PySide6.QtCore import QAbstractTableModel, QModelIndex, QSortFilterProxyModel, Qt, Signal
from PySide6.QtGui import QPalette
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QSizePolicy,
    QStyle,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from fossildb_gui import fmt, settings

_SORT_ROLE = Qt.ItemDataRole.UserRole


def _raw_bytes(c) -> int:
    return c.rawKeyBytes + c.rawValueBytes


def _compressions(c) -> list[str]:
    return sorted({name for level in c.levels for name in level.compressions})


def _ratio(c) -> float | None:
    return _raw_bytes(c) / c.liveSstFilesBytes if c.liveSstFilesBytes else None


def _levels(c) -> str:
    return " · ".join(f"L{level.level}: {level.fileCount}" for level in c.levels)


def _levels_tooltip(c) -> str:
    return "\n".join(
        f"Level {level.level}: {level.fileCount} files, {fmt.size(level.sizeBytes)}, {', '.join(level.compressions)}"
        for level in c.levels
    )


def _data_age(c) -> str:
    if not c.HasField("oldestSstFileCreationTimeSeconds"):
        return ""
    oldest, newest = fmt.date(c.oldestSstFileCreationTimeSeconds), fmt.date(c.newestSstFileCreationTimeSeconds)
    return oldest if oldest == newest else f"{oldest} – {newest}"


@dataclass
class Column:
    title: str
    value: Callable[[Any], Any]  # for sorting
    text: Callable[[Any], str]
    tooltip: str
    numeric: bool = True
    hidden: bool = False


COLUMNS = [
    Column("Collection", lambda c: c.name, lambda c: c.name, "", numeric=False),
    Column(
        "Column family",
        lambda c: c.columnFamily,
        lambda c: c.columnFamily,
        "The RocksDB column family holding this collection. Differs from the name when a RocksDB options file is used.",
        numeric=False,
    ),
    Column(
        "Versions",
        lambda c: c.estimatedVersionCount,
        lambda c: fmt.count(c.estimatedVersionCount),
        "Estimated number of stored versions (of all keys). Deleted or overwritten entries count until a compaction "
        "removes them.",
    ),
    Column(
        "On disk",
        lambda c: c.liveSstFilesBytes,
        lambda c: fmt.size(c.liveSstFilesBytes),
        "Size of the data files (compressed)",
    ),
    Column(
        "Uncompressed",
        _raw_bytes,
        lambda c: fmt.size(_raw_bytes(c)),
        "Size of keys and values in the data files before compression",
    ),
    Column(
        "Ratio",
        lambda c: _ratio(c) or 0,
        lambda c: f"{_ratio(c):.1f}×" if _ratio(c) else "",
        "Compression ratio: uncompressed size / size on disk",
    ),
    Column(
        "Avg. value",
        lambda c: c.rawValueBytes / c.sstEntries if c.sstEntries else 0,
        lambda c: fmt.size(c.rawValueBytes / c.sstEntries) if c.sstEntries else "",
        "Average uncompressed value size in the data files",
    ),
    Column("Files", lambda c: c.sstFileCount, lambda c: fmt.count(c.sstFileCount), "Number of data (SST) files"),
    Column("Levels", lambda c: len(c.levels), _levels, "Files per level of the LSM tree", numeric=False),
    Column(
        "Compression",
        lambda c: ", ".join(_compressions(c)),
        lambda c: ", ".join(_compressions(c)),
        "Compression types of the data files. CompactAllData converts old files to the current compression.",
        numeric=False,
    ),
    Column(
        "Unflushed",
        lambda c: c.memtableEntries,
        lambda c: fmt.count(c.memtableEntries),
        "Recent writes held in memory (memtables), not yet written to data files",
    ),
    Column(
        "Deletions",
        lambda c: c.sstDeletions + c.memtableDeletions,
        lambda c: fmt.count(c.sstDeletions + c.memtableDeletions),
        "Deletion markers that a compaction has not yet removed",
    ),
    Column(
        "Pending compaction",
        lambda c: c.estimatedPendingCompactionBytes,
        lambda c: fmt.size(c.estimatedPendingCompactionBytes),
        "Estimated bytes that compactions still have to rewrite",
    ),
    Column(
        "Index memory",
        lambda c: c.tableReadersMemoryBytes,
        lambda c: fmt.size(c.tableReadersMemoryBytes),
        "Memory used for the index and filter blocks of the data files",
    ),
    Column(
        "Written",
        lambda c: c.oldestSstFileCreationTimeSeconds,
        _data_age,
        "When the oldest and newest data files were written",
        numeric=False,
    ),
    Column("First key", lambda c: c.firstKey, lambda c: c.firstKey, "", numeric=False),
    Column("Last key", lambda c: c.lastKey, lambda c: c.lastKey, "", numeric=False),
]


class CollectionsModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._collections: list = []

    def set_collections(self, collections) -> None:
        self.beginResetModel()
        self._collections = list(collections)
        self.endResetModel()

    def collection_name(self, row: int) -> str:
        return self._collections[row].name

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
            return column.value(collection)
        if role == Qt.ItemDataRole.TextAlignmentRole and column.numeric:
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        if role == Qt.ItemDataRole.ToolTipRole and column.title == "Levels":
            return _levels_tooltip(collection)
        if role == Qt.ItemDataRole.ToolTipRole and column.title in ("First key", "Last key"):
            return column.text(collection)
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation != Qt.Orientation.Horizontal:
            return None
        if role == Qt.ItemDataRole.DisplayRole:
            return COLUMNS[section].title
        if role == Qt.ItemDataRole.ToolTipRole:
            return COLUMNS[section].tooltip or None
        return None


class _Value(QLabel):
    """A selectable value. With elide, long text (paths) is shortened in the middle and shown in full as tooltip."""

    def __init__(self, elide: bool = False):
        super().__init__()
        self._full = ""
        self._elide = elide
        self.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        if elide:
            self.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
            self.setMinimumWidth(120)

    def setText(self, text: str) -> None:
        self._full = text
        self._update_text()

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._update_text()

    def _update_text(self) -> None:
        if not self._elide:
            super().setText(self._full)
            return
        shown = self.fontMetrics().elidedText(self._full, Qt.TextElideMode.ElideMiddle, self.width())
        super().setText(shown)
        self.setToolTip(self._full if shown != self._full else "")


# The server information, as (label, elide) per grid column
_SERVER_FIELDS = [
    [("Address", False), ("Version", False), ("Running since", False), ("Status", False)],
    [("Data directory", True), ("Backup directory", True), ("Options file", True), ("Write-ahead log", False)],
    [("Updated", False), ("Disk", False), ("Collections", False)],
]
_LEGACY_FIELDS = {"Address", "Version", "Updated"}


class OverviewWidget(QWidget):
    open_collection = Signal(str)
    refresh_requested = Signal()

    def __init__(self, connection_label: str, parent=None):
        super().__init__(parent)
        self._connection_label = connection_label

        refresh = QPushButton("Refresh")
        refresh.clicked.connect(self.refresh_requested)

        # Server information, in a compact grid so that the collections table gets the space
        server = QGridLayout()
        server.setHorizontalSpacing(8)
        server.setVerticalSpacing(4)
        self._labels: dict[str, QLabel] = {}
        self._captions: dict[str, QLabel] = {}
        for group, fields in enumerate(_SERVER_FIELDS):
            for row, (name, elide) in enumerate(fields):
                caption = QLabel(f"{name}:")
                # Dimmed, so that the values stand out
                palette = caption.palette()
                palette.setColor(QPalette.ColorRole.WindowText, palette.color(QPalette.ColorRole.PlaceholderText))
                caption.setPalette(palette)
                self._captions[name] = caption
                self._labels[name] = _Value(elide)
                server.addWidget(caption, row, 2 * group)
                if name == "Updated":
                    updated = QHBoxLayout()
                    updated.addWidget(self._labels[name])
                    updated.addStretch()
                    updated.addWidget(refresh)
                    server.addLayout(updated, row, 2 * group + 1)
                else:
                    server.addWidget(self._labels[name], row, 2 * group + 1)
            server.setColumnStretch(2 * group + 1, 1)
        # The paths get most of the room
        server.setColumnStretch(3, 3)
        self._labels["Address"].setText(connection_label)
        self._labels["Status"].setTextFormat(Qt.TextFormat.RichText)

        server.setContentsMargins(0, 0, 0, 0)
        # Space between the column groups
        for column in (2, 4):
            self._captions_spacing(server, column)

        # Collections of servers with GetDatabaseInfo
        self._model = CollectionsModel(self)
        self._proxy = QSortFilterProxyModel(self)
        self._proxy.setSourceModel(self._model)
        self._proxy.setSortRole(_SORT_ROLE)
        self._table = QTableView()
        self._table.setModel(self._proxy)
        self._table.setSortingEnabled(True)
        self._table.sortByColumn(0, Qt.SortOrder.AscendingOrder)
        self._table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        # Activation follows the desktop setting, e.g. KDE can open items with a single click
        single_click = self._table.style().styleHint(QStyle.StyleHint.SH_ItemView_ActivateItemOnSingleClick)
        self._table.setToolTip(f"{'Click' if single_click else 'Double-click'} a collection to open it")
        self._table.verticalHeader().hide()
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self._table.activated.connect(self._activated)

        # Manual entry for servers without GetDatabaseInfo
        self._manual_box = QGroupBox("Open a collection")
        self._manual_combo = QComboBox()
        self._manual_combo.setEditable(True)
        self._manual_combo.setMinimumWidth(320)
        self._manual_combo.lineEdit().setPlaceholderText("collection name")
        self._manual_combo.lineEdit().returnPressed.connect(self._open_manual)
        open_button = QPushButton("Open")
        open_button.clicked.connect(self._open_manual)
        manual_layout = QVBoxLayout(self._manual_box)
        manual_layout.addWidget(
            QLabel(
                "This server is older and cannot list its collections or report statistics.\n"
                "Enter the name of the collection to open."
            )
        )
        row = QHBoxLayout()
        row.addWidget(self._manual_combo)
        row.addWidget(open_button)
        row.addStretch()
        manual_layout.addLayout(row)

        layout = QVBoxLayout(self)
        layout.addLayout(server)
        layout.addWidget(self._table, stretch=1)
        layout.addWidget(self._manual_box)
        layout.addStretch(0)

    @staticmethod
    def _captions_spacing(grid: QGridLayout, column: int) -> None:
        for row in range(grid.rowCount()):
            item = grid.itemAtPosition(row, column)
            if item is not None and isinstance(item.widget(), QLabel):
                item.widget().setContentsMargins(16, 0, 0, 0)

    def set_info(self, info) -> None:
        """Shows the DatabaseInfoProto, or the manual collection entry if info is None."""
        labels = self._labels
        labels["Updated"].setText(time.strftime("%H:%M:%S"))
        legacy = info is None
        self._manual_box.setVisible(legacy)
        self._table.setVisible(not legacy)
        for name, label in labels.items():
            visible = not legacy or name in _LEGACY_FIELDS
            label.setVisible(visible)
            self._captions[name].setVisible(visible)
        if legacy:
            labels["Version"].setText("older FossilDB without GetDatabaseInfo")
            self._fill_manual_suggestions()
            return

        labels["Version"].setText(f"FossilDB {info.fossildbVersion}, RocksDB {info.rocksdbVersion}")
        started = info.startTimeMillis / 1000
        labels["Running since"].setText(f"{fmt.timestamp(started)} ({fmt.duration(time.time() - started)})")
        labels["Data directory"].setText(info.dataDir)
        labels["Backup directory"].setText(info.backupDir)
        labels["Options file"].setText(info.rocksOptionsFile if info.HasField("rocksOptionsFile") else "none")
        used = info.diskTotalBytes - info.diskUsableBytes
        labels["Disk"].setText(
            f"{fmt.size(info.diskUsableBytes)} free of {fmt.size(info.diskTotalBytes)} ({fmt.size(used)} used)"
        )
        labels["Write-ahead log"].setText(fmt.size(info.walSizeBytes))
        labels["Status"].setText(self._status_text(info))

        self._model.set_collections(info.collections)
        differing_families = any(c.columnFamily != c.name for c in info.collections)
        self._table.setColumnHidden(1, not differing_families)
        self._table.resizeColumnsToContents()
        for column in range(len(COLUMNS)):
            self._table.setColumnWidth(column, min(self._table.columnWidth(column), 320))

        total_disk = sum(c.liveSstFilesBytes for c in info.collections)
        total_versions = sum(c.estimatedVersionCount for c in info.collections)
        labels["Collections"].setText(
            f"{len(info.collections)}, {fmt.size(total_disk)} on disk, ~{fmt.count(total_versions)} versions"
        )

    @staticmethod
    def _status_text(info) -> str:
        warnings = []
        if info.writesStopped:
            warnings.append("<b style='color:#d33'>Writes are stopped</b>")
        if info.delayedWriteRateBytesPerSecond:
            warnings.append(f"<b>Writes are slowed down</b> to {fmt.size(info.delayedWriteRateBytesPerSecond)}/s")
        if info.backgroundErrors:
            warnings.append(f"<b style='color:#d33'>{info.backgroundErrors} background errors</b>")
        activity = []
        if info.backupInProgress:
            activity.append("backup in progress")
        if info.compactionInProgress:
            activity.append("CompactAllData in progress")
        if info.runningCompactions:
            activity.append(f"{info.runningCompactions} compactions running")
        if info.runningFlushes:
            activity.append(f"{info.runningFlushes} flushes running")
        return " · ".join(warnings + activity) or "OK, idle"

    def _fill_manual_suggestions(self) -> None:
        current = self._manual_combo.currentText()
        recent = settings.recent_collections(self._connection_label)
        self._manual_combo.clear()
        self._manual_combo.addItems(recent + [c for c in settings.KNOWN_COLLECTIONS if c not in recent])
        self._manual_combo.setCurrentText(current)

    def _open_manual(self) -> None:
        name = self._manual_combo.currentText().strip()
        if name:
            self.open_collection.emit(name)

    def _activated(self, index) -> None:
        self.open_collection.emit(self._model.collection_name(self._proxy.mapToSource(index).row()))
