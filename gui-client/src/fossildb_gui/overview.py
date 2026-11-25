"""The first tab: information about the server and its collections."""

import html
import time
from dataclasses import dataclass
from typing import Any, Callable

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
    QProxyStyle,
    QPushButton,
    QSizePolicy,
    QStyle,
    QStyleFactory,
    QStyleOptionHeader,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from fossildb_gui import fmt, settings
from fossildb_gui.settings import ConnectionSpec
from fossildb_gui.tree import PaddedDelegate, take_list_key

_SORT_ROLE = Qt.ItemDataRole.UserRole
AUTO_REFRESH_SECONDS = 1


def _raw_bytes(c) -> int:
    return c.rawKeyBytes + c.rawValueBytes


def _compressions(c) -> str:
    return ", ".join(sorted({name for level in c.levels for name in level.compressions}))


def _ratio(c) -> float | None:
    return _raw_bytes(c) / c.liveSstFilesBytes if c.liveSstFilesBytes else None


def _deletions(c) -> int:
    return c.sstDeletions + c.memtableDeletions


def _data_age(c) -> str:
    if not c.HasField("oldestSstFileCreationTimeSeconds"):
        return ""
    oldest, newest = fmt.date(c.oldestSstFileCreationTimeSeconds), fmt.date(c.newestSstFileCreationTimeSeconds)
    return oldest if oldest == newest else f"{oldest} – {newest}"


# Deletion markers are worth a warning from this share of all entries on, since they slow down reads until a
# compaction removes them
_DELETIONS_WARNING_SHARE = 0.1


def _warnings(c) -> list[str]:
    warnings = []
    if c.estimatedPendingCompactionBytes:
        warnings.append(f"⏳ {fmt.size(c.estimatedPendingCompactionBytes)} to compact")
    elif c.compactionPending:
        warnings.append("⏳ compaction pending")
    entries = c.sstEntries + c.memtableEntries
    if entries and _deletions(c) >= _DELETIONS_WARNING_SHARE * entries:
        warnings.append(f"⚠ {fmt.compact_count(_deletions(c))} deletions")
    return warnings


def _detail_rows(c) -> list[tuple[str, str]]:
    """All statistics of a collection, for the tooltip and "Copy info summary"."""
    rows = [
        ("Column family", c.columnFamily if c.columnFamily != c.name else ""),
        ("Versions", f"{fmt.count(c.estimatedVersionCount)} (estimated)"),
        ("On disk", fmt.size(c.liveSstFilesBytes)),
        ("Uncompressed", fmt.size(_raw_bytes(c))),
        ("Average value", fmt.size(c.rawValueBytes / c.sstEntries) if c.sstEntries else ""),
        ("Data files", fmt.count(c.sstFileCount)),
        *(
            (f"Level {level.level}", f"{fmt.count(level.fileCount)} files, {fmt.size(level.sizeBytes)}, "
             f"{', '.join(level.compressions)}")
            for level in c.levels
        ),
        ("Unflushed", f"{fmt.count(c.memtableEntries)} entries in memory"),
        ("Deletions", f"{fmt.count(_deletions(c))} not yet compacted away"),
        ("Pending compaction", fmt.size(c.estimatedPendingCompactionBytes)),
        ("Index memory", fmt.size(c.tableReadersMemoryBytes)),
        ("Written", _data_age(c)),
        ("First key", c.firstKey),
        ("Last key", c.lastKey),
        ("Status", "   ".join(_warnings(c))),
    ]
    return [(name, value) for name, value in rows if value]


def _details(c) -> str:
    """The statistics as HTML tooltip."""
    cells = "".join(
        f"<tr><td style='padding-right:12px'>{name}</td><td>{html.escape(value)}</td></tr>"
        for name, value in _detail_rows(c)
    )
    return f"<b>{html.escape(c.name)}</b><table>{cells}</table>"


def summary_text(c, server: str) -> str:
    """The statistics as plain text, e.g. for pasting into a chat."""
    rows = _detail_rows(c)
    width = max(len(name) for name, _ in rows) + 2
    lines = [f"{c.name} on {server}, {time.strftime('%Y-%m-%d %H:%M')}"]
    return "\n".join(lines + [f"  {name + ':':<{width}}{value}" for name, value in rows])


@dataclass
class Column:
    title: str
    value: Callable[[Any], Any]  # for sorting
    text: Callable[[Any], str]
    tooltip: str
    numeric: bool = True


COLUMNS = [
    Column("Collection", lambda c: c.name, lambda c: c.name, "Hover over a collection for all its statistics", False),
    Column(
        "Column family",
        lambda c: c.columnFamily,
        lambda c: c.columnFamily,
        "The RocksDB column family holding this collection. Differs from the name when a RocksDB options file is used.",
        numeric=False,
    ),
    Column(
        "On disk",
        lambda c: c.liveSstFilesBytes,
        lambda c: fmt.size(c.liveSstFilesBytes),
        "Size of the data files (compressed). The bar compares it to the largest collection.",
    ),
    Column(
        "Versions",
        lambda c: c.estimatedVersionCount,
        lambda c: fmt.compact_count(c.estimatedVersionCount),
        "Estimated number of stored versions (of all keys). Deleted or overwritten entries count until a compaction "
        "removes them.",
    ),
    Column(
        "Compression",
        lambda c: _ratio(c) or 0,
        lambda c: " ".join(part for part in (f"{_ratio(c):.1f}×" if _ratio(c) else "", _compressions(c)) if part),
        "Compression ratio (uncompressed size / size on disk) and compression types of the data files. "
        "CompactAllData converts old files to the current compression.",
        numeric=False,
    ),
    Column(
        "Status",
        lambda c: " ".join(_warnings(c)),
        lambda c: "   ".join(_warnings(c)),
        "Pending compactions, and deletion markers that make up at least "
        f"{_DELETIONS_WARNING_SHARE:.0%} of the entries",
        numeric=False,
    ),
]
_SIZE_COLUMN = 2
_BAR_ROLE = Qt.ItemDataRole.UserRole + 1  # share of the largest collection, 0 to 1


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
        if same_rows:
            if collections:
                self.dataChanged.emit(self.index(0, 0), self.index(len(collections) - 1, len(COLUMNS) - 1))
        else:
            self.endResetModel()
        return not same_rows

    def collection_name(self, row: int) -> str:
        return self._collections[row].name

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
            return column.value(collection)
        if role == _BAR_ROLE:
            return collection.liveSstFilesBytes / self._largest if self._largest else 0
        if role == Qt.ItemDataRole.TextAlignmentRole and column.numeric:
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        if role == Qt.ItemDataRole.ToolTipRole:
            return _details(collection)
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation != Qt.Orientation.Horizontal:
            return None
        if role == Qt.ItemDataRole.DisplayRole:
            return COLUMNS[section].title
        if role == Qt.ItemDataRole.ToolTipRole:
            return COLUMNS[section].tooltip or None
        if role == Qt.ItemDataRole.TextAlignmentRole:
            horizontal = Qt.AlignmentFlag.AlignRight if COLUMNS[section].numeric else Qt.AlignmentFlag.AlignLeft
            return horizontal | Qt.AlignmentFlag.AlignVCenter
        return None


class _PaddedHeaderStyle(QProxyStyle):
    """Indents the header labels like the cells of PaddedDelegate."""

    def drawControl(self, element, option, painter, widget=None) -> None:
        if element == QStyle.ControlElement.CE_HeaderLabel:
            option = QStyleOptionHeader(option)
            option.rect = option.rect.adjusted(PaddedDelegate.PADDING, 0, -PaddedDelegate.PADDING, 0)
        super().drawControl(element, option, painter, widget)

    def sizeFromContents(self, contents_type, option, size, widget=None):
        size = super().sizeFromContents(contents_type, option, size, widget)
        if contents_type == QStyle.ContentsType.CT_HeaderSection:
            size.setWidth(size.width() + 2 * PaddedDelegate.PADDING)
        return size


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
        bar_space = option.rect.width() - self._TEXT_SPACE - 2 * self.PADDING
        if bar_space <= 0 or share <= 0:
            return
        color = option.palette.color(QPalette.ColorRole.Highlight)
        color.setAlphaF(0.55)
        height = max(4, option.rect.height() // 3)
        bar = QRect(option.rect.left() + self.PADDING, option.rect.center().y() - height // 2, max(2, round(bar_space * share)), height)
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(color)
        painter.drawRoundedRect(bar, 2, 2)
        painter.restore()


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
    [("Updated", False), ("Disk", False), ("Collections", False), ("Size", False)],
]
_LEGACY_FIELDS = {"Address", "Version", "Updated"}


class OverviewWidget(QWidget):
    open_collection = Signal(str)
    refresh_requested = Signal()
    auto_refresh_requested = Signal()  # only sent while the overview is visible

    def __init__(self, spec: ConnectionSpec, parent=None):
        super().__init__(parent)
        self._connection_label = spec.label
        self._server_title = spec.title
        self._info = None  # the DatabaseInfoProto shown

        refresh = QPushButton("Refresh")
        refresh.clicked.connect(self.refresh_requested)
        self._auto_refresh = QPushButton("Auto-refresh")
        self._auto_refresh.setCheckable(True)
        self._auto_refresh.setToolTip(f"Refresh every {AUTO_REFRESH_SECONDS} s while the overview is shown")
        self._auto_refresh_timer = QTimer(self, interval=AUTO_REFRESH_SECONDS * 1000)
        self._auto_refresh_timer.timeout.connect(self._auto_refresh_tick)
        self._auto_refresh.toggled.connect(self._auto_refresh_toggled)

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
                    updated.addSpacing(12)
                    updated.addStretch()
                    updated.addWidget(refresh)
                    updated.addWidget(self._auto_refresh)
                    server.addLayout(updated, row, 2 * group + 1)
                else:
                    server.addWidget(self._labels[name], row, 2 * group + 1)
            server.setColumnStretch(2 * group + 1, 1)
        # The paths get most of the room
        server.setColumnStretch(3, 3)
        self._labels["Address"].setText(spec.title)
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
        self._table = _CollectionsView()
        # The style is not owned by the header, so it is kept here
        self._header_style = _PaddedHeaderStyle(QStyleFactory.create(self._table.style().name()))
        self._table.horizontalHeader().setStyle(self._header_style)
        self._table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._table.customContextMenuRequested.connect(self._context_menu)
        self._table.setWordWrap(False)  # otherwise long keys are cut at hyphens instead of at the end
        self._table.setTabKeyNavigation(False)  # Tab moves to the next control, not to the next cell
        self._table.setModel(self._proxy)
        self._table.setSortingEnabled(True)
        self._table.sortByColumn(_SIZE_COLUMN, Qt.SortOrder.DescendingOrder)
        self._table.setShowGrid(False)
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().setDefaultSectionSize(self._table.fontMetrics().height() + 10)
        self._table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        # Activation follows the desktop setting, e.g. KDE can open items with a single click
        single_click = self._table.style().styleHint(QStyle.StyleHint.SH_ItemView_ActivateItemOnSingleClick)
        self._table.setToolTip(f"{'Click' if single_click else 'Double-click'} a collection to open it")
        self._table.verticalHeader().hide()
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setItemDelegate(PaddedDelegate(self._table))
        self._table.setItemDelegateForColumn(_SIZE_COLUMN, _SizeBarDelegate(self._table))
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
        self._labels["Updated"].setText(time.strftime("%H:%M:%S"))
        self._info = info
        self._show(info)

    def redisplay(self) -> None:
        """Shows the same information again, e.g. with other size units."""
        if self._info is not None:
            self._show(self._info)
            self._table.viewport().update()

    def _show(self, info) -> None:
        labels = self._labels
        legacy = info is None
        self._manual_box.setVisible(legacy)
        self._auto_refresh.setEnabled(not legacy)  # nothing changes without GetDatabaseInfo
        if legacy:
            self._auto_refresh.setChecked(False)
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

        if self._model.set_collections(info.collections):
            # Only for new rows, so that refreshing keeps the column widths the user chose
            differing_families = any(c.columnFamily != c.name for c in info.collections)
            self._table.setColumnHidden(1, not differing_families)
            self._table.resizeColumnsToContents()
            self._table.setColumnWidth(0, min(self._table.columnWidth(0), 320))
            self._table.setColumnWidth(_SIZE_COLUMN, 240)

        total_versions = sum(c.estimatedVersionCount for c in info.collections)
        labels["Collections"].setText(f"{len(info.collections)} with ~{fmt.compact_count(total_versions)} versions")
        on_disk = sum(c.liveSstFilesBytes for c in info.collections)
        uncompressed = sum(_raw_bytes(c) for c in info.collections)
        ratio = f" ({uncompressed / on_disk:.1f}×)" if on_disk else ""
        labels["Size"].setText(f"{fmt.size(on_disk)} on disk, {fmt.size(uncompressed)} uncompressed{ratio}")
        labels["Size"].setToolTip(
            "Data files of all collections. Recent writes that are still in memory and the write-ahead log are not included."
        )

    def take_list_key(self, event) -> bool:
        """Handles an arrow key that no other widget used: moves the focus to the list, and selects its first row if
        none is selected yet, or else moves the selection. False if there is no list."""
        if not self._table.isVisible() or not self._model.rowCount():
            return False
        return take_list_key(self._table, event)

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
        if not self._table.other_button:
            self.open_collection.emit(self._name_at(index))

    def _name_at(self, index) -> str:
        return self._model.collection_name(self._proxy.mapToSource(index).row())

    def _context_menu(self, position) -> None:
        index = self._table.indexAt(position)
        if not index.isValid():
            return
        name = self._name_at(index)
        collection = self._model.collection(self._proxy.mapToSource(index).row())
        value = index.data() or ""
        clipboard = QGuiApplication.clipboard()
        menu = QMenu(self)
        menu.addAction("Copy collection name", lambda: clipboard.setText(name))
        if index.column() > 0 and value:
            menu.addAction(f"Copy “{value}”", lambda: clipboard.setText(value))
        menu.addAction("Copy info summary", lambda: clipboard.setText(summary_text(collection, self._server_title)))
        menu.addSeparator()
        menu.addAction("Open", lambda: self.open_collection.emit(name))
        menu.exec(self._table.viewport().mapToGlobal(position))
