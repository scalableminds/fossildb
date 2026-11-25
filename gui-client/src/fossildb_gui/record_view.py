"""Shows one key: its versions, and the selected version's value as structure, typed message and hex dump."""

import json
import re
from collections import OrderedDict
from pathlib import Path

from PySide6.QtCore import QRegularExpression, QStringListModel, Qt, QTimer, Signal
from PySide6.QtGui import QFontDatabase, QGuiApplication, QPalette, QRegularExpressionValidator
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QCompleter,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListView,
    QMenu,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QStyledItemDelegate,
    QStackedWidget,
    QTabWidget,
    QToolButton,
    QTreeView,
    QVBoxLayout,
    QWidget,
)

from fossildb_gui import decoding, fmt, settings, wire
from fossildb_gui.schema_manager import SchemaManager
from fossildb_gui.tasks import Latest, TaskRunner
from fossildb_gui.tree import MAX_CHILDREN, FilterProxy, PaddedDelegate, TreeModel, root_node

HEX_DUMP_LIMIT = 256 * 1024
TEXT_LIMIT = 1024 * 1024
# Smaller JSON documents are shown fully expanded, larger ones only the first levels
JSON_EXPAND_ALL_LINES = 200
_CACHED_VALUES = 32
_NO_TYPE = "(none)"


def _sanitize_filename(name: str) -> str:
    return re.sub(r"[^-\w.]", "_", name.strip().replace(" ", "_"))


def _add_expand_buttons(layout: QHBoxLayout, view: QTreeView) -> tuple[QToolButton, QToolButton]:
    """Adds "Expand all" and "Collapse all" for the tree view."""
    expand = QToolButton(text="Expand all")
    expand.setToolTip(f"Long lists show their first {fmt.count(MAX_CHILDREN)} items")
    expand.clicked.connect(view.expandAll)
    collapse = QToolButton(text="Collapse all")
    collapse.clicked.connect(view.collapseAll)
    layout.addWidget(expand)
    layout.addWidget(collapse)
    return expand, collapse


def _add_filter(layout: QHBoxLayout, view: QTreeView) -> QLineEdit:
    """Adds a field that filters the view's rows, for a view made by _tree_view."""
    field = QLineEdit()
    field.setPlaceholderText("Filter")
    field.setToolTip("Shows the rows that contain this text, with their parents and contents")
    field.setClearButtonEnabled(True)
    field.setMaximumWidth(260)
    proxy: FilterProxy = view.model()
    # Filtering looks at the whole value, also at parts not expanded yet, which takes a moment for large ones
    timer = QTimer(field, singleShot=True, interval=250)
    field.textChanged.connect(lambda: timer.start())

    def apply() -> None:
        proxy.set_text(field.text())
        if proxy.text:
            view.expandAll()

    timer.timeout.connect(apply)
    # A new value keeps the filter, and shows its matches
    proxy.modelReset.connect(lambda: proxy.text and QTimer.singleShot(0, view.expandAll))
    layout.addWidget(field)
    return field


def _tree_view(model: TreeModel, widths: list[int]) -> QTreeView:
    view = QTreeView()
    view.setModel(FilterProxy(model, view))
    for column, width in enumerate(widths):
        view.header().resizeSection(column, width)
    view.setUniformRowHeights(True)  # much faster for long lists
    view.setAlternatingRowColors(True)
    view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

    def menu(position):
        index = view.indexAt(position)
        if not index.isValid():
            return
        text = index.data(Qt.ItemDataRole.ToolTipRole) or index.data()
        context = QMenu(view)
        context.addAction("Copy", lambda: QGuiApplication.clipboard().setText(str(text)))
        context.exec(view.viewport().mapToGlobal(position))

    view.customContextMenuRequested.connect(menu)
    return view


class _ExpandedSummaryDelegate(QStyledItemDelegate):
    """Dims the summary of expanded arrays and objects, e.g. [0, 0, 0], since their items are shown below."""

    def __init__(self, view: QTreeView):
        super().__init__(view)
        self._view = view

    def initStyleOption(self, option, index) -> None:
        super().initStyleOption(option, index)
        key_index = index.siblingAtColumn(0)
        # Qt also reports leaves as expanded after expandAll
        if index.model().hasChildren(key_index) and self._view.isExpanded(key_index):
            dimmed = option.palette.color(QPalette.ColorRole.PlaceholderText)
            option.palette.setColor(QPalette.ColorRole.Text, dimmed)


def _decode(data: bytes, message_class):
    """Runs in a background thread. Prepares everything that is shown for one value."""
    try:
        structure = (decoding.wire_nodes(data), None)
    except wire.WireError as e:
        structure = (None, f"Not a protobuf message ({e})")
    if not data:
        structure = (None, "The value is empty")
    typed = _decode_typed(data, message_class) if message_class is not None else None
    text = _as_text(data)
    json_text = _as_json(text, data) if text is not None else (None, None, None)
    return structure, typed, text, json_text, wire.hex_dump(data, HEX_DUMP_LIMIT)


def _as_text(data: bytes) -> str | None:
    """The value as text (at most TEXT_LIMIT bytes of it). None if it is not text."""
    if not data:
        return None
    text = None
    # Cutting at the limit can split a multi-byte character, so try a few bytes shorter
    for end in range(TEXT_LIMIT, TEXT_LIMIT - 4, -1) if len(data) > TEXT_LIMIT else [len(data)]:
        text = wire.as_text(memoryview(data)[:end])
        if text is not None:
            break
    return text


def _as_json(text: str, data: bytes):
    """(pretty-printed text, tree nodes, None) for JSON, or (None, None, the reason why it isn't JSON)."""
    if len(data) > TEXT_LIMIT:
        return None, None, f"The value is larger than {fmt.size(TEXT_LIMIT)}, too large to format. Download it."
    try:
        value = json.loads(text)
    except ValueError as e:
        return None, None, f"Not valid JSON: {e}"
    return json.dumps(value, indent=2, ensure_ascii=False), decoding.json_nodes(value), None


def _decode_typed(data: bytes, message_class):
    message = message_class()
    try:
        message.ParseFromString(data)
    except Exception as e:
        return None, f"Could not decode as {message_class.DESCRIPTOR.full_name}: {e}", None
    note = None
    if not message.IsInitialized():
        missing = ", ".join(message.FindInitializationErrors())
        note = f"Required fields are missing ({missing}). Is the right message type selected?"
    return decoding.message_nodes(message), note, message


def _message_json(message) -> str:
    from google.protobuf import json_format

    # Field names as in the .proto files; fields that the schema doesn't know are left out
    return json_format.MessageToJson(message, preserving_proto_field_name=True, indent=2, ensure_ascii=False)


# Wide splitter handles leave space between the panels, and all of that space can be dragged
SPLITTER_HANDLE_WIDTH = 9

# Versions are listed page by page, newest first; more are loaded while scrolling
VERSIONS_PAGE_SIZE = 1000
# Loading more versions starts when fewer than this are left below the visible ones
VERSIONS_PREFETCH_ROWS = 100

# Measuring the total size downloads all versions, in requests of about this size
MEASURE_TARGET_BYTES = 16 << 20
MEASURE_FIRST_REQUEST_VERSIONS = 16
MEASURE_MAX_REQUEST_VERSIONS = 1000


class RecordView(QWidget):
    key_deleted = Signal(str)

    def __init__(self, connection, collection: str, runner: TaskRunner, schemas: SchemaManager, window, parent=None):
        super().__init__(parent)
        self._connection = connection
        self._collection = collection
        self._runner = runner
        self._schemas = schemas
        self._window = window  # MainWindow, for writes_enabled and the status bar
        self._key: str | None = None
        self._versions: list[int] = []
        self._version: int | None = None
        self._values: OrderedDict[tuple[str, int], bytes] = OrderedDict()
        self._latest_versions = Latest()
        self._latest_value = Latest()
        self._user_chose_tab = False
        self._reselect: int | None = None  # version to select after reloading
        self._more_versions = False  # older versions exist that are not listed yet
        self._loading_versions = False
        self._sizes: dict[int, int] = {}  # measured value size per version
        self._shown_sizes: dict[int, int] = {}  # sizes in the version list, measured or of loaded values
        self._latest_measure = Latest()
        self._measuring = False

        self._title = QLabel("Select a key")
        self._title.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._title.setTextFormat(Qt.TextFormat.PlainText)  # keys may contain "<"
        self._title.setWordWrap(True)
        font = self._title.font()
        font.setBold(True)
        self._title.setFont(font)

        # Versions
        self._versions_label = QLabel()
        self._versions_model = QStringListModel(self)
        self._versions_view = QListView()
        self._versions_view.setModel(self._versions_model)
        self._versions_view.setUniformItemSizes(True)
        self._versions_view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._versions_view.selectionModel().currentChanged.connect(self._version_selected)
        versions_panel = QWidget()
        versions_layout = QVBoxLayout(versions_panel)
        versions_layout.setContentsMargins(0, 0, 0, 0)
        versions_layout.addWidget(self._versions_label)
        versions_layout.addWidget(self._versions_view)
        self._go_to_version = QLineEdit()
        self._go_to_version.setPlaceholderText("Go to version (Enter)")
        self._go_to_version.setValidator(QRegularExpressionValidator(QRegularExpression(r"\d{1,19}"), self))
        self._go_to_version.setClearButtonEnabled(True)
        self._go_to_version.returnPressed.connect(self._go_to_version_entered)
        versions_layout.addWidget(self._go_to_version)
        scroll_bar = self._versions_view.verticalScrollBar()
        scroll_bar.valueChanged.connect(self._prefetch_versions)
        scroll_bar.rangeChanged.connect(self._prefetch_versions)
        self._measure_button = QPushButton("Measure all sizes")
        self._measure_button.setToolTip(
            "Adds up the sizes of all versions of this key. FossilDB can't report sizes, so this downloads every "
            "version.\nThe size on disk is smaller, because the data files are compressed."
        )
        self._measure_button.clicked.connect(self._toggle_measuring)
        self._measure_label = QLabel()
        self._measure_label.setWordWrap(True)
        self._measure_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        versions_layout.addWidget(self._measure_button)
        versions_layout.addWidget(self._measure_label)

        # Value
        self._value_label = QLabel()
        self._download_button = QPushButton("Download…")
        self._download_button.clicked.connect(self._download)
        self._delete_version_button = QPushButton("Delete version…")
        self._delete_version_button.clicked.connect(self._delete_version)
        self._delete_key_button = QPushButton("Delete key…")
        self._delete_key_button.setToolTip("Delete all versions of this key")
        self._delete_key_button.clicked.connect(self._delete_key)
        actions = QHBoxLayout()
        actions.addWidget(self._value_label, stretch=1)
        actions.addWidget(self._download_button)
        actions.addWidget(self._delete_version_button)
        actions.addWidget(self._delete_key_button)

        self._structure_model = TreeModel(decoding.WIRE_HEADERS, parent=self, right_aligned=(decoding.SIZE_COLUMN,))
        self._structure_view = _tree_view(self._structure_model, [180, 120, 80, 280])
        self._structure_view.setItemDelegateForColumn(decoding.SIZE_COLUMN, PaddedDelegate(self._structure_view))
        self._structure_note = QLabel()
        self._structure_note.setWordWrap(True)
        structure_tab = QWidget()
        structure_layout = QVBoxLayout(structure_tab)
        structure_bar = QHBoxLayout()
        self._structure_expand_button, self._structure_collapse_button = _add_expand_buttons(
            structure_bar, self._structure_view
        )
        structure_bar.addStretch()
        self._structure_filter = _add_filter(structure_bar, self._structure_view)
        structure_layout.addLayout(structure_bar)
        structure_layout.addWidget(self._structure_note)
        structure_layout.addWidget(self._structure_view)

        self._type_combo = QComboBox()
        self._type_combo.setEditable(True)
        self._type_combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self._type_combo.setMinimumContentsLength(30)
        self._type_combo.activated.connect(self._type_chosen)
        self._typed_model = TreeModel(decoding.TYPED_HEADERS, parent=self, right_aligned=(decoding.SIZE_COLUMN,))
        self._typed_view = _tree_view(self._typed_model, [240, 200, 80])
        self._typed_view.setItemDelegateForColumn(decoding.SIZE_COLUMN, PaddedDelegate(self._typed_view))
        self._typed_note = QLabel()
        self._typed_note.setWordWrap(True)
        typed_tab = QWidget()
        typed_layout = QVBoxLayout(typed_tab)
        type_row = QHBoxLayout()
        type_row.addWidget(QLabel(f"Message type for {collection}:"))
        type_row.addWidget(self._type_combo, stretch=1)
        self._guess_button = QToolButton(text="Guess")
        self._guess_button.setToolTip("Find the message type that knows all fields of this value")
        self._guess_button.clicked.connect(self._guess_type)
        type_row.addWidget(self._guess_button)
        typed_bar = QHBoxLayout()
        _add_expand_buttons(typed_bar, self._typed_view)
        self._typed_message = None
        self._json_menu_button = QToolButton(text="JSON")
        self._json_menu_button.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self._json_menu_button.setToolTip("The value as JSON, with the field names of the .proto files")
        json_menu = QMenu(self._json_menu_button)
        json_menu.addAction("Copy as JSON", self._copy_json)
        json_menu.addAction("Save as JSON…", self._save_json)
        self._json_menu_button.setMenu(json_menu)
        self._json_menu_button.setEnabled(False)
        typed_bar.addWidget(self._json_menu_button)
        typed_bar.addStretch()
        self._typed_filter = _add_filter(typed_bar, self._typed_view)
        typed_layout.addLayout(type_row)
        typed_layout.addLayout(typed_bar)
        typed_layout.addWidget(self._typed_note)
        typed_layout.addWidget(self._typed_view)

        self._text = QPlainTextEdit()
        self._text.setReadOnly(True)
        self._text.setFont(QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        self._text_note = QLabel()
        text_tab = QWidget()
        text_layout = QVBoxLayout(text_tab)
        text_layout.addWidget(self._text_note)
        text_layout.addWidget(self._text)

        self._json = QPlainTextEdit()
        self._json.setReadOnly(True)
        self._json.setFont(QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        self._json_model = TreeModel(decoding.JSON_HEADERS, parent=self)
        self._json_tree = _tree_view(self._json_model, [240])
        self._json_tree.setItemDelegateForColumn(1, _ExpandedSummaryDelegate(self._json_tree))
        self._json_stack = QStackedWidget()
        self._json_stack.addWidget(self._json_tree)
        self._json_stack.addWidget(self._json)
        self._json_note = QLabel()
        self._json_note.setWordWrap(True)
        json_bar = QHBoxLayout()
        self._json_tree_button = QToolButton(text="Tree", checkable=True, checked=True, autoExclusive=True)
        self._json_raw_button = QToolButton(text="Raw", checkable=True, autoExclusive=True)
        self._json_tree_button.toggled.connect(self._json_mode_changed)
        for widget in (self._json_tree_button, self._json_raw_button):
            json_bar.addWidget(widget)
        json_bar.addSpacing(12)
        self._json_expand_button, self._json_collapse_button = _add_expand_buttons(json_bar, self._json_tree)
        json_bar.addStretch()
        self._json_filter = _add_filter(json_bar, self._json_tree)
        json_tab = QWidget()
        json_layout = QVBoxLayout(json_tab)
        json_layout.addLayout(json_bar)
        json_layout.addWidget(self._json_note)
        json_layout.addWidget(self._json_stack)

        self._hex = QPlainTextEdit()
        self._hex.setReadOnly(True)
        self._hex.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._hex.setFont(QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        self._hex_note = QLabel()

        hex_tab = QWidget()
        hex_layout = QVBoxLayout(hex_tab)
        hex_layout.addWidget(self._hex_note)
        hex_layout.addWidget(self._hex)

        self._tabs = QTabWidget()
        self._structure_tab = self._tabs.addTab(structure_tab, "Protobuf (raw)")
        self._typed_tab = self._tabs.addTab(typed_tab, "Protobuf (typed)")
        self._text_tab = self._tabs.addTab(text_tab, "Text")
        self._json_tab = self._tabs.addTab(json_tab, "JSON")
        self._hex_tab = self._tabs.addTab(hex_tab, "Hex")
        self._tabs.tabBarClicked.connect(lambda _: setattr(self, "_user_chose_tab", True))

        value_panel = QWidget()
        value_layout = QVBoxLayout(value_panel)
        value_layout.setContentsMargins(0, 0, 0, 0)
        value_layout.addLayout(actions)
        value_layout.addWidget(self._tabs)

        splitter = QSplitter()
        splitter.addWidget(versions_panel)
        splitter.addWidget(value_panel)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([160, 800])
        splitter.setHandleWidth(SPLITTER_HANDLE_WIDTH)
        settings.remember_splitter(splitter, "versions")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._title)
        layout.addWidget(splitter, stretch=1)

        schemas.changed.connect(self._schemas_changed)
        self._fill_type_combo()
        self.show_key(None)

    # Writes

    def update_write_actions(self) -> None:
        writable = self._window.writes_enabled
        tip = "" if writable else " (enable writes in the Database menu first)"
        has_version = self._version is not None
        self._delete_version_button.setEnabled(writable and has_version)
        self._delete_key_button.setEnabled(writable and self._key is not None and bool(self._versions))
        self._delete_version_button.setToolTip("Delete the selected version" + tip)
        self._delete_key_button.setToolTip("Delete all versions of this key" + tip)

    # Keys and versions

    def show_key(self, key: str | None) -> None:
        self._key = key
        self._versions = []
        self._version = None
        self._versions_model.setStringList([])
        self._clear_value()
        self._latest_measure.invalidate()
        self._set_measuring(False)
        self._sizes = {}
        self._shown_sizes = {}
        self._measure_label.setText("")
        self._measure_button.setEnabled(False)
        generation = self._latest_versions.next()
        self._latest_value.invalidate()
        self.update_write_actions()
        self._download_button.setEnabled(False)
        if key is None:
            self._title.setText("Select a key")
            self._versions_label.setText("")
            return
        created = fmt.object_id_date(key)
        self._title.setText(f"{self._collection} / {key}" + (f"  (ObjectId created {created})" if created else ""))
        self._versions_label.setText("Loading versions…")
        self._more_versions = False
        self._loading_versions = True
        self._runner.submit(
            # One more than a page, to know whether there are more
            lambda: self._connection.list_versions(self._collection, key, VERSIONS_PAGE_SIZE + 1),
            self._latest_versions.wrap(generation, self._versions_loaded),
            self._latest_versions.wrap(generation, self._versions_failed),
            context=self,
        )

    def reload(self) -> None:
        reselect = self._version
        self._values.clear()
        self.show_key(self._key)
        self._reselect = reselect

    def _versions_loaded(self, versions: list[int]) -> None:
        self._loading_versions = False
        self._more_versions = len(versions) > VERSIONS_PAGE_SIZE
        versions = versions[:VERSIONS_PAGE_SIZE]
        self._versions = versions
        self._versions_model.setStringList([str(v) for v in versions])
        self._update_versions_label()
        # Without a scroll bar (all fit), no scrolling would ever load more, so check once the list is laid out
        QTimer.singleShot(0, self._prefetch_versions)
        self._measure_button.setEnabled(bool(versions))
        self.update_write_actions()
        if not versions:
            self._value_label.setText("This key has no versions (it does not exist).")
            return
        row = versions.index(self._reselect) if self._reselect in versions else 0
        self._reselect = None
        self._versions_view.setCurrentIndex(self._versions_model.index(row))

    def _update_versions_label(self) -> None:
        count = len(self._versions)
        if self._more_versions:
            self._versions_label.setText(f"{fmt.count(count)}+ versions")
            self._versions_label.setToolTip("Scroll down for older ones")
        else:
            self._versions_label.setText(f"{fmt.count(count)} versions" if count != 1 else "1 version")
            self._versions_label.setToolTip("")

    def _prefetch_versions(self) -> None:
        if not self._more_versions or self._loading_versions:
            return
        view = self._versions_view
        last_visible = view.indexAt(view.viewport().rect().bottomLeft()).row()
        if last_visible == -1 or len(self._versions) - 1 - last_visible < VERSIONS_PREFETCH_ROWS:
            self._load_more_versions()

    def _load_more_versions(self) -> None:
        self._loading_versions = True
        key, offset = self._key, len(self._versions)
        generation = self._latest_versions.current()
        self._runner.submit(
            lambda: self._connection.list_versions(self._collection, key, VERSIONS_PAGE_SIZE + 1, offset),
            self._latest_versions.wrap(generation, self._more_versions_loaded),
            self._latest_versions.wrap(generation, self._versions_failed),
            context=self,
        )

    def _more_versions_loaded(self, versions: list[int]) -> None:
        self._loading_versions = False
        self._more_versions = len(versions) > VERSIONS_PAGE_SIZE
        self._append_versions(versions[:VERSIONS_PAGE_SIZE])
        self._update_versions_label()
        QTimer.singleShot(0, self._prefetch_versions)  # e.g. scrolled to the end meanwhile, or all fit

    def _append_versions(self, versions: list[int]) -> None:
        # New versions may have been written meanwhile, which shifts the pages
        known = set(self._versions)
        versions = [v for v in versions if v not in known]
        if not versions:
            return
        start = len(self._versions)
        self._versions.extend(versions)
        model = self._versions_model
        model.insertRows(start, len(versions))
        for row, version in enumerate(versions, start):
            model.setData(model.index(row), str(version))

    def _go_to_version_entered(self) -> None:
        text = self._go_to_version.text()
        if not text or self._key is None:
            return
        version = int(text)
        if version in self._versions:
            self._versions_view.setCurrentIndex(self._versions_model.index(self._versions.index(version)))
            self._versions_view.scrollTo(self._versions_view.currentIndex())
            return
        # Not listed (yet): load it directly
        self._versions_view.selectionModel().clearCurrentIndex()
        self._versions_view.clearSelection()
        self._version = version
        self.update_write_actions()
        self._load_value()

    def _versions_failed(self, error: Exception) -> None:
        self._loading_versions = False
        self._versions_label.setText("Could not load versions")
        self._value_label.setText(str(error))

    def _version_selected(self, current, _previous) -> None:
        if not current.isValid():
            return
        self._version = self._versions[current.row()]
        self.update_write_actions()
        self._load_value()

    # Total size

    def _toggle_measuring(self) -> None:
        if self._measuring:
            self._latest_measure.invalidate()
            self._set_measuring(False)
            self._measure_label.setText(f"Stopped. {self._measured_text()}")
            return
        self._sizes = {}
        self._set_measuring(True)
        generation = self._latest_measure.next()
        if not self._more_versions:
            self._measure_label.setText("Measuring…")
            self._measure_from(generation, 0, MEASURE_FIRST_REQUEST_VERSIONS)
            return
        # All versions are measured, so the rest of the list is needed first
        self._measure_label.setText("Listing all versions…")
        key = self._key

        def listed(versions: list[int]) -> None:
            self._more_versions = False
            self._append_versions(versions)
            self._update_versions_label()
            self._measure_label.setText("Measuring…")
            self._measure_from(generation, 0, MEASURE_FIRST_REQUEST_VERSIONS)

        self._runner.submit(
            lambda: self._connection.list_versions(self._collection, key),
            self._latest_measure.wrap(generation, listed),
            self._latest_measure.wrap(generation, self._measuring_failed),
            context=self,
        )

    def _set_measuring(self, measuring: bool) -> None:
        self._measuring = measuring
        self._measure_button.setText("Stop measuring" if measuring else "Measure all sizes")

    def _measure_from(self, generation: int, start: int, count: int) -> None:
        # Versions are sorted newest first, so this requests the range from batch[0] down to batch[-1]
        batch = self._versions[start : start + count]
        key = self._key
        self._runner.submit(
            lambda: self._connection.version_sizes(self._collection, key, batch[0], batch[-1]),
            self._latest_measure.wrap(generation, lambda sizes: self._measured(generation, start + len(batch), sizes)),
            self._latest_measure.wrap(generation, self._measuring_failed),
            context=self,
        )

    def _measured(self, generation: int, next_start: int, sizes: list[tuple[int, int]]) -> None:
        for version, size in sizes:
            self._sizes[version] = size
        self._show_version_sizes(sizes)
        if next_start >= len(self._versions):
            self._set_measuring(False)
            self._measure_label.setText(self._measured_text())
            return
        self._measure_label.setText(f"Measuring… {self._measured_text()}")
        # Aim for requests of about MEASURE_TARGET_BYTES, based on the sizes so far
        average = sum(size for _, size in sizes) / len(sizes) if sizes else 0
        count = int(MEASURE_TARGET_BYTES / average) if average else MEASURE_MAX_REQUEST_VERSIONS
        self._measure_from(generation, next_start, max(1, min(count, MEASURE_MAX_REQUEST_VERSIONS)))

    def _show_version_sizes(self, sizes: list[tuple[int, int]]) -> None:
        """Adds the sizes to the version list, e.g. "7  ·  1.2 kB"."""
        rows = {version: row for row, version in enumerate(self._versions)}
        for version, size in sizes:
            self._shown_sizes[version] = size
            if version in rows:
                self._versions_model.setData(self._versions_model.index(rows[version]), f"{version}  ·  {fmt.size(size)}")

    @property
    def versions_view(self) -> QListView:
        return self._versions_view

    def focus_versions(self) -> bool:
        """Moves the keyboard focus to the version list, keeping the selected version. False if there are none yet."""
        if not self._versions:
            return False
        if not self._versions_view.currentIndex().isValid():
            self._versions_view.setCurrentIndex(self._versions_model.index(0))
        self._versions_view.setFocus()
        return True

    def redisplay(self) -> None:
        """Shows everything again, e.g. with other size units."""
        self._show_version_sizes(list(self._shown_sizes.items()))
        label = self._measure_label.text()
        if not self._measuring and self._sizes and not label.startswith("Measuring failed"):
            self._measure_label.setText(("Stopped. " if label.startswith("Stopped") else "") + self._measured_text())
        if self._version is not None:
            self._load_value()  # from the cache; decoding again also updates the sizes in the trees

    def _measured_text(self) -> str:
        total = sum(self._sizes.values())
        measured = len(self._sizes)
        if measured < len(self._versions):
            return f"{fmt.size(total)} in the first {fmt.count(measured)} of {fmt.count(len(self._versions))} versions"
        if not measured:
            return "No versions"
        largest = max(self._sizes, key=self._sizes.get)
        return (
            f"<b>Total: {fmt.size(total)}</b> in {fmt.count(measured)} version{'s' if measured != 1 else ''}"
            f"<br>Average {fmt.size(total / measured)}, largest {fmt.size(self._sizes[largest])} (version {largest})"
        )

    def _measuring_failed(self, error: Exception) -> None:
        self._set_measuring(False)
        self._measure_label.setText(f"Measuring failed: {error}")

    # Values

    def _clear_value(self) -> None:
        self._value_label.setText("")
        self._structure_model.set_root(root_node([]))
        self._typed_model.set_root(root_node([]))
        self._structure_note.setText("")
        self._typed_note.setText("")
        self._text.setPlainText("")
        self._text_note.setText("")
        self._json.setPlainText("")
        self._json_model.set_root(root_node([]))
        self._json_note.setText("")
        self._hex.setPlainText("")
        self._hex_note.setText("")

    def _load_value(self) -> None:
        key, version = self._key, self._version
        generation = self._latest_value.next()
        self._value_label.setText(f"Version {version} · loading…")
        self._download_button.setEnabled(False)
        message_class = self._message_class()

        def load():
            cached = self._values.get((key, version))
            if cached is not None:
                data, actual = cached, version
            else:
                data, actual = self._connection.get_version(self._collection, key, version)
            return actual, data, _decode(data, message_class)

        self._runner.submit(
            load,
            self._latest_value.wrap(generation, lambda result: self._value_loaded(key, version, *result)),
            self._latest_value.wrap(generation, lambda e: self._value_label.setText(f"Version {version}: {e}")),
            context=self,
        )

    def _value_loaded(self, key: str, requested: int, version: int, data: bytes, decoded) -> None:
        # From "Go to version", a version that doesn't exist gives the next older one
        self._version = version
        self.update_write_actions()
        self._values[(key, version)] = data
        self._values.move_to_end((key, version))
        while len(self._values) > _CACHED_VALUES:
            self._values.popitem(last=False)
        self._download_button.setEnabled(True)
        self._show_version_sizes([(version, len(data))])
        size = f"{fmt.size(len(data))} ({fmt.count(len(data))} byte{'s' if len(data) != 1 else ''})"
        older = f" (there is no version {requested}, this is the next older one)" if version != requested else ""
        self._value_label.setText(f"Version {version} · {size}{older}")
        if version in self._versions and not self._versions_view.currentIndex().isValid():
            self._versions_view.selectionModel().blockSignals(True)
            self._versions_view.setCurrentIndex(self._versions_model.index(self._versions.index(version)))
            self._versions_view.selectionModel().blockSignals(False)

        (structure, structure_note), typed, text, (json_text, json_nodes, json_note), hex_text = decoded
        self._structure_model.set_root(root_node(structure or []))
        self._structure_note.setText(structure_note or "")
        self._structure_note.setVisible(bool(structure_note))
        self._show_typed(typed)
        self._text.setPlainText(text or "")
        is_protobuf = structure is not None
        for tab in (self._structure_tab, self._typed_tab):
            self._tabs.setTabEnabled(tab, is_protobuf)
            self._tabs.setTabToolTip(tab, "" if is_protobuf else structure_note)
        is_text = text is not None
        for tab in (self._text_tab, self._json_tab):
            self._tabs.setTabEnabled(tab, is_text)
            self._tabs.setTabToolTip(tab, "" if is_text else "The value is not text")
        if is_text and len(data) > TEXT_LIMIT:
            self._text_note.setText(f"Showing the first {fmt.size(TEXT_LIMIT)}. Download the value to see all of it.")
        else:
            self._text_note.setText("")
        self._text_note.setVisible(bool(self._text_note.text()))
        self._json.setPlainText(json_text or "")
        self._json_model.set_root(root_node(json_nodes or []))
        if json_text is not None and json_text.count("\n") < JSON_EXPAND_ALL_LINES:
            self._json_tree.expandAll()
        else:
            self._json_tree.expandToDepth(0)
        self._json_note.setText(json_note or "")
        self._json_note.setVisible(bool(json_note))
        self._hex.setPlainText(hex_text)
        truncated = len(data) > HEX_DUMP_LIMIT
        self._hex_note.setText(
            f"Showing the first {fmt.size(HEX_DUMP_LIMIT)}. Download the value to see all of it." if truncated else ""
        )
        self._hex_note.setVisible(truncated)

        if not self._user_chose_tab:
            if is_protobuf and typed is not None and typed[0] is not None:
                self._tabs.setCurrentIndex(self._typed_tab)
            elif json_text is not None:
                self._tabs.setCurrentIndex(self._json_tab)
            elif text is not None:
                self._tabs.setCurrentIndex(self._text_tab)
            elif structure:
                self._tabs.setCurrentIndex(self._structure_tab)
            else:
                self._tabs.setCurrentIndex(self._hex_tab)

    def _json_mode_changed(self) -> None:
        tree = self._json_tree_button.isChecked()
        self._json_stack.setCurrentWidget(self._json_tree if tree else self._json)
        self._json_expand_button.setEnabled(tree)
        self._json_collapse_button.setEnabled(tree)
        self._json_filter.setEnabled(tree)

    def _show_typed(self, typed) -> None:
        self._typed_message = typed[2] if typed is not None else None
        self._json_menu_button.setEnabled(self._typed_message is not None)
        if typed is None:
            self._typed_model.set_root(root_node([]))
            self._typed_note.setText(self._typed_hint())
            self._typed_note.setVisible(True)
            return
        nodes, note, _ = typed
        self._typed_model.set_root(root_node(nodes or []))
        self._typed_note.setText(note or "")
        self._typed_note.setVisible(bool(note))

    # Message types

    def _typed_hint(self) -> str:
        if self._schemas.loading:
            return "Loading schemas…"
        if self._schemas.error:
            return f"Loading schemas failed: {self._schemas.error}"
        if self._schemas.schemas is None:
            return "To decode values with their message types, load a folder of .proto files (Schemas menu)."
        return f"Choose the message type of the values in {self._collection}."

    def _message_class(self):
        name = settings.message_type_for(self._collection)
        if not name or self._schemas.schemas is None:
            return None
        try:
            return self._schemas.schemas.message_class(name)
        except Exception:
            return None

    def _fill_type_combo(self) -> None:
        schemas = self._schemas.schemas
        names = schemas.message_names if schemas is not None else []
        self._type_combo.blockSignals(True)
        self._type_combo.clear()
        self._type_combo.addItems([_NO_TYPE] + names)
        completer = QCompleter(names, self._type_combo)
        completer.setFilterMode(Qt.MatchFlag.MatchContains)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self._type_combo.setCompleter(completer)
        chosen = settings.message_type_for(self._collection)
        self._type_combo.setCurrentText(chosen if chosen in names else _NO_TYPE)
        self._type_combo.setEnabled(bool(names))
        self._type_combo.blockSignals(False)
        if not names:
            self._typed_note.setText(self._typed_hint())

    def _guess_type(self) -> None:
        schemas = self._schemas.schemas
        data = self._values.get((self._key, self._version))
        if schemas is None or data is None:
            self._window.statusBar().showMessage("Load .proto files and select a version first", 8000)
            return
        self._guess_button.setEnabled(False)
        self._window.statusBar().showMessage(f"Trying {fmt.count(len(schemas.message_names))} message types…")
        key, version = self._key, self._version

        def done(guesses) -> None:
            self._guess_button.setEnabled(True)
            if (key, version) != (self._key, self._version):
                return
            self._window.statusBar().showMessage(self._guess_result(guesses), 20000)

        def failed(error: Exception) -> None:
            self._guess_button.setEnabled(True)
            self._window.statusBar().showMessage(f"Guessing failed: {error}", 20000)

        self._runner.submit(lambda: decoding.guess_message_types(data, schemas), done, failed, context=self)

    def _guess_result(self, guesses) -> str:
        """Chooses the best fitting type, and says why."""
        fitting = [g for g in guesses if g.fits]
        if not fitting:
            if not guesses:
                return "No message type can parse this value"
            closest = guesses[0]
            return f"No message type knows all fields of this value. Closest: {closest.name} ({fmt.count(closest.unknown)} unknown)"
        best = fitting[0]
        self._type_combo.setCurrentText(best.name)
        self._type_chosen(0)
        others = [g.name for g in fitting[1:4] if g.known == best.known]
        also = f". Equally possible: {', '.join(others)}" if others else ""
        return f"Guessed {best.name}: knows all {fmt.count(best.known)} fields{also}"

    def _type_chosen(self, _index: int) -> None:
        name = self._type_combo.currentText()
        settings.set_message_type_for(self._collection, "" if name == _NO_TYPE else name)
        self._user_chose_tab = False
        if self._version is not None:
            self._load_value()

    def _schemas_changed(self) -> None:
        self._fill_type_combo()
        if self._version is not None:
            self._load_value()

    # Actions

    def _copy_json(self) -> None:
        self._with_json(lambda text: (
            QGuiApplication.clipboard().setText(text),
            self._window.statusBar().showMessage(f"Copied {fmt.size(len(text.encode()))} of JSON", 8000),
        ))

    def _save_json(self) -> None:
        name = _sanitize_filename(f"{self._collection}_{self._key}_{self._version}") + ".json"
        path, _ = QFileDialog.getSaveFileName(self, "Save as JSON", str(Path.home() / name), "JSON (*.json)")
        if path:
            self._with_json(lambda text: (
                Path(path).write_text(text, encoding="utf-8"),
                self._window.statusBar().showMessage(f"Saved JSON to {path}", 8000),
            ))

    def _with_json(self, use) -> None:
        """Converts the typed value to JSON in the background, which can take a moment for large ones."""
        message = self._typed_message
        if message is not None:
            self._runner.submit(
                lambda: _message_json(message),
                use,
                lambda e: self._window.statusBar().showMessage(f"Converting to JSON failed: {e}", 10000),
                context=self,
            )

    def _download(self) -> None:
        data = self._values.get((self._key, self._version))
        if data is None:
            return
        name = _sanitize_filename(f"{self._collection}_{self._key}_{self._version}") + ".bin"
        path, _ = QFileDialog.getSaveFileName(self, "Save value", str(Path.home() / name))
        if path:
            Path(path).write_bytes(data)
            self._window.statusBar().showMessage(f"Saved {fmt.size(len(data))} to {path}", 8000)

    def _delete_version(self) -> None:
        key, version = self._key, self._version
        if not self._window.confirm_write(
            "Delete version",
            f"Delete version {version} of key\n\n{key}\n\nin {self._collection}? This cannot be undone.",
        ):
            return
        self._run_delete(
            lambda: self._connection.delete_version(self._collection, key, version),
            f"Deleted version {version} of {key}",
        )

    def _delete_key(self) -> None:
        key = self._key
        if not self._window.confirm_write(
            "Delete key",
            f"Delete all {fmt.count(len(self._versions))} versions of key\n\n{key}\n\nin {self._collection}? "
            "This cannot be undone.",
        ):
            return
        self._run_delete(lambda: self._connection.delete_key(self._collection, key), f"Deleted all versions of {key}")

    def _run_delete(self, delete, message: str) -> None:
        key = self._key

        def done(_):
            self._window.statusBar().showMessage(message, 8000)
            self._values = OrderedDict((k, v) for k, v in self._values.items() if k[0] != key)
            self._runner.submit(
                lambda: self._connection.list_versions(self._collection, key),
                lambda versions: self.key_deleted.emit(key) if not versions else self.show_key(key),
                context=self,
            )

        self._runner.submit(delete, done, lambda e: QMessageBox.critical(self, "Delete failed", str(e)), context=self)
