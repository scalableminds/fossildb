"""Reusable widgets and view helpers."""

from PySide6.QtCore import QPoint, Qt, QTimer
from PySide6.QtGui import QGuiApplication, QKeyEvent
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QProxyStyle,
    QSizePolicy,
    QSplitter,
    QStyle,
    QStyledItemDelegate,
    QStyleFactory,
    QStyleOptionHeader,
    QStyleOptionViewItem,
    QToolButton,
    QTreeView,
    QVBoxLayout,
    QWidget,
)

from fossildb_gui import fmt, settings
from fossildb_gui.tree import MAX_CHILDREN, FilterProxy, Node, TreeModel, root_node

PADDING = 8
# Wide splitter handles leave space between the panels, and all of that space can be dragged
SPLITTER_HANDLE_WIDTH = 9


class PaddedDelegate(QStyledItemDelegate):
    """Keeps text away from the cell borders, so right- and left-aligned neighbors don't touch."""

    def paint(self, painter, option, index) -> None:
        # Background and selection across the whole cell, then the content within the padding
        background = QStyleOptionViewItem(option)
        self.initStyleOption(background, index)
        background.text = ""
        style = option.widget.style() if option.widget else QApplication.style()
        style.drawPrimitive(QStyle.PrimitiveElement.PE_PanelItemViewItem, background, painter, option.widget)
        content = QStyleOptionViewItem(option)
        content.rect = option.rect.adjusted(PADDING, 0, -PADDING, 0)
        super().paint(painter, content, index)

    def sizeHint(self, option, index):
        size = super().sizeHint(option, index)
        size.setWidth(size.width() + 2 * PADDING)
        return size


class _PaddedHeaderStyle(QProxyStyle):
    def drawControl(self, element, option, painter, widget=None) -> None:
        if element == QStyle.ControlElement.CE_HeaderLabel:
            option = QStyleOptionHeader(option)
            option.rect = option.rect.adjusted(PADDING, 0, -PADDING, 0)
        super().drawControl(element, option, painter, widget)

    def sizeFromContents(self, contents_type, option, size, widget=None):
        size = super().sizeFromContents(contents_type, option, size, widget)
        if contents_type == QStyle.ContentsType.CT_HeaderSection:
            size.setWidth(size.width() + 2 * PADDING)
        return size


def pad_header(view: QAbstractItemView) -> None:
    """Indents the header labels of a table like the cells of PaddedDelegate."""
    header = view.horizontalHeader()
    style = _PaddedHeaderStyle(QStyleFactory.create(view.style().name()))
    style.setParent(header)  # the header doesn't own its style
    header.setStyle(style)


class ElidedLabel(QLabel):
    """A selectable text that is shortened in the middle when it doesn't fit, e.g. a path, with all of it as tooltip."""

    def __init__(self):
        super().__init__()
        self._full = ""
        self.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        self.setMinimumWidth(120)

    def setText(self, text: str) -> None:
        self._full = text
        self._update_text()

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._update_text()

    def _update_text(self) -> None:
        shown = self.fontMetrics().elidedText(self._full, Qt.TextElideMode.ElideMiddle, self.width())
        super().setText(shown)
        self.setToolTip(self._full if shown != self._full else "")


def take_list_key(view: QAbstractItemView, event) -> bool:
    """Gives the view the focus and an arrow key that was pressed elsewhere. Without a selection, the first row is
    selected instead, since Qt would otherwise move on from an invisible current row.

    False if the view has the focus already: then it didn't use the key itself (e.g. Up in the first row), and
    sending it again would make it come back here, endlessly."""
    if view.hasFocus():
        return False
    view.setFocus()
    if view.selectionModel().hasSelection():
        QApplication.sendEvent(view, QKeyEvent(event.type(), event.key(), event.modifiers(), event.text()))
    else:
        view.setCurrentIndex(view.model().index(0, 0))
    return True


def fetch_early(view: QAbstractItemView, rows: int) -> None:
    """Loads the next page of the view's model (canFetchMore/fetchMore) once fewer than rows are left below the
    visible ones. Qt itself only does that when scrolled to the very end, or while the rows don't fill the view."""

    def check() -> None:
        model = view.model()
        last_visible = view.indexAt(QPoint(0, view.viewport().height() - 1)).row()
        if last_visible != -1 and model.rowCount() - 1 - last_visible < rows and model.canFetchMore(view.rootIndex()):
            model.fetchMore(view.rootIndex())

    scroll_bar = view.verticalScrollBar()
    scroll_bar.valueChanged.connect(check)
    scroll_bar.rangeChanged.connect(check)


def splitter(left: QWidget, right: QWidget, left_width: int, name: str) -> QSplitter:
    """Two panels side by side; the right one gets extra room. The sizes the user chooses are remembered by name."""
    result = QSplitter()
    result.addWidget(left)
    result.addWidget(right)
    result.setStretchFactor(1, 1)
    result.setSizes([left_width, 4 * left_width])
    result.setHandleWidth(SPLITTER_HANDLE_WIDTH)
    settings.remember_splitter(result, name)
    return result


def dimmed(label: QLabel) -> QLabel:
    """Shows the label in the color of placeholder text, e.g. captions next to values."""
    palette = label.palette()
    palette.setColor(label.foregroundRole(), palette.color(palette.ColorRole.PlaceholderText))
    label.setPalette(palette)
    return label


class TreeTab(QWidget):
    """A tree of Nodes, with "Expand all", "Collapse all", a filter and a note above it.

    Further controls can be added to the bar, or as rows above it (layout().insertLayout(0, …)).
    """

    def __init__(self, headers: list[str], widths: list[int], size_column: int | None = None, parent=None):
        super().__init__(parent)
        self.model = TreeModel(headers, parent=self, right_aligned=() if size_column is None else (size_column,))
        self.view = QTreeView()
        self.view.setModel(FilterProxy(self.model, self.view))
        for column, width in enumerate(widths):
            self.view.header().resizeSection(column, width)
        self.view.setUniformRowHeights(True)  # much faster for long lists
        self.view.setAlternatingRowColors(True)
        if size_column is not None:
            self.view.setItemDelegateForColumn(size_column, PaddedDelegate(self.view))
        self.view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.view.customContextMenuRequested.connect(self._context_menu)

        self.expand_button = QToolButton(text="Expand all")
        self.expand_button.setToolTip(f"Long lists show their first {fmt.count(MAX_CHILDREN)} items")
        self.expand_button.clicked.connect(self.view.expandAll)
        self.collapse_button = QToolButton(text="Collapse all")
        self.collapse_button.clicked.connect(self.view.collapseAll)
        self.filter = self._filter_field()
        self.bar = QHBoxLayout()
        self.bar.addWidget(self.expand_button)
        self.bar.addWidget(self.collapse_button)
        self.bar.addStretch()
        self.bar.addWidget(self.filter)
        self.note = QLabel()
        self.note.setWordWrap(True)
        self.note.hide()

        layout = QVBoxLayout(self)
        layout.addLayout(self.bar)
        layout.addWidget(self.note)
        layout.addWidget(self.view)

    def add_to_bar(self, widget: QWidget) -> None:
        """Adds a control after the expand and collapse buttons."""
        self.bar.insertWidget(2, widget)

    def set_nodes(self, nodes: list[Node] | None) -> None:
        self.model.set_root(root_node(nodes or []))

    def set_note(self, text: str | None) -> None:
        self.note.setText(text or "")
        self.note.setVisible(bool(text))

    def _filter_field(self) -> QLineEdit:
        field = QLineEdit()
        field.setPlaceholderText("Filter")
        field.setToolTip("Shows the rows that contain this text, with their parents and contents")
        field.setClearButtonEnabled(True)
        field.setMaximumWidth(260)
        proxy: FilterProxy = self.view.model()
        # Filtering looks at the whole value, also at parts not expanded yet, which takes a moment for large ones
        timer = QTimer(field, singleShot=True, interval=250)
        field.textChanged.connect(lambda: timer.start())

        def apply() -> None:
            proxy.set_text(field.text())
            if proxy.text:
                self.view.expandAll()

        timer.timeout.connect(apply)
        # A new value keeps the filter, and shows its matches
        proxy.modelReset.connect(lambda: proxy.text and QTimer.singleShot(0, self.view.expandAll))
        return field

    def _context_menu(self, position) -> None:
        index = self.view.indexAt(position)
        if not index.isValid():
            return
        # The tooltip has the whole text, where the cell may show it shortened
        text = index.data(Qt.ItemDataRole.ToolTipRole) or index.data()
        menu = QMenu(self.view)
        menu.addAction("Copy", lambda: QGuiApplication.clipboard().setText(str(text)))
        menu.exec(self.view.viewport().mapToGlobal(position))
