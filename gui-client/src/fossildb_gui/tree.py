"""A tree model whose children are created only when a node is expanded, so large values stay fast, and a proxy
that filters it."""

from collections.abc import Callable, Iterable

from PySide6.QtCore import QAbstractItemModel, QModelIndex, QPersistentModelIndex, QSortFilterProxyModel, Qt

from fossildb_gui import fmt

# Children beyond this are summarized in one placeholder node
MAX_CHILDREN = 10_000


class Node:
    __slots__ = ("columns", "tooltip", "column_tooltips", "parent", "row", "_children", "_loader")

    def __init__(
        self,
        columns: tuple[str, ...],
        loader: Callable[[], Iterable["Node"]] | None = None,
        tooltip: str | None = None,
        column_tooltips: dict[int, str] | None = None,
    ):
        self.columns = columns
        self.tooltip = tooltip  # for all columns, except those in column_tooltips
        self.column_tooltips = column_tooltips
        self.parent: Node | None = None
        self.row = 0
        self._children: list[Node] | None = None if loader else []
        self._loader = loader

    @property
    def has_children(self) -> bool:
        return self._loader is not None or bool(self._children)

    @property
    def children(self) -> list["Node"]:
        if self._children is None:
            try:
                children = list(self._loader())
            except Exception as e:
                children = [Node((f"Error: {e}",))]
            for row, child in enumerate(children):
                child.parent = self
                child.row = row
            self._children = children
        return self._children


def root_node(children: Iterable[Node]) -> Node:
    children = list(children)
    return Node(("",), loader=lambda: children)


def limited(nodes: Iterable[Node], total: int | None = None) -> Iterable[Node]:
    """At most MAX_CHILDREN nodes, plus a placeholder if there are more."""
    for count, node in enumerate(nodes):
        if count == MAX_CHILDREN:
            more = f"{fmt.count(total - MAX_CHILDREN)} more" if total is not None else "more"
            yield Node((f"… {more} not shown",))
            return
        yield node


class TreeModel(QAbstractItemModel):
    def __init__(self, headers: list[str], root: Node | None = None, parent=None, right_aligned: tuple[int, ...] = ()):
        super().__init__(parent)
        self._headers = headers
        self._right_aligned = set(right_aligned)
        self._root = root or root_node([])

    def set_root(self, root: Node) -> None:
        self.beginResetModel()
        self._root = root
        self.endResetModel()

    def _node(self, index: QModelIndex | QPersistentModelIndex) -> Node:
        return index.internalPointer() if index.isValid() else self._root

    def index(self, row, column, parent=QModelIndex()):
        if not self.hasIndex(row, column, parent):
            return QModelIndex()
        return self.createIndex(row, column, self._node(parent).children[row])

    def parent(self, index=QModelIndex()):
        if not index.isValid():
            return QModelIndex()
        parent = index.internalPointer().parent
        if parent is None or parent is self._root:
            return QModelIndex()
        return self.createIndex(parent.row, 0, parent)

    def hasChildren(self, parent=QModelIndex()):
        if parent.isValid() and parent.column() > 0:
            return False
        return self._node(parent).has_children

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid() and parent.column() > 0:
            return 0
        return len(self._node(parent).children)

    def columnCount(self, parent=QModelIndex()):
        return len(self._headers)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        node = index.internalPointer()
        if role == Qt.ItemDataRole.DisplayRole:
            return node.columns[index.column()] if index.column() < len(node.columns) else ""
        if role == Qt.ItemDataRole.ToolTipRole:
            if node.column_tooltips and index.column() in node.column_tooltips:
                return node.column_tooltips[index.column()]
            return node.tooltip
        if role == Qt.ItemDataRole.TextAlignmentRole and index.column() in self._right_aligned:
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self._headers[section]
        return None


class FilterProxy(QSortFilterProxyModel):
    """Shows the rows whose text contains the filter (in any column), with their parents and children."""

    def __init__(self, source: TreeModel, parent=None):
        super().__init__(parent)
        self.setSourceModel(source)
        self.setRecursiveFilteringEnabled(True)  # parents of matches
        self.setAutoAcceptChildRows(True)  # children of matches, e.g. the fields of a matching message
        self._text = ""

    @property
    def text(self) -> str:
        return self._text

    def set_text(self, text: str) -> None:
        if hasattr(self, "beginFilterChange"):  # Qt >= 6.10; invalidateRowsFilter is deprecated there
            self.beginFilterChange()
            self._text = text.strip().casefold()
            self.endFilterChange(QSortFilterProxyModel.Direction.Rows)
        else:
            self._text = text.strip().casefold()
            self.invalidateRowsFilter()

    def filterAcceptsRow(self, source_row: int, source_parent) -> bool:
        if not self._text:
            return True
        node = self.sourceModel().index(source_row, 0, source_parent).internalPointer()
        return any(self._text in column.casefold() for column in node.columns)
