"""A list model whose items are loaded page by page in the background, as views ask for more (fetchMore)."""

from collections.abc import Callable

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt, Signal
from PySide6.QtGui import QGuiApplication, QPalette

from fossildb_gui.tasks import Latest, TaskRunner


class PagedModel(QAbstractTableModel):
    """Subclasses say how to load a page (page_loader), and what to show for an item (item_data).

    While a page loads, a last row "Loading more…" tells the user who scrolled to the end that more is coming.
    """

    status_changed = Signal()  # loading, loaded or failed
    page_loaded = Signal(int)  # the row of the page's first item

    def __init__(self, runner: TaskRunner, page_size: int, parent=None):
        super().__init__(parent)
        self._runner = runner
        self._page_size = page_size
        self._latest = Latest()
        self.items: list = []
        self.has_more = False
        self.loading = False
        self.error: str | None = None

    def page_loader(self, limit: int) -> Callable[[], list]:
        """A function that loads the next page of at most limit items, in a background thread."""
        raise NotImplementedError

    def item_data(self, item, column: int, role):
        raise NotImplementedError

    def reset(self, load: bool = True) -> None:
        """Forgets the items, and loads the first page (unless load is False)."""
        self.beginResetModel()
        self.items = []
        self.has_more = load
        self.error = None
        self.loading = False
        self._latest.invalidate()
        self.endResetModel()
        self.fetchMore()

    def append(self, items: list) -> None:
        if items:
            self.beginInsertRows(QModelIndex(), len(self.items), len(self.items) + len(items) - 1)
            self.items.extend(items)
            self.endInsertRows()

    def remove(self, item) -> None:
        try:
            row = self.items.index(item)
        except ValueError:
            return
        self.beginRemoveRows(QModelIndex(), row, row)
        del self.items[row]
        self.endRemoveRows()
        self.status_changed.emit()

    def is_item_row(self, row: int) -> bool:
        return 0 <= row < len(self.items)

    # Paging

    def canFetchMore(self, parent=QModelIndex()):
        return not parent.isValid() and self.has_more and not self.loading and self.error is None

    def fetchMore(self, parent=QModelIndex()):
        if not self.canFetchMore(parent):
            return
        self._set_loading(True)
        self.status_changed.emit()
        generation = self._latest.next()
        self._runner.submit(
            self.page_loader(self._page_size + 1),  # one more than a page, to know whether there are more
            self._latest.wrap(generation, self._page_loaded),
            self._latest.wrap(generation, self._page_failed),
            context=self,
        )

    def _set_loading(self, loading: bool) -> None:
        if loading == self.loading:
            return
        # The "Loading more…" row, which isn't needed before the first page
        if self.items:
            row = len(self.items)
            (self.beginInsertRows if loading else self.beginRemoveRows)(QModelIndex(), row, row)
        self.loading = loading
        if self.items:
            (self.endInsertRows if loading else self.endRemoveRows)()

    def _page_loaded(self, items: list) -> None:
        self._set_loading(False)
        self.has_more = len(items) > self._page_size
        first_row = len(self.items)
        self.append(self.new_items(items[: self._page_size]))
        self.status_changed.emit()
        self.page_loaded.emit(first_row)

    def new_items(self, page: list) -> list:
        """The items of a page that should be added, e.g. without those that are known already."""
        return page

    def _page_failed(self, error: Exception) -> None:
        self._set_loading(False)
        self.error = str(error)
        self.status_changed.emit()

    # Model

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self.items) + (1 if self.loading and self.items else 0)

    def flags(self, index):
        return super().flags(index) if self.is_item_row(index.row()) else Qt.ItemFlag.NoItemFlags

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if self.is_item_row(index.row()):
            return self.item_data(self.items[index.row()], index.column(), role)
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
