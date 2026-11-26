"""The versions of a key: listed newest first and page by page, with their sizes once known, a field to go to any
version, and measuring the sizes of all versions."""

from PySide6.QtCore import QModelIndex, QRegularExpression, Qt, Signal
from PySide6.QtGui import QRegularExpressionValidator
from PySide6.QtWidgets import QAbstractItemView, QLabel, QLineEdit, QListView, QPushButton, QVBoxLayout, QWidget

from fossildb_gui import fmt
from fossildb_gui.paged_model import PagedModel
from fossildb_gui.tasks import Latest, TaskRunner
from fossildb_gui.widgets import fetch_early

VERSIONS_PAGE_SIZE = 1000
# Counting the versions of a key means listing them, so it stops here; more are shown as "10 000+"
MAX_VERSION_COUNT = 10_000
# Loading more versions starts when fewer than this are left below the visible ones
VERSIONS_PREFETCH_ROWS = 100

# Measuring the sizes downloads all versions, in requests of about this size
MEASURE_TARGET_BYTES = 16 << 20
MEASURE_FIRST_REQUEST_VERSIONS = 16
MEASURE_MAX_REQUEST_VERSIONS = 1000


def count_versions(connection, collection: str, key: str) -> int:
    """The number of versions, up to MAX_VERSION_COUNT + 1 (meaning "more"). Runs in a background thread."""
    return len(connection.list_versions(collection, key, MAX_VERSION_COUNT + 1))


class VersionListModel(PagedModel):
    """The versions of a key, newest first, shown with their sizes once known, e.g. "7  ·  1.2 kB"."""

    def __init__(self, connection, collection: str, runner: TaskRunner, parent=None):
        super().__init__(runner, VERSIONS_PAGE_SIZE, parent)
        self._connection = connection
        self._collection = collection
        self.key: str | None = None
        self.sizes: dict[int, int] = {}
        self._rows: dict[int, int] = {}  # version → row

    @property
    def versions(self) -> list[int]:
        return self.items

    def show_key(self, key: str | None) -> None:
        self.key = key
        self.sizes = {}
        self._rows = {}
        self.reset(load=key is not None)

    def page_loader(self, limit: int):
        key, offset = self.key, len(self.items)
        return lambda: self._connection.list_versions(self._collection, key, limit, offset)

    def new_items(self, page: list[int]) -> list[int]:
        # New versions may have been written meanwhile, which shifts the pages
        return [version for version in page if version not in self._rows]

    def append(self, versions: list[int]) -> None:
        for row, version in enumerate(versions, len(self.items)):
            self._rows[version] = row
        super().append(versions)

    def add_all(self, versions: list[int]) -> None:
        """Adds what's missing from the complete list of versions; there is nothing more to load then."""
        self._latest.invalidate()  # a page that is still loading
        self._set_loading(False)
        self.append(self.new_items(versions))
        self.has_more = False
        self.status_changed.emit()

    def row(self, version: int) -> int | None:
        return self._rows.get(version)

    def set_sizes(self, sizes: list[tuple[int, int]]) -> None:
        for version, size in sizes:
            self.sizes[version] = size
            row = self._rows.get(version)
            if row is not None:
                self.dataChanged.emit(self.index(row, 0), self.index(row, 0))

    def redisplay(self) -> None:
        if self.items:
            self.dataChanged.emit(self.index(0, 0), self.index(len(self.items) - 1, 0))

    def columnCount(self, parent=QModelIndex()):
        return 1

    def item_data(self, version: int, column: int, role):
        if role == Qt.ItemDataRole.DisplayRole:
            size = self.sizes.get(version)
            return str(version) if size is None else f"{version}  ·  {fmt.size(size)}"
        return None


class VersionsPanel(QWidget):
    version_selected = Signal(int)  # chosen in the list
    version_requested = Signal(int)  # entered in "Go to version", but not listed (yet)
    versions_changed = Signal()  # e.g. loaded

    def __init__(self, connection, collection: str, runner: TaskRunner, parent=None):
        super().__init__(parent)
        self._connection = connection
        self._collection = collection
        self._runner = runner
        self._select: int | None = None  # version to select once the first page is loaded
        self.count: int | None = None  # of all versions, up to MAX_VERSION_COUNT + 1 (meaning "more")
        self._counting = Latest()

        self.model = VersionListModel(connection, collection, runner, self)
        self.model.status_changed.connect(self._update_label)
        self.model.status_changed.connect(self.versions_changed)
        self.model.page_loaded.connect(self._page_loaded)
        self.label = QLabel()
        self.view = QListView()
        self.view.setModel(self.model)
        self.view.setUniformItemSizes(True)
        self.view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.view.selectionModel().currentChanged.connect(self._current_changed)
        fetch_early(self.view, VERSIONS_PREFETCH_ROWS)

        self.go_to = QLineEdit()
        self.go_to.setPlaceholderText("Go to version (Enter)")
        self.go_to.setValidator(QRegularExpressionValidator(QRegularExpression(r"\d{1,19}"), self))
        self.go_to.setClearButtonEnabled(True)
        self.go_to.returnPressed.connect(self._go_to_entered)

        self._measurement = Latest()
        self.measuring = False
        self.measured: dict[int, int] = {}  # by the last measurement
        self.measure_button = QPushButton()
        self.measure_button.setToolTip(
            "Adds up the sizes of all versions of this key. FossilDB can't report sizes, so this downloads every "
            "version.\nThe size on disk is smaller, because the data files are compressed."
        )
        self.measure_button.clicked.connect(self._toggle_measuring)
        self.measure_label = QLabel()
        self.measure_label.setWordWrap(True)
        self.measure_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.label)
        layout.addWidget(self.view)
        layout.addWidget(self.go_to)
        layout.addWidget(self.measure_button)
        layout.addWidget(self.measure_label)
        self.show_key(None)

    @property
    def versions(self) -> list[int]:
        return self.model.versions

    def show_key(self, key: str | None, select: int | None = None, count: int | None = None) -> None:
        """Lists the versions of the key, and then selects the given version, or else the newest.

        count: the number of versions, if known already (e.g. from the key list); else it is counted here, since the
        list itself only knows it once all pages are loaded."""
        self._measurement.invalidate()
        self._set_measuring(False)
        self.measured = {}
        self.measure_label.setText("")
        self._select = select
        self.count = count
        generation = self._counting.next()
        self.model.show_key(key)
        self._update_label()
        if key is not None and count is None:
            self._runner.submit(
                lambda: count_versions(self._connection, self._collection, key),
                self._counting.wrap(generation, self._counted),
                context=self,
            )

    def _counted(self, count: int) -> None:
        self.count = count
        self._update_label()

    def versions_text(self) -> str:
        """E.g. "1 version", "4 321 versions", or "10 000+ versions" for more than can be counted."""
        model = self.model
        if not model.has_more and not model.loading and model.error is None:
            return fmt.plural(len(model.versions), "version")
        if self.count is not None:
            if self.count > MAX_VERSION_COUNT:
                return f"{fmt.count(MAX_VERSION_COUNT)}+ versions"
            return fmt.plural(self.count, "version")
        return f"{fmt.count(len(model.versions))}+ versions"

    def select(self, version: int) -> None:
        """Marks the version as the current one, if it is listed, without announcing it."""
        row = self.model.row(version)
        if row is not None:
            self.view.selectionModel().blockSignals(True)
            self.view.setCurrentIndex(self.model.index(row, 0))
            self.view.selectionModel().blockSignals(False)
            self.view.scrollTo(self.view.currentIndex())
        else:
            self.view.selectionModel().clearCurrentIndex()
            self.view.clearSelection()

    def set_size(self, version: int, size: int) -> None:
        self.model.set_sizes([(version, size)])

    def redisplay(self) -> None:
        """Shows the sizes again, e.g. with other units."""
        self.model.redisplay()
        if not self.measuring and self.measured:
            stopped = self.measure_label.text().startswith("Stopped")
            self.measure_label.setText(("Stopped. " if stopped else "") + self._measured_text())

    def focus(self) -> bool:
        """Moves the keyboard focus to the list, keeping the selected version. False if there are no versions yet."""
        if not self.versions:
            return False
        if not self.view.currentIndex().isValid():
            self.view.setCurrentIndex(self.model.index(0, 0))
        self.view.setFocus()
        return True

    def _page_loaded(self, first_row: int) -> None:
        if first_row == 0 and self.versions:
            row = self.model.row(self._select) if self._select is not None else None
            self._select = None
            self.view.setCurrentIndex(self.model.index(row or 0, 0))

    def _update_label(self) -> None:
        model = self.model
        count = len(model.versions)
        self.label.setToolTip("")
        if model.key is None:
            text = ""
        elif model.error:
            text = "Could not load versions"
            self.label.setToolTip(model.error)
        elif model.loading and not count:
            text = "Loading versions…"
        elif not count:
            text = "No versions (the key does not exist)"
        else:
            text = self.versions_text()
            if model.has_more:
                self.label.setToolTip(f"{fmt.count(count)} listed so far, scroll down for older ones")
        self.label.setText(text)
        self.measure_button.setEnabled(count > 0)

    def _current_changed(self, current, _previous) -> None:
        if current.isValid() and self.model.is_item_row(current.row()):
            self.version_selected.emit(self.versions[current.row()])

    def _go_to_entered(self) -> None:
        if not self.go_to.text() or self.model.key is None:
            return
        version = int(self.go_to.text())
        row = self.model.row(version)
        if row is None:
            self.select(version)  # clears the selection
            self.version_requested.emit(version)
        else:
            self.view.setCurrentIndex(self.model.index(row, 0))
            self.view.scrollTo(self.view.currentIndex())

    # Measuring the sizes of all versions

    def _toggle_measuring(self) -> None:
        if self.measuring:
            self._measurement.invalidate()
            self._set_measuring(False)
            self.measure_label.setText(f"Stopped. {self._measured_text()}")
            return
        self.measured = {}
        self._set_measuring(True)
        generation = self._measurement.next()
        if not self.model.has_more:
            self._measure_from(generation, 0, MEASURE_FIRST_REQUEST_VERSIONS)
            return
        # All versions are measured, so the rest of the list is needed first
        self.measure_label.setText("Listing all versions…")
        key = self.model.key

        def listed(versions: list[int]) -> None:
            self.model.add_all(versions)
            self._measure_from(generation, 0, MEASURE_FIRST_REQUEST_VERSIONS)

        self._runner.submit(
            lambda: self._connection.list_versions(self._collection, key),
            self._measurement.wrap(generation, listed),
            self._measurement.wrap(generation, self._measuring_failed),
            context=self,
        )

    def _set_measuring(self, measuring: bool) -> None:
        self.measuring = measuring
        self.measure_button.setText("Stop measuring" if measuring else "Measure all sizes")

    def _measure_from(self, generation: int, start: int, count: int) -> None:
        self.measure_label.setText(f"Measuring… {self._measured_text()}" if self.measured else "Measuring…")
        # Versions are sorted newest first, so this requests the range from batch[0] down to batch[-1]
        batch = self.versions[start : start + count]
        key = self.model.key
        self._runner.submit(
            lambda: self._connection.version_sizes(self._collection, key, batch[0], batch[-1]),
            self._measurement.wrap(generation, lambda sizes: self._measured(generation, start + len(batch), sizes)),
            self._measurement.wrap(generation, self._measuring_failed),
            context=self,
        )

    def _measured(self, generation: int, next_start: int, sizes: list[tuple[int, int]]) -> None:
        self.measured.update(sizes)
        self.model.set_sizes(sizes)
        if next_start >= len(self.versions):
            self._set_measuring(False)
            self.measure_label.setText(self._measured_text())
            return
        # Aim for requests of about MEASURE_TARGET_BYTES, based on the sizes so far
        average = sum(size for _, size in sizes) / len(sizes) if sizes else 0
        count = int(MEASURE_TARGET_BYTES / average) if average else MEASURE_MAX_REQUEST_VERSIONS
        self._measure_from(generation, next_start, max(1, min(count, MEASURE_MAX_REQUEST_VERSIONS)))

    def _measured_text(self) -> str:
        total = sum(self.measured.values())
        measured = len(self.measured)
        if measured < len(self.versions):
            return f"{fmt.size(total)} in the first {fmt.count(measured)} of {fmt.count(len(self.versions))} versions"
        if not measured:
            return "No versions"
        largest = max(self.measured, key=self.measured.get)
        return (
            f"<b>Total: {fmt.size(total)}</b> in {fmt.plural(measured, 'version')}"
            f"<br>Average {fmt.size(total / measured)}, largest {fmt.size(self.measured[largest])} (version {largest})"
        )

    def _measuring_failed(self, error: Exception) -> None:
        self._set_measuring(False)
        self.measure_label.setText(f"Measuring failed: {error}")
