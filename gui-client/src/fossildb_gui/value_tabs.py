"""The tabs that show a value: protobuf without and with schema, text, JSON and hex dump."""

import re
from pathlib import Path

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFontDatabase, QGuiApplication, QPalette
from PySide6.QtWidgets import (
    QComboBox,
    QCompleter,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMenu,
    QPlainTextEdit,
    QStackedWidget,
    QStyledItemDelegate,
    QTabWidget,
    QToolButton,
    QTreeView,
    QVBoxLayout,
    QWidget,
)

from fossildb_gui import fmt, settings, type_guess
from fossildb_gui.host import Host
from fossildb_gui.json_tree import JSON_HEADERS, VALUE_COLUMN
from fossildb_gui.protobuf_tree import SIZE_COLUMN, TYPED_HEADERS, WIRE_HEADERS
from fossildb_gui.schema_manager import SchemaManager
from fossildb_gui.tasks import TaskRunner
from fossildb_gui.values import HEX_DUMP_LIMIT, TEXT_LIMIT, DecodedValue, message_to_json
from fossildb_gui.widgets import TreeTab

# Smaller JSON documents are shown fully expanded, larger ones only the first levels
JSON_EXPAND_ALL_LINES = 200
_NO_TYPE = "(none)"


def file_name(*parts) -> str:
    """A file name from e.g. collection, key and version, without characters that file systems don't allow."""
    return re.sub(r"[^-\w.]", "_", "_".join(str(part) for part in parts).strip().replace(" ", "_"))


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
            option.palette.setColor(QPalette.ColorRole.Text, option.palette.color(QPalette.ColorRole.PlaceholderText))


class _TextTab(QWidget):
    """Plain text in a fixed-width font, with a note above it."""

    def __init__(self, wrap: bool = True, parent=None):
        super().__init__(parent)
        self.text = QPlainTextEdit()
        self.text.setReadOnly(True)
        self.text.setFont(QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        if not wrap:
            self.text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self.note = QLabel()
        self.note.setWordWrap(True)
        self.note.hide()
        layout = QVBoxLayout(self)
        layout.addWidget(self.note)
        layout.addWidget(self.text)

    def show_text(self, text: str | None, note: str | None = None) -> None:
        self.text.setPlainText(text or "")
        self.note.setText(note or "")
        self.note.setVisible(bool(note))


class ValueTabs(QWidget):
    message_type_changed = Signal()  # the value needs decoding again

    def __init__(self, collection: str, schemas: SchemaManager, runner: TaskRunner, host: Host, parent=None):
        super().__init__(parent)
        self._collection = collection
        self._schemas = schemas
        self._runner = runner
        self._host = host
        self._data: bytes | None = None  # the value shown, e.g. for guessing its type
        self._name = ""  # for file names
        self._message = None  # the value parsed with the chosen message type
        self._user_chose_tab = False

        self.raw = TreeTab(WIRE_HEADERS, [180, 120, 80, 280], SIZE_COLUMN)
        self.typed = self._typed_tab()
        self.text = _TextTab()
        self.json_tab = self._json_tab()
        self.hex = _TextTab(wrap=False)

        self.tabs = QTabWidget()
        self.tabs.addTab(self.raw, "Protobuf (raw)")
        self.tabs.addTab(self.typed, "Protobuf (typed)")
        self.tabs.addTab(self.text, "Text")
        self.tabs.addTab(self.json_tab, "JSON")
        self.tabs.addTab(self.hex, "Hex")
        self.tabs.tabBarClicked.connect(lambda _: setattr(self, "_user_chose_tab", True))
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.tabs)

        schemas.changed.connect(self._schemas_changed)
        self._fill_type_combo()
        self.clear()

    def _typed_tab(self) -> TreeTab:
        tab = TreeTab(TYPED_HEADERS, [240, 200, 80], SIZE_COLUMN)
        self.type_combo = QComboBox()
        self.type_combo.setEditable(True)
        self.type_combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.type_combo.setMinimumContentsLength(30)
        self.type_combo.activated.connect(self._type_chosen)
        self.guess_button = QToolButton(text="Guess")
        self.guess_button.setToolTip("Find the message type that knows all fields of this value")
        self.guess_button.clicked.connect(self._guess_type)
        type_row = QHBoxLayout()
        type_row.addWidget(QLabel(f"Message type for {self._collection}:"))
        type_row.addWidget(self.type_combo, stretch=1)
        type_row.addWidget(self.guess_button)
        tab.layout().insertLayout(0, type_row)

        self.json_menu_button = QToolButton(text="JSON")
        self.json_menu_button.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self.json_menu_button.setToolTip("The value as JSON, with the field names of the .proto files")
        menu = QMenu(self.json_menu_button)
        menu.addAction("Copy as JSON", self.copy_json)
        menu.addAction("Save as JSON…", self._save_json)
        self.json_menu_button.setMenu(menu)
        tab.add_to_bar(self.json_menu_button)
        return tab

    def _json_tab(self) -> TreeTab:
        """The JSON tree, or the pretty-printed text instead (Raw)."""
        tab = TreeTab(JSON_HEADERS, [240])
        tab.view.setItemDelegateForColumn(VALUE_COLUMN, _ExpandedSummaryDelegate(tab.view))
        self.json_raw = QPlainTextEdit()
        self.json_raw.setReadOnly(True)
        self.json_raw.setFont(QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        self.json_stack = QStackedWidget()
        tab.layout().replaceWidget(tab.view, self.json_stack)
        self.json_stack.addWidget(tab.view)
        self.json_stack.addWidget(self.json_raw)
        self.json_tree_button = QToolButton(text="Tree", checkable=True, checked=True, autoExclusive=True)
        self.json_raw_button = QToolButton(text="Raw", checkable=True, autoExclusive=True)
        self.json_tree_button.toggled.connect(self._json_mode_changed)
        tab.bar.insertWidget(0, self.json_tree_button)
        tab.bar.insertWidget(1, self.json_raw_button)
        tab.bar.insertSpacing(2, 12)
        return tab

    # Showing a value

    def clear(self) -> None:
        self._data = None
        self._message = None
        self.json_menu_button.setEnabled(False)
        for tab in (self.raw, self.typed, self.json_tab):
            tab.set_nodes(None)
            tab.set_note(None)
        for tab in (self.text, self.hex):
            tab.show_text(None)
        self.json_raw.setPlainText("")
        self.typed.set_note(self._typed_hint())

    def show_value(self, value: DecodedValue, data: bytes, name: str) -> None:
        """Shows the decoded value; name is used for file names, e.g. collection, key and version."""
        self._data = data
        self._name = name
        self._message = value.message
        self.json_menu_button.setEnabled(value.message is not None)

        self.raw.set_nodes(value.protobuf_nodes)
        self.typed.set_nodes(value.typed_nodes)
        if self.message_class() is None:
            self.typed.set_note(self._typed_hint())
        else:
            self.typed.set_note(value.typed_note)
        for tab in (self.raw, self.typed):
            self._enable(tab, value.is_protobuf, value.not_protobuf)

        truncated = f"Showing the first {fmt.size(TEXT_LIMIT)}. Download the value to see all of it."
        self.text.show_text(value.text, truncated if value.text_truncated else None)
        self.json_raw.setPlainText(value.json_text or "")
        self.json_tab.set_nodes(value.json_nodes)
        self.json_tab.set_note(value.not_json)
        if value.json_text is not None and value.json_text.count("\n") < JSON_EXPAND_ALL_LINES:
            self.json_tab.view.expandAll()
        else:
            self.json_tab.view.expandToDepth(0)
        for tab in (self.text, self.json_tab):
            self._enable(tab, value.is_text, "The value is not text")

        truncated = f"Showing the first {fmt.size(HEX_DUMP_LIMIT)}. Download the value to see all of it."
        self.hex.show_text(value.hex_dump, truncated if value.hex_truncated else None)

        if not self._user_chose_tab:
            self.tabs.setCurrentWidget(self._best_tab(value))

    def _enable(self, tab: QWidget, enabled: bool, reason: str | None) -> None:
        index = self.tabs.indexOf(tab)
        self.tabs.setTabEnabled(index, enabled)
        self.tabs.setTabToolTip(index, "" if enabled else reason or "")

    def _best_tab(self, value: DecodedValue) -> QWidget:
        if value.is_protobuf and value.typed_nodes is not None:
            return self.typed
        if value.json_text is not None:
            return self.json_tab
        if value.is_text:
            return self.text
        return self.raw if value.protobuf_nodes else self.hex

    def _json_mode_changed(self) -> None:
        tree = self.json_tree_button.isChecked()
        self.json_stack.setCurrentWidget(self.json_tab.view if tree else self.json_raw)
        for widget in (self.json_tab.expand_button, self.json_tab.collapse_button, self.json_tab.filter):
            widget.setEnabled(tree)

    # Message types

    def message_class(self):
        """The class of the message type chosen for the collection, or None."""
        name = settings.message_type_for(self._collection)
        if not name or self._schemas.schemas is None:
            return None
        try:
            return self._schemas.schemas.message_class(name)
        except Exception:  # e.g. not in the schemas anymore
            return None

    def _typed_hint(self) -> str | None:
        if self._schemas.loading:
            return "Loading schemas…"
        if self._schemas.error:
            return f"Loading schemas failed: {self._schemas.error}"
        if self._schemas.schemas is None:
            return "To decode values with their message types, load a folder of .proto files (Schemas menu)."
        if self.message_class() is None:
            return f"Choose the message type of the values in {self._collection}."
        return None

    def _fill_type_combo(self) -> None:
        schemas = self._schemas.schemas
        names = schemas.message_names if schemas is not None else []
        self.type_combo.blockSignals(True)
        self.type_combo.clear()
        self.type_combo.addItems([_NO_TYPE] + names)
        completer = QCompleter(names, self.type_combo)
        completer.setFilterMode(Qt.MatchFlag.MatchContains)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.type_combo.setCompleter(completer)
        chosen = settings.message_type_for(self._collection)
        self.type_combo.setCurrentText(chosen if chosen in names else _NO_TYPE)
        self.type_combo.setEnabled(bool(names))
        self.type_combo.blockSignals(False)

    def _schemas_changed(self) -> None:
        self._fill_type_combo()
        self.typed.set_note(self._typed_hint())
        self.message_type_changed.emit()

    def _type_chosen(self, _index: int = 0) -> None:
        name = self.type_combo.currentText()
        settings.set_message_type_for(self._collection, "" if name == _NO_TYPE else name)
        self._user_chose_tab = False
        self.message_type_changed.emit()

    def _guess_type(self) -> None:
        schemas, data = self._schemas.schemas, self._data
        if schemas is None or data is None:
            self._host.show_status("Load .proto files and select a version first", 8000)
            return
        self.guess_button.setEnabled(False)
        self._host.show_status(f"Trying {fmt.plural(len(schemas.message_names), 'message type')}…")

        def done(guesses: list[type_guess.TypeGuess]) -> None:
            self.guess_button.setEnabled(True)
            if data is self._data:  # still the same value
                self._host.show_status(self._use_guess(guesses), 20000)

        def failed(error: Exception) -> None:
            self.guess_button.setEnabled(True)
            self._host.show_status(f"Guessing failed: {error}", 20000)

        self._runner.submit(lambda: type_guess.guess_message_types(data, schemas), done, failed, context=self)

    def _use_guess(self, guesses: list[type_guess.TypeGuess]) -> str:
        """Chooses the best fitting type, and says why."""
        fitting = [g for g in guesses if g.fits]
        if not fitting:
            if not guesses:
                return "No message type can parse this value"
            closest = guesses[0]
            return (
                f"No message type knows all fields of this value. "
                f"Closest: {closest.name} ({fmt.count(closest.unknown)} unknown)"
            )
        best = fitting[0]
        self.type_combo.setCurrentText(best.name)
        self._type_chosen()
        others = [g.name for g in fitting[1:4] if g.known == best.known]
        also = f". Equally possible: {', '.join(others)}" if others else ""
        return f"Guessed {best.name}: knows all {fmt.plural(best.known, 'field')}{also}"

    # As JSON

    def copy_json(self) -> None:
        def copy(text: str) -> None:
            QGuiApplication.clipboard().setText(text)
            self._host.show_status(f"Copied {fmt.size(len(text.encode()))} of JSON", 8000)

        self._with_json(copy)

    def _save_json(self) -> None:
        name = file_name(self._name) + ".json"
        path, _ = QFileDialog.getSaveFileName(self, "Save as JSON", str(Path.home() / name), "JSON (*.json)")
        if not path:
            return

        def save(text: str) -> None:
            Path(path).write_text(text, encoding="utf-8")
            self._host.show_status(f"Saved JSON to {path}", 8000)

        self._with_json(save)

    def _with_json(self, use) -> None:
        """Converts the typed value to JSON in the background, which can take a moment for large ones."""
        message = self._message
        if message is not None:
            self._runner.submit(
                lambda: message_to_json(message),
                use,
                lambda e: self._host.show_status(f"Converting to JSON failed: {e}", 10000),
                context=self,
            )
