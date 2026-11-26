"""One window per connection: the overview tab, plus one tab per opened collection."""

import sys

from PySide6.QtCore import QPoint, Qt, QTimer, Signal
from PySide6.QtGui import QAction, QActionGroup, QKeySequence
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QTabWidget,
    QVBoxLayout,
)

from fossildb_gui import __version__, fmt, settings
from fossildb_gui.key_browser import KeyBrowser
from fossildb_gui.overview import OverviewWidget
from fossildb_gui.schema_manager import SchemaManager
from fossildb_gui.settings import ConnectionSpec
from fossildb_gui.tasks import Latest, TaskRunner


def _set_size_units(decimal: bool) -> None:
    fmt.set_decimal(decimal)
    settings.set_decimal_sizes(decimal)
    for widget in QApplication.topLevelWidgets():
        if isinstance(widget, MainWindow):
            widget.redisplay()


class _ExportDialog(QDialog):
    """Asks for the directory on the server to export the database into, and an optional RocksDB options file."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Export to New Directory")
        self._directory = QLineEdit()
        self._directory.setPlaceholderText("e.g. /srv/fossildb/export-2026-11")
        self._options = QLineEdit()
        self._options.setPlaceholderText("optional, e.g. /srv/fossildb/options.ini")
        form = QFormLayout()
        form.addRow(QLabel("Paths on the server, not on this computer."))
        form.addRow("New data directory:", self._directory)
        form.addRow("RocksDB options file:", self._options)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        self._ok = buttons.button(QDialogButtonBox.StandardButton.Ok)
        self._ok.setEnabled(False)
        self._directory.textChanged.connect(lambda text: self._ok.setEnabled(bool(text.strip())))
        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(buttons)
        self.setMinimumWidth(480)

    def values(self) -> tuple[str, str | None]:
        return self._directory.text().strip(), self._options.text().strip() or None


class MainWindow(QMainWindow):
    """Implements Host for the panels in its tabs."""

    new_connection_requested = Signal()

    def __init__(self, spec: ConnectionSpec, connection, info, runner: TaskRunner, schemas: SchemaManager):
        super().__init__()
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        self.setWindowTitle(f"FossilDB – {spec.title}")
        self._restore_geometry()
        self._spec = spec
        self._connection = connection
        self._runner = runner
        self._schemas = schemas
        self._info_latest = Latest()
        self._info_loading = False
        self.writes_enabled = False
        self._running: set[str] = set()  # long server operations, e.g. "Compaction"

        self._overview = OverviewWidget(spec)
        self._overview.open_collection.connect(self.open_collection)
        self._overview.refresh_requested.connect(self.refresh_info)
        self._overview.auto_refresh_requested.connect(self._auto_refresh_info)
        self._overview.set_info(info)

        self._tabs = QTabWidget()
        self._tabs.setDocumentMode(True)
        self._tabs.setTabsClosable(True)
        self._tabs.setMovable(True)
        self._tabs.addTab(self._overview, "Overview")
        self._tabs.tabBar().setTabButton(0, self._tabs.tabBar().ButtonPosition.RightSide, None)
        self._tabs.tabCloseRequested.connect(self._close_tab)
        self.setCentralWidget(self._tabs)

        self._write_label = QLabel()
        self.statusBar().addPermanentWidget(self._write_label)
        self._create_menus()
        self._update_write_state()

        if connection.tunnel is not None:
            self._tunnel_timer = QTimer(self, interval=2000)
            self._tunnel_timer.timeout.connect(self._check_tunnel)
            self._tunnel_timer.start()

    def _create_menus(self) -> None:
        file_menu = self.menuBar().addMenu("&File")
        file_menu.addAction("New Connection…", QKeySequence.StandardKey.New, self.new_connection_requested.emit)
        file_menu.addAction("Close Tab", QKeySequence.StandardKey.Close, self._close_current_tab)
        # On macOS, "Ctrl" means the Command key, and Cmd+Tab belongs to the system; Meta is the Control key there
        ctrl = "Meta" if sys.platform == "darwin" else "Ctrl"
        next_tab = file_menu.addAction("Next Tab", lambda: self._switch_tab(1))
        next_tab.setShortcuts([QKeySequence(f"{ctrl}+Tab"), QKeySequence(f"{ctrl}+PgDown")])
        previous_tab = file_menu.addAction("Previous Tab", lambda: self._switch_tab(-1))
        # With Shift, Qt reports the Tab key as Backtab
        previous_tab.setShortcuts(
            [QKeySequence(f"{ctrl}+Shift+Backtab"), QKeySequence(f"{ctrl}+Shift+Tab"), QKeySequence(f"{ctrl}+PgUp")]
        )
        file_menu.addSeparator()
        file_menu.addAction("Close Window", QKeySequence("Ctrl+Shift+W"), self.close)
        file_menu.addAction("Quit", QKeySequence.StandardKey.Quit, self._quit)

        database_menu = self.menuBar().addMenu("&Database")
        database_menu.addAction("Refresh", QKeySequence.StandardKey.Refresh, self._refresh)
        database_menu.addAction("Open Collection…", QKeySequence("Ctrl+O"), self._ask_collection)
        database_menu.addAction("Filter Keys", QKeySequence.StandardKey.Find, self._focus_filter)
        database_menu.addSeparator()
        self._writes_action = QAction("Enable Writes", self, checkable=True)
        self._writes_action.setToolTip("Allow deleting data, compactions, backups and exports on this connection")
        self._writes_action.toggled.connect(self._set_writes_enabled)
        database_menu.addAction(self._writes_action)
        self._compact_action = database_menu.addAction("Compact All Data…", self._compact)
        self._backup_action = database_menu.addAction("Create Backup…", self._backup)
        self._export_action = database_menu.addAction("Export to New Directory…", self._export)

        schema_menu = self.menuBar().addMenu("&Schemas")
        schema_menu.addAction("Load .proto Folder…", self._choose_schema_folder)
        schema_menu.addAction("Reload Schemas", lambda: self._schemas.load())

        view_menu = self.menuBar().addMenu("&View")
        view_menu.addSection("Size Units")
        units = QActionGroup(self)
        self._decimal_action = view_menu.addAction("Decimal Sizes (kB, MB, GB)")
        self._binary_action = view_menu.addAction("Binary Sizes (KiB, MiB, GiB)")
        for action in (self._decimal_action, self._binary_action):
            action.setCheckable(True)
            units.addAction(action)
        (self._decimal_action if fmt.is_decimal() else self._binary_action).setChecked(True)
        self._decimal_action.triggered.connect(lambda: _set_size_units(True))
        self._binary_action.triggered.connect(lambda: _set_size_units(False))

        help_menu = self.menuBar().addMenu("&Help")
        help_menu.addAction("About", self._about)

    # Tabs

    def open_collection(self, name: str) -> None:
        for index in range(1, self._tabs.count()):
            if self._tabs.widget(index).collection == name:
                self._tabs.setCurrentIndex(index)
                return
        settings.add_recent_collection(self._spec.label, name)
        browser = KeyBrowser(self._connection, name, self._runner, self._schemas, self)
        self._tabs.setCurrentIndex(self._tabs.addTab(browser, name))

    def redisplay(self) -> None:
        """Shows everything again, e.g. after the size units changed."""
        (self._decimal_action if fmt.is_decimal() else self._binary_action).setChecked(True)
        self._overview.redisplay()
        for index in range(1, self._tabs.count()):
            self._tabs.widget(index).record.redisplay()

    def _switch_tab(self, step: int) -> None:
        count = self._tabs.count()
        if count > 1:
            self._tabs.setCurrentIndex((self._tabs.currentIndex() + step) % count)

    def _close_tab(self, index: int) -> None:
        if index > 0:
            widget = self._tabs.widget(index)
            self._tabs.removeTab(index)
            widget.deleteLater()

    def _close_current_tab(self) -> None:
        self._close_tab(self._tabs.currentIndex())

    def _current_browser(self) -> KeyBrowser | None:
        widget = self._tabs.currentWidget()
        return widget if isinstance(widget, KeyBrowser) else None

    def _ask_collection(self) -> None:
        name, ok = QInputDialog.getText(self, "Open Collection", "Collection name:")
        if ok and name.strip():
            self.open_collection(name.strip())

    def _focus_filter(self) -> None:
        browser = self._current_browser()
        if browser is not None:
            browser.focus_prefix()

    # Refreshing

    def _refresh(self) -> None:
        browser = self._current_browser()
        if browser is not None:
            browser.refresh()
        else:
            self.refresh_info()

    def refresh_info(self, quiet: bool = False) -> None:
        generation = self._info_latest.next()
        self._info_loading = True
        if not quiet:
            self.show_status(self._REFRESHING)
        self._runner.submit(
            self._connection.database_info,
            self._info_latest.wrap(generation, self._info_loaded),
            self._info_latest.wrap(generation, self._info_failed),
            context=self,
        )

    def _auto_refresh_info(self) -> None:
        # A slow server gets no new request before it answered the previous one
        if not self._info_loading:
            self.refresh_info(quiet=True)

    def _info_failed(self, error: Exception) -> None:
        self._info_loading = False
        self.show_status(f"Refresh failed: {error}")

    _REFRESHING = "Refreshing…"

    def _info_loaded(self, info) -> None:
        self._info_loading = False
        # Only its own message, e.g. not that a backup finished
        if self.statusBar().currentMessage() == self._REFRESHING:
            self.show_status("")
        self._overview.set_info(info)

    # Writes

    def _set_writes_enabled(self, enabled: bool) -> None:
        self.writes_enabled = enabled
        self._update_write_state()

    def _update_write_state(self) -> None:
        if self.writes_enabled:
            self._write_label.setText("<b style='color:#d33'>Writes enabled</b>")
        else:
            self._write_label.setText("Read-only")
        # The server refuses some operations while others run, e.g. no backup during a compaction
        idle = not self._running
        for action in (self._compact_action, self._backup_action, self._export_action):
            action.setEnabled(self.writes_enabled and idle)
        for index in range(1, self._tabs.count()):
            self._tabs.widget(index).update_write_actions()

    def show_status(self, message: str, timeout_ms: int = 0) -> None:
        if message:
            self.statusBar().showMessage(message, timeout_ms)
        else:
            self.statusBar().clearMessage()

    def confirm_write(self, title: str, text: str, require_text: str | None = None) -> bool:
        """Asks before a destructive operation. With require_text, the user has to type it to confirm."""
        if not self.writes_enabled:
            return False
        text = f"Server: {self._spec.title}\n\n{text}"
        if require_text is None:
            answer = QMessageBox.warning(
                self,
                title,
                text,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Cancel,
            )
            return answer == QMessageBox.StandardButton.Yes
        typed, ok = QInputDialog.getText(
            self, title, f"{text}\n\nType the prefix to confirm:", QLineEdit.EchoMode.Normal, ""
        )
        return ok and typed == require_text

    def _compact(self) -> None:
        if self.confirm_write(
            "Compact All Data",
            "Rewrite all data files of the database? This removes deleted and overwritten entries and converts old "
            "files to the current compression.\n\nIt causes heavy disk load and can take hours on large databases. "
            "Backups and restores are not possible while it runs.",
        ):
            self._run_operation("Compaction", self._connection.compact_all_data, lambda _: "Compaction finished")

    def _backup(self) -> None:
        if self.confirm_write(
            "Create Backup",
            "Create a backup in the server's backup directory? It replaces the previous backup, since the server "
            "keeps only the newest one. Only data files that changed since then are copied.\n\nThis can take long and "
            "causes disk load. Writes continue meanwhile.",
        ):
            self._run_operation(
                "Backup",
                self._connection.backup,
                lambda reply: f"Backup {reply.id} created ({fmt.size(reply.size)})",
            )

    def _export(self) -> None:
        dialog = _ExportDialog(self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        directory, options_file = dialog.values()
        if self.confirm_write(
            "Export to New Directory",
            f"Copy the whole database into {directory} on the server"
            + (f", with the RocksDB options from {options_file}" if options_file else "")
            + "?\n\nThis reads and writes all data, which can take hours on large databases, and needs as much free "
            "disk space as the database itself.",
        ):
            self._run_operation(
                "Export",
                lambda: self._connection.export_db(directory, options_file),
                lambda _: f"Export to {directory} finished",
            )

    def _run_operation(self, name: str, call, done_message) -> None:
        """Runs a long server operation in the background, with its progress in the status bar."""
        self._running.add(name)
        self._update_write_state()
        self.show_status(f"{name} running on the server… (this can take long)")
        self.refresh_info(quiet=True)  # the overview shows what runs

        def finished(result, error: Exception | None) -> None:
            self._running.discard(name)
            self._update_write_state()
            if error is None:
                self.show_status(done_message(result), 15000)
            else:
                self.show_status("")
                QMessageBox.critical(self, f"{name} failed", str(error))
            self.refresh_info(quiet=True)

        self._runner.submit(call, lambda result: finished(result, None), lambda e: finished(None, e), context=self)

    # Schemas

    def _choose_schema_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, "Folder with .proto files", self._schemas.folder)
        if folder:
            self._schemas.load(folder)
            self.show_status(f"Loading schemas from {folder}…", 5000)

    # Misc

    def _check_tunnel(self) -> None:
        tunnel = self._connection.tunnel
        if not self._connection.check_tunnel():
            self._tunnel_timer.stop()
            # The connection refuses all calls from now on; also make clear that nothing can be written anymore
            self._writes_action.setChecked(False)
            self._writes_action.setEnabled(False)
            message = tunnel.exit_message()
            self.show_status(f"The SSH tunnel was closed: {message}")
            QMessageBox.warning(
                self,
                "SSH tunnel closed",
                f"The SSH tunnel to {tunnel.ssh_host} was closed:\n\n{message}\n\nReconnect via File → New Connection.",
            )

    def _about(self) -> None:
        QMessageBox.about(
            self,
            "About FossilDB Client",
            f"FossilDB Client {__version__}\n\nConnected to {self._spec.title}",
        )

    def _quit(self) -> None:
        QApplication.closeAllWindows()

    # Keys that move to the list of the current tab when no other widget used them, e.g. while a button has the focus
    _LIST_KEYS = {Qt.Key.Key_Up, Qt.Key.Key_Down, Qt.Key.Key_PageUp, Qt.Key.Key_PageDown}

    def keyPressEvent(self, event) -> None:
        list_key = event.key() in self._LIST_KEYS and not event.modifiers()
        if not (list_key and self._tabs.currentWidget().take_list_key(event)):
            super().keyPressEvent(event)

    def closeEvent(self, event) -> None:
        if self._running:
            running = ", ".join(sorted(self._running))
            answer = QMessageBox.question(
                self,
                f"{running} running",
                f"{running} is still running on the server. It continues there if you close this window, but you "
                "won't be notified when it finishes. Close anyway?",
            )
            if answer != QMessageBox.StandardButton.Yes:
                event.ignore()
                return
        settings.set_window_geometry(self.saveGeometry())
        self._runner.submit(self._connection.close)
        super().closeEvent(event)

    # Default size, as share of the screen's available area
    DEFAULT_SCREEN_SHARE = 0.8
    CASCADE_OFFSET = 32

    def _restore_geometry(self) -> None:
        geometry = settings.window_geometry()
        if geometry is None or not self.restoreGeometry(geometry):
            available = self.screen().availableGeometry()
            size = available.size() * self.DEFAULT_SCREEN_SHARE
            self.resize(size)
            self.move(available.center() - QPoint(size.width() // 2, size.height() // 2))
        # A second window would otherwise cover the first one exactly. (On Wayland, the compositor places windows.)
        others = [
            w for w in QApplication.topLevelWidgets() if isinstance(w, MainWindow) and w is not self and w.isVisible()
        ]
        if any(other.pos() == self.pos() for other in others):
            self.move(self.pos() + QPoint(self.CASCADE_OFFSET, self.CASCADE_OFFSET))
