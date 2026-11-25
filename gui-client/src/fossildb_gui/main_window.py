"""One window per connection: the overview tab, plus one tab per opened collection."""

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtWidgets import (
    QFileDialog,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QTabWidget,
)

from fossildb_gui import __version__, settings
from fossildb_gui.key_browser import KeyBrowser
from fossildb_gui.overview import OverviewWidget
from fossildb_gui.schema_manager import SchemaManager
from fossildb_gui.settings import ConnectionSpec
from fossildb_gui.tasks import Latest, TaskRunner


class MainWindow(QMainWindow):
    new_connection_requested = Signal()

    def __init__(self, spec: ConnectionSpec, connection, info, runner: TaskRunner, schemas: SchemaManager):
        super().__init__()
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        self.setWindowTitle(f"FossilDB – {spec.title}")
        self.resize(1400, 850)
        self._spec = spec
        self._connection = connection
        self._runner = runner
        self._schemas = schemas
        self._info_latest = Latest()
        self._info_loading = False
        self.writes_enabled = False
        self._compacting = False

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
        file_menu.addSeparator()
        file_menu.addAction("Close Window", QKeySequence("Ctrl+Shift+W"), self.close)
        file_menu.addAction("Quit", QKeySequence.StandardKey.Quit, self._quit)

        database_menu = self.menuBar().addMenu("&Database")
        database_menu.addAction("Refresh", QKeySequence.StandardKey.Refresh, self._refresh)
        database_menu.addAction("Open Collection…", QKeySequence("Ctrl+O"), self._ask_collection)
        database_menu.addAction("Filter Keys", QKeySequence.StandardKey.Find, self._focus_filter)
        database_menu.addSeparator()
        self._writes_action = QAction("Enable Writes", self, checkable=True)
        self._writes_action.setToolTip("Allow deleting data and compacting the database on this connection")
        self._writes_action.toggled.connect(self._set_writes_enabled)
        database_menu.addAction(self._writes_action)
        self._compact_action = database_menu.addAction("Compact All Data…", self._compact)

        schema_menu = self.menuBar().addMenu("&Schemas")
        schema_menu.addAction("Load .proto Folder…", self._choose_schema_folder)
        schema_menu.addAction("Reload Schemas", lambda: self._schemas.load())

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
            self.statusBar().showMessage("Refreshing…")
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
        self.statusBar().showMessage(f"Refresh failed: {error}")

    def _info_loaded(self, info) -> None:
        self._info_loading = False
        self.statusBar().clearMessage()
        self._overview.set_info(info)

    # Writes

    @property
    def connection_label(self) -> str:
        return self._connection.label

    def _set_writes_enabled(self, enabled: bool) -> None:
        self.writes_enabled = enabled
        self._update_write_state()

    def _update_write_state(self) -> None:
        if self.writes_enabled:
            self._write_label.setText("<b style='color:#d33'>Writes enabled</b>")
        else:
            self._write_label.setText("Read-only")
        self._compact_action.setEnabled(self.writes_enabled and not self._compacting)
        for index in range(1, self._tabs.count()):
            self._tabs.widget(index).update_write_actions()

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
        if not self.confirm_write(
            "Compact All Data",
            "Rewrite all data files of the database? This removes deleted and overwritten entries and converts old "
            "files to the current compression.\n\nIt causes heavy disk load and can take hours on large databases. "
            "Backups and restores are not possible while it runs.",
        ):
            return
        self._compacting = True
        self._update_write_state()
        self.statusBar().showMessage("Compacting all data… (this can take long)")

        def finished(error: Exception | None) -> None:
            self._compacting = False
            self._update_write_state()
            if error is None:
                self.statusBar().showMessage("Compaction finished", 10000)
                self.refresh_info()
            else:
                self.statusBar().clearMessage()
                QMessageBox.critical(self, "Compaction failed", str(error))

        self._runner.submit(
            self._connection.compact_all_data, lambda _: finished(None), finished, context=self
        )

    # Schemas

    def _choose_schema_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, "Folder with .proto files", self._schemas.folder)
        if folder:
            self._schemas.load(folder)
            self.statusBar().showMessage(f"Loading schemas from {folder}…", 5000)

    # Misc

    def _check_tunnel(self) -> None:
        tunnel = self._connection.tunnel
        if not tunnel.is_alive():
            self._tunnel_timer.stop()
            message = tunnel.exit_message()
            self.statusBar().showMessage(f"The SSH tunnel was closed: {message}")
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
        from PySide6.QtWidgets import QApplication

        QApplication.closeAllWindows()

    def closeEvent(self, event) -> None:
        if self._compacting:
            answer = QMessageBox.question(
                self,
                "Compaction running",
                "CompactAllData is still running on the server. It continues there if you close this window, but you "
                "won't be notified when it finishes. Close anyway?",
            )
            if answer != QMessageBox.StandardButton.Yes:
                event.ignore()
                return
        self._runner.submit(self._connection.close)
        super().closeEvent(event)
