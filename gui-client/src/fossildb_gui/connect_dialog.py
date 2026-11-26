"""The window for choosing a FossilDB server to connect to."""

from PySide6.QtCore import QEvent, Qt, Signal
from PySide6.QtGui import QAction, QColor, QGuiApplication, QIcon, QKeySequence, QPainter, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QRadioButton,
    QVBoxLayout,
    QWidget,
)

from fossildb_gui import settings
from fossildb_gui.settings import ConnectionSpec
from fossildb_gui.tasks import Latest, TaskRunner
from fossildb_gui.tunnel import local_ports_in_use

_GREEN = QColor(46, 160, 67)
_GRAY = QColor(150, 150, 150)
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}


def _dot(color: QColor | None) -> QIcon:
    """A small colored circle for the reachability of a connection; transparent for None."""
    pixmap = QPixmap(12, 12)
    pixmap.fill(Qt.GlobalColor.transparent)
    if color is not None:
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(color)
        painter.drawEllipse(2, 2, 8, 8)
        painter.end()
    return QIcon(pixmap)


def _discovery():
    # Imported in the background threads, since it loads grpc, which would delay showing the dialog
    from fossildb_gui import discovery

    return discovery


def _local_port(spec: ConnectionSpec) -> int | None:
    """The port of a direct connection to this computer, which the search for local servers can skip."""
    host, _, port = spec.address.rpartition(":")
    return int(port) if not spec.ssh_host and host in _LOCAL_HOSTS and port.isdigit() else None


class ConnectDialog(QDialog):
    # Emitted with the open Connection and its DatabaseInfoProto (None for servers without GetDatabaseInfo)
    connected = Signal(object, object, object)  # spec, connection, info

    def __init__(self, runner: TaskRunner, initial: ConnectionSpec | None = None, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle("Connect to FossilDB")
        self.setMinimumWidth(460)
        self._runner = runner
        self._latest = Latest()
        self._focus_before = None

        self._recent = QListWidget()
        for spec in settings.recent_connections():
            item = QListWidgetItem()
            self._set_item_spec(item, spec)
            # A transparent dot, so that the text doesn't move once the check shows a colored one
            item.setIcon(_dot(None))
            self._recent.addItem(item)
        self._recent.setContextMenuPolicy(Qt.ContextMenuPolicy.ActionsContextMenu)
        rename = QAction("Rename…", self._recent)
        rename.setShortcut(QKeySequence(Qt.Key.Key_F2))
        rename.setShortcutContext(Qt.ShortcutContext.WidgetShortcut)
        rename.triggered.connect(self._rename_selected)
        remove = QAction("Remove from list", self._recent)
        remove.setShortcut(QKeySequence.StandardKey.Delete)
        remove.setShortcutContext(Qt.ShortcutContext.WidgetShortcut)
        remove.triggered.connect(self._remove_selected)
        self._recent.addActions([rename, remove])
        self._recent.setToolTip("Right-click to rename (F2) or remove (Del) a connection")
        # Choosing a connection (by click or arrow keys) fills in the fields; double-click or Enter connects
        self._recent.currentItemChanged.connect(
            lambda item, _: item and self._fill(item.data(Qt.ItemDataRole.UserRole))
        )
        self._recent.itemClicked.connect(lambda item: self._fill(item.data(Qt.ItemDataRole.UserRole)))
        self._recent.itemDoubleClicked.connect(self._double_clicked)
        self._recent.installEventFilter(self)

        self._direct = QRadioButton("Direct")
        self._tunnel = QRadioButton("SSH tunnel")
        self._direct.setChecked(True)
        self._ssh_host = QLineEdit()
        self._ssh_host.setPlaceholderText("user@host or a host from ~/.ssh/config")
        self._address = QLineEdit("localhost:7155")
        self._name = QLineEdit()
        self._name.setPlaceholderText("optional, e.g. production")
        # A name filled in from a recent connection doesn't belong to a different address
        self._ssh_host.textEdited.connect(self._clear_filled_name)
        self._address.textEdited.connect(self._clear_filled_name)
        self._filled_name = ""
        self._address_label = QLabel()

        mode = QWidget()
        mode_layout = QHBoxLayout(mode)
        mode_layout.setContentsMargins(0, 0, 0, 0)
        mode_layout.addWidget(self._direct)
        mode_layout.addWidget(self._tunnel)
        mode_layout.addStretch()

        form = self._form = QFormLayout()
        form.addRow("Connection:", mode)
        form.addRow("SSH host:", self._ssh_host)
        form.addRow(self._address_label, self._address)
        form.addRow("Name:", self._name)

        self._status = QLabel()
        self._status.setWordWrap(True)
        self._status.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._status.hide()

        self._buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Cancel)
        self._connect_button = self._buttons.addButton("Connect", QDialogButtonBox.ButtonRole.AcceptRole)
        self._buttons.accepted.connect(lambda: self._connect(self._spec()))
        self._buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        self._recent_label = QLabel("Recent connections:")
        layout.addWidget(self._recent_label)
        layout.addWidget(self._recent)
        has_recent = self._recent.count() > 0
        self._recent_label.setVisible(has_recent)
        self._recent.setVisible(has_recent)
        layout.addLayout(form)
        layout.addWidget(self._status)
        layout.addWidget(self._buttons)
        # Only now that the button belongs to the dialog; before, Qt doesn't register it as the dialog's default, and
        # Enter in a text field would do nothing. Cancel shouldn't take over as default when it gets the focus.
        self._connect_button.setDefault(True)
        self._buttons.button(QDialogButtonBox.StandardButton.Cancel).setAutoDefault(False)

        if initial is not None:
            self._fill(initial)
        elif self._recent.count():
            self._recent.setCurrentRow(0)
            self._fill(self._recent.item(0).data(Qt.ItemDataRole.UserRole))
        self._update_mode()
        self._direct.toggled.connect(self._mode_changed)
        (self._recent if self._recent.count() else self._address).setFocus()
        self._start_checks()

    def _mode_changed(self) -> None:
        # Grows or shrinks the dialog by the SSH host row, instead of letting the list absorb it
        before = self._recent.height()
        self._update_mode()
        self.layout().activate()
        if self._recent.isVisible():
            self.resize(self.width(), self.height() + before - self._recent.height())
        else:
            self.resize(self.width(), self.minimumSizeHint().height())

    def _fill(self, spec: ConnectionSpec) -> None:
        self._tunnel.setChecked(bool(spec.ssh_host))
        self._direct.setChecked(not spec.ssh_host)
        self._ssh_host.setText(spec.ssh_host)
        self._address.setText(spec.address)
        self._name.setText(spec.name)
        self._filled_name = spec.name

    def _clear_filled_name(self) -> None:
        if self._filled_name and self._name.text() == self._filled_name:
            self._name.clear()
        self._filled_name = ""

    @staticmethod
    def _set_item_spec(item: QListWidgetItem, spec: ConnectionSpec) -> None:
        item.setText(spec.title)
        item.setData(Qt.ItemDataRole.UserRole, spec)

    def _rename_selected(self) -> None:
        item = self._recent.currentItem()
        if item is None:
            return
        spec = item.data(Qt.ItemDataRole.UserRole)
        name, ok = QInputDialog.getText(self, "Rename connection", f"Name for {spec.label}:", text=spec.name)
        if not ok:
            return
        renamed = settings.rename_recent_connection(spec, name)
        self._set_item_spec(item, renamed)
        if self._spec() == renamed:
            self._name.setText(renamed.name)
            self._filled_name = renamed.name

    def _remove_selected(self) -> None:
        item = self._recent.currentItem()
        if item is not None:
            settings.remove_recent_connection(item.data(Qt.ItemDataRole.UserRole))
            self._recent.takeItem(self._recent.row(item))

    def _update_mode(self) -> None:
        tunnel = self._tunnel.isChecked()
        self._form.setRowVisible(self._ssh_host, tunnel)
        self._address_label.setText("FossilDB address\n(seen from SSH host):" if tunnel else "FossilDB address:")

    def _spec(self) -> ConnectionSpec:
        ssh_host = self._ssh_host.text().strip() if self._tunnel.isChecked() else ""
        return ConnectionSpec(self._address.text().strip(), ssh_host, self._name.text().strip())

    def _double_clicked(self, item: QListWidgetItem) -> None:
        if QGuiApplication.mouseButtons() & Qt.MouseButton.LeftButton or not QGuiApplication.mouseButtons():
            self._connect(item.data(Qt.ItemDataRole.UserRole))

    def eventFilter(self, watched, event) -> bool:
        # Enter in the recent connections connects to the chosen one
        if (
            watched is self._recent
            and event.type() == QEvent.Type.KeyPress
            and event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter)
            and self._recent.currentItem() is not None
        ):
            self._connect(self._recent.currentItem().data(Qt.ItemDataRole.UserRole))
            return True
        return super().eventFilter(watched, event)

    def connect_to(self, spec: ConnectionSpec) -> None:
        self._fill(spec)
        self._connect(spec)

    def _connect(self, spec: ConnectionSpec) -> None:
        if not spec.address:
            self._show_status("Please enter the FossilDB address, e.g. localhost:7155")
            return
        self._fill(spec)
        # Disabling the inputs takes away their focus, so it is given back if connecting fails
        self._focus_before = QApplication.focusWidget()
        self._set_busy(True)
        self._show_status(f"Connecting to {spec.title} …")

        def connect():
            # Imported here, so that the window shows before grpc is loaded
            from fossildb_gui.connection import connect

            connection = connect(spec.address, spec.ssh_host or None)
            try:
                return connection, connection.database_info()
            except Exception:
                connection.close()
                raise

        generation = self._latest.next()
        self._runner.submit(
            connect,
            lambda result: self._connected(generation, spec, *result),
            self._latest.wrap(generation, self._failed),
        )

    def _connected(self, generation: int, spec: ConnectionSpec, connection, info) -> None:
        if not self._latest.is_current(generation):
            # Cancelled in the meantime
            self._runner.submit(connection.close)
            return
        spec = settings.add_recent_connection(spec)
        self._set_busy(False)
        self.connected.emit(spec, connection, info)
        self.accept()

    def _failed(self, error: Exception) -> None:
        self._set_busy(False)
        self._show_status(f"<b>Connection failed:</b> {error}")
        focus = self._focus_before
        if focus is not None and self.isAncestorOf(focus) and focus.isVisible() and focus.isEnabled():
            focus.setFocus()

    def _show_status(self, text: str) -> None:
        self._status.setText(text)
        self._status.show()

    def _set_busy(self, busy: bool) -> None:
        self._connect_button.setEnabled(not busy)
        for widget in (self._recent, self._direct, self._tunnel, self._ssh_host, self._address, self._name):
            widget.setEnabled(not busy)
        if not busy:
            self._update_mode()

    def reject(self) -> None:
        # A connection attempt that is still running is closed when it completes
        self._latest.invalidate()
        super().reject()

    def done(self, result: int) -> None:
        self._checks.shutdown()
        super().done(result)

    # Checking the recent connections, and looking for servers on this computer

    def _start_checks(self) -> None:
        # Their own threads, so that they never delay connecting
        self._checks = TaskRunner(max_workers=8, parent=self)
        for spec in settings.recent_connections():
            if not spec.ssh_host:  # checking a tunnel would mean opening an SSH connection for each
                self._checks.submit(
                    lambda spec=spec: _discovery().check(spec.address),
                    lambda error, spec=spec: self._checked(spec, error),
                    context=self,
                )
        # Servers in the recent connections are listed already, and the app's own tunnels lead to remote ones
        known = {_local_port(spec) for spec in settings.recent_connections()} - {None} | local_ports_in_use()
        self._checks.submit(lambda: _discovery().find_local(known), self._found_local, context=self)

    def _checked(self, spec: ConnectionSpec, error: str | None) -> None:
        for row in range(self._recent.count()):
            item = self._recent.item(row)
            if item.data(Qt.ItemDataRole.UserRole) == spec:
                item.setIcon(_dot(_GREEN if error is None else _GRAY))
                item.setToolTip("FossilDB answers" if error is None else f"Not reachable: {error}")

    def _found_local(self, ports: list[int]) -> None:
        if not ports:
            return
        if self._recent.count():
            header = QListWidgetItem("On this computer")
            header.setFlags(Qt.ItemFlag.NoItemFlags)
            font = header.font()
            font.setItalic(True)
            header.setFont(font)
            self._recent.addItem(header)
        else:
            self._recent_label.setText("FossilDB on this computer:")
        for port in ports:
            item = QListWidgetItem()
            self._set_item_spec(item, ConnectionSpec(f"localhost:{port}"))
            item.setIcon(_dot(_GREEN))
            item.setToolTip(f"FossilDB answers on port {port} of this computer")
            self._recent.addItem(item)
        if not self._recent.isVisible():
            self._recent_label.show()
            self._recent.show()
