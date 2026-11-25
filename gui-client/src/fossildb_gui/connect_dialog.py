"""The window for choosing a FossilDB server to connect to."""

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
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


class ConnectDialog(QDialog):
    # Emitted with the open Connection and its DatabaseInfoProto (None for servers without GetDatabaseInfo)
    connected = Signal(object, object, object)  # spec, connection, info

    def __init__(self, runner: TaskRunner, initial: ConnectionSpec | None = None, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle("Connect to FossilDB")
        self.setMinimumWidth(460)
        self._runner = runner
        self._latest = Latest()

        self._recent = QListWidget()
        for spec in settings.recent_connections():
            item = QListWidgetItem(spec.label)
            item.setData(Qt.ItemDataRole.UserRole, spec)
            self._recent.addItem(item)
        self._recent.itemClicked.connect(lambda item: self._fill(item.data(Qt.ItemDataRole.UserRole)))
        self._recent.itemActivated.connect(lambda item: self._connect(item.data(Qt.ItemDataRole.UserRole)))

        self._direct = QRadioButton("Direct")
        self._tunnel = QRadioButton("SSH tunnel")
        self._direct.setChecked(True)
        self._ssh_host = QLineEdit()
        self._ssh_host.setPlaceholderText("user@host or a host from ~/.ssh/config")
        self._address = QLineEdit("localhost:7155")
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

        self._status = QLabel()
        self._status.setWordWrap(True)
        self._status.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._status.hide()

        self._buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Cancel)
        self._connect_button = self._buttons.addButton("Connect", QDialogButtonBox.ButtonRole.AcceptRole)
        self._connect_button.setDefault(True)
        self._buttons.accepted.connect(lambda: self._connect(self._spec()))
        self._buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        if self._recent.count():
            layout.addWidget(QLabel("Recent connections:"))
            layout.addWidget(self._recent)
        layout.addLayout(form)
        layout.addWidget(self._status)
        layout.addWidget(self._buttons)

        if initial is not None:
            self._fill(initial)
        elif self._recent.count():
            self._recent.setCurrentRow(0)
            self._fill(self._recent.item(0).data(Qt.ItemDataRole.UserRole))
        self._update_mode()
        self._direct.toggled.connect(self._mode_changed)

    def _mode_changed(self) -> None:
        # Grows or shrinks the dialog by the SSH host row, instead of letting the list absorb it
        before = self._recent.height()
        self._update_mode()
        self.layout().activate()
        if self._recent.count():
            self.resize(self.width(), self.height() + before - self._recent.height())
        else:
            self.resize(self.width(), self.minimumSizeHint().height())

    def _fill(self, spec: ConnectionSpec) -> None:
        self._tunnel.setChecked(bool(spec.ssh_host))
        self._direct.setChecked(not spec.ssh_host)
        self._ssh_host.setText(spec.ssh_host)
        self._address.setText(spec.address)

    def _update_mode(self) -> None:
        tunnel = self._tunnel.isChecked()
        self._form.setRowVisible(self._ssh_host, tunnel)
        self._address_label.setText("FossilDB address\n(seen from SSH host):" if tunnel else "FossilDB address:")

    def _spec(self) -> ConnectionSpec:
        ssh_host = self._ssh_host.text().strip() if self._tunnel.isChecked() else ""
        return ConnectionSpec(self._address.text().strip(), ssh_host)

    def connect_to(self, spec: ConnectionSpec) -> None:
        self._fill(spec)
        self._connect(spec)

    def _connect(self, spec: ConnectionSpec) -> None:
        if not spec.address:
            self._show_status("Please enter the FossilDB address, e.g. localhost:7155")
            return
        self._fill(spec)
        self._set_busy(True)
        self._show_status(f"Connecting to {spec.label} …")

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
        settings.add_recent_connection(spec)
        self._set_busy(False)
        self.connected.emit(spec, connection, info)
        self.accept()

    def _failed(self, error: Exception) -> None:
        self._set_busy(False)
        self._show_status(f"<b>Connection failed:</b> {error}")

    def _show_status(self, text: str) -> None:
        self._status.setText(text)
        self._status.show()

    def _set_busy(self, busy: bool) -> None:
        self._connect_button.setEnabled(not busy)
        for widget in (self._recent, self._direct, self._tunnel, self._ssh_host, self._address):
            widget.setEnabled(not busy)
        if not busy:
            self._update_mode()

    def reject(self) -> None:
        # A connection attempt that is still running is closed when it completes
        self._latest.invalidate()
        super().reject()
