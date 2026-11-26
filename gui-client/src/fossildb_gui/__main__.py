"""Entry point: fossildb-gui [address] [--ssh host]"""

import argparse
import sys
from pathlib import Path

from PySide6.QtCore import QTimer
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication

from fossildb_gui import __version__


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="fossildb-gui", description="Desktop client for FossilDB")
    parser.add_argument("address", nargs="?", help="connect to this FossilDB address (host:port) right away")
    parser.add_argument("--ssh", metavar="HOST", help="connect through an SSH tunnel to HOST")
    parser.add_argument("--version", action="version", version=__version__)
    return parser.parse_args(argv)


def _app_icon() -> QIcon:
    # Rendered from the repository's icon.svg by scripts/render_icon.py
    icon = QIcon()
    for path in sorted((Path(__file__).parent / "resources").glob("icon-*.png")):
        icon.addFile(str(path))
    return icon


class Application:
    def __init__(self, qt_app: QApplication):
        from fossildb_gui import fmt, settings
        from fossildb_gui.schema_manager import SchemaManager

        fmt.set_decimal(settings.decimal_sizes())
        from fossildb_gui.tasks import TaskRunner

        self._qt_app = qt_app
        self.runner = TaskRunner(parent=qt_app)
        self.schemas = SchemaManager(self.runner, parent=qt_app)
        self._open: set = set()  # windows and dialogs, kept referenced while open
        qt_app.aboutToQuit.connect(self.runner.shutdown)
        qt_app.aboutToQuit.connect(settings.flush)

    def show_connect_dialog(self, spec=None, connect_now: bool = False) -> None:
        from fossildb_gui.connect_dialog import ConnectDialog

        dialog = ConnectDialog(self.runner, spec)
        dialog.connected.connect(self._open_window)
        dialog.finished.connect(lambda _: self._open.discard(dialog))
        self._open.add(dialog)
        dialog.show()
        if connect_now:
            dialog.connect_to(spec)

    def _open_window(self, spec, connection, info) -> None:
        from fossildb_gui.main_window import MainWindow

        window = MainWindow(spec, connection, info, self.runner, self.schemas)
        window.new_connection_requested.connect(self.show_connect_dialog)
        window.destroyed.connect(lambda: self._open.discard(window))
        self._open.add(window)
        window.show()


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    qt_app = QApplication(sys.argv[:1])
    qt_app.setApplicationName("FossilDB Client")
    qt_app.setOrganizationName("scalableminds")
    qt_app.setOrganizationDomain("scalableminds.com")
    qt_app.setWindowIcon(_app_icon())

    from fossildb_gui.settings import ConnectionSpec

    app = Application(qt_app)
    if args.address:
        app.show_connect_dialog(ConnectionSpec(args.address, args.ssh or ""), connect_now=True)
    else:
        app.show_connect_dialog()

    def warm_up():
        # Loads grpc, the API and the schemas while the user looks at the connect dialog
        app.runner.submit(lambda: __import__("fossildb_gui.connection"))
        app.schemas.load()

    QTimer.singleShot(0, warm_up)
    sys.exit(qt_app.exec())


if __name__ == "__main__":
    main()
