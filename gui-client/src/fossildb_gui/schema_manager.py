"""Holds the loaded .proto schemas for the whole application."""

from pathlib import Path

from PySide6.QtCore import QObject, Signal

from fossildb_gui import settings
from fossildb_gui.tasks import TaskRunner


class SchemaManager(QObject):
    changed = Signal()  # schemas were loaded, reloaded or failed to load

    def __init__(self, runner: TaskRunner, parent: QObject | None = None):
        super().__init__(parent)
        self._runner = runner
        self.schemas = None  # schemas.SchemaSet
        self.error: str | None = None
        self.loading = False

    @property
    def folder(self) -> str:
        return settings.schema_folder()

    def load(self, folder: str | None = None) -> None:
        """Loads the schemas from the folder (or the configured one) in the background."""
        if folder is not None:
            settings.set_schema_folder(folder)
        folder = self.folder
        if not folder:
            return
        self.loading = True

        def compile_schemas():
            from fossildb_gui.schemas import load_schemas

            return load_schemas(Path(folder))

        self._runner.submit(compile_schemas, self._loaded, self._failed, context=self)

    def _loaded(self, schemas) -> None:
        self.schemas, self.error, self.loading = schemas, None, False
        self.changed.emit()

    def _failed(self, error: Exception) -> None:
        self.schemas, self.error, self.loading = None, str(error), False
        self.changed.emit()
