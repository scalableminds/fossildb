"""What the panels of a window need from it, so that they don't depend on the main window itself."""

from typing import Protocol


class Host(Protocol):
    writes_enabled: bool  # whether the user enabled writes (deletes, compactions, …) in this window

    def confirm_write(self, title: str, text: str, require_text: str | None = None) -> bool:
        """Asks before a destructive operation. With require_text, the user has to type it to confirm."""

    def show_status(self, message: str, timeout_ms: int = 0) -> None:
        """Shows the message in the status bar; an empty one clears it."""
