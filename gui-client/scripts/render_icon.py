"""Renders the repository's icon.svg to the PNG app icons in src/fossildb_gui/resources.

The PNGs are checked in, because loading the large SVG would slow down every start.
Run again after icon.svg changes: uv run python scripts/render_icon.py
"""

from pathlib import Path

from PySide6.QtCore import QSize
from PySide6.QtGui import QGuiApplication, QIcon

SIZES = [16, 24, 32, 48, 64, 128, 256, 512]

root = Path(__file__).resolve().parent.parent
app = QGuiApplication(["render_icon", "-platform", "offscreen"])
icon = QIcon(str(root.parent / "icon.svg"))
for size in SIZES:
    pixmap = icon.pixmap(QSize(size, size))
    assert not pixmap.isNull(), "Rendering icon.svg failed"
    pixmap.save(str(root / "src/fossildb_gui/resources" / f"icon-{size}.png"))
