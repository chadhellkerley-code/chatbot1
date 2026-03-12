from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QFont, QIcon, QPainter, QPixmap


def _build_brand_logo_pixmap(size: int) -> QPixmap:
    pixmap = QPixmap(size, size)
    pixmap.fill(Qt.transparent)

    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.Antialiasing, True)
    painter.setPen(Qt.NoPen)
    painter.setBrush(QColor("#0b1220"))
    painter.drawRoundedRect(0, 0, size, size, 5, 5)

    font = QFont("Consolas")
    font.setBold(True)
    font.setPixelSize(max(9, int(size * 0.55)))
    painter.setFont(font)

    baseline_y = int(size * 0.72)
    painter.setPen(QColor("#ffffff"))
    painter.drawText(int(size * 0.18), baseline_y, ">")
    painter.setPen(QColor("#2563eb"))
    painter.drawText(int(size * 0.45), baseline_y, "_")
    painter.end()
    return pixmap


def _build_brand_icon() -> QIcon:
    icon = QIcon()
    for size in (16, 20, 24, 32, 48):
        icon.addPixmap(_build_brand_logo_pixmap(size))
    return icon

