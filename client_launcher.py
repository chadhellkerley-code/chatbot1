# client_launcher.py
# -*- coding: utf-8 -*-
"""Punto de entrada para ejecutables generados."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _configure_playwright_browsers() -> None:
    if os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        return
    base = getattr(sys, "_MEIPASS", None)
    if base:
        candidate = Path(base) / "playwright_browsers"
    else:
        candidate = Path(__file__).resolve().parent / "playwright_browsers"
    if not candidate.exists():
        exe_parent = Path(getattr(sys, "executable", "")).resolve().parent
        candidate = exe_parent / "playwright_browsers"
    if candidate.exists():
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(candidate)


_configure_playwright_browsers()

from license_client import launch_with_license


if __name__ == "__main__":
    launch_with_license()
