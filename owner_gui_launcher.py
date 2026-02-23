# owner_gui_launcher.py
# -*- coding: utf-8 -*-
"""Entrypoint dedicado para build owner GUI."""

from __future__ import annotations

import os

from gui_app import launch_gui_app


if __name__ == "__main__":
    # El build owner no debe bloquearse por gate de licencia de distribucion.
    os.environ.setdefault("LICENSE_ALREADY_VALIDATED", "1")
    raise SystemExit(launch_gui_app(mode="owner"))
