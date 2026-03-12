# client_launcher.py
# -*- coding: utf-8 -*-
"""Punto de entrada para ejecutables generados."""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from launchers.client_launcher import _launch_entrypoint


if __name__ == "__main__":
    raise SystemExit(_launch_entrypoint())
