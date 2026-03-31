# owner_gui_launcher.py
# -*- coding: utf-8 -*-
"""Entrypoint dedicado para build owner GUI."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from bootstrap import bootstrap_application


def _has_owner_account_data(base: Path) -> bool:
    return (
        (base / "storage" / "accounts" / "accounts.json").exists()
        or (base / "data" / "accounts" / "accounts.json").exists()
    )


def _maybe_reuse_owner_project_data() -> None:
    if os.environ.get("APP_DATA_ROOT"):
        return
    if not getattr(sys, "frozen", False):
        return

    try:
        exe_dir = Path(sys.executable).resolve().parent
    except Exception:
        return

    # If the bundle already has explicit data, keep bundle-local mode.
    if _has_owner_account_data(exe_dir):
        return

    # Dev fallback: when running from <repo>/dist/<bundle>/, reuse repo owner data.
    candidate = exe_dir.parent.parent
    if not candidate.exists():
        return
    if not (candidate / "runtime" / "runtime_parity.py").exists():
        return
    if not _has_owner_account_data(candidate):
        return
    os.environ["APP_DATA_ROOT"] = str(candidate)


if __name__ == "__main__":
    # El build owner no debe bloquearse por gate de licencia de distribucion.
    os.environ.setdefault("LICENSE_ALREADY_VALIDATED", "1")
    _maybe_reuse_owner_project_data()
    bootstrap_application("owner", defer_housekeeping=True)
    from gui.gui_app import launch_gui_app

    raise SystemExit(launch_gui_app(mode="owner"))
