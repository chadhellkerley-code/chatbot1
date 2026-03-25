from __future__ import annotations

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from bootstrap import bootstrap_application
from gui.gui_app import launch_gui_app


if __name__ == "__main__":
    os.environ.setdefault("LICENSE_ALREADY_VALIDATED", "1")
    bootstrap_application(
        "owner",
        install_root_hint=_PROJECT_ROOT,
        app_root_hint=_PROJECT_ROOT,
        force=True,
        defer_housekeeping=True,
    )
    raise SystemExit(launch_gui_app(mode="owner"))
