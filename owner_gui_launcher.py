from __future__ import annotations

import os
import runpy
from pathlib import Path


if __name__ == "__main__":
    # El build owner no debe bloquearse por gate de licencia de distribución.
    os.environ.setdefault("LICENSE_ALREADY_VALIDATED", "1")

    target = Path(__file__).resolve().parent / "launchers" / "owner_gui_launcher.py"
    runpy.run_path(str(target), run_name="__main__")
