from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from build.helpers import (
    OWNER_EXE_STEM,
    assemble_owner_layout,
    build_onefile_executable,
    current_version,
    dist_root,
    prepare_workspace,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the InstaCRM owner executable.")
    parser.add_argument("--version", default="")
    parser.add_argument("--no-playwright", action="store_true")
    args = parser.parse_args()

    version = str(args.version or current_version()).strip() or "dev"
    temp_root, workspace = prepare_workspace("instacrm_owner_build")
    try:
        built = build_onefile_executable(
            workspace,
            entrypoint="launchers/owner_gui_launcher.py",
            exe_name=OWNER_EXE_STEM,
            include_playwright=not args.no_playwright,
            windowed=True,
        )
        final_dir = dist_root() / "InstaCRM_owner"
        assemble_owner_layout(
            built,
            target_dir=final_dir,
            version=version,
            bundle_playwright=not args.no_playwright,
        )
        if built.exists():
            built.unlink()
        print(f"Owner folder: {final_dir}")
        return 0
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
