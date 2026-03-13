from __future__ import annotations

import argparse
from pathlib import Path
import sys

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from build.helpers import (
    build_client_distribution,
    current_version,
    dist_root,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the InstaCRM client package.")
    parser.add_argument("--license-payload", type=Path, default=None)
    parser.add_argument("--version", default="")
    parser.add_argument("--no-playwright", action="store_true")
    args = parser.parse_args()

    version = str(args.version or current_version()).strip() or "dev"
    final_dir = dist_root() / "InstaCRM"
    build_client_distribution(
        target_dir=final_dir,
        version=version,
        license_payload=args.license_payload,
        bundle_playwright=not args.no_playwright,
    )
    print(f"Client folder: {final_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
