from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def move_file_to_backup(path: str | Path, backup_dir: str | Path) -> Path:
    source = Path(path)
    target_dir = Path(backup_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = target_dir / f"{source.name}.deleted.{stamp}.bak"
    counter = 1
    while target.exists():
        target = target_dir / f"{source.name}.deleted.{stamp}.{counter}.bak"
        counter += 1
    return source.replace(target)
