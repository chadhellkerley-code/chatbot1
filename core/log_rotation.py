from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from paths import logs_root, screenshots_root, storage_root, traces_root

logger = logging.getLogger(__name__)
_DAILY_ARCHIVE_STEM_RE = re.compile(
    r"^(?P<base>.+?)(?P<dates>(?:\.\d{8})+)(?P<counter>\.\d+)?$"
)


def _resolve_path(path: str | Path) -> Path:
    return Path(path)


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _daily_archive_parts(path: Path) -> tuple[str, list[str], str] | None:
    match = _DAILY_ARCHIVE_STEM_RE.match(path.stem)
    if match is None:
        return None
    base = str(match.group("base") or "").strip(".")
    if not base:
        return None
    dates = [token for token in str(match.group("dates") or "").split(".") if token]
    if not dates:
        return None
    counter = str(match.group("counter") or "")
    return base, dates, counter


def _is_daily_archive(path: Path) -> bool:
    return _daily_archive_parts(path) is not None


def normalize_repeated_daily_archive(path: str | Path) -> Path | None:
    target = _resolve_path(path)
    parts = _daily_archive_parts(target)
    if parts is None:
        return None
    base, dates, counter = parts
    if len(dates) < 2 or len(set(dates)) != 1:
        return None

    candidate = target.with_name(f"{base}.{dates[-1]}{counter}{target.suffix}")
    if candidate == target:
        return None

    dedupe_counter = 1
    while candidate.exists():
        candidate = target.with_name(f"{base}.{dates[-1]}.{dedupe_counter}{target.suffix}")
        dedupe_counter += 1

    try:
        os.replace(target, candidate)
        logger.info("Normalized repeated archived log %s -> %s", target, candidate)
        return candidate
    except Exception:
        logger.exception("No se pudo normalizar archivo archivado repetido %s", target)
        return None


def rotate_jsonl(path: str | Path, max_size_mb: float) -> Path | None:
    target = _resolve_path(path)
    if not target.exists():
        return None
    max_bytes = max(1, int(float(max_size_mb) * 1024 * 1024))
    try:
        if target.stat().st_size < max_bytes:
            return None
    except Exception:
        return None

    rotated = target.with_name(f"{target.stem}.{_timestamp()}{target.suffix}")
    counter = 1
    while rotated.exists():
        rotated = target.with_name(f"{target.stem}.{_timestamp()}.{counter}{target.suffix}")
        counter += 1
    try:
        os.replace(target, rotated)
        logger.info("Rotated JSONL log %s -> %s", target, rotated)
        return rotated
    except Exception:
        logger.exception("No se pudo rotar JSONL %s", target)
        return None


def rotate_daily_file(path: str | Path) -> Path | None:
    target = _resolve_path(path)
    if not target.exists():
        return None
    if _is_daily_archive(target):
        return None
    try:
        modified = datetime.fromtimestamp(target.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return None
    today = datetime.now(timezone.utc).date()
    if modified.date() >= today:
        return None
    day_label = modified.strftime("%Y%m%d")
    rotated = target.with_name(f"{target.stem}.{day_label}{target.suffix}")
    counter = 1
    while rotated.exists():
        rotated = target.with_name(f"{target.stem}.{day_label}.{counter}{target.suffix}")
        counter += 1
    try:
        os.replace(target, rotated)
        logger.info("Rotated daily log %s -> %s", target, rotated)
        return rotated
    except Exception:
        logger.exception("No se pudo rotar log diario %s", target)
        return None


def cleanup_old_files(directory: str | Path, max_files: int) -> list[Path]:
    target = _resolve_path(directory)
    if max_files < 0 or not target.exists() or not target.is_dir():
        return []
    try:
        files = [item for item in target.iterdir() if item.is_file()]
    except Exception:
        logger.exception("No se pudo inspeccionar directorio para limpieza: %s", target)
        return []
    if len(files) <= max_files:
        return []
    files.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    removed: list[Path] = []
    for item in files[max_files:]:
        try:
            item.unlink()
            removed.append(item)
        except Exception:
            logger.exception("No se pudo eliminar archivo antiguo: %s", item)
    if removed:
        logger.info("Cleaned %d old files from %s", len(removed), target)
    return removed


def run_retention_maintenance(root: str | Path) -> dict[str, object]:
    base = _resolve_path(root)
    storage_dir = storage_root(base)
    logs_dir = logs_root(base)
    legacy_logs_dir = storage_dir / "logs"
    log_dirs = [logs_dir]
    if legacy_logs_dir != logs_dir:
        log_dirs.append(legacy_logs_dir)
    rotated: list[str] = []
    cleaned: list[str] = []
    normalized: list[str] = []

    jsonl_candidates: list[Path] = []
    for candidate_dir in [storage_dir, *log_dirs]:
        if not candidate_dir.exists():
            continue
        jsonl_candidates.extend(item for item in candidate_dir.glob("*.jsonl") if item.is_file())
    for path in jsonl_candidates:
        rotated_path = rotate_jsonl(path, max_size_mb=20.0)
        if rotated_path is not None:
            rotated.append(str(rotated_path))

    daily_candidates: list[Path] = []
    if storage_dir.exists():
        daily_candidates.extend(
            item
            for item in storage_dir.iterdir()
            if item.is_file() and item.suffix.lower() in {".log", ".txt"}
        )
    for candidate_dir in log_dirs:
        if candidate_dir.exists():
            daily_candidates.extend(
                item
                for item in candidate_dir.iterdir()
                if item.is_file() and item.suffix.lower() in {".log", ".txt"}
            )
    normalized_candidates: list[Path] = []
    for path in daily_candidates:
        normalized_path = normalize_repeated_daily_archive(path)
        if normalized_path is not None:
            normalized.append(str(normalized_path))
            normalized_candidates.append(normalized_path)
        else:
            normalized_candidates.append(path)
    for path in normalized_candidates:
        rotated_path = rotate_daily_file(path)
        if rotated_path is not None:
            rotated.append(str(rotated_path))

    for directory in (
        storage_dir / "debug_screenshots",
        screenshots_root(base),
        storage_dir / "dm_failures",
        traces_root(base),
    ):
        removed = cleanup_old_files(directory, max_files=200)
        cleaned.extend(str(item) for item in removed)

    return {
        "normalized": normalized,
        "rotated": rotated,
        "cleaned": cleaned,
    }
