from __future__ import annotations

import copy
import json
import logging
import os
import shutil
import tempfile
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("storage.integrity")
_REPLACE_RETRY_DELAYS = (0.01, 0.05, 0.1)

_LOCKS_GUARD = threading.RLock()
_PATH_LOCKS: dict[str, threading.RLock] = {}
_CACHE_GUARD = threading.RLock()
_JSON_CACHE: OrderedDict[str, tuple[tuple[int, int], Any]] = OrderedDict()
_JSONL_CACHE: OrderedDict[str, tuple[tuple[int, int], list[Any]]] = OrderedDict()
_CACHE_LIMIT = 256


def _normalize_path(path: str | Path) -> Path:
    return Path(path)


def _clone_default(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _path_lock(path: str | Path) -> threading.RLock:
    normalized = str(_normalize_path(path).resolve())
    with _LOCKS_GUARD:
        current = _PATH_LOCKS.get(normalized)
        if current is None:
            current = threading.RLock()
            _PATH_LOCKS[normalized] = current
        return current


def path_lock(path: str | Path) -> threading.RLock:
    """Expose the per-path re-entrant lock for multi-step atomic updates."""
    return _path_lock(path)


def _cache_key(path: str | Path) -> str:
    return str(_normalize_path(path).resolve())


def _cache_token(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return int(stat.st_mtime_ns), int(stat.st_size)


def _cache_lookup(
    cache: OrderedDict[str, tuple[tuple[int, int], Any]],
    key: str,
    token: tuple[int, int],
) -> Any | None:
    with _CACHE_GUARD:
        entry = cache.get(key)
        if entry is None or entry[0] != token:
            return None
        cache.move_to_end(key)
        return copy.deepcopy(entry[1])


def _cache_store(
    cache: OrderedDict[str, tuple[tuple[int, int], Any]],
    key: str,
    token: tuple[int, int],
    value: Any,
) -> None:
    with _CACHE_GUARD:
        cache[key] = (token, copy.deepcopy(value))
        cache.move_to_end(key)
        while len(cache) > _CACHE_LIMIT:
            cache.popitem(last=False)


def _invalidate_cache(path: str | Path) -> None:
    key = _cache_key(path)
    with _CACHE_GUARD:
        _JSON_CACHE.pop(key, None)
        _JSONL_CACHE.pop(key, None)


def atomic_replace_file(path: str | Path, temp_path: str | Path) -> Path:
    target = _normalize_path(path)
    pending = _normalize_path(temp_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    for attempt, delay in enumerate((0.0, *_REPLACE_RETRY_DELAYS), start=1):
        try:
            if delay:
                time.sleep(delay)
            os.replace(pending, target)
            break
        except PermissionError:
            if attempt == len(_REPLACE_RETRY_DELAYS) + 1:
                raise
    _invalidate_cache(target)
    return target


def _atomic_replace_bytes(path: str | Path, payload: bytes) -> Path:
    target = _normalize_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        dir=str(target.parent),
        prefix=f"{target.name}.",
        suffix=".tmp",
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        return atomic_replace_file(target, temp_path)
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def atomic_write_json(path: str | Path, data: Any) -> Path:
    target = _normalize_path(path)
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    lock = _path_lock(target)
    with lock:
        return _atomic_replace_bytes(target, payload)


def atomic_write_text(path: str | Path, text: str, *, encoding: str = "utf-8") -> Path:
    target = _normalize_path(path)
    payload = text.encode(encoding)
    lock = _path_lock(target)
    with lock:
        return _atomic_replace_bytes(target, payload)


def _backup_corrupted_file(path: Path, *, reason: str = "json_parse_error") -> Path | None:
    if not path.exists():
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = path.with_name(f"{path.name}.{reason}.{stamp}.bak")
    counter = 1
    while backup_path.exists():
        backup_path = path.with_name(f"{path.name}.{reason}.{stamp}.{counter}.bak")
        counter += 1
    try:
        shutil.copy2(path, backup_path)
        return backup_path
    except Exception:
        logger.exception("No se pudo respaldar archivo corrupto: %s", path)
        return None


def _repair_corrupted_json_file(path: Path, default: Any, *, label: str, error: Exception) -> Any:
    backup_path = _backup_corrupted_file(path)
    logger.error(
        "Archivo JSON corrupto detectado en %s%s: %s",
        path,
        f" ({label})" if label else "",
        error,
    )
    if backup_path is not None:
        logger.error("Respaldo de integridad creado en %s", backup_path)
    repaired = _clone_default(default)
    try:
        if isinstance(repaired, (dict, list)):
            atomic_write_json(path, repaired)
        else:
            atomic_write_text(path, str(repaired))
    except Exception:
        logger.exception("No se pudo re-crear archivo limpio para %s", path)
    return repaired


def load_json_file(
    path: str | Path,
    default: Any,
    *,
    label: str = "",
    repair_on_error: bool = True,
) -> Any:
    target = _normalize_path(path)
    if not target.exists():
        return _clone_default(default)
    key = _cache_key(target)
    try:
        token = _cache_token(target)
    except Exception:
        token = None
    if token is not None:
        cached = _cache_lookup(_JSON_CACHE, key, token)
        if cached is not None:
            return cached
    try:
        with target.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except json.JSONDecodeError as exc:
        if not repair_on_error:
            raise
        return _repair_corrupted_json_file(target, default, label=label, error=exc)
    except Exception:
        raise
    if token is not None:
        _cache_store(_JSON_CACHE, key, token, loaded)
    return _clone_default(loaded)


def load_jsonl_entries(path: str | Path, *, label: str = "") -> list[Any]:
    target = _normalize_path(path)
    if not target.exists():
        return []
    key = _cache_key(target)
    try:
        token = _cache_token(target)
    except Exception:
        token = None
    if token is not None:
        cached = _cache_lookup(_JSONL_CACHE, key, token)
        if cached is not None:
            return cached
    items: list[Any] = []
    valid_lines: list[str] = []
    backup_done = False
    corrupted = False
    try:
        with target.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                raw = line.strip()
                if not raw:
                    continue
                try:
                    items.append(json.loads(raw))
                    valid_lines.append(raw)
                except json.JSONDecodeError as exc:
                    corrupted = True
                    if not backup_done:
                        backup_done = _backup_corrupted_file(target, reason="jsonl_parse_error") is not None
                    logger.error(
                        "Archivo JSONL corrupto detectado en %s%s linea=%d: %s",
                        target,
                        f" ({label})" if label else "",
                        line_no,
                        exc,
                    )
                    continue
    except Exception:
        raise
    if corrupted:
        try:
            payload = "\n".join(valid_lines)
            if payload:
                payload += "\n"
            atomic_write_text(target, payload)
        except Exception:
            logger.exception("No se pudo re-crear JSONL limpio para %s", target)
        return items
    if token is not None:
        _cache_store(_JSONL_CACHE, key, token, items)
    return items


def atomic_append_jsonl(path: str | Path, entry: Any, *, max_size_mb: float = 20.0) -> Path:
    target = _normalize_path(path)
    line = json.dumps(entry, ensure_ascii=False).encode("utf-8") + b"\n"
    lock = _path_lock(target)
    with lock:
        from core.log_rotation import rotate_jsonl

        rotate_jsonl(target, max_size_mb=max_size_mb)
        existing = b""
        if target.exists():
            existing = target.read_bytes()
        return _atomic_replace_bytes(target, existing + line)
