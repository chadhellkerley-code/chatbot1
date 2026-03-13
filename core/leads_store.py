from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from core.storage_backups import move_file_to_backup
from core.storage_atomic import atomic_write_json, atomic_write_text, load_json_file, path_lock

_INVISIBLE_USERNAME_CHARS = ("\ufeff", "\u200b", "\u200c", "\u200d", "\u200e", "\u200f")
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}
_WINDOWS_INVALID_FILENAME_CHARS = frozenset('<>:"/\\|?*')


class LeadListStoreError(ValueError):
    pass


def normalize_lead_username(raw: object) -> str:
    value = str(raw or "").strip()
    for bad in _INVISIBLE_USERNAME_CHARS:
        value = value.replace(bad, "")
    value = value.strip().lstrip("@").strip()
    if not value:
        return ""
    return value.strip()


def normalize_lead_usernames(values: Iterable[object]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in values:
        username = normalize_lead_username(raw)
        key = username.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(username)
    return ordered


class LeadListStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.deleted_dir = self.root / "_deleted"
        self._summary_index_path = self.root / ".lead_list_index.json"

    @staticmethod
    def _read_path(path: Path) -> list[str]:
        return normalize_lead_usernames(path.read_text(encoding="utf-8").splitlines())

    @staticmethod
    def _serialize_usernames(usernames: Iterable[object]) -> str:
        payload = "\n".join(normalize_lead_usernames(usernames))
        if payload:
            payload += "\n"
        return payload

    @staticmethod
    def _summary_entry_from_path(path: Path, *, count: int) -> dict[str, int]:
        stat = path.stat()
        return {
            "count": max(0, int(count)),
            "mtime_ns": int(stat.st_mtime_ns),
            "size_bytes": int(stat.st_size),
        }

    @staticmethod
    def _count_usernames_from_path(path: Path) -> int:
        seen: set[str] = set()
        count = 0
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                username = normalize_lead_username(raw_line)
                key = username.lower()
                if not key or key in seen:
                    continue
                seen.add(key)
                count += 1
        return count

    def _load_summary_index(self) -> dict[str, dict[str, int]]:
        payload = load_json_file(self._summary_index_path, {}, label="lead_list_store.summary_index")
        if not isinstance(payload, dict):
            return {}
        index: dict[str, dict[str, int]] = {}
        for raw_name, raw_entry in payload.items():
            if not isinstance(raw_entry, dict):
                continue
            name = str(raw_name or "").strip()
            if not name:
                continue
            try:
                index[name] = {
                    "count": max(0, int(raw_entry.get("count") or 0)),
                    "mtime_ns": max(0, int(raw_entry.get("mtime_ns") or 0)),
                    "size_bytes": max(0, int(raw_entry.get("size_bytes") or 0)),
                }
            except Exception:
                continue
        return index

    def _save_summary_index(self, index: dict[str, dict[str, int]]) -> None:
        atomic_write_json(self._summary_index_path, index)

    def _update_summary_index(self, name: str, *, count: int, path: Path | None = None) -> None:
        target = path or self.path_for(name)
        with path_lock(self._summary_index_path):
            index = self._load_summary_index()
            if target.exists():
                index[name] = self._summary_entry_from_path(target, count=count)
            else:
                index.pop(name, None)
            self._save_summary_index(index)

    def _clear_summary_index(self, name: str) -> None:
        with path_lock(self._summary_index_path):
            index = self._load_summary_index()
            if name in index:
                index.pop(name, None)
                self._save_summary_index(index)

    def list_names(self) -> list[str]:
        return sorted((path.stem for path in self.root.glob("*.txt")), key=str.lower)

    def summary(self, name: object) -> dict[str, Any]:
        clean_name = self.validate_name(name)
        path = self.path_for(clean_name)
        if not path.exists():
            return {"name": clean_name, "count": 0}
        with path_lock(self._summary_index_path):
            index = self._load_summary_index()
            try:
                stat = path.stat()
            except OSError:
                return {"name": clean_name, "count": 0}
            cached = index.get(clean_name) or {}
            if (
                int(cached.get("mtime_ns") or 0) == int(stat.st_mtime_ns)
                and int(cached.get("size_bytes") or 0) == int(stat.st_size)
            ):
                count = max(0, int(cached.get("count") or 0))
            else:
                count = self._count_usernames_from_path(path)
                index[clean_name] = self._summary_entry_from_path(path, count=count)
                self._save_summary_index(index)
            return {"name": clean_name, "count": count}

    def list_summaries(self) -> list[dict[str, Any]]:
        names = self.list_names()
        with path_lock(self._summary_index_path):
            index = self._load_summary_index()
            changed = False
            stale_names = [name for name in index if name not in names]
            for stale_name in stale_names:
                index.pop(stale_name, None)
                changed = True

            rows: list[dict[str, Any]] = []
            for name in names:
                path = self.root / f"{name}.txt"
                if not path.exists():
                    if name in index:
                        index.pop(name, None)
                        changed = True
                    continue
                try:
                    stat = path.stat()
                except OSError:
                    rows.append({"name": name, "count": 0})
                    continue
                cached = index.get(name) or {}
                if (
                    int(cached.get("mtime_ns") or 0) == int(stat.st_mtime_ns)
                    and int(cached.get("size_bytes") or 0) == int(stat.st_size)
                ):
                    count = max(0, int(cached.get("count") or 0))
                else:
                    count = self._count_usernames_from_path(path)
                    index[name] = self._summary_entry_from_path(path, count=count)
                    changed = True
                rows.append({"name": name, "count": count})

            if changed:
                self._save_summary_index(index)
        return rows

    def validate_name(self, name: object) -> str:
        clean_name = str(name or "").strip()
        if not clean_name:
            raise LeadListStoreError("Nombre de lista invalido.")
        if clean_name in {".", ".."}:
            raise LeadListStoreError("Nombre de lista invalido.")
        if clean_name.endswith((" ", ".")):
            raise LeadListStoreError("El nombre de lista no puede terminar en espacio o punto.")
        if any(ord(char) < 32 for char in clean_name):
            raise LeadListStoreError("El nombre de lista contiene caracteres no validos.")
        if any(char in _WINDOWS_INVALID_FILENAME_CHARS for char in clean_name):
            raise LeadListStoreError(
                'El nombre de lista contiene caracteres no permitidos (\\ / : * ? " < > |).'
            )
        reserved_key = clean_name.split(".", 1)[0].upper()
        if reserved_key in _WINDOWS_RESERVED_NAMES:
            raise LeadListStoreError("El nombre de lista usa un identificador reservado del sistema.")

        candidate = (self.root / f"{clean_name}.txt").resolve()
        root_resolved = self.root.resolve()
        if candidate.parent != root_resolved:
            raise LeadListStoreError("El nombre de lista apunta fuera del almacenamiento permitido.")
        return clean_name

    def path_for(self, name: object) -> Path:
        clean_name = self.validate_name(name)
        return self.root / f"{clean_name}.txt"

    def load(self, name: object) -> list[str]:
        path = self.path_for(name)
        if not path.exists():
            return []
        return self._read_path(path)

    def save(self, name: object, usernames: Iterable[object]) -> Path:
        clean_name = self.validate_name(name)
        normalized = normalize_lead_usernames(usernames)
        path = self.path_for(clean_name)
        with path_lock(path):
            saved_path = atomic_write_text(path, self._serialize_usernames(normalized))
            self._update_summary_index(clean_name, count=len(normalized), path=saved_path)
        return saved_path

    def append(self, name: object, usernames: Iterable[object]) -> Path:
        clean_name = self.validate_name(name)
        path = self.path_for(clean_name)
        with path_lock(path):
            existing = self._read_path(path) if path.exists() else []
            merged = normalize_lead_usernames([*existing, *usernames])
            saved_path = atomic_write_text(path, self._serialize_usernames(merged))
            self._update_summary_index(clean_name, count=len(merged), path=saved_path)
        return saved_path

    def delete(self, name: object) -> bool:
        clean_name = self.validate_name(name)
        path = self.path_for(clean_name)
        with path_lock(path):
            if not path.exists():
                return False
            move_file_to_backup(path, self.deleted_dir)
            self._clear_summary_index(clean_name)
        return True
