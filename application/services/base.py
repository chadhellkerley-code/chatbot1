from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence

from core.alias_identity import normalize_alias_display
from core.storage_atomic import atomic_write_json, load_json_file, load_jsonl_entries
from paths import accounts_root, leads_root, runtime_base, storage_root


class ServiceError(RuntimeError):
    pass


@dataclass(frozen=True)
class ServiceContext:
    root_dir: Path

    @classmethod
    def default(cls, root_dir: Path | None = None) -> "ServiceContext":
        base = Path(root_dir) if root_dir else Path(__file__).resolve().parents[2]
        return cls(root_dir=runtime_base(base))

    def storage_path(self, *parts: str) -> Path:
        path = storage_root(self.root_dir)
        for part in parts:
            path = path / part
        return path

    def accounts_path(self, *parts: str) -> Path:
        path = accounts_root(self.root_dir)
        for part in parts:
            path = path / part
        return path

    def leads_path(self, *parts: str) -> Path:
        path = leads_root(self.root_dir)
        for part in parts:
            path = path / part
        return path

    def read_json(self, path: Path, default: Any) -> Any:
        return load_json_file(path, default, label=f"application.services:{path.name}")

    def read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        try:
            payload = load_jsonl_entries(path, label=f"application.services:{path.name}")
        except Exception:
            return []
        return [item for item in payload if isinstance(item, dict)]

    def write_json(self, path: Path, payload: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(path, payload)
        return path


def dedupe_usernames(values: Sequence[Any]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in values:
        username = str(raw or "").strip().lstrip("@")
        key = username.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(username)
    return ordered


def normalize_alias(value: Any, *, default: str = "default") -> str:
    alias = normalize_alias_display(value)
    return alias or default


@contextmanager
def scripted_module_io(module: Any, responses: Sequence[Any], *, default: str = "") -> Iterator[None]:
    queue = deque(str(item) if item is not None else "" for item in responses)
    originals: dict[str, Any] = {}

    def _pop(_prompt: str = "") -> str:
        if queue:
            return queue.popleft()
        return str(default or "")

    def _ask(prompt: str = "") -> str:
        return _pop(prompt)

    def _ask_int(prompt: str, min_value: int = 0, default: int | None = None) -> int:
        raw = _pop(prompt).strip()
        if not raw and default is not None:
            return max(int(min_value), int(default))
        try:
            value = int(float(raw))
        except Exception:
            value = int(default if default is not None else min_value)
        return max(int(min_value), value)

    def _press_enter(_msg: str = "") -> None:
        return None

    replacements = {
        "ask": _ask,
        "ask_int": _ask_int,
        "press_enter": _press_enter,
    }
    try:
        for attr_name, replacement in replacements.items():
            if not hasattr(module, attr_name):
                continue
            originals[attr_name] = getattr(module, attr_name)
            setattr(module, attr_name, replacement)
        yield
    finally:
        for attr_name, original in originals.items():
            setattr(module, attr_name, original)
