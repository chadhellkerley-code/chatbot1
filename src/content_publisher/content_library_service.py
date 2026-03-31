from __future__ import annotations

import csv
import json
import shutil
import sqlite3
import threading
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from paths import runtime_base


_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
_VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".webm", ".m4v"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _default_root_dir(root_dir: str | Path | None = None) -> Path:
    if root_dir:
        base = Path(root_dir)
    else:
        base = Path(__file__).resolve().parents[2]
    resolved = runtime_base(base).resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _slug(value: Any, *, fallback: str = "item") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    cleaned = "".join(char if char.isalnum() else "_" for char in text)
    collapsed = "_".join(part for part in cleaned.split("_") if part)
    return collapsed or fallback


def _caption_preview(text: Any, *, limit: int = 92) -> str:
    clean = str(text or "").strip()
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 1)].rstrip() + "…"


class ContentPublisherError(RuntimeError):
    pass


class ContentLibraryService:
    def __init__(self, root_dir: str | Path | None = None) -> None:
        self.root_dir = _default_root_dir(root_dir)
        self.db_path = self.root_dir / "data" / "content_library.sqlite3"
        self.media_root = self.root_dir / "data" / "content_library"
        self.media_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _ensure_schema(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                create table if not exists content_library (
                    id integer primary key autoincrement,
                    source_profile text not null,
                    media_path text not null,
                    caption text not null default '',
                    media_type text not null,
                    created_at text not null
                );

                create unique index if not exists idx_content_library_media_path
                    on content_library(media_path);
                create index if not exists idx_content_library_created_at
                    on content_library(created_at desc, id desc);
                """
            )

    def relative_path(self, path: str | Path) -> str:
        candidate = Path(path)
        if not candidate.is_absolute():
            return candidate.as_posix()
        try:
            return candidate.resolve().relative_to(self.root_dir.resolve()).as_posix()
        except Exception:
            return candidate.resolve().as_posix()

    def resolve_path(self, value: str | Path) -> Path:
        candidate = Path(value)
        if candidate.is_absolute():
            return candidate
        return self.root_dir / candidate

    def prepare_entry_dir(self, source_profile: str, entry_key: str) -> Path:
        folder_name = f"{_slug(source_profile, fallback='profile')}_{_slug(entry_key, fallback='entry')}"
        directory = self.media_root / folder_name
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _copy_media_files(
        self,
        *,
        entry_dir: Path,
        media_type: str,
        media_files: Sequence[str | Path],
    ) -> list[Path]:
        copied: list[Path] = []
        if media_type == "carousel":
            prefix = "slide"
        elif media_type == "video":
            prefix = "video"
        else:
            prefix = "image"
        for index, raw_file in enumerate(media_files, start=1):
            source = Path(raw_file)
            if not source.exists():
                raise ContentPublisherError(f"No existe el archivo multimedia: {source}")
            suffix = source.suffix.lower() or ".jpg"
            target_name = f"{prefix}_{index:02d}{suffix}" if media_type == "carousel" else f"{prefix}{suffix}"
            target = entry_dir / target_name
            if source.resolve() != target.resolve():
                shutil.copy2(source, target)
            copied.append(target)
        return copied

    def _write_carousel_manifest(
        self,
        *,
        entry_dir: Path,
        source_profile: str,
        caption: str,
        created_at: str,
        media_files: Sequence[Path],
    ) -> Path:
        manifest_path = entry_dir / "manifest.json"
        payload = {
            "source_profile": source_profile,
            "caption": caption,
            "media_type": "carousel",
            "created_at": created_at,
            "items": [path.name for path in media_files],
        }
        manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest_path

    def _upsert_entry(
        self,
        *,
        source_profile: str,
        media_path: Path,
        caption: str,
        media_type: str,
        created_at: str,
    ) -> dict[str, Any]:
        relative_media_path = self.relative_path(media_path)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                insert into content_library (
                    source_profile,
                    media_path,
                    caption,
                    media_type,
                    created_at
                )
                values (?, ?, ?, ?, ?)
                on conflict(media_path) do update set
                    source_profile = excluded.source_profile,
                    caption = excluded.caption,
                    media_type = excluded.media_type,
                    created_at = excluded.created_at
                """,
                (
                    str(source_profile or "").strip(),
                    relative_media_path,
                    str(caption or ""),
                    str(media_type or "image").strip().lower(),
                    created_at,
                ),
            )
            row = connection.execute(
                "select * from content_library where media_path = ? limit 1",
                (relative_media_path,),
            ).fetchone()
        if row is None:
            raise ContentPublisherError("No se pudo guardar la entrada en la biblioteca.")
        return self._hydrate_entry(dict(row))

    def store_media_entry(
        self,
        *,
        source_profile: str,
        media_type: str,
        media_files: Sequence[str | Path],
        caption: str = "",
        entry_key: str = "",
        created_at: str | None = None,
    ) -> dict[str, Any]:
        normalized_media_type = str(media_type or "").strip().lower()
        if normalized_media_type == "carousel":
            clean_media_type = "carousel"
        elif normalized_media_type == "video":
            clean_media_type = "video"
        else:
            clean_media_type = "image"
        entry_dir = self.prepare_entry_dir(source_profile, entry_key or _utc_now_iso())
        copied_files = self._copy_media_files(
            entry_dir=entry_dir,
            media_type=clean_media_type,
            media_files=media_files,
        )
        if not copied_files:
            raise ContentPublisherError("No hay archivos multimedia para guardar.")
        created = str(created_at or _utc_now_iso()).strip() or _utc_now_iso()
        media_path = (
            self._write_carousel_manifest(
                entry_dir=entry_dir,
                source_profile=source_profile,
                caption=str(caption or ""),
                created_at=created,
                media_files=copied_files,
            )
            if clean_media_type == "carousel"
            else copied_files[0]
        )
        return self._upsert_entry(
            source_profile=source_profile,
            media_path=media_path,
            caption=str(caption or ""),
            media_type=clean_media_type,
            created_at=created,
        )

    def _manifest_files(self, manifest_path: Path) -> list[Path]:
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ContentPublisherError(f"No se pudo leer el manifiesto del carrusel: {manifest_path}") from exc
        items = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(items, list):
            items = []
        files = [
            manifest_path.parent / str(item or "").strip()
            for item in items
            if str(item or "").strip()
        ]
        return [path for path in files if path.exists()]

    def resolve_media_bundle(self, media_path: str | Path) -> dict[str, Any]:
        relative_media_path = self.relative_path(media_path)
        absolute_path = self.resolve_path(relative_media_path)
        if not absolute_path.exists():
            raise ContentPublisherError(f"No existe la ruta multimedia: {relative_media_path}")
        if absolute_path.suffix.lower() == ".json":
            files = self._manifest_files(absolute_path)
            return {
                "media_type": "carousel",
                "media_path": relative_media_path,
                "manifest_path": str(absolute_path),
                "files": [str(path) for path in files],
            }
        suffix = absolute_path.suffix.lower()
        media_type = "video" if suffix in _VIDEO_EXTENSIONS else "image"
        return {
            "media_type": media_type,
            "media_path": relative_media_path,
            "manifest_path": "",
            "files": [str(absolute_path)],
        }

    def _hydrate_entry(self, row: dict[str, Any]) -> dict[str, Any]:
        hydrated = dict(row)
        created_at = str(hydrated.get("created_at") or "").strip()
        media_path = str(hydrated.get("media_path") or "").strip()
        bundle = self.resolve_media_bundle(media_path)
        media_files = [str(item) for item in bundle.get("files") or [] if str(item or "").strip()]
        preview_path = str(media_files[0] if media_files else "")
        hydrated["media_type"] = str(hydrated.get("media_type") or bundle.get("media_type") or "image").strip()
        hydrated["media_files"] = media_files
        hydrated["preview_path"] = preview_path
        hydrated["media_count"] = len(media_files)
        hydrated["caption_preview"] = _caption_preview(hydrated.get("caption"))
        hydrated["created_at_label"] = created_at.replace("T", " ").replace("+00:00", " UTC")
        return hydrated

    def list_entries(self, *, ids: Sequence[int] | None = None) -> list[dict[str, Any]]:
        query = "select * from content_library"
        params: list[Any] = []
        if ids:
            clean_ids = [int(item) for item in ids if int(item)]
            if clean_ids:
                placeholders = ",".join("?" for _ in clean_ids)
                query += f" where id in ({placeholders})"
                params.extend(clean_ids)
        query += " order by datetime(created_at) desc, id desc"
        with self._lock, self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        entries: list[dict[str, Any]] = []
        for row in rows:
            try:
                entries.append(self._hydrate_entry(dict(row)))
            except ContentPublisherError:
                continue
        return entries

    def get_entry(self, entry_id: int) -> dict[str, Any]:
        rows = self.list_entries(ids=[entry_id])
        if not rows:
            raise ContentPublisherError(f"No existe el contenido con id={entry_id}.")
        return rows[0]

    def _export_rows(self, entry_ids: Sequence[int]) -> list[dict[str, Any]]:
        rows = self.list_entries(ids=entry_ids)
        if not rows:
            raise ContentPublisherError("Selecciona al menos un contenido para exportar.")
        return rows

    def export_json(self, entry_ids: Sequence[int], destination: str | Path) -> Path:
        rows = self._export_rows(entry_ids)
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = [
            {
                "id": int(row.get("id") or 0),
                "source_profile": str(row.get("source_profile") or ""),
                "media_path": str(row.get("media_path") or ""),
                "caption": str(row.get("caption") or ""),
                "media_type": str(row.get("media_type") or ""),
                "created_at": str(row.get("created_at") or ""),
                "media_files": list(row.get("media_files") or []),
            }
            for row in rows
        ]
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return target

    def export_csv(self, entry_ids: Sequence[int], destination: str | Path) -> Path:
        rows = self._export_rows(entry_ids)
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "id",
                    "source_profile",
                    "media_path",
                    "media_type",
                    "caption",
                    "created_at",
                    "media_files",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "id": int(row.get("id") or 0),
                        "source_profile": str(row.get("source_profile") or ""),
                        "media_path": str(row.get("media_path") or ""),
                        "media_type": str(row.get("media_type") or ""),
                        "caption": str(row.get("caption") or ""),
                        "created_at": str(row.get("created_at") or ""),
                        "media_files": "|".join(str(item) for item in row.get("media_files") or []),
                    }
                )
        return target

    def export_zip(self, entry_ids: Sequence[int], destination: str | Path) -> Path:
        rows = self._export_rows(entry_ids)
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        metadata = [
            {
                "id": int(row.get("id") or 0),
                "source_profile": str(row.get("source_profile") or ""),
                "media_path": str(row.get("media_path") or ""),
                "media_type": str(row.get("media_type") or ""),
                "caption": str(row.get("caption") or ""),
                "created_at": str(row.get("created_at") or ""),
            }
            for row in rows
        ]
        with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "metadata.json",
                json.dumps(metadata, ensure_ascii=False, indent=2),
            )
            for row in rows:
                entry_prefix = f"content_{int(row.get('id') or 0):04d}"
                written_paths: set[str] = set()
                media_path = self.resolve_path(str(row.get("media_path") or ""))
                if media_path.exists():
                    archive.write(media_path, arcname=f"{entry_prefix}/{media_path.name}")
                    written_paths.add(str(media_path.resolve()))
                for file_path in row.get("media_files") or []:
                    resolved = Path(file_path)
                    if not resolved.exists():
                        continue
                    resolved_key = str(resolved.resolve())
                    if resolved_key in written_paths:
                        continue
                    archive.write(resolved, arcname=f"{entry_prefix}/{resolved.name}")
                    written_paths.add(resolved_key)
        return target
