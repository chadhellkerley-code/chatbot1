# update_system.py
# -*- coding: utf-8 -*-
"""Sistema de actualización automática usando GitHub Releases."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, TypedDict

import requests

from config import SETTINGS
from core.storage_atomic import atomic_write_json, load_json_file
from licensekit import _fetch_licenses
from paths import app_root, runtime_base, storage_root, updates_root
from ui import Fore, banner, full_line, style_text
from utils import ask, ok, press_enter, warn

_INSTALL_ROOT = runtime_base(Path(__file__).resolve().parent)
_APP_ROOT = app_root(_INSTALL_ROOT)
_STORAGE_ROOT = storage_root(_INSTALL_ROOT)
_UPDATES_ROOT = updates_root(_INSTALL_ROOT)
_UPDATE_CONFIG_FILE = _STORAGE_ROOT / "update_config.json"
_UPDATE_MANIFEST_FILE = _STORAGE_ROOT / "update_manifest.json"
_UPDATE_CACHE_DIR = _UPDATES_ROOT / "cache"
_UPDATE_BACKUP_DIR = _UPDATES_ROOT / "backups"
_UPDATE_STAGING_DIR = _UPDATES_ROOT / "staging"
_UPDATE_STATE_DIR = _UPDATES_ROOT / "state"
_UPDATE_PENDING_FILE = _UPDATE_STATE_DIR / "pending_update.json"
_UPDATE_APPLYING_FILE = _UPDATE_STATE_DIR / "apply_in_progress.json"
_UPDATE_LAST_APPLIED_FILE = _UPDATE_STATE_DIR / "last_applied.json"
_UPDATE_ERROR_LOG = _UPDATE_STATE_DIR / "update_error.log"

# Configuración por defecto - GitHub
_DEFAULT_GITHUB_REPO = "chadhellkerley-code/chatbot"
_DEFAULT_UPDATE_CHECK_INTERVAL = 3600  # 1 hora
_GITHUB_TOKEN_ENV = "GITHUB_TOKEN"
_UPDATE_CAMPAIGN_ACTIVE_STATUSES = {"starting", "running", "stopping"}
_UPDATE_RUNTIME_ACTIVE_STATES = {"starting", "running", "stopping"}
_UPDATE_CAMPAIGN_STALE_SECONDS = 45.0
_UPDATE_CONNECTOR_STALE_SECONDS = 60.0


UpdateCheckStatus = Literal["update_available", "up_to_date", "error"]


class UpdateCheckResult(TypedDict):
    status: UpdateCheckStatus
    checked: bool
    update_available: bool
    message: str
    current_version: str
    latest_version: Optional[str]
    update_info: Optional[Dict[str, Any]]
    github_repo: str


def _ensure_update_dirs() -> None:
    for directory in (
        _UPDATE_CACHE_DIR,
        _UPDATE_BACKUP_DIR,
        _UPDATE_STAGING_DIR,
        _UPDATE_STATE_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def _write_update_state(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, payload)


def _remove_path(path: Path) -> None:
    try:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()
    except Exception:
        return


def _safe_version_slug(value: str) -> str:
    raw = str(value or "").strip().lower()
    cleaned = [ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in raw]
    slug = "".join(cleaned).strip("._")
    return slug or "latest"


def _github_headers() -> Dict[str, str]:
    token = (os.environ.get(_GITHUB_TOKEN_ENV) or "").strip()
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _load_update_config() -> Dict[str, Any]:
    """Carga la configuración de actualizaciones."""
    _ensure_update_dirs()
    if not _UPDATE_CONFIG_FILE.exists():
        return {
            "auto_check_enabled": True,
            "check_interval_seconds": _DEFAULT_UPDATE_CHECK_INTERVAL,
            "last_check_ts": 0,
            "current_version": _get_current_version(),
        }
    try:
        data = load_json_file(_UPDATE_CONFIG_FILE, {}, label="update_system.config")
        resolved_version = _get_current_version()
        if data.get("current_version") != resolved_version:
            data["current_version"] = resolved_version
        data.pop("exe_asset_name", None)
        # Forzar repo fijo y oculto
        return data
    except Exception:
        return {
            "auto_check_enabled": True,
            "check_interval_seconds": _DEFAULT_UPDATE_CHECK_INTERVAL,
            "last_check_ts": 0,
            "current_version": _get_current_version(),
        }


def _save_update_config(config: Dict[str, Any]) -> None:
    """Guarda la configuración de actualizaciones."""
    normalized = dict(config)
    normalized.pop("exe_asset_name", None)
    _write_update_state(_UPDATE_CONFIG_FILE, normalized)


def _query_sqlite_rows(db_path: Path, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    try:
        with sqlite3.connect(db_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(query, params).fetchall()
    except Exception:
        return []
    return [dict(row) for row in rows]


def _coerce_timestamp(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _coerce_iso_timestamp(value: Any) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _timestamp_is_recent(timestamp: float | None, stale_after_seconds: float) -> bool:
    if timestamp is None:
        return False
    return (time.time() - timestamp) <= max(1.0, float(stale_after_seconds or 1.0))


def _load_update_campaign_state() -> dict[str, Any]:
    rows = _query_sqlite_rows(
        _INSTALL_ROOT / "data" / "app_state.db",
        """
        SELECT run_id, status, payload_json, synced_at
        FROM campaign_state
        ORDER BY synced_at DESC, started_at DESC, run_id DESC
        LIMIT 1
        """,
    )
    if not rows:
        return {}
    row = rows[0]
    payload: dict[str, Any] = {}
    raw_payload = str(row.get("payload_json") or "").strip()
    if raw_payload:
        try:
            decoded = json.loads(raw_payload)
        except Exception:
            decoded = {}
        if isinstance(decoded, dict):
            payload = decoded
    payload["run_id"] = str(payload.get("run_id") or row.get("run_id") or "").strip()
    payload["status"] = str(payload.get("status") or row.get("status") or "").strip()
    payload["_synced_at_ts"] = _coerce_iso_timestamp(row.get("synced_at"))
    return payload


def _runtime_alias_state_blocks_update(state: dict[str, Any]) -> bool:
    if not isinstance(state, dict):
        return False
    worker_state = str(state.get("worker_state") or "").strip().lower()
    if not bool(state.get("is_running")) and worker_state not in _UPDATE_RUNTIME_ACTIVE_STATES:
        return False
    delay_max_ms = max(0, int(state.get("delay_max_ms") or 0))
    stale_after = max(30.0, (delay_max_ms / 1000.0) + 20.0)
    heartbeat = _coerce_timestamp(state.get("last_heartbeat_at")) or _coerce_timestamp(state.get("updated_at"))
    return _timestamp_is_recent(heartbeat, stale_after)


def _load_update_runtime_alias_states() -> list[dict[str, Any]]:
    rows = _query_sqlite_rows(
        _STORAGE_ROOT / "inbox_rm.sqlite3",
        """
        SELECT alias_id, is_running, worker_state, delay_max_ms, last_heartbeat_at, updated_at
        FROM runtime_alias_state
        ORDER BY alias_id ASC
        """,
    )
    states: list[dict[str, Any]] = []
    for row in rows:
        payload = {
            "alias_id": str(row.get("alias_id") or "").strip(),
            "is_running": bool(row.get("is_running")),
            "worker_state": str(row.get("worker_state") or "").strip(),
            "delay_max_ms": int(row.get("delay_max_ms") or 0),
            "last_heartbeat_at": _coerce_timestamp(row.get("last_heartbeat_at")),
            "updated_at": _coerce_timestamp(row.get("updated_at")),
        }
        if _runtime_alias_state_blocks_update(payload):
            states.append(payload)
    return states


def _load_update_pending_inbox_jobs() -> list[dict[str, Any]]:
    return _query_sqlite_rows(
        _STORAGE_ROOT / "inbox_rm.sqlite3",
        """
        SELECT id, task_type, job_type, thread_key, account_id, state
        FROM inbox_send_queue_jobs
        WHERE state IN ('queued', 'processing')
        ORDER BY priority DESC, scheduled_at ASC, created_at ASC, id ASC
        LIMIT 25
        """,
    )


def _session_connector_state_blocks_update(state: dict[str, Any]) -> bool:
    if not isinstance(state, dict):
        return False
    connector_state = str(state.get("state") or "").strip().lower()
    if connector_state not in {"ready", "degraded"}:
        return False
    heartbeat = _coerce_timestamp(state.get("last_heartbeat_at")) or _coerce_timestamp(state.get("updated_at"))
    return _timestamp_is_recent(heartbeat, _UPDATE_CONNECTOR_STALE_SECONDS)


def _load_update_session_connector_states() -> list[dict[str, Any]]:
    rows = _query_sqlite_rows(
        _STORAGE_ROOT / "inbox_rm.sqlite3",
        """
        SELECT account_id, alias_id, state, last_heartbeat_at, updated_at
        FROM session_connector_state
        ORDER BY alias_id ASC, account_id ASC
        """,
    )
    states: list[dict[str, Any]] = []
    for row in rows:
        payload = {
            "account_id": str(row.get("account_id") or "").strip(),
            "alias_id": str(row.get("alias_id") or "").strip(),
            "state": str(row.get("state") or "").strip(),
            "last_heartbeat_at": _coerce_timestamp(row.get("last_heartbeat_at")),
            "updated_at": _coerce_timestamp(row.get("updated_at")),
        }
        if _session_connector_state_blocks_update(payload):
            states.append(payload)
    return states


def _evaluate_update_runtime_preflight(
    *,
    campaign_state: dict[str, Any] | None,
    runtime_alias_states: list[dict[str, Any]] | None,
    pending_inbox_jobs: list[dict[str, Any]] | None,
    session_connectors: list[dict[str, Any]] | None,
) -> tuple[bool, list[str], dict[str, Any]]:
    blockers: list[str] = []
    details = {
        "campaign_state": dict(campaign_state or {}),
        "runtime_alias_states": [dict(item) for item in runtime_alias_states or [] if isinstance(item, dict)],
        "pending_inbox_jobs": [dict(item) for item in pending_inbox_jobs or [] if isinstance(item, dict)],
        "session_connectors": [dict(item) for item in session_connectors or [] if isinstance(item, dict)],
    }

    campaign = details["campaign_state"]
    campaign_status = str(campaign.get("status") or "").strip().lower()
    campaign_synced_at = _coerce_timestamp(campaign.get("_synced_at_ts"))
    campaign_active = bool(campaign.get("task_active")) or campaign_status in _UPDATE_CAMPAIGN_ACTIVE_STATUSES
    if campaign_active and _timestamp_is_recent(campaign_synced_at, _UPDATE_CAMPAIGN_STALE_SECONDS):
        run_id = str(campaign.get("run_id") or "").strip() or "sin_run_id"
        blockers.append(f"Campana activa detectada (run_id={run_id}, status={campaign_status or 'running'}).")

    live_aliases = details["runtime_alias_states"]
    if live_aliases:
        alias_labels = [
            f"{str(item.get('alias_id') or '').strip() or 'sin_alias'} ({str(item.get('worker_state') or 'running').strip() or 'running'})"
            for item in live_aliases[:5]
        ]
        extra_aliases = len(live_aliases) - len(alias_labels)
        alias_suffix = f" +{extra_aliases} mas" if extra_aliases > 0 else ""
        blockers.append(f"Inbox runtime activo en alias: {', '.join(alias_labels)}{alias_suffix}.")

    pending_jobs = details["pending_inbox_jobs"]
    if pending_jobs:
        job_types = sorted(
            {
                str(item.get("job_type") or item.get("task_type") or "").strip().lower() or "unknown"
                for item in pending_jobs
                if isinstance(item, dict)
            }
        )
        blockers.append(
            f"Inbox tiene jobs pendientes/procesando: {len(pending_jobs)} detectados"
            + (f" ({', '.join(job_types[:5])})." if job_types else ".")
        )

    live_connectors = details["session_connectors"]
    if live_connectors:
        connector_labels = [
            f"{str(item.get('account_id') or '').strip() or 'sin_cuenta'} ({str(item.get('state') or 'ready').strip() or 'ready'})"
            for item in live_connectors[:5]
        ]
        extra_connectors = len(live_connectors) - len(connector_labels)
        connector_suffix = f" +{extra_connectors} mas" if extra_connectors > 0 else ""
        blockers.append(f"Conectores auxiliares del inbox vivos: {', '.join(connector_labels)}{connector_suffix}.")

    return (not blockers), blockers, details


def _check_update_runtime_preflight() -> tuple[bool, str, dict[str, Any]]:
    ok_to_update, blockers, details = _evaluate_update_runtime_preflight(
        campaign_state=_load_update_campaign_state(),
        runtime_alias_states=_load_update_runtime_alias_states(),
        pending_inbox_jobs=_load_update_pending_inbox_jobs(),
        session_connectors=_load_update_session_connector_states(),
    )
    if ok_to_update:
        return True, "Sin runtimes vivos detectados. El update puede continuar.", details
    lines = [
        "Update bloqueado: hay actividad operativa viva que puede dejar el estado inconsistente.",
        *[f"- {item}" for item in blockers],
        "Deten la actividad activa y reintenta el update.",
    ]
    return False, "\n".join(lines), details


def _get_current_version() -> str:
    """Obtiene la versión actual de la aplicación."""
    # Prioridad: manifest local aplicado
    manifest_paths = [
        _APP_ROOT / "app_version.json",
        _UPDATE_MANIFEST_FILE,
        _APP_ROOT / "update_manifest.json",
        Path(__file__).resolve().parent / "update_manifest.json",
    ]
    for manifest_path in manifest_paths:
        if manifest_path.exists():
            try:
                manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest_version = str(manifest_data.get("version") or "").strip()
                if manifest_version:
                    return manifest_version
            except Exception:
                pass
    for version_file in (_APP_ROOT / "VERSION", Path(__file__).resolve().parent / "VERSION"):
        if version_file.exists():
            try:
                return version_file.read_text(encoding="utf-8").strip()
            except Exception:
                pass
    
    # Fallback: usar hash del código principal
    try:
        app_file = Path(__file__).resolve().parent / "app.py"
        if app_file.exists():
            content = app_file.read_text(encoding="utf-8")
            hash_obj = hashlib.md5(content.encode())
            return hash_obj.hexdigest()[:8]
    except Exception:
        pass
    
    return "unknown"


def _get_latest_release_from_github(repo: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene la última release de GitHub.
    
    Args:
        repo: Repositorio en formato "usuario/repo"
    
    Returns:
        Información de la release o None
    """
    try:
        # API pública de GitHub - no requiere autenticación para releases públicas
        url = f"https://api.github.com/repos/{repo}/releases/latest"
        response = requests.get(url, timeout=10, headers=_github_headers())
        
        if response.status_code == 200:
            release_data = response.json()
            
            # Buscar el archivo update_manifest.json en los assets
            manifest_url = None
            update_zip_url = None
            update_zip_name = None
            zip_assets: List[Dict[str, str]] = []
            
            for asset in release_data.get("assets", []):
                asset_name = asset.get("name", "")
                if asset_name == "update_manifest.json":
                    manifest_url = asset.get("browser_download_url")
                elif asset_name.endswith(".zip"):
                    zip_assets.append(
                        {
                            "name": asset_name,
                            "url": asset.get("browser_download_url", ""),
                        }
                    )
                    if "update" in asset_name.lower():
                        update_zip_url = asset.get("browser_download_url")
                        update_zip_name = asset_name
            
            # Si no hay manifest en assets, intentar descargarlo desde el tag
            if not manifest_url:
                tag = release_data.get("tag_name", "")
                manifest_url = f"https://raw.githubusercontent.com/{repo}/{tag}/update_manifest.json"
            
            # Descargar manifest
            manifest_data = None
            if manifest_url:
                try:
                    manifest_response = requests.get(manifest_url, timeout=10)
                    if manifest_response.status_code == 200:
                        manifest_data = manifest_response.json()
                except Exception:
                    pass
            
            # Si no hay manifest, crear uno básico desde la release
            if not manifest_data:
                manifest_data = {
                    "version": release_data.get("tag_name", "unknown"),
                    "description": release_data.get("body", ""),
                    "release_date": release_data.get("published_at", ""),
                }
            
            # Permitir que el manifest defina el ZIP exacto
            manifest_zip_name = str(manifest_data.get("zip_filename") or "").strip()
            manifest_zip_url = str(manifest_data.get("zip_url") or manifest_data.get("download_url") or "").strip()
            if manifest_zip_name:
                for asset in zip_assets:
                    if asset.get("name") == manifest_zip_name:
                        update_zip_name = asset["name"]
                        update_zip_url = asset["url"]
                        break
            elif manifest_zip_url:
                update_zip_url = manifest_zip_url
            elif not update_zip_url and len(zip_assets) == 1:
                update_zip_name = zip_assets[0]["name"]
                update_zip_url = zip_assets[0]["url"]
            
            return {
                "version": release_data.get("tag_name", manifest_data.get("version", "unknown")),
                "description": release_data.get("body", manifest_data.get("description", "")),
                "release_date": release_data.get("published_at", manifest_data.get("release_date", "")),
                "download_url": update_zip_url,
                "zip_filename": update_zip_name,
                "manifest": manifest_data,
                "release_url": release_data.get("html_url", ""),
            }
        
        return None
    except Exception as exc:
        return None


def _get_release_asset(repo: str, asset_name: str) -> Optional[Dict[str, str]]:
    """Obtiene la URL de un asset específico de la última release."""
    try:
        url = f"https://api.github.com/repos/{repo}/releases/latest"
        response = requests.get(url, timeout=10, headers=_github_headers())
        if response.status_code != 200:
            return None
        release_data = response.json()
        for asset in release_data.get("assets", []):
            if asset.get("name") == asset_name:
                return {
                    "download_url": asset.get("browser_download_url", ""),
                    "tag_name": release_data.get("tag_name", ""),
                }
        # fallback: case-insensitive match
        target_lower = asset_name.lower()
        for asset in release_data.get("assets", []):
            name = str(asset.get("name") or "")
            if name.lower() == target_lower:
                return {
                    "download_url": asset.get("browser_download_url", ""),
                    "tag_name": release_data.get("tag_name", ""),
                }
        return None
    except Exception:
        return None


def _list_release_assets(repo: str) -> List[str]:
    try:
        url = f"https://api.github.com/repos/{repo}/releases/latest"
        response = requests.get(url, timeout=10, headers=_github_headers())
        if response.status_code != 200:
            return []
        release_data = response.json()
        return [
            str(asset.get("name") or "")
            for asset in release_data.get("assets", [])
            if asset.get("name")
        ]
    except Exception:
        return []


def _build_update_check_result(
    *,
    status: UpdateCheckStatus,
    message: str,
    current_version: str,
    github_repo: str,
    checked: bool = True,
    latest_version: Optional[str] = None,
    update_info: Optional[Dict[str, Any]] = None,
) -> UpdateCheckResult:
    normalized_info = dict(update_info) if isinstance(update_info, dict) else None
    normalized_latest = str(latest_version or "").strip() or None
    if normalized_latest is None and normalized_info:
        candidate = str(normalized_info.get("version") or "").strip()
        normalized_latest = candidate or None
    return {
        "status": status,
        "checked": bool(checked),
        "update_available": status == "update_available",
        "message": str(message or "").strip(),
        "current_version": str(current_version or "").strip(),
        "latest_version": normalized_latest,
        "update_info": normalized_info,
        "github_repo": str(github_repo or "").strip(),
    }


def check_for_updates(
    github_repo: Optional[str] = None,
    force: bool = False,
) -> UpdateCheckResult:
    """
    Verifica si hay actualizaciones disponibles desde GitHub.
    
    Args:
        github_repo: Repositorio en formato "usuario/repo" (opcional)
        force: Forzar verificación incluso si no es momento
    
    Returns:
        Dict estructurado con status, message, version actual y release detectada.
    """
    config = _load_update_config()
    github_repo = str(github_repo or _DEFAULT_GITHUB_REPO).strip() or _DEFAULT_GITHUB_REPO
    current_version = config.get("current_version", _get_current_version())
    
    if not force:
        last_check = config.get("last_check_ts", 0)
        check_interval = config.get("check_interval_seconds", _DEFAULT_UPDATE_CHECK_INTERVAL)
        if time.time() - last_check < check_interval:
            return _build_update_check_result(
                status="up_to_date",
                checked=False,
                message="Aún no es momento de verificar actualizaciones.",
                current_version=current_version,
                github_repo=github_repo,
            )
    
    release_info = _get_latest_release_from_github(github_repo)
    if not release_info:
        return _build_update_check_result(
            status="error",
            message="No se pudo conectar con GitHub o no hay releases disponibles.",
            current_version=current_version,
            github_repo=github_repo,
        )
    
    latest_version = release_info.get("version", "")
    if not latest_version:
        return _build_update_check_result(
            status="error",
            message="No se pudo determinar la versión disponible.",
            current_version=current_version,
            update_info=release_info,
            github_repo=github_repo,
        )
    
    # Comparar versiones (puede ser tag como "v1.0.1" o "1.0.1")
    current_clean = current_version.lstrip("v")
    latest_clean = latest_version.lstrip("v")
    
    if latest_clean == current_clean:
        config["last_check_ts"] = time.time()
        _save_update_config(config)
        return _build_update_check_result(
            status="up_to_date",
            message=f"Ya tienes la versión más reciente ({current_version}).",
            current_version=current_version,
            latest_version=latest_version,
            update_info=release_info,
            github_repo=github_repo,
        )
    
    config["last_check_ts"] = time.time()
    _save_update_config(config)
    
    return _build_update_check_result(
        status="update_available",
        message=f"Actualización disponible: {latest_version} (actual: {current_version})",
        current_version=current_version,
        latest_version=latest_version,
        update_info=release_info,
        github_repo=github_repo,
    )


def _staging_dir_for_version(version: str) -> Path:
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    return _UPDATE_STAGING_DIR / f"{stamp}_{_safe_version_slug(version)}"


def _resolve_staged_payload_root(extract_root: Path) -> Path:
    try:
        entries = [item for item in extract_root.iterdir() if item.name != "__MACOSX"]
    except Exception:
        return extract_root
    if len(entries) == 1 and entries[0].is_dir():
        child = entries[0]
        if any(
            (child / name).exists()
            for name in ("InstaCRM.exe", "app", "update_manifest.json", "storage", "VERSION")
        ):
            return child
    return extract_root


def _staged_manifest_candidates(payload_root: Path) -> List[Path]:
    return [
        payload_root / "update_manifest.json",
        payload_root / "app" / "update_manifest.json",
        storage_root(payload_root, scoped=False, honor_env=False) / "update_manifest.json",
        payload_root / "app" / "app_version.json",
    ]


def _load_staged_manifest(payload_root: Path) -> Dict[str, Any]:
    for candidate in _staged_manifest_candidates(payload_root):
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _expected_executable_name(manifest: Optional[Dict[str, Any]] = None) -> str:
    candidates: List[str] = []
    if isinstance(manifest, dict):
        candidates.append(str(manifest.get("executable_name") or "").strip())

    for manifest_path in (
        _APP_ROOT / "app_version.json",
        _APP_ROOT / "update_manifest.json",
        _UPDATE_MANIFEST_FILE,
    ):
        if not manifest_path.exists():
            continue
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        candidates.append(str(payload.get("executable_name") or "").strip())

    for candidate in candidates:
        if candidate:
            return Path(candidate).name

    if getattr(sys, "frozen", False):
        current_name = Path(getattr(sys, "executable", "") or "").name
        if current_name:
            return current_name
    return "InstaCRM.exe" if sys.platform.startswith("win") else "InstaCRM"


def _validate_staged_release_payload(
    payload_root: Path,
    manifest: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    exe_name = _expected_executable_name(manifest)
    payload_exe = payload_root / exe_name
    payload_app = payload_root / "app"

    if not payload_exe.exists() or not payload_exe.is_file():
        return False, f"El ZIP descargado no contiene el ejecutable requerido ({exe_name})."
    if not payload_app.exists() or not payload_app.is_dir():
        return False, "El ZIP descargado no contiene la carpeta app/ requerida por el layout distribuido."
    return True, "payload_full_package_ok"


def _verify_staged_manifest(payload_root: Path, manifest: Dict[str, Any]) -> Tuple[bool, str]:
    file_rows = manifest.get("files")
    if not isinstance(file_rows, list):
        integrity = manifest.get("integrity")
        if isinstance(integrity, dict):
            maybe_rows = integrity.get("files")
            if isinstance(maybe_rows, list):
                file_rows = maybe_rows
    if not isinstance(file_rows, list):
        return True, "manifest_hash_hooks_not_used"

    verified = 0
    for row in file_rows:
        if not isinstance(row, dict):
            continue
        rel_path = str(row.get("path") or "").strip()
        expected_hash = str(row.get("sha256") or row.get("sha256_hash") or "").strip().lower()
        if not rel_path or not expected_hash:
            continue
        candidate = payload_root / rel_path
        if not candidate.exists() or not candidate.is_file():
            return False, f"manifest_file_missing:{rel_path}"
        actual_hash = _calculate_file_hash(candidate).lower()
        if actual_hash != expected_hash:
            return False, f"manifest_hash_mismatch:{rel_path}"
        verified += 1
    if verified <= 0:
        return True, "manifest_hash_hooks_empty"
    return True, f"manifest_files_verified:{verified}"


def _stage_update_archive(
    update_file: Path,
    *,
    update_info: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    _ensure_update_dirs()
    version = str((update_info or {}).get("version") or update_file.stem or "latest")
    stage_root = _staging_dir_for_version(version)
    extract_root = stage_root / "payload"
    try:
        stage_root.mkdir(parents=True, exist_ok=False)
        extract_root.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(update_file, "r") as zip_ref:
            zip_ref.extractall(extract_root)
        payload_root = _resolve_staged_payload_root(extract_root)
        manifest = _load_staged_manifest(payload_root)
        verified, detail = _verify_staged_manifest(payload_root, manifest)
        if not verified:
            _remove_path(stage_root)
            return False, None, f"Falló verificación del stage: {detail}"
        stage_info = {
            "version": version,
            "archive_path": str(update_file),
            "stage_root": str(stage_root),
            "payload_root": str(payload_root),
            "manifest": manifest,
            "manifest_verification": detail,
            "created_at": time.time(),
        }
        return True, stage_info, f"Actualización preparada en stage: {stage_root.name}"
    except Exception as exc:
        _remove_path(stage_root)
        return False, None, f"No se pudo preparar el stage de actualización: {exc}"


def download_update(
    update_info: Dict[str, Any],
) -> Tuple[bool, Optional[Path], str]:
    """
    Descarga una actualización desde GitHub.
    
    Args:
        update_info: Información de la release de GitHub
    
    Returns:
        (exito, ruta_archivo, mensaje)
    """
    download_url = update_info.get("download_url")
    if not download_url:
        return False, None, "No se encontró archivo de actualización en la release."
    
    _ensure_update_dirs()
    version = update_info.get("version", "latest")
    temp_file = _UPDATE_CACHE_DIR / f"update_{version}.zip"
    
    try:
        # GitHub permite descargas directas sin autenticación para releases públicas
        response = requests.get(download_url, stream=True, timeout=300)
        response.raise_for_status()
        
        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0
        
        with temp_file.open("wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"\rDescargando: {percent:.1f}%", end="", flush=True)
        
        print()  # Nueva línea después del progreso
        
        # Verificar hash si está disponible en el manifest
        manifest = update_info.get("manifest", {})
        expected_hash = manifest.get("sha256_hash")
        if expected_hash:
            actual_hash = _calculate_file_hash(temp_file)
            if actual_hash.lower() != expected_hash.lower():
                temp_file.unlink()
                return False, None, "El archivo descargado no coincide con el hash esperado."
        
        return True, temp_file, f"Actualización descargada: {temp_file.name}"
    except Exception as exc:
        if temp_file.exists():
            temp_file.unlink()
        return False, None, f"Error al descargar: {exc}"


def _download_asset_to_path(download_url: str, dest_path: Path) -> Tuple[bool, str]:
    try:
        response = requests.get(download_url, stream=True, timeout=300)
        response.raise_for_status()
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with dest_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True, "Archivo descargado correctamente."
    except Exception as exc:
        return False, f"Error al descargar asset: {exc}"


def _schedule_exe_replace_windows_legacy(source_path: Path, target_path: Path) -> Tuple[bool, str]:
    """Programa el reemplazo del EXE usando un .bat (Windows)."""
    try:
        _UPDATE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        batch_path = _UPDATE_CACHE_DIR / "update_exe.bat"
        backup_path = _UPDATE_BACKUP_DIR / f"{target_path.stem}.{int(time.time())}.bak"
        _UPDATE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        batch_content = "\n".join(
            [
                "@echo off",
                "setlocal",
                f"set SRC=\"{source_path}\"",
                f"set DST=\"{target_path}\"",
                f"set BAK=\"{backup_path}\"",
                ":loop",
                "timeout /t 1 >nul",
                "if exist %DST% (",
                "  move /Y %DST% %BAK% >nul 2>&1",
                ")",
                "move /Y %SRC% %DST% >nul 2>&1",
                "if exist %SRC% goto loop",
                "endlocal",
            ]
        )
        batch_path.write_text(batch_content, encoding="utf-8")
        subprocess.Popen(
            ["cmd", "/c", str(batch_path)],
            creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0),
        )
        return True, "Actualización programada. Cierra y reinicia el sistema."
    except Exception as exc:
        return False, f"No se pudo programar el reemplazo del EXE: {exc}"


def _schedule_staged_update_windows(
    stage_info: Dict[str, Any],
    *,
    backup: bool = True,
) -> Tuple[bool, str]:
    try:
        runtime_ok, runtime_message, _runtime_details = _check_update_runtime_preflight()
        if not runtime_ok:
            return False, runtime_message
        _ensure_update_dirs()
        version = str(stage_info.get("version") or "latest")
        payload_root = Path(str(stage_info.get("payload_root") or ""))
        stage_root = Path(str(stage_info.get("stage_root") or ""))
        if not payload_root.exists():
            return False, "El stage no contiene payload utilizable."

        manifest = stage_info.get("manifest") if isinstance(stage_info.get("manifest"), dict) else {}
        exe_name = _expected_executable_name(manifest)
        install_root = _INSTALL_ROOT
        target_exe = install_root / exe_name
        payload_exe = payload_root / exe_name
        payload_app = payload_root / "app"
        backup_root = _UPDATE_BACKUP_DIR / f"{int(time.time())}_{_safe_version_slug(version)}"
        script_path = _UPDATE_CACHE_DIR / "apply_staged_update.ps1"
        helper_log = _UPDATE_STATE_DIR / "apply_helper.log"
        applying_payload = {
            **stage_info,
            "status": "applying",
            "backup_root": str(backup_root),
            "scheduled_at": time.time(),
        }
        pending_payload = {
            **stage_info,
            "status": "pending_restart",
            "backup_requested": bool(backup),
            "pid": os.getpid(),
            "backup_root": str(backup_root),
            "scheduled_at": time.time(),
        }
        _write_update_state(_UPDATE_PENDING_FILE, pending_payload)

        ps_script = f"""
$ErrorActionPreference = 'Stop'
$PidToWait = {os.getpid()}
$InstallRoot = {json.dumps(str(install_root))}
$PayloadRoot = {json.dumps(str(payload_root))}
$PayloadApp = {json.dumps(str(payload_app))}
$PayloadExe = {json.dumps(str(payload_exe))}
$TargetExe = {json.dumps(str(target_exe))}
$TargetApp = Join-Path $InstallRoot 'app'
$TargetAppOld = Join-Path $InstallRoot 'app.__old__'
$TargetAppNew = Join-Path $InstallRoot 'app.__new__'
$PendingFile = {json.dumps(str(_UPDATE_PENDING_FILE))}
$ApplyingFile = {json.dumps(str(_UPDATE_APPLYING_FILE))}
$LastAppliedFile = {json.dumps(str(_UPDATE_LAST_APPLIED_FILE))}
$HelperLog = {json.dumps(str(helper_log))}
$ErrorLog = {json.dumps(str(_UPDATE_ERROR_LOG))}
$BackupRoot = {json.dumps(str(backup_root))}
$BackupEnabled = {'$true' if backup else '$false'}
$ScheduledPayload = {json.dumps(json.dumps(applying_payload, ensure_ascii=False, indent=2))}

function Write-Log([string]$Message) {{
    $stamp = (Get-Date).ToUniversalTime().ToString('s') + 'Z'
    Add-Content -Path $HelperLog -Value "$stamp $Message"
}}

function Restore-Backup {{
    if (-not (Test-Path $BackupRoot)) {{
        return
    }}
    $backupExe = Join-Path $BackupRoot ([System.IO.Path]::GetFileName($TargetExe))
    $backupApp = Join-Path $BackupRoot 'app'
    if (Test-Path $backupExe) {{
        Copy-Item -Path $backupExe -Destination $TargetExe -Force
    }}
    if (Test-Path $backupApp) {{
        if (Test-Path $TargetApp) {{
            Remove-Item -Path $TargetApp -Recurse -Force
        }}
        Copy-Item -Path $backupApp -Destination $TargetApp -Recurse -Force
    }}
}}

while (Get-Process -Id $PidToWait -ErrorAction SilentlyContinue) {{
    Start-Sleep -Seconds 1
}}

try {{
    New-Item -ItemType Directory -Force -Path {json.dumps(str(_UPDATE_STATE_DIR))} | Out-Null
    New-Item -ItemType Directory -Force -Path {json.dumps(str(_UPDATE_BACKUP_DIR))} | Out-Null
    Set-Content -Path $ApplyingFile -Value $ScheduledPayload -Encoding UTF8
    Write-Log "apply_start version={version}"
    if ($BackupEnabled) {{
        New-Item -ItemType Directory -Force -Path $BackupRoot | Out-Null
        if (Test-Path $TargetExe) {{
            Copy-Item -Path $TargetExe -Destination (Join-Path $BackupRoot ([System.IO.Path]::GetFileName($TargetExe))) -Force
        }}
        if (Test-Path $TargetApp) {{
            Copy-Item -Path $TargetApp -Destination (Join-Path $BackupRoot 'app') -Recurse -Force
        }}
    }}
    if (Test-Path $TargetAppNew) {{
        Remove-Item -Path $TargetAppNew -Recurse -Force
    }}
    if (Test-Path $PayloadApp) {{
        Copy-Item -Path $PayloadApp -Destination $TargetAppNew -Recurse -Force
    }}
    if (Test-Path $PayloadApp) {{
        if (Test-Path $TargetAppOld) {{
            Remove-Item -Path $TargetAppOld -Recurse -Force
        }}
        if (Test-Path $TargetApp) {{
            Rename-Item -Path $TargetApp -NewName 'app.__old__'
        }}
        if (Test-Path $TargetAppNew) {{
            Rename-Item -Path $TargetAppNew -NewName 'app'
        }}
    }}
    if (Test-Path $PayloadExe) {{
        $NewExe = "$TargetExe.new"
        if (Test-Path $NewExe) {{
            Remove-Item -Path $NewExe -Force
        }}
        Copy-Item -Path $PayloadExe -Destination $NewExe -Force
        if (Test-Path $TargetExe) {{
            Remove-Item -Path $TargetExe -Force
        }}
        Move-Item -Path $NewExe -Destination $TargetExe -Force
    }}
    if (Test-Path $TargetAppOld) {{
        Remove-Item -Path $TargetAppOld -Recurse -Force
    }}
    Set-Content -Path $LastAppliedFile -Value $ScheduledPayload -Encoding UTF8
    if (Test-Path $PendingFile) {{
        Remove-Item -Path $PendingFile -Force
    }}
    if (Test-Path $ApplyingFile) {{
        Remove-Item -Path $ApplyingFile -Force
    }}
    Write-Log "apply_success version={version}"
    if (Test-Path $TargetExe) {{
        Start-Process -FilePath $TargetExe
    }}
}}
catch {{
    $_ | Out-File -FilePath $ErrorLog -Encoding UTF8 -Append
    Write-Log "apply_failure version={version}"
    Restore-Backup
}}
"""
        script_path.write_text(ps_script.strip() + "\n", encoding="utf-8")
        subprocess.Popen(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script_path),
            ],
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return True, "Actualización staged. Cierra la aplicación para completar el reemplazo seguro."
    except Exception as exc:
        return False, f"No se pudo programar la actualización staged: {exc}"


def _update_executable_from_release_legacy() -> None:
    _check_and_apply_update()
    return
    """Descarga y programa la actualización del EXE desde GitHub Release."""
    # Safety guard: one-dir builds depend on sibling folders (_internal, browsers, etc.).
    # Replacing only the .exe can leave runtime in a broken mixed state.
    if _installation_requires_full_package_update():
        warn("Esta instalación requiere actualización por paquete completo.")
        print(
            "Para actualizar sin romper librerías o metadata del app layout, "
            "reemplaza la carpeta completa del programa desde un ZIP de release."
        )
        print(
            "No uses 'Actualizar programa (EXE)' en este equipo para esta instalación."
        )
        press_enter()
        return

    config = _load_update_config()
    github_repo = config.get("github_repo", _DEFAULT_GITHUB_REPO)
    if not github_repo or "/" not in github_repo:
        warn("Repositorio GitHub no configurado correctamente.")
        press_enter()
        return
    default_asset = None
    if getattr(sys, "frozen", False):
        default_asset = Path(sys.executable).name
    if not default_asset:
        default_asset = "InstaCRM.exe"
    asset_name = config.get("exe_asset_name") or default_asset
    ok(f"Buscando asset '{asset_name}' en la última release...")
    asset = _get_release_asset(github_repo, asset_name)
    if not asset or not asset.get("download_url"):
        assets = _list_release_assets(github_repo)
        warn(f"No se encontró el asset '{asset_name}' en la última release.")
        if assets:
            print("Assets disponibles:")
            for name in assets:
                print(f" - {name}")
        else:
            token_hint = (
                f"Tip: si el repo es privado o hay rate limit, configura {_GITHUB_TOKEN_ENV}."
            )
            print(token_hint)
        press_enter()
        return
    download_url = asset["download_url"]
    temp_path = _UPDATE_CACHE_DIR / asset_name
    print("Descargando EXE desde GitHub...")
    success, msg = _download_asset_to_path(download_url, temp_path)
    if not success:
        warn(msg)
        press_enter()
        return
    ok(msg)
    target_path = Path(sys.executable) if getattr(sys, "frozen", False) else (Path(__file__).resolve().parent / asset_name)
    if sys.platform.startswith("win"):
        success, msg = _schedule_exe_replace_windows(temp_path, target_path)
        if success:
            ok(msg)
            print(style_text("Reinicia el sistema para tener nuevas updates.", color=Fore.YELLOW, bold=True))
        else:
            warn(msg)
    else:
        try:
            if target_path.exists():
                _UPDATE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
                backup_path = _UPDATE_BACKUP_DIR / f"{target_path.stem}.{int(time.time())}.bak"
                shutil.copy2(target_path, backup_path)
            shutil.copy2(temp_path, target_path)
            ok("EXE actualizado.")
            print(style_text("Reinicia el sistema para tener nuevas updates.", color=Fore.YELLOW, bold=True))
        except Exception as exc:
            warn(f"No se pudo reemplazar el EXE: {exc}")
    press_enter()


def _update_executable_from_release() -> None:
    """Compatibilidad legacy: redirige al flujo full-package."""
    _check_and_apply_update()


def update_single_file_from_release(
    repo: str,
    asset_name: str,
    target_path: Path,
    backup: bool = True,
) -> Tuple[bool, str]:
    """Descarga un asset de la última release y reemplaza un archivo local."""
    asset = _get_release_asset(repo, asset_name)
    if not asset or not asset.get("download_url"):
        return False, f"No se encontró el asset {asset_name} en la última release."
    download_url = asset["download_url"]
    _ensure_update_dirs()
    temp_path = _UPDATE_CACHE_DIR / asset_name
    success, msg = _download_asset_to_path(download_url, temp_path)
    if not success:
        return False, msg
    try:
        if backup and target_path.exists():
            _UPDATE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            backup_path = _UPDATE_BACKUP_DIR / f"{asset_name}.{int(time.time())}.bak"
            shutil.copy2(target_path, backup_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(temp_path, target_path)
        return True, f"{asset_name} actualizado desde la release."
    except Exception as exc:
        return False, f"Error al reemplazar archivo: {exc}"


def _calculate_file_hash(file_path: Path) -> str:
    """Calcula el hash SHA256 de un archivo."""
    sha256 = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def apply_update(
    update_file: Path,
    backup: bool = True,
) -> Tuple[bool, str]:
    """
    Aplica una actualización descargada.
    
    Args:
        update_file: Ruta al archivo ZIP de actualización
        backup: Si hacer backup antes de actualizar
    
    Returns:
        (exito, mensaje)
    """
    def _report_failure(message: str) -> None:
        try:
            from src.telemetry import report_update_failed

            report_update_failed(
                str(message or "update_failed"),
                payload={"update_file": str(update_file), "backup": bool(backup)},
            )
        except Exception:
            return

    if not update_file.exists():
        message = "El archivo de actualización no existe."
        _report_failure(message)
        return False, message
    if update_file.suffix.lower() != ".zip":
        message = "La actualización debe ser un archivo ZIP."
        _report_failure(message)
        return False, message
    runtime_ok, runtime_message, _runtime_details = _check_update_runtime_preflight()
    if not runtime_ok:
        _report_failure(runtime_message)
        return False, runtime_message

    try:
        staged_ok, stage_info, stage_message = _stage_update_archive(update_file)
        if not staged_ok or stage_info is None:
            _report_failure(stage_message)
            return False, stage_message

        payload_root = Path(str(stage_info.get("payload_root") or ""))
        manifest = stage_info.get("manifest") or {}
        version = str(manifest.get("version") or stage_info.get("version") or "unknown")

        payload_ok, payload_message = _validate_staged_release_payload(payload_root, manifest)
        if not payload_ok:
            _remove_path(Path(str(stage_info.get("stage_root") or "")))
            # Payload invalid for the only supported update model.
            (
                False,
                "El ZIP descargado no contiene la carpeta app/ requerida para esta instalación multiparte.",
            )
            _report_failure(payload_message)
            return False, payload_message

        if sys.platform.startswith("win"):
            success, message = _schedule_staged_update_windows(stage_info, backup=backup)
            if not success:
                _report_failure(message)
                return False, message
            return True, f"{message} Versión staged: {version}."

        pending_payload = {
            **stage_info,
            "status": "pending_manual_apply",
            "backup_requested": bool(backup),
            "scheduled_at": time.time(),
        }
        _write_update_state(_UPDATE_PENDING_FILE, pending_payload)
        return True, (
            "Actualización staged correctamente. "
            f"Revisa {_UPDATE_PENDING_FILE} para completar la aplicación manual de la versión {version}."
        )
    except Exception as exc:
        message = f"Error al aplicar actualización staged: {exc}"
        _report_failure(message)
        return False, message


def auto_update_check() -> None:
    """Verifica automáticamente actualizaciones si está habilitado."""
    config = _load_update_config()
    if not config.get("auto_check_enabled", True):
        return
    
    github_repo = _DEFAULT_GITHUB_REPO
    result = check_for_updates(
        github_repo=github_repo,
        force=False,
    )

    update_info = result.get("update_info") if isinstance(result, dict) else None
    message = str(result.get("message") or "").strip() if isinstance(result, dict) else ""
    if result.get("status") == "update_available" and isinstance(update_info, dict):
        print(style_text(f"[Actualización] {message}", color=Fore.YELLOW))


def _menu_updates_legacy() -> None:
    """Menú de gestión de actualizaciones."""
    while True:
        banner()
        print(full_line())
        print(style_text("Sistema de Actualizaciones (GitHub)", color=Fore.CYAN, bold=True))
        print(full_line())
        
        config = _load_update_config()
        current_version = config.get("current_version", _get_current_version())
        auto_check = config.get("auto_check_enabled", True)
        
        print(f"Versión actual: {style_text(current_version, color=Fore.GREEN, bold=True)}")
        print(f"Verificación automática: {'Habilitada' if auto_check else 'Deshabilitada'}")
        print()
        print("Modelo soportado: paquete completo (ZIP con EXE + app/).")
        print()
        print("3) Habilitar/Deshabilitar verificación automática")
        print("2) Habilitar/Deshabilitar verificacion automatica")
        print("3) Ver historial de actualizaciones")
        print()
        
        choice = ask("Opción: ").strip()
        
        if choice == "1":
            _update_executable_from_release()
        elif choice == "2":
            _configure_exe_asset_name()
        elif choice == "3":
            _toggle_auto_check()
        elif choice == "4":
            _show_update_history()
        elif choice == "5":
            break
        else:
            warn("Opción inválida.")
            press_enter()


def menu_updates() -> None:
    """Menú alineado al único flujo soportado: full-package."""
    while True:
        banner()
        print(full_line())
        print(style_text("Sistema de Actualizaciones (GitHub)", color=Fore.CYAN, bold=True))
        print(full_line())

        config = _load_update_config()
        current_version = config.get("current_version", _get_current_version())
        auto_check = config.get("auto_check_enabled", True)

        print(f"Version actual: {style_text(current_version, color=Fore.GREEN, bold=True)}")
        print(f"Verificacion automatica: {'Habilitada' if auto_check else 'Deshabilitada'}")
        print()
        print("Modelo soportado: paquete completo (ZIP con EXE + app/).")
        print()
        print("1) Descargar e instalar actualizacion")
        print("2) Habilitar/Deshabilitar verificacion automatica")
        print("3) Ver historial de actualizaciones")
        print("4) Volver")
        print()

        choice = ask("Opcion: ").strip()

        if choice == "1":
            _check_and_apply_update()
        elif choice == "2":
            _toggle_auto_check()
        elif choice == "3":
            _show_update_history()
        elif choice == "4":
            break
        else:
            warn("Opcion invalida.")
            press_enter()


def _check_and_apply_update() -> None:
    """Verifica y aplica actualizaciones si están disponibles."""
    config = _load_update_config()
    github_repo = config.get("github_repo", _DEFAULT_GITHUB_REPO)
    
    print("Verificando actualizaciones en GitHub...")
    result = check_for_updates(
        github_repo=github_repo,
        force=True,
    )

    status = str(result.get("status") or "").strip()
    update_info = result.get("update_info") if isinstance(result, dict) else None
    message = str(result.get("message") or "").strip() if isinstance(result, dict) else ""

    if status != "update_available" or not isinstance(update_info, dict):
        warn(message or "No se pudo verificar actualizaciones.")
        press_enter()
        return
    
    ok(message)
    print()
    print(f"Versión disponible: {update_info.get('version', 'unknown')}")
    print(f"Descripción: {update_info.get('description', 'Sin descripción')[:200]}...")
    print(f"Fecha: {update_info.get('release_date', 'Desconocida')}")
    release_url = update_info.get("release_url")
    if release_url:
        print(f"Ver en GitHub: {release_url}")
    print()
    
    choice = ask("¿Descargar e instalar esta actualización? (s/N): ").strip().lower()
    if choice != "s":
        warn("Actualización cancelada.")
        press_enter()
        return

    runtime_ok, runtime_message, _runtime_details = _check_update_runtime_preflight()
    if not runtime_ok:
        warn(runtime_message)
        press_enter()
        return
    
    print("Descargando actualización desde GitHub...")
    success, update_file, msg = download_update(update_info)
    if not success:
        warn(msg)
        press_enter()
        return
    
    ok(msg)
    print()
    
    choice = ask("¿Aplicar actualización ahora? (s/N): ").strip().lower()
    if choice != "s":
        warn("Actualización descargada pero no aplicada. Se aplicará en el próximo inicio.")
        press_enter()
        return
    
    print("Aplicando actualización...")
    success, msg = apply_update(update_file, backup=True)
    if success:
        ok(msg)
        print()
        print(style_text("IMPORTANTE: Reinicia la aplicación para completar la actualización.", color=Fore.YELLOW, bold=True))
    else:
        warn(msg)
    press_enter()


def _update_responder_py_from_release() -> None:
    """Actualiza solo responder.py desde la última release."""
    config = _load_update_config()
    github_repo = config.get("github_repo", _DEFAULT_GITHUB_REPO)
    if not github_repo or "/" not in github_repo:
        warn("Repositorio GitHub no configurado correctamente.")
        press_enter()
        return
    choice = ask("¿Actualizar responder.py desde la última release? (s/N): ").strip().lower()
    if choice != "s":
        warn("Actualización cancelada.")
        press_enter()
        return
    target_path = Path(__file__).resolve().parent / "core" / "responder.py"
    ok("Descargando responder.py desde GitHub...")
    success, msg = update_single_file_from_release(
        github_repo,
        "responder.py",
        target_path,
        backup=True,
    )
    if success:
        ok(msg)
        print(style_text("Reinicia la aplicación para aplicar cambios.", color=Fore.YELLOW, bold=True))
    else:
        warn(msg)
    press_enter()


def _configure_github_repo() -> None:
    """Configura el repositorio de GitHub para actualizaciones."""
    config = _load_update_config()
    current_repo = config.get("github_repo", _DEFAULT_GITHUB_REPO)
    
    print(f"Repositorio actual: {current_repo}")
    print("Formato: usuario/repositorio (ejemplo: mi-usuario/mi-app)")
    new_repo = ask("Nuevo repositorio (Enter para mantener): ").strip()
    
    if new_repo:
        # Validar formato básico
        if "/" not in new_repo or new_repo.count("/") != 1:
            warn("Formato inválido. Debe ser: usuario/repositorio")
            press_enter()
            return
        
        config["github_repo"] = new_repo
        _save_update_config(config)
        ok(f"Repositorio configurado: {new_repo}")
    else:
        warn("Sin cambios.")
    
    press_enter()


def _configure_exe_asset_name_legacy() -> None:
    """Configura el nombre del asset EXE en GitHub Releases."""
    config = _load_update_config()
    current_name = config.get("exe_asset_name") or ""
    print(f"Asset EXE actual: {current_name or '(usar nombre del exe)'}")
    new_name = ask("Nuevo nombre de asset EXE (Enter para mantener): ").strip()
    if new_name:
        config["exe_asset_name"] = new_name
        _save_update_config(config)
        ok(f"Asset EXE configurado: {new_name}")
    else:
        warn("Sin cambios.")
    press_enter()


def _configure_exe_asset_name() -> None:
    """Compatibilidad legacy: el modelo EXE-only ya no está soportado."""
    warn("La configuracion de asset EXE fue retirada.")
    print("El unico flujo soportado es actualizar con ZIP full-package (EXE + app/).")
    press_enter()


def _toggle_auto_check() -> None:
    """Habilita o deshabilita la verificación automática."""
    config = _load_update_config()
    current = config.get("auto_check_enabled", True)
    new_value = not current
    
    config["auto_check_enabled"] = new_value
    _save_update_config(config)
    
    status = "habilitada" if new_value else "deshabilitada"
    ok(f"Verificación automática {status}.")
    press_enter()


def _show_update_history() -> None:
    """Muestra el historial de actualizaciones."""
    config = _load_update_config()
    current_version = config.get("current_version", _get_current_version())
    
    print(f"Versión actual instalada: {style_text(current_version, color=Fore.GREEN, bold=True)}")
    print()
    
    # Aquí se podría leer un historial de actualizaciones si se guarda
    warn("Historial de actualizaciones no disponible aún.")
    press_enter()
