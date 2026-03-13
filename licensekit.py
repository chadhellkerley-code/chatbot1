# licensekit.py
# -*- coding: utf-8 -*-
"""Herramientas de gestión y entrega de licencias."""

from __future__ import annotations

import datetime as dt
import json
import os
import secrets
import shutil
import string
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from config import SETTINGS, read_env_local, refresh_settings, update_env_local
from core.storage_atomic import atomic_write_json, atomic_write_text, load_json_file
from supabase_migrations import ensure_licenses_table as run_ensure_licenses_table
from ui import Fore, banner, full_line, style_text
from utils import ask, ask_int, ok, press_enter, warn

_TABLE = "licenses"
_DATE_FMT = "%Y-%m-%d"
_STATUS_ACTIVE = "active"
_STATUS_EXPIRED = "expired"
_STATUS_PAUSED = "paused"
_STATUS_REVOKED = "revoked"
_TABLE_SQL = textwrap.dedent(
    """
    create table if not exists public.licenses (
        id uuid primary key default gen_random_uuid(),
        client_name text not null,
        client_email text,
        license_key text not null unique,
        expires_at timestamptz not null,
        status text not null default 'active',
        created_at timestamptz not null default now()
    );
    """
).strip()
_STORAGE_ROOT = Path(__file__).resolve().parent / "storage"
_PAYLOAD_PATH = _STORAGE_ROOT / "license_payload.json"
_LICENSES_FILE = _STORAGE_ROOT / "licenses.json"


def _load_local_licenses() -> List[Dict[str, Any]]:
    if not _LICENSES_FILE.exists():
        return []
    try:
        data = load_json_file(_LICENSES_FILE, [], label="licensekit.local_licenses")
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    records: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            records.append(_normalize_record(dict(item)))
    return records


def _save_local_licenses(records: List[Dict[str, Any]]) -> None:
    _LICENSES_FILE.parent.mkdir(parents=True, exist_ok=True)
    serialized = [dict(_normalize_record(dict(rec))) for rec in records]
    atomic_write_json(_LICENSES_FILE, serialized)


def _find_local_license(license_key: str) -> Optional[Dict[str, Any]]:
    key = (license_key or "").strip()
    if not key:
        return None
    for record in _load_local_licenses():
        if str(record.get("license_key", "")) == key:
            return record
    return None


def _upsert_local_license(record: Dict[str, Any]) -> Dict[str, Any]:
    records = [rec for rec in _load_local_licenses() if rec.get("license_key") != record.get("license_key")]
    normalized = _normalize_record(dict(record))
    records.append(normalized)
    _save_local_licenses(records)
    return normalized


def _delete_local_license_record(license_key: str) -> bool:
    key = (license_key or "").strip()
    if not key:
        return False
    records = _load_local_licenses()
    new_records = [rec for rec in records if rec.get("license_key") != key]
    if len(new_records) == len(records):
        return False
    _save_local_licenses(new_records)
    return True


def _supabase_credentials() -> Tuple[str, str]:
    env_local = read_env_local()
    url = (env_local.get("SUPABASE_URL") or SETTINGS.supabase_url or "").strip()
    key = (env_local.get("SUPABASE_KEY") or SETTINGS.supabase_key or "").strip()
    return url, key


def _missing_table_text() -> str:
    return (
        "La tabla 'licenses' no existe en Supabase.\n"
        "Creala ejecutando en el editor SQL (schema public):\n"
        f"{_TABLE_SQL}"
    )


def _show_missing_table_help() -> None:
    message = _missing_table_text()
    warn(message.splitlines()[0])
    print(full_line(color=Fore.BLUE))
    for line in message.splitlines()[1:]:
        print(line)
    print(full_line(color=Fore.BLUE))
    press_enter()


def _load_local_payload() -> Dict[str, Any]:
    if not _PAYLOAD_PATH.exists():
        return {}
    try:
        payload = load_json_file(_PAYLOAD_PATH, {}, label="licensekit.local_payload")
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _ensure_supabase(*, interactive: bool = True) -> Tuple[bool, Optional[str], Optional[str]]:
    url, key = _supabase_credentials()
    if url and key:
        return True, url, key

    if not interactive:
        return False, url or None, key or None

    warn("Faltan SUPABASE_URL y/o SUPABASE_KEY.")
    confirm = ask("¿Querés configurarlos ahora? (s/N): ").strip().lower()
    if confirm != "s":
        warn("Operación cancelada.")
        press_enter()
        return False, None, None

    url = ask("SUPABASE_URL: ").strip()
    key = ask("SUPABASE_KEY: ").strip()
    if not url or not key:
        warn("Se requieren ambos valores.")
        press_enter()
        return False, None, None

    update_env_local({"SUPABASE_URL": url, "SUPABASE_KEY": key})
    refresh_settings()
    ok("Credenciales guardadas en .env.local.")
    press_enter()
    return True, url, key


def _is_missing_table(error: Optional[str], status: int) -> bool:
    if status == 404:
        return True
    if error and "PGRST208" in error:
        return True
    return False


def _request(
    method: str,
    endpoint: str,
    *,
    json_payload: Any | None = None,
    url_override: str | None = None,
    key_override: str | None = None,
) -> Tuple[Any | None, Optional[str], int]:
    url, key = _supabase_credentials()
    if url_override is not None:
        url = url_override
    if key_override is not None:
        key = key_override
    if not url or not key:
        return None, "Faltan credenciales de Supabase."

    base = url.rstrip("/") + "/rest/v1/"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }
    if json_payload is not None:
        headers["Content-Type"] = "application/json"
        headers.setdefault("Prefer", "return=representation")

    try:
        response = requests.request(
            method.upper(),
            base + endpoint.lstrip("/"),
            headers=headers,
            json=json_payload,
            timeout=15,
        )
    except requests.RequestException as exc:  # pragma: no cover - red de Supabase
        return None, str(exc), 0

    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:  # pragma: no cover - fallback legible
            detail = response.text
        return None, f"{response.status_code}: {detail}", response.status_code

    if not response.text:
        return None, None, response.status_code

    try:
        return response.json(), None, response.status_code
    except ValueError:
        return response.text, None, response.status_code


def _ensure_table_ready(url: str, key: str, *, interactive: bool = True) -> bool:
    _, error, status = _request(
        "get",
        f"{_TABLE}?select=license_key&limit=1",
        url_override=url,
        key_override=key,
    )
    if _is_missing_table(error, status):
        if interactive:
            warn("La tabla de licencias no existe en Supabase.")
            choice = ask("¿Crear tabla automáticamente? (s/N): ").strip().lower()
            if choice == "s":
                created, message = run_ensure_licenses_table(url, key)
                if created:
                    ok("Tabla 'licenses' creada en Supabase.")
                    _, error, status = _request(
                        "get",
                        f"{_TABLE}?select=license_key&limit=1",
                        url_override=url,
                        key_override=key,
                    )
                    if not error:
                        return True
                else:
                    warn(message)
                    press_enter()
            else:
                _show_missing_table_help()
            return False
        _show_missing_table_help()
        return False
    if error:
        warn(f"No se pudo comprobar la tabla de licencias: {error}")
        press_enter()
        return False
    return True


def _parse_iso(value: str | None) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        value = value.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


def _is_expired(record: Dict[str, Any]) -> bool:
    expires = _parse_iso(record.get("expires_at"))
    if not expires:
        return False
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=dt.timezone.utc)
    now = dt.datetime.now(dt.timezone.utc)
    return expires < now


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    status = str(record.get("status", _STATUS_ACTIVE)).lower()
    if status not in {_STATUS_ACTIVE, _STATUS_EXPIRED, _STATUS_REVOKED, _STATUS_PAUSED}:
        status = _STATUS_ACTIVE
    record["status"] = status
    if _is_expired(record):
        record["status"] = _STATUS_EXPIRED
    return record


def _status_label(record: Dict[str, Any]) -> Tuple[str, str]:
    status = str(record.get("status", "")).lower()
    if status == _STATUS_REVOKED:
        return "Revocada", Fore.RED
    if status == _STATUS_PAUSED:
        return "Pausada", Fore.YELLOW
    if status == _STATUS_EXPIRED or _is_expired(record):
        return "Vencida", Fore.YELLOW
    return "Activa", Fore.GREEN


def _format_date(value: str | None) -> str:
    parsed = _parse_iso(value)
    if not parsed:
        return "-"
    return parsed.strftime(_DATE_FMT)


def _days_left(record: Dict[str, Any]) -> str:
    expires = _parse_iso(record.get("expires_at"))
    if not expires:
        return "-"
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=dt.timezone.utc)
    now = dt.datetime.now(dt.timezone.utc)
    delta = expires - now
    if delta.total_seconds() <= 0:
        return "0"
    return str(int(delta.total_seconds() // 86400))


def _mask_key(value: str) -> str:
    value = (value or "").strip()
    if len(value) <= 8:
        return value[:4] + "…"
    return f"{value[:4]}…{value[-4:]}"


def _safe_client_folder(name: str) -> str:
    clean = [c if c.isalnum() or c in {" ", "-", "_"} else "_" for c in name]
    result = "".join(clean).strip()
    return result or "Cliente"


def _client_delivery_folder(client_name: str) -> Path:
    return _desktop_root() / "Clientes" / _safe_client_folder(client_name)


def _desktop_root() -> Path:
    env_override = os.environ.get("DELIVERY_ROOT") or os.environ.get("DESKTOP_DIR")
    if env_override:
        candidate = Path(env_override).expanduser()
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except Exception:
            pass

    home = Path.home()
    candidates: List[Path] = []

    xdg_config = home / ".config" / "user-dirs.dirs"
    if xdg_config.exists():
        try:
            content = xdg_config.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            content = ""
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("XDG_DESKTOP_DIR") and "=" in line:
                _, value = line.split("=", 1)
                value = value.strip().strip('"')
                value = value.replace("$HOME", str(home))
                candidates.append(Path(value))

    for name in ("Desktop", "desktop", "Escritorio", "escritorio"):
        candidates.append(home / name)

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.expanduser()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.exists():
            return resolved

    default = home / "Desktop"
    default.mkdir(parents=True, exist_ok=True)
    return default


def _is_active_record(record: Dict[str, Any]) -> bool:
    status = str(record.get("status", "")).lower()
    if status != _STATUS_ACTIVE:
        return False
    expires = _parse_iso(record.get("expires_at"))
    if not expires:
        return False
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=dt.timezone.utc)
    return expires > dt.datetime.now(dt.timezone.utc)


def _count_files(root: Path) -> int:
    total = 0
    for _, _, files in os.walk(root):
        total += len(files)
    return total


def _robocopy_threads() -> int:
    raw = (os.environ.get("ROBOCOPY_MT") or "8").strip()
    try:
        value = int(raw)
    except Exception:
        value = 8
    value = max(1, min(128, value))
    return value


def _robocopy_timeout() -> int:
    raw = (os.environ.get("ROBOCOPY_TIMEOUT") or "1800").strip()
    try:
        value = int(raw)
    except Exception:
        value = 1800
    return max(60, value)

def _robocopy_tree(source: Path, destination: Path) -> Tuple[bool, str]:
    if not shutil.which("robocopy"):
        return False, "robocopy no disponible"
    threads = _robocopy_threads()
    timeout = _robocopy_timeout()
    command = [
        "robocopy",
        str(source),
        str(destination),
        "/E",
        "/R:2",
        "/W:1",
        "/XJ",
        "/NFL",
        "/NDL",
        "/NJH",
        "/NJS",
        "/NP",
    ]
    if threads > 1:
        command.append(f"/MT:{threads}")
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"robocopy excedio el timeout ({timeout}s)"
    output = "\n".join(line for line in [result.stdout, result.stderr] if line)
    if result.returncode >= 8:
        return False, output.strip()
    return True, output.strip()


def _copy_tree_robust(source: Path, destination: Path, *, verify: bool = True) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform.startswith("win"):
        ok, detail = _robocopy_tree(source, destination)
        if not ok:
            raise RuntimeError(f"Robocopy fallo: {detail or 'sin detalle'}")
    else:
        shutil.copytree(source, destination)
    if not verify:
        return
    source_count = _count_files(source)
    destination_count = _count_files(destination)
    if destination_count < source_count:
        raise RuntimeError(
            f"Copia incompleta: {destination_count}/{source_count} archivos"
        )


def _copy_directory_contents(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination, ignore_errors=True)
    destination.mkdir(parents=True, exist_ok=True)
    for item in source.iterdir():
        target = destination / item.name
        if item.is_dir():
            _copy_tree_robust(item, target)
            continue
        shutil.copy2(item, target)



def _prepare_delivery_bundle(record: Dict[str, Any], artifact_path: Path) -> Path:
    return _export_client_distribution(record, artifact_path)


def _export_client_distribution(record: Dict[str, Any], artifact_path: Path) -> Path:
    client_name = str(record.get("client_name") or "Cliente")
    delivery_dir = _client_delivery_folder(client_name)
    legacy_zip = delivery_dir / f"{_safe_client_folder(client_name)}-HerramientaIG.zip"

    try:
        if artifact_path.is_dir():
            _copy_directory_contents(artifact_path, delivery_dir)
        else:
            if delivery_dir.exists():
                shutil.rmtree(delivery_dir, ignore_errors=True)
            delivery_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(artifact_path, delivery_dir / artifact_path.name)
        _write_license_export(record, folder=delivery_dir)
        if legacy_zip.exists():
            legacy_zip.unlink()
    except Exception as exc:
        if delivery_dir.exists():
            shutil.rmtree(delivery_dir, ignore_errors=True)
        raise RuntimeError(f"Could not export the client folder: {exc}") from exc

    return delivery_dir


def _render_table(records: Iterable[Dict[str, Any]]) -> None:
    rows: List[Tuple[str, str, str, str, str, str, str]] = []
    for idx, rec in enumerate(records, start=1):
        status, color = _status_label(rec)
        status_txt = style_text(status, color=color, bold=True)
        rows.append(
            (
                str(idx),
                rec.get("client_name", "-"),
                _mask_key(rec.get("license_key", "-")),
                _format_date(rec.get("created_at")),
                _format_date(rec.get("expires_at")),
                _days_left(rec),
                status_txt,
            )
        )

    if not rows:
        warn("No hay licencias registradas.")
        return

    headers = ("#", "Cliente", "Key", "Creada", "Vence", "Días", "Estado")
    widths = [max(len(h), *(len(row[i]) for row in rows)) for i, h in enumerate(headers)]
    line = full_line(color=Fore.BLUE)
    print(line)
    header_row = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(style_text(header_row, color=Fore.CYAN, bold=True))
    print(line)
    for row in rows:
        body_row = "  ".join(row[i].ljust(widths[i]) for i in range(len(headers)))
        print(body_row)
    print(line)


def _show_active_licenses() -> None:
    """Muestra todas las licencias, sincronizando con el backend si está disponible."""
    banner()
    print(style_text("Gestión de Licencias", color=Fore.CYAN, bold=True))
    print(full_line())
    
    # Intentar sincronizar con backend
    synced = False
    url, key = _supabase_credentials()
    if url and key:
        print("Sincronizando con backend...")
        try:
            data, error, status = _request("get", f"{_TABLE}?select=*&order=created_at.desc")
            if not error and isinstance(data, list):
                # Sincronizar licencias del backend con local
                for remote_record in data:
                    _upsert_local_license(remote_record)
                synced = True
                ok(f"Sincronizadas {len(data)} licencias desde el backend.")
            else:
                warn(f"No se pudo sincronizar: {error or 'Error desconocido'}")
        except Exception as exc:
            warn(f"Error al sincronizar: {exc}")
    
    # Cargar todas las licencias (local + sincronizadas)
    all_records = _load_local_licenses()
    
    if not all_records:
        warn("No hay licencias registradas.")
        press_enter()
        return
    
    # Separar por estado
    active_records = [r for r in all_records if _is_active_record(r)]
    expired_records = [r for r in all_records if str(r.get("status", "")).lower() == _STATUS_EXPIRED or (_is_expired(r) and str(r.get("status", "")).lower() != _STATUS_REVOKED)]
    revoked_records = [r for r in all_records if str(r.get("status", "")).lower() == _STATUS_REVOKED]
    paused_records = [r for r in all_records if str(r.get("status", "")).lower() == _STATUS_PAUSED]
    
    print()
    print(style_text("=== LICENCIAS ACTIVAS ===", color=Fore.GREEN, bold=True))
    if active_records:
        active_records.sort(key=lambda r: r.get("expires_at") or "")
        _render_table(active_records)
    else:
        print("  (ninguna)")
    
    print()
    print(style_text("=== LICENCIAS VENCIDAS ===", color=Fore.YELLOW, bold=True))
    if expired_records:
        expired_records.sort(key=lambda r: r.get("expires_at") or "", reverse=True)
        _render_table(expired_records)
    else:
        print("  (ninguna)")
    
    print()
    print(style_text("=== LICENCIAS REVOCADAS ===", color=Fore.RED, bold=True))
    if revoked_records:
        revoked_records.sort(key=lambda r: r.get("expires_at") or "", reverse=True)
        _render_table(revoked_records)
    else:
        print("  (ninguna)")
    
    print()
    print(style_text("=== LICENCIAS PAUSADAS ===", color=Fore.YELLOW, bold=True))
    if paused_records:
        paused_records.sort(key=lambda r: r.get("expires_at") or "", reverse=True)
        _render_table(paused_records)
    else:
        print("  (ninguna)")
    
    print()
    print(f"Total: {len(all_records)} licencias")
    if synced:
        print(style_text("✓ Sincronizado con backend", color=Fore.GREEN))
    
    press_enter()


def _fetch_licenses(sync_with_backend: bool = True) -> List[Dict[str, Any]]:
    """
    Obtiene todas las licencias, sincronizando con el backend si está disponible.
    
    Args:
        sync_with_backend: Si sincronizar con Supabase antes de retornar
    """
    if sync_with_backend:
        url, key = _supabase_credentials()
        if url and key:
            try:
                data, error, status = _request("get", f"{_TABLE}?select=*&order=created_at.desc")
                if not error and isinstance(data, list):
                    # Sincronizar licencias del backend con local
                    for remote_record in data:
                        _upsert_local_license(remote_record)
            except Exception:
                pass  # Si falla, usar solo licencias locales
    
    return _load_local_licenses()


def _select_license(records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not records:
        warn("No hay licencias para seleccionar.")
        press_enter()
        return None
    _render_table(records)
    choice = ask("Seleccioná número de licencia (vacío para cancelar): ").strip()
    if not choice:
        warn("Operación cancelada.")
        press_enter()
        return None
    try:
        idx = int(choice)
    except ValueError:
        warn("Número inválido.")
        press_enter()
        return None
    if not 1 <= idx <= len(records):
        warn("Fuera de rango.")
        press_enter()
        return None
    return records[idx - 1]


def _update_license(license_key: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    record = _find_local_license(license_key)
    if not record:
        warn("No se encontró la licencia.")
        press_enter()
        return None
    updated = dict(record)
    updated.update(payload)
    updated = _upsert_local_license(updated)
    ok("Licencia actualizada.")
    press_enter()
    return updated


def _extend_license(record: Dict[str, Any]) -> None:
    extra_days = ask_int("Cantidad de días a extender (>=1): ", min_value=1, default=30)
    current_exp = _parse_iso(record.get("expires_at")) or dt.datetime.now(dt.timezone.utc)
    new_exp = current_exp + dt.timedelta(days=extra_days)
    payload = {
        "expires_at": new_exp.astimezone(dt.timezone.utc).isoformat(),
        "status": _STATUS_ACTIVE,
    }
    _update_license(record["license_key"], payload)


def _update_status(record: Dict[str, Any], status: str, verb: str) -> None:
    confirm = ask(f"Confirmás {verb} la licencia? (s/N): ").strip().lower()
    if confirm != "s":
        warn("Sin cambios.")
        press_enter()
        return
    payload = {"status": status}
    _update_license(record["license_key"], payload)


def _delete_license(record: Dict[str, Any]) -> None:
    confirm = ask("Confirmás eliminar la licencia? (s/N): ").strip().lower()
    if confirm != "s":
        warn("Sin cambios.")
        press_enter()
        return
    if _delete_local_license_record(record["license_key"]):
        ok("Licencia eliminada.")
    else:
        warn("No se pudo eliminar la licencia.")
    press_enter()


def _license_actions_loop(license_key: str) -> None:
    while True:
        record = _fetch_single(license_key)
        if not record:
            warn("No se encontró la licencia seleccionada.")
            press_enter()
            return
        banner()
        print(full_line())
        print(style_text("Gestión de licencia", color=Fore.CYAN, bold=True))
        print(full_line())
        _render_table([record])
        print("1) Extender vigencia")
        print("2) Pausar licencia")
        print("3) Activar licencia")
        print("4) Revocar licencia")
        print("5) Eliminar licencia")
        print("6) Volver")
        choice = ask("Opción: ").strip()
        status = str(record.get("status", "")).lower()
        if choice == "1":
            _extend_license(record)
        elif choice == "2":
            if status == _STATUS_PAUSED:
                warn("La licencia ya está en pausa.")
                press_enter()
            else:
                _update_status(record, _STATUS_PAUSED, "pausar")
        elif choice == "3":
            if status == _STATUS_ACTIVE:
                warn("La licencia ya está activa.")
                press_enter()
            else:
                _update_status(record, _STATUS_ACTIVE, "activar")
        elif choice == "4":
            _build_universal_executable()
        elif choice == "5":
            break
        elif choice == "5":
            break
            break
        elif choice == "6":
            break
        else:
            warn("Opción inválida.")
            press_enter()


def _fetch_single(license_key: str) -> Optional[Dict[str, Any]]:
    return _find_local_license(license_key)


def _generate_key() -> str:
    try:
        from src.licensing import generate_license_key

        return generate_license_key()
    except Exception:
        return "".join(secrets.choice(string.digits) for _ in range(15))


def _license_admin_client():
    from src.licensing import SupabaseLicenseClient

    return SupabaseLicenseClient(admin=True)


def _create_managed_license_record(
    client_name: str,
    *,
    days: int,
    email: str | None = None,
) -> Dict[str, Any]:
    clean_name = str(client_name or "").strip()
    if not clean_name:
        raise ValueError("Client name is required.")

    expires_at = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=max(30, int(days or 30)))
    notes = ""
    clean_email = str(email or "").strip()
    if clean_email:
        notes = f"Contact email: {clean_email}"

    record = _license_admin_client().create_license(
        client_name=clean_name,
        plan_name="standard",
        max_devices=2,
        expires_at=expires_at.isoformat(),
        notes=notes,
    )
    return _upsert_local_license(record)


def _write_license_export(record: Dict[str, Any], *, folder: Path) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    license_key = str(record.get("license_key") or "").strip()
    if not license_key:
        raise ValueError("License key is required for export.")

    atomic_write_text(folder / "license.key", license_key + "\n")

    client_name = str(record.get("client_name") or "Client").strip() or "Client"
    expires_at = _format_date(record.get("expires_at")) or "-"
    executable_name = "InstaCRM.exe" if sys.platform.startswith("win") else "InstaCRM"
    instructions = textwrap.dedent(
        f"""
        Client: {client_name}
        License Key: {license_key}
        Expires: {expires_at}

        How to use:
        1) If this folder already contains {executable_name}, run it directly.
        2) Otherwise copy license.key next to the client executable.
        3) Launch the client and allow the online license validation to complete.
        """
    ).strip()
    atomic_write_text(folder / "INSTRUCCIONES.txt", instructions + "\n")
    return folder


def _build_client_distribution_for_record(record: Dict[str, Any]) -> Tuple[bool, Optional[Path], str]:
    try:
        from build.helpers import build_client_distribution, dist_root
    except Exception as exc:
        return False, None, f"Could not import the client builder: {exc}"

    client_name = str(record.get("client_name") or "cliente").strip() or "cliente"
    target_dir = dist_root() / f"InstaCRM_{_safe_client_folder(client_name).replace(' ', '_')}"
    try:
        build_dir = build_client_distribution(
            target_dir=target_dir,
            license_key=str(record.get("license_key") or "").strip(),
        )
    except Exception as exc:
        return False, None, f"Could not build the client distribution: {exc}"
    return True, build_dir, f"Client distribution generated at {build_dir}"


def _package_license(
    record: Dict[str, Any], url: str, key: str
) -> Tuple[bool, Optional[Path], str]:
    del url, key
    success, artifact_path, message = _build_client_distribution_for_record(record)
    if not success or not artifact_path:
        return False, None, message

    try:
        bundle_path = _export_client_distribution(record, artifact_path)
    except Exception as exc:  # pragma: no cover - filesystem failures
        return False, None, f"Build generated but export failed: {exc}"

    return True, bundle_path, message


def _package_license_local(record: Dict[str, Any]) -> Tuple[bool, Optional[Path], str]:
    success, artifact_path, message = _build_client_distribution_for_record(record)
    if not success or not artifact_path:
        return False, None, message

    try:
        bundle_path = _export_client_distribution(record, artifact_path)
    except Exception as exc:  # pragma: no cover - filesystem failures
        return False, None, f"Build generated but export failed: {exc}"

    return True, bundle_path, message


def _build_executable(record: Dict[str, Any], url: str, key: str) -> None:
    choice = ask("¿Generar build para esta licencia? (s/N): ").strip().lower()
    if choice != "s":
        warn("Operación cancelada.")
        press_enter()
        return

    success, bundle_path, message = _package_license(record, url, key)
    if success:
        ok(f"{message}. Carpeta generada en: {bundle_path}")
    else:
        warn(message)
    press_enter()


def _create_license(url: str, key: str) -> None:
    banner()
    print(full_line())
    print(style_text("Nueva licencia", color=Fore.CYAN, bold=True))
    print(full_line())
    client = ask("Nombre del cliente: ").strip()
    if not client:
        warn("Se requiere un nombre de cliente.")
        press_enter()
        return
    email = ask("Email del cliente (opcional): ").strip()
    duration = ask_int("Duración en días (mínimo 30): ", min_value=30, default=30)
    issued = dt.datetime.now(dt.timezone.utc)
    expires = issued + dt.timedelta(days=duration)
    payload = {
        "license_key": _generate_key(),
        "client_name": client,
        "client_email": email or None,
        "created_at": issued.astimezone(dt.timezone.utc).isoformat(),
        "expires_at": expires.astimezone(dt.timezone.utc).isoformat(),
        "status": _STATUS_ACTIVE,
    }
    insert_payload = payload.copy()
    if not email:
        insert_payload.pop("client_email", None)
    data, error, status = _request("post", _TABLE, json_payload=[insert_payload])
    if error and "client_email" in error.lower():
        insert_payload.pop("client_email", None)
        data, error, status = _request("post", _TABLE, json_payload=[insert_payload])
    if _is_missing_table(error, status):
        _show_missing_table_help()
        return
    if error:
        warn(f"No se pudo crear la licencia: {error}")
        press_enter()
        return
    if isinstance(data, list) and data:
        record = data[0]
    else:
        record = _fetch_single(payload["license_key"]) or payload

    ok(f"Licencia creada para {client}.")
    _render_table([record])
    success, bundle_path, message = _package_license(record, url, key)
    if success:
        ok(
            f"Carpeta de entrega generada en: {bundle_path}. Último paso: compartir con el cliente."
        )
    else:
        warn(message)
    press_enter()


def _create_license_local(*, package: bool = True) -> None:
    banner()
    print(full_line())
    print(style_text("Nueva licencia", color=Fore.CYAN, bold=True))
    print(full_line())
    client = ask("Nombre del cliente: ").strip()
    if not client:
        warn("Se requiere un nombre de cliente.")
        press_enter()
        return
    email = ask("Email del cliente (opcional): ").strip()
    duration = ask_int("Dias de validez (minimo 30): ", min_value=30, default=30)
    issued = dt.datetime.now(dt.timezone.utc)
    expires = issued + dt.timedelta(days=duration)
    record = {
        "license_key": _generate_key(),
        "client_name": client,
        "client_email": email or None,
        "created_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
        "status": _STATUS_ACTIVE,
    }
    record = _upsert_local_license(record)
    ok(f"Licencia creada para {client}.")
    _render_table([record])
    if package:
        success, bundle_path, message = _package_license_local(record)
        if success:
            ok(f"Licencia creada. Carpeta de entrega generada en: {bundle_path}")
        else:
            warn(message)
    else:
        ok("Licencia creada. Exportacion omitida.")
    press_enter()


def _manage_license_simple() -> None:
    records = _fetch_licenses()
    if not records:
        press_enter()
        return
    record = _select_license(records)
    if not record:
        return

    while True:
        current = _fetch_single(record["license_key"]) or record
        banner()
        print(style_text("Gestion de licencia", color=Fore.CYAN, bold=True))
        print(full_line())
        _render_table([current])
        print("1) Extender licencia")
        print("2) Revocar licencia")
        print("3) Eliminar licencia")
        print("4) Volver")
        choice = ask("Opcion: ").strip()
        if choice == "1":
            _extend_license(current)
        elif choice == "2":
            _update_status(current, _STATUS_REVOKED, "revocar")
            break
        elif choice == "3":
            _delete_license(current)
            break
        elif choice == "4":
            break
        else:
            warn("Opcion invalida.")


def verify_license_remote(
    license_key: str,
    supabase_url: str,
    supabase_key: str,
) -> Tuple[bool, str, Dict[str, Any]]:
    """Valida una licencia usando Supabase."""

    if not license_key:
        return False, "Falta la licencia.", {}
    if not supabase_url or not supabase_key:
        return False, "Faltan credenciales de Supabase.", {}
    endpoint = f"{_TABLE}?license_key=eq.{license_key}&select=*"
    data, error, status = _request(
        "get", endpoint, url_override=supabase_url, key_override=supabase_key
    )
    if _is_missing_table(error, status):
        return False, _missing_table_text(), {}
    if error:
        return False, error, {}
    if not isinstance(data, list) or not data:
        return False, "Licencia inexistente.", {}
    record = data[0]
    status_value = str(record.get("status", "")).lower()
    if status_value == _STATUS_REVOKED:
        return False, "Licencia revocada.", record
    if status_value == _STATUS_EXPIRED or _is_expired(record):
        return False, "Licencia vencida.", record
    if status_value and status_value != _STATUS_ACTIVE:
        return False, f"Licencia en estado {status_value}.", record
    return True, "", record


def validate_license_payload(
    input_key: str, payload: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Valida una licencia contra el payload incrustado en el cliente."""

    record = _normalize_record(dict(payload or {}))
    expected = str(record.get("license_key", "")).strip()
    if not expected:
        return False, "Licencia no configurada.", {}
    provided = (input_key or "").strip()
    if provided != expected:
        return False, "Licencia incorrecta.", record
    status = record.get("status", _STATUS_ACTIVE)
    if status == _STATUS_REVOKED:
        return False, "Licencia revocada.", record
    if status == _STATUS_PAUSED:
        return False, "Licencia pausada.", record
    if _is_expired(record):
        return False, "Licencia vencida.", record
    return True, "", record


def enforce_startup_validation() -> None:
    if os.environ.get("LICENSE_ALREADY_VALIDATED") == "1":
        return
    if not SETTINGS.client_distribution:
        return
    try:
        from license_client import LicenseStartupError, launch_with_license

        launch_with_license()
        os.environ["LICENSE_ALREADY_VALIDATED"] = "1"
    except LicenseStartupError as exc:
        print(full_line(color=Fore.RED))
        print(style_text("Licencia inválida", color=Fore.RED, bold=True))
        print(str(exc.user_message or "No se pudo validar la licencia."))
        print(full_line(color=Fore.RED))
        sys.exit(2)


def _build_universal_executable() -> None:
    try:
        from tools.build_executable import build_for_license
    except Exception as exc:  # pragma: no cover - entorno sin modulo
        warn(f"No se pudo importar el generador: {exc}")
        press_enter()
        return

    banner()
    print(full_line())
    print(style_text("Ejecutable universal", color=Fore.CYAN, bold=True))
    print(full_line())
    print("Opciones de build:")
    print("1) Completo (incluye Playwright + browsers) - mas lento")
    print("2) Rapido (Playwright externo) - requiere 'playwright install' en cliente")
    print("3) Reusar ejecutable universal existente")
    choice = ask("Opcion: ").strip()
    if choice not in {"1", "2", "3"}:
        warn("Opcion invalida.")
        press_enter()
        return

    record = {
        "license_key": "",
        "client_name": "universal",
        "expires_at": None,
        "status": _STATUS_ACTIVE,
    }

    if choice == "3":
        dist_dir = Path(__file__).resolve().parent / "dist"
        candidates = [
            dist_dir / "insta_cli_universal.exe",
            dist_dir / "insta_cli_universal",
            dist_dir / "insta_cli_universal.app",
        ]
        existing = next((item for item in candidates if item.exists()), None)
        if not existing:
            warn("No existe un ejecutable universal previo en dist/.")
            press_enter()
            return

        ok(f"Ejecutable universal existente: {existing}")

        use_onefile = not existing.is_dir()
        browsers_root = existing if existing.is_dir() else existing.parent
        bundle_dir = browsers_root / "playwright_browsers"
        bundle_mode = "all" if bundle_dir.exists() else "external"
        mode_label = "onefile" if use_onefile else "onedir"
        browsers_label = "incluidos" if bundle_mode == "all" else "externos"
        print(f"Actualizando con modo {mode_label} y Playwright {browsers_label}...")

        previous = {
            "LICENSE_REMOTE_ONLY": os.environ.get("LICENSE_REMOTE_ONLY"),
            "INCLUDE_PLAYWRIGHT": os.environ.get("INCLUDE_PLAYWRIGHT"),
            "PLAYWRIGHT_BUNDLE": os.environ.get("PLAYWRIGHT_BUNDLE"),
            "PYINSTALLER_ONEFILE": os.environ.get("PYINSTALLER_ONEFILE"),
            "BUILD_MODE": os.environ.get("BUILD_MODE"),
        }
        os.environ["LICENSE_REMOTE_ONLY"] = "1"
        os.environ["INCLUDE_PLAYWRIGHT"] = "1"
        os.environ["PYINSTALLER_ONEFILE"] = "1" if use_onefile else "0"
        os.environ["PLAYWRIGHT_BUNDLE"] = bundle_mode
        os.environ["BUILD_MODE"] = "full"
        try:
            success, artifact_path, message = build_for_license(record)
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        if success:
            ok(message)
            if bundle_mode == "external":
                print("Nota: build rapido. En la PC del cliente ejecutar: python -m playwright install")
            if use_onefile and bundle_mode == "all":
                print("Nota: se requiere conservar playwright_browsers al lado del EXE.")
        else:
            warn(message)
        press_enter()
        return

    if choice == "2":
        use_onefile = True
        build_mode = "minimal"
    else:
        onefile_choice = ask("Generar un solo EXE (onefile)? (s/N): ").strip().lower()
        use_onefile = onefile_choice == "s"
        build_mode = "full"

    previous = {
        "LICENSE_REMOTE_ONLY": os.environ.get("LICENSE_REMOTE_ONLY"),
        "INCLUDE_PLAYWRIGHT": os.environ.get("INCLUDE_PLAYWRIGHT"),
        "PLAYWRIGHT_BUNDLE": os.environ.get("PLAYWRIGHT_BUNDLE"),
        "PYINSTALLER_ONEFILE": os.environ.get("PYINSTALLER_ONEFILE"),
        "BUILD_MODE": os.environ.get("BUILD_MODE"),
    }
    os.environ["LICENSE_REMOTE_ONLY"] = "1"
    os.environ["INCLUDE_PLAYWRIGHT"] = "1"
    os.environ["PYINSTALLER_ONEFILE"] = "1" if use_onefile else "0"
    os.environ["BUILD_MODE"] = build_mode
    if choice == "2":
        os.environ["PLAYWRIGHT_BUNDLE"] = "external"
    else:
        os.environ["PLAYWRIGHT_BUNDLE"] = "all"
    try:
        success, artifact_path, message = build_for_license(record)
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    if success:
        ok(message)
        if choice == "2":
            print("Nota: build rapido. En la PC del cliente ejecutar: python -m playwright install")
        if use_onefile and choice == "1":
            print("Nota: se requiere conservar playwright_browsers al lado del EXE.")
    else:
        warn(message)
    press_enter()


def _build_owner_macos_universal_executable() -> None:
    try:
        from tools.build_owner_macos_universal import build_owner_macos_universal
    except Exception as exc:  # pragma: no cover - entorno sin modulo
        warn(f"No se pudo importar el builder owner macOS: {exc}")
        press_enter()
        return

    banner()
    print(full_line())
    print(style_text("Ejecutable owner universal macOS", color=Fore.CYAN, bold=True))
    print(full_line())
    print("Objetivo: macOS Big Sur (11) o superior")
    print("Arquitectura: universal2 (Intel + Apple Silicon)")
    print("1) Onefile (recomendado)")
    print("2) Onedir")
    build_mode = ask("Opcion: ").strip()
    if build_mode not in {"1", "2"}:
        warn("Opcion invalida.")
        press_enter()
        return

    include_browsers = (
        ask("Incluir Chromium de Playwright dentro del bundle? (s/N): ").strip().lower() == "s"
    )
    timeout_default = 7200
    timeout_value = ask_int(
        f"Timeout PyInstaller en segundos (default {timeout_default}): ",
        min_value=600,
        default=timeout_default,
    )

    artifact_name = ask(
        "Nombre del artefacto (Enter = insta_owner_universal_macos): "
    ).strip() or "insta_owner_universal_macos"

    print("Iniciando build owner macOS universal...")
    success, artifact_path, message = build_owner_macos_universal(
        name=artifact_name,
        onefile=(build_mode == "1"),
        bundle_playwright_browsers=include_browsers,
        timeout_seconds=timeout_value,
    )
    if success:
        ok(message)
        if artifact_path is not None:
            print(f"Artefacto: {artifact_path}")
        if not include_browsers:
            print("Nota: para Playwright en destino ejecutar: python -m playwright install chromium")
    else:
        warn(message)
    press_enter()


def _create_backend_license_and_files() -> None:
    banner()
    print(full_line())
    print(style_text("Crear licencia remota", color=Fore.CYAN, bold=True))
    print(full_line())

    name = ask("Nombre del cliente: ").strip()
    if not name:
        warn("Se requiere un nombre de cliente.")
        press_enter()
        return

    email = ask("Email del cliente (opcional): ").strip() or None
    days = ask_int("Dias de validez (minimo 30): ", min_value=30, default=60)

    try:
        record = _create_managed_license_record(name, days=days, email=email)
    except Exception as exc:
        warn(f"No se pudo crear la licencia remota: {exc}")
        press_enter()
        return

    folder = _client_delivery_folder(name)
    try:
        _write_license_export(record, folder=folder)
    except Exception as exc:
        warn(f"No se pudieron exportar los archivos de licencia: {exc}")
        press_enter()
        return

    ok(f"Licencia creada. Archivos en: {folder}")
    press_enter()


def _write_backend_license_files() -> None:
    banner()
    print(full_line())
    print(style_text("Exportar archivo de licencia", color=Fore.CYAN, bold=True))
    print(full_line())

    client = ask("Nombre del cliente: ").strip()
    if not client:
        warn("Se requiere un nombre de cliente.")
        press_enter()
        return

    license_key = ask("License Key: ").strip()
    if not license_key:
        warn("License key requerida.")
        press_enter()
        return

    folder = _client_delivery_folder(client)
    try:
        _write_license_export(
            {
                "client_name": client,
                "license_key": license_key,
                "expires_at": "",
                "status": _STATUS_ACTIVE,
            },
            folder=folder,
        )
    except Exception as exc:
        warn(f"No se pudieron generar los archivos: {exc}")
        press_enter()
        return

    ok(f"Archivos generados en: {folder}")
    press_enter()


def menu_deliver() -> None:
    if SETTINGS.client_distribution:
        warn("Esta opcion no esta disponible en builds de cliente.")
        press_enter()
        return
    while True:
        banner()
        print(full_line())
        print(style_text("Entrega al cliente", color=Fore.CYAN, bold=True))
        print(full_line())
        print("1) Crear licencia en backend + archivo")
        print("2) Exportar archivo de licencia (backend)")
        print("3) Crear nueva licencia local (sin ZIP)")
        print("4) Ver todas las licencias (sincronizar con backend)")
        print("5) Eliminar o extender licencia")
        print("6) Generar ejecutable universal (backend)")
        print("7) Generar ejecutable owner universal (macOS Big Sur+)")
        print("8) Volver")
        print()
        choice = ask("Opcion: ").strip()
        if choice == "1":
            _create_backend_license_and_files()
        elif choice == "2":
            _write_backend_license_files()
        elif choice == "3":
            _create_license_local(package=False)
        elif choice == "4":
            _show_active_licenses()
        elif choice == "5":
            _manage_license_simple()
        elif choice == "6":
            _build_universal_executable()
        elif choice == "7":
            _build_owner_macos_universal_executable()
        elif choice == "8":
            break
        else:
            warn("Opcion invalida.")
            press_enter()


def list_licenses() -> List[Dict[str, Any]]:
    """Devuelve las licencias almacenadas localmente."""

    return _fetch_licenses()


def fetch_license(license_key: str) -> Optional[Dict[str, Any]]:
    """Obtiene una licencia puntual."""

    return _fetch_single(license_key)


def package_license(license_key: str) -> Tuple[bool, Optional[Path], str]:
    """Genera artefactos limpios para la licencia indicada."""

    record = _fetch_single(license_key)
    if not record:
        return False, None, "Licencia no encontrada."
    return _package_license_local(record)

