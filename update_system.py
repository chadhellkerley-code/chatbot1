# update_system.py
# -*- coding: utf-8 -*-
"""Sistema de actualización automática usando GitHub Releases."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import SETTINGS
from licensekit import _fetch_licenses
from paths import runtime_base
from ui import Fore, banner, full_line, style_text
from utils import ask, ok, press_enter, warn

_UPDATE_CONFIG_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "update_config.json"
_UPDATE_MANIFEST_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "update_manifest.json"
_UPDATE_CACHE_DIR = runtime_base(Path(__file__).resolve().parent) / "storage" / "updates_cache"
_UPDATE_BACKUP_DIR = runtime_base(Path(__file__).resolve().parent) / "storage" / "backups"

# Configuración por defecto - GitHub
_DEFAULT_GITHUB_REPO = "chadhellkerley-code/chatbot"
_DEFAULT_UPDATE_CHECK_INTERVAL = 3600  # 1 hora
_GITHUB_TOKEN_ENV = "GITHUB_TOKEN"


def _github_headers() -> Dict[str, str]:
    token = (os.environ.get(_GITHUB_TOKEN_ENV) or "").strip()
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _load_update_config() -> Dict[str, Any]:
    """Carga la configuración de actualizaciones."""
    if not _UPDATE_CONFIG_FILE.exists():
        return {
            "auto_check_enabled": True,
            "check_interval_seconds": _DEFAULT_UPDATE_CHECK_INTERVAL,
            "last_check_ts": 0,
            "current_version": _get_current_version(),
            "exe_asset_name": os.environ.get("UPDATE_EXE_ASSET", "").strip() or None,
        }
    try:
        data = json.loads(_UPDATE_CONFIG_FILE.read_text(encoding="utf-8"))
        if "current_version" not in data:
            data["current_version"] = _get_current_version()
        if "exe_asset_name" not in data:
            data["exe_asset_name"] = os.environ.get("UPDATE_EXE_ASSET", "").strip() or None
        # Forzar repo fijo y oculto
        return data
    except Exception:
        return {
            "auto_check_enabled": True,
            "check_interval_seconds": _DEFAULT_UPDATE_CHECK_INTERVAL,
            "last_check_ts": 0,
            "current_version": _get_current_version(),
            "exe_asset_name": os.environ.get("UPDATE_EXE_ASSET", "").strip() or None,
        }


def _save_update_config(config: Dict[str, Any]) -> None:
    """Guarda la configuración de actualizaciones."""
    _UPDATE_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    _UPDATE_CONFIG_FILE.write_text(
        json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _get_current_version() -> str:
    """Obtiene la versión actual de la aplicación."""
    # Prioridad: manifest local aplicado
    manifest_paths = [
        Path(__file__).resolve().parent / "storage" / "update_manifest.json",
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
    version_file = Path(__file__).resolve().parent / "VERSION"
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


def check_for_updates(
    github_repo: Optional[str] = None,
    force: bool = False,
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """
    Verifica si hay actualizaciones disponibles desde GitHub.
    
    Args:
        github_repo: Repositorio en formato "usuario/repo" (opcional)
        force: Forzar verificación incluso si no es momento
    
    Returns:
        (hay_actualizacion, info_actualizacion, mensaje)
    """
    config = _load_update_config()
    github_repo = _DEFAULT_GITHUB_REPO
    current_version = config.get("current_version", _get_current_version())
    
    if not force:
        last_check = config.get("last_check_ts", 0)
        check_interval = config.get("check_interval_seconds", _DEFAULT_UPDATE_CHECK_INTERVAL)
        if time.time() - last_check < check_interval:
            return False, None, "Aún no es momento de verificar actualizaciones."
    
    release_info = _get_latest_release_from_github(github_repo)
    if not release_info:
        return False, None, "No se pudo conectar con GitHub o no hay releases disponibles."
    
    latest_version = release_info.get("version", "")
    if not latest_version:
        return False, None, "No se pudo determinar la versión disponible."
    
    # Comparar versiones (puede ser tag como "v1.0.1" o "1.0.1")
    current_clean = current_version.lstrip("v")
    latest_clean = latest_version.lstrip("v")
    
    if latest_clean == current_clean:
        config["last_check_ts"] = time.time()
        _save_update_config(config)
        return False, None, f"Ya tienes la versión más reciente ({current_version})."
    
    config["last_check_ts"] = time.time()
    _save_update_config(config)
    
    return True, release_info, f"Actualización disponible: {latest_version} (actual: {current_version})"


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
    
    _UPDATE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
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


def _schedule_exe_replace_windows(source_path: Path, target_path: Path) -> Tuple[bool, str]:
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


def _update_executable_from_release() -> None:
    """Descarga y programa la actualización del EXE desde GitHub Release."""
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
        default_asset = "insta_cli.exe"
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
    _UPDATE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
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
    if not update_file.exists():
        return False, "El archivo de actualización no existe."
    
    workspace_root = Path(__file__).resolve().parent
    
    try:
        # Crear backup si se solicita
        if backup:
            backup_dir = workspace_root / "storage" / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_name = f"backup_{int(time.time())}"
            backup_path = backup_dir / backup_name
            shutil.copytree(workspace_root, backup_path, ignore=shutil.ignore_patterns(
                "*.pyc", "__pycache__", ".git", "storage/backups", "storage/updates_cache"
            ))
            print(f"Backup creado en: {backup_path}")
        
        # Extraer actualización
        with zipfile.ZipFile(update_file, "r") as zip_ref:
            # Listar archivos que se van a actualizar
            file_list = zip_ref.namelist()
            print(f"Actualizando {len(file_list)} archivos...")
            
            # Extraer archivos
            zip_ref.extractall(workspace_root)
        
        # Actualizar versión en configuración
        config = _load_update_config()
        update_manifest_path = workspace_root / "storage" / "update_manifest.json"
        if update_manifest_path.exists():
            try:
                manifest_data = json.loads(update_manifest_path.read_text(encoding="utf-8"))
                new_version = manifest_data.get("version", "unknown")
                config["current_version"] = new_version
                _save_update_config(config)
            except Exception:
                pass
        
        return True, "Actualización aplicada correctamente. Reinicia la aplicación."
    except Exception as exc:
        return False, f"Error al aplicar actualización: {exc}"


def auto_update_check() -> None:
    """Verifica automáticamente actualizaciones si está habilitado."""
    config = _load_update_config()
    if not config.get("auto_check_enabled", True):
        return
    
    github_repo = _DEFAULT_GITHUB_REPO
    has_update, update_info, message = check_for_updates(
        github_repo=github_repo,
        force=False,
    )
    
    if has_update and update_info:
        print(style_text(f"[Actualización] {message}", color=Fore.YELLOW))


def menu_updates() -> None:
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
        print("1) Actualizar programa (EXE)")
        print("2) Configurar nombre del EXE")
        print("3) Habilitar/Deshabilitar verificación automática")
        print("4) Ver historial de actualizaciones")
        print("5) Volver")
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


def _check_and_apply_update() -> None:
    """Verifica y aplica actualizaciones si están disponibles."""
    config = _load_update_config()
    github_repo = config.get("github_repo", _DEFAULT_GITHUB_REPO)
    
    print("Verificando actualizaciones en GitHub...")
    has_update, update_info, message = check_for_updates(
        github_repo=github_repo,
        force=True,
    )
    
    if not has_update:
        warn(message)
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
    target_path = Path(__file__).resolve().parent / "responder.py"
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


def _configure_exe_asset_name() -> None:
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
