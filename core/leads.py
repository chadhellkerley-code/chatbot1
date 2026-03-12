# leads.py
# -*- coding: utf-8 -*-
import asyncio
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.leads_filter_store import LeadFilterStore, build_filter_list_id
from core.leads_import import LeadImportError, read_usernames_from_csv, read_usernames_from_txt
from core.leads_store import (
    LeadListStore,
    normalize_lead_username,
    normalize_lead_usernames,
)
from paths import leads_root, runtime_base, storage_root
from core.templates_store import load_templates, save_templates
from utils import (
    ask,
    ask_int,
    ask_multiline,
    banner,
    err,
    ok,
    press_enter,
    title,
    warn,
)

APP_DIR = Path(__file__).resolve().parent.parent
BASE = Path(APP_DIR)
TEXT = APP_DIR / "storage" / "leads"

DEFAULT_EXPORT_ALIAS = "leads_filtrados"

FILTER_STATE_REQUIRED = "required"
FILTER_STATE_INDIFFERENT = "indifferent"
FILTER_STATE_DISABLED = "disabled"


def refresh_runtime_paths(base: Path | None = None) -> dict[str, Path]:
    global BASE, TEXT, FILTER_STORAGE, FILTER_LISTS, FILTER_CONFIG_PATH

    resolved_base = runtime_base(Path(base) if base is not None else APP_DIR)
    resolved_base.mkdir(parents=True, exist_ok=True)
    BASE = resolved_base
    TEXT = leads_root(BASE)
    FILTER_STORAGE = storage_root(BASE) / "lead_filters"
    FILTER_LISTS = FILTER_STORAGE / "lists"
    FILTER_CONFIG_PATH = FILTER_STORAGE / "filters_config.json"
    FILTER_STORAGE.mkdir(parents=True, exist_ok=True)
    FILTER_LISTS.mkdir(parents=True, exist_ok=True)
    return {
        "base": BASE,
        "leads_root": TEXT,
        "filter_storage": FILTER_STORAGE,
        "filter_lists": FILTER_LISTS,
        "filter_config": FILTER_CONFIG_PATH,
    }


def _lead_list_store() -> LeadListStore:
    return LeadListStore(TEXT)


def _list_all_accounts() -> List[Dict[str, Any]]:
    from core.accounts import list_all

    return list_all()


def list_all() -> List[Dict[str, Any]]:
    return _list_all_accounts()


def validate_list_name(name: object) -> str:
    return _lead_list_store().validate_name(name)


def list_files()->List[str]:
    return _lead_list_store().list_names()

def _normalize_lead_username(raw: object) -> str:
    """
    Normaliza usernames importados desde archivos/listas.
    Evita falsos "username_not_found" por BOM o caracteres invisibles.
    """
    return normalize_lead_username(raw)

def load_list(name:str)->List[str]:
    return _lead_list_store().load(name)

def append_list(name:str, usernames:List[str]):
    _lead_list_store().append(name, usernames)


def save_list(name: str, usernames: List[str]) -> None:
    _lead_list_store().save(name, usernames)

def import_csv(path:str, name:str):
    path=Path(path)
    if not path.exists():
        warn("CSV no encontrado."); return
    try:
        users = read_usernames_from_csv(path)
    except LeadImportError as exc:
        warn(str(exc)); return
    append_list(name, users)
    ok(f"Importados {len(users)} a {name}.")

def show_list(name:str):
    users=load_list(name)
    print(f"{name}: {len(users)} usuarios")
    for i,u in enumerate(users[:50],1):
        print(f"{i:02d}. @{u}")
    if len(users)>50: print(f"... (+{len(users)-50})")

def delete_list(name:str):
    if _lead_list_store().delete(name): ok("Eliminada.")
    else: warn("No existe.")


def _template_preview(text: str, limit: int = 60) -> str:
    cleaned = " ".join((text or "").splitlines()).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)] + "..."


def _list_templates() -> List[Dict[str, str]]:
    templates = load_templates()
    if not templates:
        warn("No hay plantillas guardadas.")
        return []
    for idx, item in enumerate(templates, start=1):
        preview = _template_preview(item.get("text", ""))
        print(f" {idx}) {item.get('name', '')} - {preview}")
    return templates


def _select_template_index(templates: List[Dict[str, str]]) -> Optional[int]:
    if not templates:
        return None
    choice = ask("Selecciona numero de plantilla (Enter para cancelar): ").strip()
    if not choice:
        return None
    if not choice.isdigit():
        warn("Seleccion invalida.")
        return None
    idx = int(choice)
    if 1 <= idx <= len(templates):
        return idx - 1
    warn("Seleccion fuera de rango.")
    return None


def menu_templates() -> None:
    while True:
        banner()
        title("Plantillas")
        templates = load_templates()
        print(f"Plantillas guardadas: {len(templates)}")
        print("\n1) Crear plantilla")
        print("2) Listar plantillas")
        print("3) Editar plantilla")
        print("4) Eliminar plantilla")
        print("5) Volver\n")
        op = ask("Opcion: ").strip()
        if op == "1":
            name = ask("Nombre de la plantilla: ").strip()
            if not name:
                warn("Nombre requerido.")
                press_enter()
                continue
            text = ask_multiline("Texto de la plantilla:")
            if not text:
                warn("Texto requerido.")
                press_enter()
                continue
            templates = load_templates()
            templates.append({"name": name, "text": text})
            save_templates(templates)
            ok("Plantilla guardada.")
            press_enter()
        elif op == "2":
            banner()
            title("Listado de plantillas")
            _list_templates()
            press_enter()
        elif op == "3":
            banner()
            title("Editar plantilla")
            templates = _list_templates()
            idx = _select_template_index(templates)
            if idx is None:
                press_enter()
                continue
            current = templates[idx]
            print("\nTexto actual:\n")
            print(current.get("text", ""))
            new_name = ask(f"Nombre ({current.get('name', '')}): ").strip()
            new_text = ask_multiline("Nuevo texto (vacio para mantener):")
            if new_name:
                current["name"] = new_name
            if new_text:
                current["text"] = new_text
            templates[idx] = current
            save_templates(templates)
            ok("Plantilla actualizada.")
            press_enter()
        elif op == "4":
            banner()
            title("Eliminar plantilla")
            templates = _list_templates()
            idx = _select_template_index(templates)
            if idx is None:
                press_enter()
                continue
            target = templates[idx]
            confirm = ask(f"Eliminar '{target.get('name', '')}'? (s/N): ").strip().lower()
            if confirm == "s":
                templates.pop(idx)
                save_templates(templates)
                ok("Plantilla eliminada.")
            else:
                warn("Sin cambios.")
            press_enter()
        elif op == "5":
            break
        else:
            warn("Opcion invalida.")
            press_enter()

def menu_leads():
    while True:
        banner()
        title("Listas de leads")
        files=list_files()
        if files: print("Disponibles:", ", ".join(files))
        else: print("(aÃºn no hay listas)")
        print("\n1) Crear lista y agregar manual")
        print("2) Importar CSV a una lista")
        print("3) Ver lista")
        print("4) Eliminar lista")
        print("5) Gestionar plantillas")
        print("6) Filtrado de Leads")
        print("7) Volver\n")
        op=ask("Opcion: ").strip()
        if op=="1":
            name=ask("Nombre de la lista: ").strip() or "default"
            print("PegÃ¡ usernames (uno por lÃ­nea). LÃ­nea vacÃ­a para terminar:")
            lines=[]
            while True:
                s=ask("")
                if not s: break
                lines.append(s)
            append_list(name, lines); ok("Guardado."); press_enter()
        elif op=="2":
            path=ask("Ruta del CSV: ")
            name=ask("Importar a la lista (nombre): ").strip() or "default"
            import_csv(path, name); press_enter()
        elif op=="3":
            name=ask("Nombre de la lista: ").strip()
            show_list(name); press_enter()
        elif op=="4":
            name=ask("Nombre de la lista: ").strip()
            delete_list(name); press_enter()
        elif op=="5":
            menu_templates()
        elif op=="6":
            filter_leads_pipeline()
        elif op=="7":
            break
        else:
            warn("OpciÃ³n invÃ¡lida."); press_enter()


@dataclass
class ScrapedUser:
    username: str
    biography: str
    full_name: str
    follower_count: int
    media_count: int
    is_private: bool
    profile_pic_url: str = ""
    user_id: str = ""
    external_url: str = ""
    is_verified: bool = False


@dataclass
class ClassicFilterConfig:
    min_followers: int
    min_posts: int
    privacy: str  # public | private | any
    link_in_bio: str  # yes | no | any
    include_keywords: List[str]
    exclude_keywords: List[str]
    language: str  # es | pt | en | any
    min_followers_state: str = FILTER_STATE_DISABLED
    min_posts_state: str = FILTER_STATE_DISABLED
    privacy_state: str = FILTER_STATE_DISABLED
    link_in_bio_state: str = FILTER_STATE_DISABLED
    include_keywords_state: str = FILTER_STATE_DISABLED
    exclude_keywords_state: str = FILTER_STATE_DISABLED
    language_state: str = FILTER_STATE_DISABLED


@dataclass
class TextFilterConfig:
    enabled: bool
    criteria: str
    model_path: str
    state: str = FILTER_STATE_DISABLED
    engine_thresholds: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ImageFilterConfig:
    enabled: bool
    prompt: str
    state: str = FILTER_STATE_DISABLED
    engine_thresholds: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LeadFilterConfig:
    classic: ClassicFilterConfig
    text: TextFilterConfig
    image: ImageFilterConfig


@dataclass
class LeadFilterRunConfig:
    alias: str
    accounts: List[str]
    concurrency: int
    delay_min: float
    delay_max: float
    headless: Optional[bool] = None
    max_runtime_seconds: float = 3600.0


def _dedupe_preserve_order(usernames: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for username in usernames:
        key = username.strip().lstrip("@").lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(username.strip().lstrip("@"))
    return ordered


def _normalize_filter_state(raw: Any, *, default: str = FILTER_STATE_DISABLED) -> str:
    value = str(raw or "").strip().lower()
    mapping = {
        "required": FILTER_STATE_REQUIRED,
        "requerido": FILTER_STATE_REQUIRED,
        "requerida": FILTER_STATE_REQUIRED,
        "indifferent": FILTER_STATE_INDIFFERENT,
        "indiferente": FILTER_STATE_INDIFFERENT,
        "disabled": FILTER_STATE_DISABLED,
        "deshabilitado": FILTER_STATE_DISABLED,
        "desactivado": FILTER_STATE_DISABLED,
        "off": FILTER_STATE_DISABLED,
    }
    normalized = mapping.get(value, value)
    valid_states = {
        FILTER_STATE_REQUIRED,
        FILTER_STATE_INDIFFERENT,
        FILTER_STATE_DISABLED,
    }
    if normalized not in valid_states:
        fallback = str(default or FILTER_STATE_DISABLED).strip().lower()
        fallback = mapping.get(fallback, fallback)
        if fallback in valid_states:
            return fallback
        return FILTER_STATE_DISABLED
    return normalized


def _filter_state_label(state: str) -> str:
    normalized = _normalize_filter_state(state)
    if normalized == FILTER_STATE_REQUIRED:
        return "REQUIRED"
    if normalized == FILTER_STATE_INDIFFERENT:
        return "INDIFFERENT"
    return "DISABLED"


def _is_filter_active(state: str) -> bool:
    normalized = _normalize_filter_state(state)
    return normalized in {FILTER_STATE_REQUIRED, FILTER_STATE_INDIFFERENT}


def _normalize_choice(value: Any, *, allowed: set[str], default: str) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in allowed:
        return candidate
    return default


def _normalize_keywords(values: Iterable[Any]) -> List[str]:
    deduped: List[str] = []
    seen: set[str] = set()
    for raw in values:
        candidate = str(raw or "").strip()
        key = candidate.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _sanitize_classic_filter_config(cfg: ClassicFilterConfig) -> ClassicFilterConfig:
    min_followers = max(0, int(cfg.min_followers or 0))
    min_posts = max(0, int(cfg.min_posts or 0))
    privacy = _normalize_choice(cfg.privacy, allowed={"public", "private", "any"}, default="any")
    link_in_bio = _normalize_choice(cfg.link_in_bio, allowed={"yes", "no", "any"}, default="any")
    language = _normalize_choice(cfg.language, allowed={"es", "pt", "en", "any"}, default="any")
    include_keywords = _normalize_keywords(cfg.include_keywords)
    exclude_keywords = _normalize_keywords(cfg.exclude_keywords)

    min_followers_state = _normalize_filter_state(
        cfg.min_followers_state,
        default=FILTER_STATE_REQUIRED if min_followers > 0 else FILTER_STATE_DISABLED,
    )
    min_posts_state = _normalize_filter_state(
        cfg.min_posts_state,
        default=FILTER_STATE_REQUIRED if min_posts > 0 else FILTER_STATE_DISABLED,
    )
    privacy_state = _normalize_filter_state(
        cfg.privacy_state,
        default=FILTER_STATE_REQUIRED if privacy != "any" else FILTER_STATE_DISABLED,
    )
    link_in_bio_state = _normalize_filter_state(
        cfg.link_in_bio_state,
        default=FILTER_STATE_REQUIRED if link_in_bio != "any" else FILTER_STATE_DISABLED,
    )
    include_keywords_state = _normalize_filter_state(
        cfg.include_keywords_state,
        default=FILTER_STATE_REQUIRED if include_keywords else FILTER_STATE_DISABLED,
    )
    exclude_keywords_state = _normalize_filter_state(
        cfg.exclude_keywords_state,
        default=FILTER_STATE_REQUIRED if exclude_keywords else FILTER_STATE_DISABLED,
    )
    language_state = _normalize_filter_state(
        cfg.language_state,
        default=FILTER_STATE_REQUIRED if language != "any" else FILTER_STATE_DISABLED,
    )

    if min_followers <= 0:
        min_followers_state = FILTER_STATE_DISABLED
    if min_posts <= 0:
        min_posts_state = FILTER_STATE_DISABLED
    if privacy == "any":
        privacy_state = FILTER_STATE_DISABLED
    if link_in_bio == "any":
        link_in_bio_state = FILTER_STATE_DISABLED
    if not include_keywords:
        include_keywords_state = FILTER_STATE_DISABLED
    if not exclude_keywords:
        exclude_keywords_state = FILTER_STATE_DISABLED
    if language == "any":
        language_state = FILTER_STATE_DISABLED

    return ClassicFilterConfig(
        min_followers=min_followers,
        min_posts=min_posts,
        privacy=privacy,
        link_in_bio=link_in_bio,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        language=language,
        min_followers_state=min_followers_state,
        min_posts_state=min_posts_state,
        privacy_state=privacy_state,
        link_in_bio_state=link_in_bio_state,
        include_keywords_state=include_keywords_state,
        exclude_keywords_state=exclude_keywords_state,
        language_state=language_state,
    )


def _default_text_engine_thresholds_payload() -> Dict[str, Any]:
    from src.text_engine_thresholds import (
        default_text_engine_thresholds,
        text_engine_thresholds_to_dict,
    )

    return text_engine_thresholds_to_dict(default_text_engine_thresholds())


def _sanitize_text_engine_thresholds_payload(raw: Any) -> Dict[str, Any]:
    from src.text_engine_thresholds import sanitize_text_engine_thresholds_payload

    return sanitize_text_engine_thresholds_payload(raw if isinstance(raw, dict) else {})


def _default_image_engine_thresholds_payload() -> Dict[str, Any]:
    from src.image_engine_thresholds import (
        default_image_engine_thresholds,
        image_engine_thresholds_to_dict,
    )

    return image_engine_thresholds_to_dict(default_image_engine_thresholds())


def _sanitize_image_engine_thresholds_payload(raw: Any) -> Dict[str, Any]:
    from src.image_engine_thresholds import sanitize_image_engine_thresholds_payload

    return sanitize_image_engine_thresholds_payload(raw if isinstance(raw, dict) else {})


def _prompt_filter_state(label: str, *, default: str) -> str:
    default_state = _normalize_filter_state(default)
    default_choice = {
        FILTER_STATE_REQUIRED: "1",
        FILTER_STATE_INDIFFERENT: "2",
        FILTER_STATE_DISABLED: "3",
    }[default_state]
    print(f"\nEstado para '{label}':")
    print("[1] REQUIRED")
    print("[2] INDIFFERENT")
    print("[3] DISABLED")
    choice = ask(f"Opcion ({default_choice} por defecto): ").strip() or default_choice
    if choice == "1":
        return FILTER_STATE_REQUIRED
    if choice == "2":
        return FILTER_STATE_INDIFFERENT
    if choice == "3":
        return FILTER_STATE_DISABLED
    warn("Estado invalido. Se usara DISABLED.")
    return FILTER_STATE_DISABLED


def _should_stop(stop_event: asyncio.Event) -> bool:
    if stop_event.is_set():
        return True
    if _runtime_stop_is_set():
        stop_event.set()
        warn("Deteniendo filtrado por solicitud del usuario.")
        return True
    if _poll_quit_key():
        stop_event.set()
        _request_runtime_stop("se presionÃ³ Q")
        warn("Deteniendo filtrado por solicitud del usuario (Q).")
        return True
    return False


def _runtime_stop_is_set() -> bool:
    try:
        from runtime.runtime import STOP_EVENT
    except Exception:
        return False
    try:
        return bool(STOP_EVENT.is_set())
    except Exception:
        return False


def _request_runtime_stop(reason: str) -> None:
    try:
        from runtime.runtime import request_stop
    except Exception:
        return
    try:
        request_stop(reason)
    except Exception:
        return


def _reset_runtime_stop_event() -> None:
    try:
        from runtime.runtime import reset_stop_event
    except Exception:
        return
    try:
        reset_stop_event()
    except Exception:
        return


def _poll_quit_key() -> bool:
    try:
        if os.name == "nt":
            import msvcrt  # type: ignore

            if not msvcrt.kbhit():
                return False
            key = msvcrt.getch()
            return key in (b"q", b"Q")
        import select

        ready, _, _ = select.select([sys.stdin], [], [], 0)
        if ready:
            key = sys.stdin.read(1)
            return key.lower() == "q"
    except Exception:
        return False


def _run_async(coro):
    from src.runtime.playwright_runtime import run_coroutine_sync

    return run_coroutine_sync(coro)


def _resolve_accounts(requested_usernames: List[str]) -> List[Dict[str, Any]]:
    try:
        records = list_all()
    except Exception as exc:
        warn(f"No se pudieron cargar las cuentas: {exc}")
        return []

    by_username: Dict[str, Dict[str, Any]] = {}
    for account in records:
        raw_username = str(account.get("username") or "").strip().lstrip("@")
        if not raw_username:
            continue
        key = raw_username.lower()
        if key not in by_username:
            by_username[key] = account

    if not requested_usernames:
        return list(by_username.values())

    resolved: List[Dict[str, Any]] = []
    missing: List[str] = []
    normalized_requested = _dedupe_preserve_order(requested_usernames)
    for handle in normalized_requested:
        key = str(handle or "").strip().lstrip("@").lower()
        if not key:
            continue
        account = by_username.get(key)
        if account is None:
            missing.append(handle)
            continue
        resolved.append(account)

    if missing:
        preview = ", ".join(f"@{item}" for item in missing[:5])
        suffix = " ..." if len(missing) > 5 else ""
        warn(f"Cuentas no encontradas para el run: {preview}{suffix}")

    return resolved


# --- Lead filtering pipeline (Playwright + Texto inteligente + Imagen) ---

refresh_runtime_paths()


def _filter_store() -> LeadFilterStore:
    return LeadFilterStore(FILTER_STORAGE)


def filter_leads_pipeline() -> None:
    while True:
        banner()
        title("Filtrado de Leads")
        print("[1] Crear nuevo filtrado")
        print("[2] Ver listas de filtrado")
        print("[3] ConfiguraciÃ³n de filtros")
        print("[0] Volver\n")
        op = ask("Opcion: ").strip()
        if op == "1":
            _filter_create_flow()
        elif op == "2":
            _filter_lists_menu()
        elif op == "3":
            _filter_config_menu()
        elif op == "0":
            break
        else:
            warn("Opcion invalida.")
            press_enter()


def _filter_create_flow() -> None:
    banner()
    title("Crear nuevo filtrado")
    usernames = _prompt_usernames_source()
    if not usernames:
        warn("No hay usernames para procesar.")
        press_enter()
        return
    list_data = _create_filter_list(usernames)
    export_alias = _prompt_export_alias(existing=list_data.get("export_alias"))
    list_data["export_alias"] = export_alias
    _save_filter_list(list_data)
    ok(f"Lista creada: {list_data.get('id')}")

    filter_cfg = _load_filter_config()
    if not filter_cfg:
        warn("No hay filtros configurados. Usa 'ConfiguraciÃ³n de filtros' primero.")
        press_enter()
        return

    run_cfg = _prompt_run_config()
    if not run_cfg:
        press_enter()
        return

    list_data["run"] = _run_config_to_dict(run_cfg)
    list_data["filters"] = _filter_config_to_dict(filter_cfg)
    _save_filter_list(list_data)

    _execute_filter_list(list_data)


def _filter_lists_menu() -> None:
    while True:
        banner()
        title("Listas de filtrado")
        print("[1] Listas completas")
        print("[2] Listas incompletas")
        print("[0] Volver\n")
        op = ask("Opcion: ").strip()
        if op == "0":
            return
        if op not in {"1", "2"}:
            warn("Opcion invalida.")
            press_enter()
            continue
        show_completed = op == "1"
        selected = _select_filter_list(show_completed)
        if not selected:
            press_enter()
            continue
        _filter_list_actions(selected)


def _filter_list_actions(list_data: Dict[str, Any]) -> None:
    while True:
        banner()
        title(f"Lista {list_data.get('id')}")
        _print_filter_list_summary(list_data)
        print("\n[1] Ver resultados")
        print("[2] Reanudar filtrado")
        print("[3] Eliminar lista")
        print("[0] Volver\n")
        op = ask("Opcion: ").strip()
        if op == "1":
            _print_filter_list_results(list_data)
            press_enter()
        elif op == "2":
            pending = _pending_count(list_data)
            if pending == 0:
                warn("La lista ya esta completa.")
                press_enter()
                continue
            _execute_filter_list(list_data, resume=True)
            return
        elif op == "3":
            confirm = ask("Eliminar esta lista? (s/N): ").strip().lower()
            if confirm == "s":
                _delete_filter_list(list_data)
                ok("Lista eliminada.")
                press_enter()
                return
        elif op == "0":
            return
        else:
            warn("Opcion invalida.")
            press_enter()


def _filter_config_menu() -> None:
    while True:
        banner()
        title("ConfiguraciÃ³n de filtros")
        print("[1] Crear nuevos filtros (elimina todos los anteriores)")
        print("[2] Modificar un filtro existente")
        print("[3] Eliminar todos los filtros")
        print("[4] Ver filtros actuales")
        print("[0] Volver\n")
        op = ask("Opcion: ").strip()
        if op == "0":
            return
        if op == "1":
            cfg = _prompt_filter_config(existing=None)
            if cfg:
                _save_filter_config(cfg)
                ok("Filtros creados.")
            press_enter()
        elif op == "2":
            current = _load_filter_config()
            if not current:
                warn("No hay filtros existentes para modificar.")
                press_enter()
                continue
            cfg = _prompt_filter_config(existing=current)
            if cfg:
                _save_filter_config(cfg)
                ok("Filtros actualizados.")
            press_enter()
        elif op == "3":
            if FILTER_CONFIG_PATH.exists():
                FILTER_CONFIG_PATH.unlink()
                ok("Filtros eliminados.")
            else:
                warn("No hay filtros para eliminar.")
            press_enter()
        elif op == "4":
            cfg = _load_filter_config()
            if not cfg:
                warn("No hay filtros configurados.")
            else:
                _print_filter_config(cfg)
            press_enter()
        else:
            warn("Opcion invalida.")
            press_enter()


def _print_filter_config(cfg: LeadFilterConfig) -> None:
    print("\nFiltros actuales:")
    print("- Clasicos:")
    print(
        f"  - Seguidores minimos: {cfg.classic.min_followers} "
        f"[{_filter_state_label(cfg.classic.min_followers_state)}]"
    )
    print(
        f"  - Posts minimos: {cfg.classic.min_posts} "
        f"[{_filter_state_label(cfg.classic.min_posts_state)}]"
    )
    print(
        f"  - Privacidad: {cfg.classic.privacy} "
        f"[{_filter_state_label(cfg.classic.privacy_state)}]"
    )
    print(
        f"  - Link en bio: {cfg.classic.link_in_bio} "
        f"[{_filter_state_label(cfg.classic.link_in_bio_state)}]"
    )
    lang_label = {
        "es": "espanol",
        "pt": "portugues",
        "en": "ingles",
        "any": "indiferente",
    }.get(cfg.classic.language, "indiferente")
    print(
        f"  - Idioma: {lang_label} "
        f"[{_filter_state_label(cfg.classic.language_state)}]"
    )
    includes = ", ".join(cfg.classic.include_keywords) if cfg.classic.include_keywords else "(ninguno)"
    excludes = ", ".join(cfg.classic.exclude_keywords) if cfg.classic.exclude_keywords else "(ninguno)"
    print(
        f"  - Keywords include: {includes} "
        f"[{_filter_state_label(cfg.classic.include_keywords_state)}]"
    )
    print(
        f"  - Keywords exclude: {excludes} "
        f"[{_filter_state_label(cfg.classic.exclude_keywords_state)}]"
    )
    print("- Texto inteligente:")
    print(
        f"  - Estado: {_filter_state_label(cfg.text.state)} "
        f"(activado: {'si' if cfg.text.enabled else 'no'})"
    )
    if _is_filter_active(cfg.text.state):
        snippet = (cfg.text.criteria or "").strip()
        snippet = (snippet[:80] + "...") if len(snippet) > 80 else snippet
        print(f"  - Criterio: {snippet or '(vacio)'}")
    print("- Imagen:")
    print(
        f"  - Estado: {_filter_state_label(cfg.image.state)} "
        f"(activado: {'si' if cfg.image.enabled else 'no'})"
    )
    if _is_filter_active(cfg.image.state):
        visual = (cfg.image.prompt or "").strip()
        visual = (visual[:80] + "...") if len(visual) > 80 else visual
        print(f"  - Prompt visual: {visual or '(vacio)'}")


def _prompt_export_alias(existing: Optional[str]) -> str:
    default = existing or DEFAULT_EXPORT_ALIAS
    alias = ask(f"Alias/nombre para guardar leads filtrados ({default} por defecto): ").strip()
    return alias or default


def _collect_qualified_usernames(list_data: Dict[str, Any]) -> List[str]:
    return [
        item.get("username", "").strip().lstrip("@")
        for item in list_data.get("items", [])
        if item.get("status") == "QUALIFIED"
    ]


def _export_to_alias(alias: str, usernames: List[str]) -> None:
    if not alias or not usernames:
        return
    existing = load_list(alias)
    merged = _dedupe_preserve_order([*existing, *usernames])
    save_list(alias, merged)
    ok(f"Leads guardados en alias '{alias}' ({len(usernames)} nuevos, total {len(merged)}).")


def _auto_export_on_complete(list_data: Dict[str, Any]) -> None:
    _refresh_list_stats(list_data)
    if list_data.get("status") != "done":
        return
    alias = list_data.get("export_alias") or DEFAULT_EXPORT_ALIAS
    qualified = _collect_qualified_usernames(list_data)
    if not qualified:
        warn("Filtrado completado pero no hubo leads calificados para guardar.")
        return
    _export_to_alias(alias, qualified)


def _handle_partial_export(list_data: Dict[str, Any]) -> None:
    _refresh_list_stats(list_data)
    alias = list_data.get("export_alias") or DEFAULT_EXPORT_ALIAS
    qualified = _collect_qualified_usernames(list_data)
    print("\nFiltrado detenido. Â¿QuÃ© querÃ©s hacer?")
    print("[1] Guardar leads calificados hasta ahora en el alias")
    print("[2] No guardar (mantener lista incompleta)")
    print("[3] Eliminar la lista incompleta")
    print("[4] Volver al menÃº (mantener lista incompleta)")
    choice = ask("Opcion: ").strip() or "2"
    if choice == "1":
        if qualified:
            _export_to_alias(alias, qualified)
        else:
            warn("No hay leads calificados aÃºn para guardar.")
        _save_filter_list(list_data)
    elif choice == "3":
        _delete_filter_list(list_data)
        ok("Lista eliminada.")
    else:
        # opciones 2 y 4: mantener lista incompleta
        _save_filter_list(list_data)


def _prompt_usernames_source() -> List[str]:
    while True:
        print("\nPaso 1 - Carga de usernames")
        print("[1] Importar CSV")
        print("[2] Importar TXT")
        print("[3] Pegar manualmente")
        print("[0] Volver\n")
        op = ask("Opcion: ").strip()
        if op == "0":
            return []
        if op == "1":
            path = ask("Ruta del CSV: ").strip()
            raw = _load_usernames_from_csv(path)
        elif op == "2":
            path = ask("Ruta del TXT: ").strip()
            raw = _load_usernames_from_txt(path)
        elif op == "3":
            raw = _load_usernames_from_paste()
        else:
            warn("Opcion invalida.")
            continue
        cleaned = _normalize_usernames(raw)
        if not cleaned:
            warn("No se cargaron usernames validos.")
            continue
        ok(f"Usernames cargados: {len(cleaned)}")
        return cleaned


def _load_usernames_from_csv(path: str) -> List[str]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        warn("CSV no encontrado.")
        return []
    try:
        return read_usernames_from_csv(file_path)
    except LeadImportError as exc:
        warn(str(exc))
        return []


def _load_usernames_from_txt(path: str) -> List[str]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        warn("TXT no encontrado.")
        return []
    try:
        return read_usernames_from_txt(file_path)
    except LeadImportError as exc:
        warn(str(exc))
        return []


def _load_usernames_from_paste() -> List[str]:
    raw = ask_multiline("Pega usernames (uno por linea):")
    return [line.strip() for line in raw.splitlines()]


def _normalize_usernames(values: Iterable[str]) -> List[str]:
    return normalize_lead_usernames(values)


def _prompt_run_config() -> Optional[LeadFilterRunConfig]:
    alias = _prompt_alias()
    if not alias:
        return None
    accounts = _prompt_accounts_for_alias(alias)
    if not accounts:
        return None
    concurrency = ask_int(
        "Cantidad de cuentas en simultaneo: ",
        min_value=1,
        default=min(5, len(accounts)),
    )
    concurrency = min(concurrency, len(accounts))
    delay_min = ask_int("Delay minimo (segundos): ", min_value=0, default=20)
    delay_max = ask_int("Delay maximo (segundos): ", min_value=0, default=max(delay_min, 40))
    if delay_max < delay_min:
        warn("El delay maximo era menor al minimo. Se invirtieron los valores.")
        delay_min, delay_max = delay_max, delay_min
    max_runtime_seconds = ask_int(
        "Duracion maxima del run (segundos, 0 sin limite): ",
        min_value=0,
        default=3600,
    )
    print("\nModo navegador:")
    print("[1] Segundo plano (headless)")
    print("[2] Visible (headful)")
    print("[3] Usar configuraciÃ³n del env (LEADS_HEADFUL/HUMAN_HEADFUL)")
    headless_choice = ask("Opcion (1/2/3): ").strip() or "1"
    if headless_choice == "1":
        headless = True
    elif headless_choice == "2":
        headless = False
    else:
        headless = None
    return LeadFilterRunConfig(
        alias=alias,
        accounts=accounts,
        concurrency=concurrency,
        delay_min=float(delay_min),
        delay_max=float(delay_max),
        headless=headless,
        max_runtime_seconds=float(max_runtime_seconds),
    )


def _prompt_alias() -> Optional[str]:
    try:
        records = list_all()
    except Exception as exc:
        warn(f"No se pudieron cargar las cuentas: {exc}")
        return None
    aliases = sorted({(it.get("alias") or "default") for it in records})
    if not aliases:
        warn("No hay alias configurados.")
        return None
    print("\nAlias disponibles:")
    for idx, alias in enumerate(aliases, start=1):
        print(f" {idx}) {alias}")
    choice = ask("Alias: ").strip()
    if not choice:
        warn("Operacion cancelada.")
        return None
    if choice.isdigit():
        idx = int(choice)
        if 1 <= idx <= len(aliases):
            return aliases[idx - 1]
    for alias in aliases:
        if alias.lower() == choice.lower():
            return alias
    warn("Alias invalido.")
    return None


def _prompt_accounts_for_alias(alias: str) -> List[str]:
    accounts = [acct for acct in list_all() if (acct.get("alias") or "default") == alias]
    if not accounts:
        warn("No hay cuentas en ese alias.")
        return []
    print("\nSelecciona cuentas:")
    print("[1] Usar todas las cuentas del alias")
    print("[2] Seleccionar cuentas manualmente")
    choice = ask("Opcion (1/2): ").strip() or "1"
    if choice == "1":
        return [acct.get("username") for acct in accounts if acct.get("username")]
    if choice != "2":
        warn("Opcion invalida.")
        return []
    for idx, acct in enumerate(accounts, start=1):
        print(f" {idx}) @{acct.get('username')}")
    raw = ask("Ingresa numeros separados por coma: ").strip()
    if not raw:
        warn("No se seleccionaron cuentas.")
        return []
    selected: List[str] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk.isdigit():
            continue
        idx = int(chunk)
        if 1 <= idx <= len(accounts):
            username = accounts[idx - 1].get("username")
            if username:
                selected.append(username)
    selected = _dedupe_preserve_order(selected)
    if not selected:
        warn("No se seleccionaron cuentas validas.")
    return selected


def _prompt_filter_config(existing: Optional[LeadFilterConfig]) -> Optional[LeadFilterConfig]:
    print("\nFiltros clasicos")
    classic = _prompt_classic_filter(existing.classic if existing else None)
    if classic is None:
        return None
    print("\nFiltro de texto inteligente")
    text = _prompt_text_filter(existing.text if existing else None)
    if text is None:
        return None
    print("\nFiltro de imagen")
    image = _prompt_image_filter(existing.image if existing else None)
    if image is None:
        return None
    return LeadFilterConfig(classic=classic, text=text, image=image)


def _prompt_classic_filter(existing: Optional[ClassicFilterConfig]) -> Optional[ClassicFilterConfig]:
    min_followers = ask_int(
        "Seguidores minimos (>= X): ",
        min_value=0,
        default=existing.min_followers if existing else 0,
    )
    min_posts = ask_int(
        "Posts minimos (>= X): ",
        min_value=0,
        default=existing.min_posts if existing else 0,
    )
    min_followers_state_default = (
        existing.min_followers_state
        if existing
        else (FILTER_STATE_REQUIRED if min_followers > 0 else FILTER_STATE_DISABLED)
    )
    min_followers_state = _prompt_filter_state(
        "Seguidores minimos",
        default=min_followers_state_default,
    )
    min_posts_state_default = (
        existing.min_posts_state
        if existing
        else (FILTER_STATE_REQUIRED if min_posts > 0 else FILTER_STATE_DISABLED)
    )
    min_posts_state = _prompt_filter_state(
        "Posts minimos",
        default=min_posts_state_default,
    )

    print("\nPrivacidad del perfil:")
    print("[1] Publico")
    print("[2] Privado")
    print("[3] Indiferente")
    default_privacy = "3"
    if existing:
        default_privacy = "1" if existing.privacy == "public" else "2" if existing.privacy == "private" else "3"
    privacy_choice = ask(f"Opcion ({default_privacy} por defecto): ").strip() or default_privacy
    privacy = "any"
    if privacy_choice == "1":
        privacy = "public"
    elif privacy_choice == "2":
        privacy = "private"
    privacy_state = _prompt_filter_state(
        "Privacidad",
        default=existing.privacy_state if existing else (FILTER_STATE_REQUIRED if privacy != "any" else FILTER_STATE_DISABLED),
    )
    if privacy_state != FILTER_STATE_DISABLED and privacy == "any":
        warn("Privacidad en 'indiferente' no puede ser REQUIRED/INDIFFERENT. Se cambia a DISABLED.")
        privacy_state = FILTER_STATE_DISABLED

    print("\nLink en biografia:")
    print("[1] Si")
    print("[2] No")
    print("[3] Ignorar")
    default_link = "3"
    if existing:
        default_link = "1" if existing.link_in_bio == "yes" else "2" if existing.link_in_bio == "no" else "3"
    link_choice = ask(f"Opcion ({default_link} por defecto): ").strip() or default_link
    link_in_bio = "any"
    if link_choice == "1":
        link_in_bio = "yes"
    elif link_choice == "2":
        link_in_bio = "no"
    link_in_bio_state = _prompt_filter_state(
        "Link en biografia",
        default=existing.link_in_bio_state if existing else (FILTER_STATE_REQUIRED if link_in_bio != "any" else FILTER_STATE_DISABLED),
    )
    if link_in_bio_state != FILTER_STATE_DISABLED and link_in_bio == "any":
        warn("Link en bio en 'ignorar' no puede ser REQUIRED/INDIFFERENT. Se cambia a DISABLED.")
        link_in_bio_state = FILTER_STATE_DISABLED

    include_keywords: List[str] = existing.include_keywords if existing else []
    if ask("Â¿Agregar palabras clave obligatorias? (s/N): ").strip().lower() == "s":
        include_keywords = _prompt_keywords(
            "Palabras clave que DEBE contener (coma o salto de linea):",
            include_keywords,
        )
    include_keywords_state = _prompt_filter_state(
        "Keywords include",
        default=existing.include_keywords_state if existing else (FILTER_STATE_REQUIRED if include_keywords else FILTER_STATE_DISABLED),
    )
    if include_keywords_state != FILTER_STATE_DISABLED and not include_keywords:
        warn("Keywords include vacio. Se cambia a DISABLED.")
        include_keywords_state = FILTER_STATE_DISABLED

    exclude_keywords: List[str] = existing.exclude_keywords if existing else []
    if ask("Â¿Agregar palabras clave prohibidas? (s/N): ").strip().lower() == "s":
        exclude_keywords = _prompt_keywords(
            "Palabras clave que NO debe contener:",
            exclude_keywords,
        )
    exclude_keywords_state = _prompt_filter_state(
        "Keywords exclude",
        default=existing.exclude_keywords_state if existing else (FILTER_STATE_REQUIRED if exclude_keywords else FILTER_STATE_DISABLED),
    )
    if exclude_keywords_state != FILTER_STATE_DISABLED and not exclude_keywords:
        warn("Keywords exclude vacio. Se cambia a DISABLED.")
        exclude_keywords_state = FILTER_STATE_DISABLED

    print("\nIdioma del perfil:")
    print("[1] EspaÃ±ol")
    print("[2] PortuguÃ©s")
    print("[3] InglÃ©s")
    print("[4] Indiferente")
    default_lang = "4"
    if existing:
        default_lang = (
            "1" if existing.language == "es" else
            "2" if existing.language == "pt" else
            "3" if existing.language == "en" else
            "4"
        )
    lang_choice = ask(f"Opcion ({default_lang} por defecto): ").strip() or default_lang
    language = "any"
    if lang_choice == "1":
        language = "es"
    elif lang_choice == "2":
        language = "pt"
    elif lang_choice == "3":
        language = "en"
    language_state = _prompt_filter_state(
        "Idioma",
        default=existing.language_state if existing else (FILTER_STATE_REQUIRED if language != "any" else FILTER_STATE_DISABLED),
    )
    if language_state != FILTER_STATE_DISABLED and language == "any":
        warn("Idioma en 'indiferente' no puede ser REQUIRED/INDIFFERENT. Se cambia a DISABLED.")
        language_state = FILTER_STATE_DISABLED

    return ClassicFilterConfig(
        min_followers=min_followers,
        min_posts=min_posts,
        privacy=privacy,
        link_in_bio=link_in_bio,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        language=language,
        min_followers_state=min_followers_state,
        min_posts_state=min_posts_state,
        privacy_state=privacy_state,
        link_in_bio_state=link_in_bio_state,
        include_keywords_state=include_keywords_state,
        exclude_keywords_state=exclude_keywords_state,
        language_state=language_state,
    )


def _prompt_text_filter(existing: Optional[TextFilterConfig]) -> Optional[TextFilterConfig]:
    default_state = (
        existing.state
        if existing
        else FILTER_STATE_DISABLED
    )
    base_thresholds = _sanitize_text_engine_thresholds_payload(
        existing.engine_thresholds if existing else {}
    )
    state = _prompt_filter_state("Texto inteligente", default=default_state)
    if state == FILTER_STATE_DISABLED:
        return TextFilterConfig(
            enabled=False,
            criteria=(existing.criteria if existing else ""),
            model_path="",
            state=FILTER_STATE_DISABLED,
            engine_thresholds=base_thresholds,
        )
    if existing and existing.criteria:
        print("(Deja vacio para mantener el criterio actual)")
    criteria = ask_multiline("Criterio de filtrado (texto):").strip()
    if not criteria and existing:
        criteria = (existing.criteria or "").strip()
    if not criteria:
        warn("El criterio de texto es obligatorio.")
        return None
    return TextFilterConfig(
        enabled=True,
        criteria=criteria,
        model_path="",
        state=state,
        engine_thresholds=base_thresholds,
    )


def _prompt_image_filter(existing: Optional[ImageFilterConfig]) -> Optional[ImageFilterConfig]:
    default_state = (
        existing.state
        if existing
        else FILTER_STATE_DISABLED
    )
    base_thresholds = _sanitize_image_engine_thresholds_payload(
        existing.engine_thresholds if existing else {}
    )
    state = _prompt_filter_state("Imagen", default=default_state)
    if state == FILTER_STATE_DISABLED:
        return ImageFilterConfig(
            enabled=False,
            prompt=(existing.prompt if existing else ""),
            state=FILTER_STATE_DISABLED,
            engine_thresholds=base_thresholds,
        )
    if existing and existing.prompt:
        print("(Deja vacio para mantener el prompt actual)")
    prompt = ask_multiline("Prompt visual:").strip()
    if not prompt and existing:
        prompt = (existing.prompt or "").strip()
    if not prompt:
        warn("El prompt visual es obligatorio.")
        return None
    return ImageFilterConfig(
        enabled=True,
        prompt=prompt,
        state=state,
        engine_thresholds=base_thresholds,
    )


def _prompt_keywords(prompt: str, existing: List[str]) -> List[str]:
    if existing:
        print(f"(Actuales: {', '.join(existing)})")
    raw = ask_multiline(prompt).strip()
    if not raw:
        return existing if existing else []
    tokens = [chunk.strip() for chunk in raw.replace("\n", ",").split(",")]
    cleaned = [tok for tok in tokens if tok]
    return _dedupe_preserve_order(cleaned)


def _create_filter_list(usernames: List[str]) -> Dict[str, Any]:
    now = _now_iso()
    list_id = build_filter_list_id()
    items = [
        {
            "username": username,
            "status": "PENDING",
            "result": "",
            "reason": "",
            "account": "",
            "updated_at": "",
        }
        for username in usernames
    ]
    data = {
        "id": list_id,
        "created_at": now,
        "status": "pending",
        "total": len(items),
        "processed": 0,
        "qualified": 0,
        "discarded": 0,
        "export_alias": DEFAULT_EXPORT_ALIAS,
        "run": {},
        "filters": {},
        "items": items,
    }
    _save_filter_list(data)
    return data


def _filter_config_to_dict(cfg: LeadFilterConfig) -> Dict[str, Any]:
    classic_cfg = _sanitize_classic_filter_config(cfg.classic)
    text_criteria = str(cfg.text.criteria or "").strip()
    text_state = _normalize_filter_state(
        cfg.text.state,
        default=FILTER_STATE_REQUIRED if cfg.text.enabled and text_criteria else FILTER_STATE_DISABLED,
    )
    if not text_criteria and text_state != FILTER_STATE_REQUIRED:
        text_state = FILTER_STATE_DISABLED
    text_enabled = _is_filter_active(text_state) and bool(text_criteria)
    image_prompt = str(cfg.image.prompt or "").strip()
    image_state = _normalize_filter_state(
        cfg.image.state,
        default=FILTER_STATE_REQUIRED if cfg.image.enabled and image_prompt else FILTER_STATE_DISABLED,
    )
    if not image_prompt and image_state != FILTER_STATE_REQUIRED:
        image_state = FILTER_STATE_DISABLED
    image_enabled = _is_filter_active(image_state) and bool(image_prompt)
    return {
        "saved_at": _now_iso(),
        "classic": {
            "min_followers": classic_cfg.min_followers,
            "min_posts": classic_cfg.min_posts,
            "privacy": classic_cfg.privacy,
            "link_in_bio": classic_cfg.link_in_bio,
            "include_keywords": classic_cfg.include_keywords,
            "exclude_keywords": classic_cfg.exclude_keywords,
            "language": classic_cfg.language,
            "min_followers_state": _normalize_filter_state(classic_cfg.min_followers_state),
            "min_posts_state": _normalize_filter_state(classic_cfg.min_posts_state),
            "privacy_state": _normalize_filter_state(classic_cfg.privacy_state),
            "link_in_bio_state": _normalize_filter_state(classic_cfg.link_in_bio_state),
            "include_keywords_state": _normalize_filter_state(classic_cfg.include_keywords_state),
            "exclude_keywords_state": _normalize_filter_state(classic_cfg.exclude_keywords_state),
            "language_state": _normalize_filter_state(classic_cfg.language_state),
        },
        "text": {
            "enabled": text_enabled,
            "criteria": text_criteria,
            "model_path": cfg.text.model_path,
            "state": text_state,
            "engine_thresholds": _sanitize_text_engine_thresholds_payload(
                cfg.text.engine_thresholds
            ),
        },
        "image": {
            "enabled": image_enabled,
            "prompt": image_prompt,
            "state": image_state,
            "engine_thresholds": _sanitize_image_engine_thresholds_payload(
                cfg.image.engine_thresholds
            ),
        },
    }


def _filter_config_from_dict(data: Dict[str, Any]) -> Optional[LeadFilterConfig]:
    if not data:
        return None
    classic_raw = data.get("classic") or {}
    text_raw = data.get("text") or {}
    image_raw = data.get("image") or {}
    min_followers = int(classic_raw.get("min_followers") or 0)
    min_posts = int(classic_raw.get("min_posts") or 0)
    privacy = _normalize_choice(classic_raw.get("privacy"), allowed={"public", "private", "any"}, default="any")
    link_in_bio = _normalize_choice(classic_raw.get("link_in_bio"), allowed={"yes", "no", "any"}, default="any")
    include_keywords = _normalize_keywords(classic_raw.get("include_keywords") or [])
    exclude_keywords = _normalize_keywords(classic_raw.get("exclude_keywords") or [])
    language = _normalize_choice(classic_raw.get("language"), allowed={"es", "pt", "en", "any"}, default="any")
    classic = _sanitize_classic_filter_config(ClassicFilterConfig(
        min_followers=min_followers,
        min_posts=min_posts,
        privacy=_normalize_choice(classic_raw.get("privacy"), allowed={"public", "private", "any"}, default="any"),
        link_in_bio=_normalize_choice(classic_raw.get("link_in_bio"), allowed={"yes", "no", "any"}, default="any"),
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        language=language,
        min_followers_state=_normalize_filter_state(
            classic_raw.get("min_followers_state"),
            default=FILTER_STATE_REQUIRED if min_followers > 0 else FILTER_STATE_DISABLED,
        ),
        min_posts_state=_normalize_filter_state(
            classic_raw.get("min_posts_state"),
            default=FILTER_STATE_REQUIRED if min_posts > 0 else FILTER_STATE_DISABLED,
        ),
        privacy_state=_normalize_filter_state(
            classic_raw.get("privacy_state"),
            default=FILTER_STATE_REQUIRED if privacy != "any" else FILTER_STATE_DISABLED,
        ),
        link_in_bio_state=_normalize_filter_state(
            classic_raw.get("link_in_bio_state"),
            default=FILTER_STATE_REQUIRED if link_in_bio != "any" else FILTER_STATE_DISABLED,
        ),
        include_keywords_state=_normalize_filter_state(
            classic_raw.get("include_keywords_state"),
            default=FILTER_STATE_REQUIRED if include_keywords else FILTER_STATE_DISABLED,
        ),
        exclude_keywords_state=_normalize_filter_state(
            classic_raw.get("exclude_keywords_state"),
            default=FILTER_STATE_REQUIRED if exclude_keywords else FILTER_STATE_DISABLED,
        ),
        language_state=_normalize_filter_state(
            classic_raw.get("language_state"),
            default=FILTER_STATE_REQUIRED if language != "any" else FILTER_STATE_DISABLED,
        ),
    ))
    text_enabled = bool(text_raw.get("enabled"))
    text_criteria = str(text_raw.get("criteria") or "")
    text_state = _normalize_filter_state(
        text_raw.get("state"),
        default=FILTER_STATE_REQUIRED if text_enabled and text_criteria else FILTER_STATE_DISABLED,
    )
    if not text_criteria and text_state != FILTER_STATE_REQUIRED:
        text_state = FILTER_STATE_DISABLED
    text_engine_thresholds = _sanitize_text_engine_thresholds_payload(
        text_raw.get("engine_thresholds") or {}
    )
    text = TextFilterConfig(
        enabled=_is_filter_active(text_state) and bool(text_criteria.strip()),
        criteria=text_criteria,
        model_path="",
        state=text_state,
        engine_thresholds=text_engine_thresholds,
    )
    image_enabled = bool(image_raw.get("enabled"))
    image_prompt = str(image_raw.get("prompt") or "")
    image_state = _normalize_filter_state(
        image_raw.get("state"),
        default=FILTER_STATE_REQUIRED if image_enabled and image_prompt else FILTER_STATE_DISABLED,
    )
    if not image_prompt and image_state != FILTER_STATE_REQUIRED:
        image_state = FILTER_STATE_DISABLED
    image_engine_thresholds = _sanitize_image_engine_thresholds_payload(
        image_raw.get("engine_thresholds") or {}
    )
    image = ImageFilterConfig(
        enabled=_is_filter_active(image_state) and bool(image_prompt.strip()),
        prompt=image_prompt,
        state=image_state,
        engine_thresholds=image_engine_thresholds,
    )
    return LeadFilterConfig(classic=classic, text=text, image=image)


def _run_config_to_dict(cfg: LeadFilterRunConfig) -> Dict[str, Any]:
    return {
        "alias": cfg.alias,
        "accounts": cfg.accounts,
        "concurrency": cfg.concurrency,
        "delay_min": cfg.delay_min,
        "delay_max": cfg.delay_max,
        "headless": cfg.headless,
        "max_runtime_seconds": cfg.max_runtime_seconds,
    }


def _run_config_from_dict(data: Dict[str, Any]) -> Optional[LeadFilterRunConfig]:
    if not data:
        return None
    headless_raw = data.get("headless")
    headless: Optional[bool]
    if isinstance(headless_raw, bool):
        headless = headless_raw
    else:
        headless = None
    raw_max_runtime = data.get("max_runtime_seconds")
    if raw_max_runtime is None:
        max_runtime_seconds = 3600.0
    else:
        max_runtime_seconds = float(raw_max_runtime or 0)
    return LeadFilterRunConfig(
        alias=str(data.get("alias") or ""),
        accounts=list(data.get("accounts") or []),
        concurrency=int(data.get("concurrency") or 1),
        delay_min=float(data.get("delay_min") or 0),
        delay_max=float(data.get("delay_max") or 0),
        headless=headless,
        max_runtime_seconds=max_runtime_seconds,
    )


def _save_filter_config(cfg: LeadFilterConfig) -> None:
    payload = _filter_config_to_dict(cfg)
    _filter_store().save_config(payload)


def _load_filter_config() -> Optional[LeadFilterConfig]:
    payload = _filter_store().load_config()
    if not payload:
        return None
    return _filter_config_from_dict(payload)


def _save_filter_list(data: Dict[str, Any]) -> None:
    list_id = data.get("id") or build_filter_list_id()
    data["id"] = list_id
    if "export_alias" not in data:
        data["export_alias"] = DEFAULT_EXPORT_ALIAS
    _filter_store().save_list(data)


def _save_filter_list_runtime_state(
    data: Dict[str, Any],
    *,
    item_indexes: Optional[Iterable[int]] = None,
) -> None:
    list_id = data.get("id") or build_filter_list_id()
    data["id"] = list_id
    if "export_alias" not in data:
        data["export_alias"] = DEFAULT_EXPORT_ALIAS
    _filter_store().save_runtime_state(data, item_indexes=item_indexes)


def _load_filter_lists() -> List[Dict[str, Any]]:
    try:
        from src.leads_payload_migration import normalize_filter_list_payload
    except Exception:
        normalize_filter_list_payload = None  # type: ignore[assignment]
    lists = _filter_store().load_lists(migrate=normalize_filter_list_payload)
    for payload in lists:
        if "export_alias" not in payload:
            payload["export_alias"] = DEFAULT_EXPORT_ALIAS
    return lists


def _load_filter_list_summaries(status: str | None = None) -> List[Dict[str, Any]]:
    try:
        from src.leads_payload_migration import normalize_filter_list_payload
    except Exception:
        normalize_filter_list_payload = None  # type: ignore[assignment]
    rows = _filter_store().list_summaries(
        migrate=normalize_filter_list_payload,
        status=status,
    )
    for payload in rows:
        if "export_alias" not in payload:
            payload["export_alias"] = DEFAULT_EXPORT_ALIAS
        payload["pending"] = max(0, int(payload.get("pending") or 0))
        payload["source_list"] = str(payload.get("source_list") or payload.get("list_name") or "")
    return rows


def _load_filter_list_by_id(list_id: str) -> Optional[Dict[str, Any]]:
    try:
        from src.leads_payload_migration import normalize_filter_list_payload
    except Exception:
        normalize_filter_list_payload = None  # type: ignore[assignment]
    row = _filter_store().load_list(list_id, migrate=normalize_filter_list_payload)
    if not row:
        return None
    if "export_alias" not in row:
        row["export_alias"] = DEFAULT_EXPORT_ALIAS
    return row


def _select_filter_list(show_completed: bool) -> Optional[Dict[str, Any]]:
    lists = _load_filter_lists()
    if not lists:
        warn("No hay listas disponibles.")
        return None
    filtered: List[Dict[str, Any]] = []
    for item in lists:
        pending = _pending_count(item)
        if show_completed and pending == 0:
            filtered.append(item)
        if not show_completed and pending > 0:
            filtered.append(item)
    if not filtered:
        warn("No hay listas en este estado.")
        return None
    print("")
    for idx, item in enumerate(filtered, start=1):
        _refresh_list_stats(item)
        created = item.get("created_at", "?")
        total = item.get("total", 0)
        processed = item.get("processed", 0)
        qualified = item.get("qualified", 0)
        discarded = item.get("discarded", 0)
        print(f" {idx}) {created} | total={total} procesados={processed} calificados={qualified} descartados={discarded}")
    choice = ask("Selecciona una lista (Enter para volver): ").strip()
    if not choice:
        return None
    if not choice.isdigit():
        warn("Seleccion invalida.")
        return None
    idx = int(choice)
    if 1 <= idx <= len(filtered):
        return filtered[idx - 1]
    warn("Seleccion invalida.")
    return None


def _print_filter_list_summary(list_data: Dict[str, Any]) -> None:
    _refresh_list_stats(list_data)
    print(f"Fecha: {list_data.get('created_at', '?')}")
    print(f"Total: {list_data.get('total', 0)}")
    print(f"Procesados: {list_data.get('processed', 0)}")
    print(f"Calificados: {list_data.get('qualified', 0)}")
    print(f"Descartados: {list_data.get('discarded', 0)}")


def _print_filter_list_results(list_data: Dict[str, Any]) -> None:
    print("\nResultados:")
    for item in list_data.get("items", []):
        username = item.get("username", "")
        result = item.get("result") or "PENDING"
        reason = item.get("reason") or "-"
        account = item.get("account") or "-"
        print(f"@{username} | {result} | {reason} | cuenta: {account}")


def _delete_filter_list(list_data: Dict[str, Any]) -> None:
    _filter_store().delete_list(list_data)


def _pending_count(list_data: Dict[str, Any]) -> int:
    items = list_data.get("items") or []
    return sum(1 for item in items if item.get("status") == "PENDING")


def _refresh_list_stats(list_data: Dict[str, Any]) -> None:
    items = list_data.get("items") or []
    qualified = sum(1 for item in items if item.get("status") == "QUALIFIED")
    discarded = sum(1 for item in items if item.get("status") == "DISCARDED")
    processed = qualified + discarded
    total = len(items)
    list_data["total"] = total
    list_data["processed"] = processed
    list_data["qualified"] = qualified
    list_data["discarded"] = discarded
    list_data["status"] = "done" if processed >= total else "pending"


def _execute_filter_list(list_data: Dict[str, Any], *, resume: bool = False) -> None:
    if not list_data.get("export_alias"):
        list_data["export_alias"] = _prompt_export_alias(existing=None)
        _save_filter_list(list_data)
    filter_cfg = _filter_config_from_dict(list_data.get("filters") or {})
    if not filter_cfg:
        filter_cfg = _load_filter_config()
    if not filter_cfg:
        warn("No hay filtros configurados para ejecutar.")
        press_enter()
        return
    run_cfg = _run_config_from_dict(list_data.get("run") or {})
    if not run_cfg:
        run_cfg = _prompt_run_config()
        if not run_cfg:
            press_enter()
            return
        list_data["run"] = _run_config_to_dict(run_cfg)
        _save_filter_list(list_data)

    try:
        _verify_dependencies_for_run(filter_cfg)
    except Exception as exc:
        err(str(exc))
        press_enter()
        return

    _clear_console()
    print("Ejecutando filtrado... (presiona Q para detener)")
    print("Inicializando cuentas y scheduler...")

    stopped = _run_async(_execute_filter_list_async(list_data, filter_cfg, run_cfg))
    _save_filter_list(list_data)

    if stopped:
        _handle_partial_export(list_data)
    else:
        _auto_export_on_complete(list_data)

    press_enter()



def _verify_dependencies_for_run(cfg: LeadFilterConfig) -> None:
    from src import leads_filter_pipeline

    leads_filter_pipeline.verify_dependencies_for_run(cfg)


async def _execute_filter_list_async(
    list_data: Dict[str, Any],
    filter_cfg: LeadFilterConfig,
    run_cfg: LeadFilterRunConfig,
) -> bool:
    from src import leads_filter_pipeline

    return await leads_filter_pipeline.execute_filter_list_async(
        list_data,
        filter_cfg,
        run_cfg,
        resolve_accounts=_resolve_accounts,
        refresh_list_stats=_refresh_list_stats,
        save_filter_list=_save_filter_list,
        save_filter_runtime_state=_save_filter_list_runtime_state,
        reset_runtime_stop_event=_reset_runtime_stop_event,
        should_stop=_should_stop,
        warn=warn,
        log_filter_result=_log_filter_result,
        update_item=_update_list_item,
    )


def _scraped_to_profile_snapshot(user: ScrapedUser):
    from src import leads_filter_pipeline

    return leads_filter_pipeline.ProfileSnapshot(
        username=str(user.username or ""),
        biography=str(user.biography or ""),
        full_name=str(user.full_name or ""),
        follower_count=int(user.follower_count or 0),
        media_count=int(user.media_count or 0),
        is_private=bool(user.is_private),
        profile_pic_url=str(user.profile_pic_url or ""),
        user_id=str(user.user_id or ""),
        external_url=str(user.external_url or ""),
        is_verified=bool(user.is_verified),
    )


def _profile_snapshot_to_scraped(profile) -> ScrapedUser:
    return ScrapedUser(
        username=str(getattr(profile, "username", "") or ""),
        biography=str(getattr(profile, "biography", "") or ""),
        full_name=str(getattr(profile, "full_name", "") or ""),
        follower_count=int(getattr(profile, "follower_count", 0) or 0),
        media_count=int(getattr(profile, "media_count", 0) or 0),
        is_private=bool(getattr(profile, "is_private", False)),
        profile_pic_url=str(getattr(profile, "profile_pic_url", "") or ""),
        user_id=str(getattr(profile, "user_id", "") or ""),
        external_url=str(getattr(profile, "external_url", "") or ""),
        is_verified=bool(getattr(profile, "is_verified", False)),
    )


def _passes_classic_filters(
    user: ScrapedUser,
    cfg: ClassicFilterConfig,
    evaluations: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[bool, str]:
    _ = evaluations
    from src import leads_filter_pipeline

    snapshot = _scraped_to_profile_snapshot(user)
    return leads_filter_pipeline.passes_classic_filters(snapshot, cfg)


async def _pw_fetch_profile_json(page, username: str) -> Optional[ScrapedUser]:
    from src import leads_filter_pipeline

    profile = await leads_filter_pipeline.fetch_profile_json(page, username)
    if profile is None:
        return None
    return _profile_snapshot_to_scraped(profile)


def _playwright_proxy_from_account(account: Dict) -> Optional[Dict[str, str]]:
    from src import leads_filter_pipeline

    return leads_filter_pipeline.playwright_proxy_from_account(account)


def _bio_has_link(bio: str) -> bool:
    from src import leads_filter_pipeline

    return leads_filter_pipeline.bio_has_link(bio)


def _text_ai_decision(api_key: str, user: ScrapedUser, criteria: str) -> Tuple[Optional[bool], str]:
    _ = api_key
    from src import leads_filter_pipeline

    return leads_filter_pipeline.text_ai_decision(user, criteria)


def _image_ai_decision(
    api_key: str,
    user: ScrapedUser,
    prompt: str,
    *,
    image_bytes: Optional[bytes] = None,
) -> Tuple[bool, str]:
    _ = api_key
    from src import leads_filter_pipeline

    return leads_filter_pipeline.image_ai_decision(user, prompt, image_bytes=image_bytes)


async def _update_list_item(
    list_data: Dict[str, Any],
    idx: int,
    account: str,
    evaluation,
    lock: asyncio.Lock,
) -> None:
    async with lock:
        item = list_data["items"][idx]
        item["status"] = "QUALIFIED" if bool(getattr(evaluation, "passed", False)) else "DISCARDED"
        item["result"] = "CALIFICA" if bool(getattr(evaluation, "passed", False)) else "NO CALIFICA"
        item["reason"] = "" if bool(getattr(evaluation, "passed", False)) else (str(getattr(evaluation, "primary_reason", "") or "descartado"))
        item["account"] = account
        item["updated_at"] = _now_iso()
        item["decision_final"] = "pass" if bool(getattr(evaluation, "passed", False)) else "fail"
        item["reasons"] = list(getattr(evaluation, "reasons", []) or [])
        item["scores"] = dict(getattr(evaluation, "scores", {}) or {})
        item["extracted"] = dict(getattr(evaluation, "extracted", {}) or {})

        _refresh_list_stats(list_data)


def _log_filter_result(username: str, account: str, result: str, reason: str) -> None:
    detail = reason or "-"
    result_label = "califica" if str(result or "").upper() == "CALIFICA" else "no califica"
    emitter = str(account or "").strip().lstrip("@") or "-"
    target = str(username or "").strip().lstrip("@") or "-"
    print(
        f"@{emitter} --> @{target} (filtrado) --> {result_label} --> {detail}",
        flush=True,
    )


def _clear_console() -> None:
    if os.environ.get("INSTACLI_DISABLE_CONSOLE_CLEAR", "").strip() in {"1", "true", "yes"}:
        return
    stdin_tty = False
    stdout_tty = False
    try:
        stdin_tty = bool(sys.stdin.isatty())
    except Exception:
        stdin_tty = False
    try:
        stdout_tty = bool(sys.stdout.isatty())
    except Exception:
        stdout_tty = False
    if not (stdin_tty or stdout_tty):
        return
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"
