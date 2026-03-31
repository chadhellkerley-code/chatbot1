# leads.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from core.leads_import import LeadImportError, read_usernames_from_csv
from core.leads_store import LeadListStore, normalize_lead_username
from core.templates_store import load_templates, save_templates
from paths import leads_root, runtime_base
from utils import ask, ask_multiline, banner, ok, press_enter, title, warn


APP_DIR = Path(__file__).resolve().parent.parent
BASE = runtime_base(APP_DIR)
BASE.mkdir(parents=True, exist_ok=True)
TEXT = leads_root(BASE)


def refresh_runtime_paths(base: Path | None = None) -> dict[str, Path]:
    global BASE, TEXT

    resolved_base = runtime_base(Path(base) if base is not None else APP_DIR)
    resolved_base.mkdir(parents=True, exist_ok=True)
    BASE = resolved_base
    TEXT = leads_root(BASE)
    return {
        "base": BASE,
        "leads_root": TEXT,
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


def list_files() -> List[str]:
    return _lead_list_store().list_names()


def _normalize_lead_username(raw: object) -> str:
    return normalize_lead_username(raw)


def load_list(name: str) -> List[str]:
    return _lead_list_store().load(name)


def append_list(name: str, usernames: List[str]) -> None:
    _lead_list_store().append(name, usernames)


def save_list(name: str, usernames: List[str]) -> None:
    _lead_list_store().save(name, usernames)


def import_csv(path: str, name: str) -> None:
    csv_path = Path(path)
    if not csv_path.exists():
        warn("CSV no encontrado.")
        return
    try:
        users = read_usernames_from_csv(csv_path)
    except LeadImportError as exc:
        warn(str(exc))
        return
    append_list(name, users)
    ok(f"Importados {len(users)} a {name}.")


def show_list(name: str) -> None:
    users = load_list(name)
    print(f"{name}: {len(users)} usuarios")
    for index, username in enumerate(users[:50], 1):
        print(f"{index:02d}. @{username}")
    if len(users) > 50:
        print(f"... (+{len(users) - 50})")


def delete_list(name: str) -> None:
    if _lead_list_store().delete(name):
        ok("Eliminada.")
    else:
        warn("No existe.")


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
        preview = _template_preview(str(item.get("text", "")))
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


def menu_leads() -> None:
    while True:
        banner()
        title("Listas de leads")
        files = list_files()
        if files:
            print("Disponibles:", ", ".join(files))
        else:
            print("(aun no hay listas)")
        print("\n1) Crear lista y agregar manual")
        print("2) Importar CSV a una lista")
        print("3) Ver lista")
        print("4) Eliminar lista")
        print("5) Gestionar plantillas")
        print("6) Volver\n")
        op = ask("Opcion: ").strip()
        if op == "1":
            name = ask("Nombre de la lista: ").strip() or "default"
            print("Pega usernames (uno por linea). Linea vacia para terminar:")
            lines: list[str] = []
            while True:
                value = ask("")
                if not value:
                    break
                lines.append(value)
            append_list(name, lines)
            ok("Guardado.")
            press_enter()
        elif op == "2":
            path = ask("Ruta del CSV: ")
            name = ask("Importar a la lista (nombre): ").strip() or "default"
            import_csv(path, name)
            press_enter()
        elif op == "3":
            name = ask("Nombre de la lista: ").strip()
            show_list(name)
            press_enter()
        elif op == "4":
            name = ask("Nombre de la lista: ").strip()
            delete_list(name)
            press_enter()
        elif op == "5":
            menu_templates()
        elif op == "6":
            break
        else:
            warn("Opcion invalida.")
            press_enter()


refresh_runtime_paths()
