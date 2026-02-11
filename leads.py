# leads.py
# -*- coding: utf-8 -*-
import asyncio
import base64
import csv
import html
import io
import json
import logging
import os
import random
import re
import shutil
import sys
import time
import unicodedata
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from accounts import (
    auto_login_with_saved_password,
    get_account,
    has_valid_session_settings,
    list_all,
    mark_connected,
    prompt_login,
)
from paths import runtime_base
from proxy_manager import apply_proxy_to_client, record_proxy_failure, should_retry_proxy
from session_store import has_session, load_into
from templates_store import load_templates, save_templates
from client_factory import get_instagram_client
from config import SETTINGS, read_env_local
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

APP_DIR = Path(__file__).resolve().parent
BASE = runtime_base(APP_DIR)
BASE.mkdir(parents=True, exist_ok=True)
TEXT = BASE / "text" / "leads"
TEXT.mkdir(parents=True, exist_ok=True)

DEFAULT_EXPORT_ALIAS = "leads_filtrados"

_TEXT_FILTER_OMITTED_LOGGED = False
_TEXT_FILTER_INVALID_KEY_LOGGED = False
_IMAGE_FILTER_OMITTED_LOGGED = False



def _looks_like_login_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(keyword in msg for keyword in ("login", "session", "credential"))


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "si", "sí"}

def list_files()->List[str]:
    return sorted([p.stem for p in TEXT.glob("*.txt")])

def load_list(name:str)->List[str]:
    p=TEXT/f"{name}.txt"
    if not p.exists(): return []
    return [line.strip().lstrip("@") for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]

def append_list(name:str, usernames:List[str]):
    p=TEXT/f"{name}.txt"
    with p.open("a", encoding="utf-8") as f:
        for u in usernames:
            f.write(u.strip().lstrip("@")+"\n")


def save_list(name: str, usernames: List[str]) -> None:
    p = TEXT / f"{name}.txt"
    with p.open("w", encoding="utf-8") as f:
        for u in usernames:
            f.write(u.strip().lstrip("@") + "\n")

def import_csv(path:str, name:str):
    path=Path(path)
    if not path.exists():
        warn("CSV no encontrado."); return
    users=[]
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row: continue
            users.append(row[0].strip().lstrip("@"))
    append_list(name, users)
    ok(f"Importados {len(users)} a {name}.")

def show_list(name:str):
    users=load_list(name)
    print(f"{name}: {len(users)} usuarios")
    for i,u in enumerate(users[:50],1):
        print(f"{i:02d}. @{u}")
    if len(users)>50: print(f"... (+{len(users)-50})")

def delete_list(name:str):
    p=TEXT/f"{name}.txt"
    if p.exists(): p.unlink(); ok("Eliminada.")
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
        else: print("(aún no hay listas)")
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
            print("Pegá usernames (uno por línea). Línea vacía para terminar:")
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
            warn("Opción inválida."); press_enter()


@dataclass
class ScrapedUser:
    username: str
    biography: str
    full_name: str
    follower_count: int
    media_count: int
    is_private: bool
    profile_pic_url: str = ""


@dataclass
class ClassicFilterConfig:
    min_followers: int
    min_posts: int
    privacy: str  # public | private | any
    link_in_bio: str  # yes | no | any
    include_keywords: List[str]
    exclude_keywords: List[str]
    language: str  # es | pt | en | any


@dataclass
class TextFilterConfig:
    enabled: bool
    criteria: str
    model_path: str


@dataclass
class ImageFilterConfig:
    enabled: bool
    prompt: str


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










_PROMPT_STOPWORDS = {
    "a",
    "acerca",
    "ademas",
    "al",
    "algo",
    "algun",
    "alguna",
    "algunas",
    "algunos",
    "ante",
    "antes",
    "aqui",
    "asi",
    "aunque",
    "busco",
    "buscar",
    "cada",
    "casi",
    "como",
    "con",
    "contra",
    "cual",
    "cuales",
    "cualquier",
    "cuenta",
    "cuyo",
    "cuya",
    "cuyos",
    "cuyas",
    "de",
    "del",
    "desde",
    "donde",
    "durante",
    "el",
    "ella",
    "ellas",
    "ellos",
    "en",
    "entre",
    "es",
    "esa",
    "esas",
    "ese",
    "eso",
    "esta",
    "estan",
    "estas",
    "este",
    "esto",
    "estos",
    "etc",
    "gente",
    "habla",
    "hablan",
    "hablar",
    "hablen",
    "hacia",
    "hacen",
    "hacer",
    "hasta",
    "incluye",
    "incluyen",
    "incluir",
    "la",
    "las",
    "le",
    "les",
    "lo",
    "los",
    "mas",
    "menos",
    "mientras",
    "misma",
    "mismas",
    "mismo",
    "mismos",
    "necesito",
    "necesitamos",
    "ningun",
    "ninguna",
    "no",
    "nos",
    "nuestro",
    "nuestra",
    "nuestras",
    "nuestros",
    "o",
    "otra",
    "otras",
    "otro",
    "otros",
    "para",
    "perfiles",
    "perfil",
    "personas",
    "pero",
    "por",
    "porque",
    "preferible",
    "preferiblemente",
    "preferentemente",
    "prefiero",
    "que",
    "quien",
    "quienes",
    "quiero",
    "quiere",
    "queremos",
    "relacion",
    "relaciona",
    "relacionado",
    "relacionada",
    "relacionados",
    "relacionadas",
    "requiere",
    "requieren",
    "requiro",
    "residan",
    "residen",
    "sea",
    "sean",
    "segun",
    "si",
    "sin",
    "sobre",
    "solamente",
    "solo",
    "somos",
    "son",
    "seguidor",
    "seguidores",
    "followers",
    "fans",
    "su",
    "sus",
    "tal",
    "tambien",
    "tan",
    "tanto",
    "tengan",
    "tener",
    "tengo",
    "tema",
    "temas",
    "tipo",
    "tipos",
    "toda",
    "todas",
    "todo",
    "todos",
    "post",
    "posts",
    "posteos",
    "publicaciones",
    "publicacion",
    "contenido",
    "contenidos",
    "trabajan",
    "trabajen",
    "ubicada",
    "ubicadas",
    "ubicado",
    "ubicados",
    "ubicacion",
    "un",
    "una",
    "unas",
    "uno",
    "unos",
    "usuarios",
    "usuario",
    "usar",
    "varias",
    "varios",
    "vive",
    "viven",
    "vivir",
    "vivan",
    "y",
    "ya",
}

_PROMPT_NEGATIONS = {
    "sin",
    "no",
    "excepto",
    "excepta",
    "exceptos",
    "exceptas",
    "excluir",
    "excluye",
    "excluyen",
    "evitar",
    "evita",
    "eviten",
    "salvo",
    "salvos",
    "salvas",
    "menos",
}

_PROMPT_FOLLOWER_KEYWORDS = (
    "seguidor",
    "seguidores",
    "followers",
    "fans",
)

_PROMPT_POST_KEYWORDS = (
    "post",
    "posts",
    "posteos",
    "publicaciones",
    "publicacion",
    "contenido",
    "contenidos",
)


def _normalize_text(value: str) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


_LANG_STOPWORDS = {
    "es": {
        "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las", "por",
        "un", "para", "con", "no", "una", "su", "al", "lo", "como", "mas", "pero",
        "sus", "le", "ya", "o", "este", "si", "porque", "esta", "entre", "cuando",
        "muy", "sin", "sobre", "tambien", "me", "hasta", "hay", "donde", "quien",
        "desde", "todo", "nos", "durante", "todos", "uno", "les", "ni", "contra",
        "otros", "ese", "eso", "ante", "ellos", "e", "mi", "mis", "tu", "tus",
        "soy", "eres",
    },
    "pt": {
        "de", "a", "o", "que", "e", "do", "da", "em", "um", "para", "com", "nao",
        "uma", "os", "no", "se", "na", "por", "mais", "as", "dos", "como", "mas",
        "foi", "ao", "ele", "das", "tem", "aos", "seu", "sua", "ou", "ser", "quando",
        "muito", "ha", "nos", "ja", "esta", "eu", "voce", "tudo", "lhe", "pela",
        "pelos", "porque", "sou", "somos", "estou",
    },
    "en": {
        "the", "be", "to", "of", "and", "a", "in", "that", "have", "i", "it", "for",
        "not", "on", "with", "he", "as", "you", "do", "at", "this", "but", "his",
        "by", "from", "they", "we", "say", "her", "she", "or", "an", "will", "my",
        "one", "all", "would", "there", "their", "what", "so", "up", "out", "if",
        "about", "who", "get", "which", "go", "me", "your", "our", "is", "are",
        "was", "were", "been",
    },
}


def _detect_language(text: str) -> str:
    normalized = _normalize_text(text)
    if not normalized:
        return "unknown"
    tokens = normalized.split()
    scores: Dict[str, int] = {}
    for lang, vocab in _LANG_STOPWORDS.items():
        scores[lang] = sum(1 for token in tokens if token in vocab)
    best_lang = max(scores, key=scores.get)
    best_score = scores.get(best_lang, 0)
    if best_score <= 0:
        return "unknown"
    tied = [lang for lang, score in scores.items() if score == best_score]
    if len(tied) > 1:
        return "unknown"
    return best_lang


def _should_stop(stop_event: asyncio.Event) -> bool:
    if stop_event.is_set():
        return True
    if _poll_quit_key():
        stop_event.set()
        warn("Deteniendo filtrado por solicitud del usuario (Q).")
        return True
    return False


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


async def _ensure_page(ctx, page):
    if ctx is None:
        return page
    try:
        if page is not None and not page.is_closed():
            return page
    except Exception:
        pass
    try:
        from src.playwright_service import get_page
    except Exception:
        return page


async def _fetch_image_bytes_with_context(page, url: str) -> Optional[bytes]:
    if not url or page is None:
        return None
    try:
        req = page.context.request
        resp = await req.get(
            url,
            headers={
                "Referer": "https://www.instagram.com/",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            },
            timeout=20_000,
        )
        if not resp.ok:
            return None
        return await resp.body()
    except Exception:
        return None


async def _screenshot_avatar_bytes(page) -> Optional[bytes]:
    if page is None:
        return None
    try:
        locator = page.locator(
            "img[alt*='perfil'], img[alt*='profile'], img[alt*='photo'], img[alt*='foto'], header img"
        )
        try:
            await locator.first.wait_for(state="visible", timeout=7_000)
        except Exception:
            pass
        count = await locator.count()
        if count <= 0:
            return None
        best_idx = 0
        best_area = 0.0
        for i in range(count):
            box = await locator.nth(i).bounding_box()
            if not box:
                continue
            area = float(box.get("width", 0) * box.get("height", 0))
            if area > best_area:
                best_area = area
                best_idx = i
        target = locator.nth(best_idx)
        try:
            await target.scroll_into_view_if_needed(timeout=5_000)
        except Exception:
            pass
        return await target.screenshot(type="jpeg")
    except Exception:
        return None


async def _get_profile_image_bytes(page, profile_pic_url: str) -> Optional[bytes]:
    image_bytes = await _screenshot_avatar_bytes(page)
    if image_bytes:
        return image_bytes
    image_bytes = await _fetch_image_bytes_with_context(page, profile_pic_url)
    if image_bytes:
        return image_bytes
    return _download_image_bytes(profile_pic_url)
    try:
        page = await get_page(ctx)
        try:
            page.set_default_timeout(20_000)
            page.set_default_navigation_timeout(45_000)
        except Exception:
            pass
        return page
    except Exception:
        return page
    return False


_RAW_PROMPT_SYNONYMS = {
    "argentina": {"argentina", "argentino", "argentina", "buenos aires", "cordoba"},
    "bolivia": {"bolivia", "boliviano", "boliviana", "la paz", "santa cruz"},
    "chile": {"chile", "chileno", "chilena", "santiago", "valparaiso"},
    "colombia": {"colombia", "colombiano", "colombiana", "bogota", "medellin"},
    "costarica": {"costa rica", "costarricense", "tico", "tica"},
    "ecuador": {"ecuador", "ecuatoriano", "ecuatoriana", "quito", "guayaquil"},
    "espana": {"espana", "spain", "madrid", "barcelona", "sevilla", "valencia", "espanol", "espanola"},
    "europa": {"europa", "europe", "europeo", "europea", "union europea"},
    "latinoamerica": {"latinoamerica", "latam", "latino", "latina"},
    "mexico": {"mexico", "mx", "cdmx", "ciudad de mexico", "mexicana", "mexicano", "monterrey", "guadalajara"},
    "peru": {"peru", "peruano", "peruana", "lima"},
    "uruguay": {"uruguay", "uruguayo", "uruguaya", "montevideo"},
    "venezuela": {"venezuela", "venezolano", "venezolana", "caracas"},
    "espanol": {"espanol", "castellano", "spanish", "hablo espanol", "idioma espanol"},
    "ingles": {"ingles", "english", "bilingue", "bilingual"},
    "portugues": {"portugues", "portuguese", "brasil", "brasileno", "brasilena", "brasilero", "brasilera"},
    "mujer": {"mujer", "mujeres", "female", "femenino", "femenina", "women", "woman", "chica", "damas", "girls"},
    "hombre": {"hombre", "hombres", "male", "masculino", "masculina", "men", "man"},
    "coaching": {"coaching", "coach", "coaches", "mentora", "mentor", "mentoring", "mentoria", "mentorias"},
    "negocios": {"negocio", "negocios", "business", "empresa", "empresas", "empresaria", "empresario", "emprendimiento", "emprendedor", "emprendedora", "startup", "startups"},
    "liderazgo": {"liderazgo", "lider", "lideres", "leader", "leadership", "liderar"},
    "marketing": {"marketing", "marketer", "mercadotecnia", "growth", "digital marketing", "publicidad", "ads"},
    "ventas": {"ventas", "sales", "vendedor", "vendedora", "seller", "comercial", "comerciales"},
    "finanzas": {"finanzas", "finance", "financiero", "financiera", "financial"},
    "tecnologia": {"tecnologia", "technology", "tech", "tecnologico", "tecnologica", "software", "it"},
    "emprendedor": {"emprendedor", "emprendedora", "emprendedores", "emprendedoras", "founder", "founders", "cofounder", "cofounders", "cofundador", "cofundadora"},
    "wellness": {"wellness", "bienestar", "health", "healthy"},
    "inversion": {"inversion", "inversiones", "investor", "investors", "angel", "venture", "capital"},
    "freelance": {"freelance", "freelancer", "independiente", "autonomo", "autonoma"},
}


def _build_prompt_synonyms() -> Dict[str, Set[str]]:
    mapping: Dict[str, Set[str]] = {}
    for key, raw_terms in _RAW_PROMPT_SYNONYMS.items():
        normalized_key = _normalize_text(key)
        bucket: Set[str] = set()
        for term in raw_terms:
            normalized_term = _normalize_text(term)
            if normalized_term and normalized_term not in _PROMPT_STOPWORDS:
                bucket.add(normalized_term)
        if normalized_key and normalized_key not in _PROMPT_STOPWORDS:
            bucket.add(normalized_key)
        if bucket:
            mapping[normalized_key] = bucket
    return mapping


_PROMPT_SYNONYMS = _build_prompt_synonyms()


def _clean_int(raw: str) -> Optional[int]:
    digits = re.sub(r"[^0-9]", "", raw or "")
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def _parse_numeric_bounds(text: str, keywords: Tuple[str, ...]) -> Tuple[Optional[int], Optional[int]]:
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    for keyword in keywords:
        if not keyword:
            continue
        pattern_min = rf"(?:mas de|al menos|minimo(?: de)?|mayor a|superior a|mas que|>=|>\s*)(\d[\d\s\.,]*)\s*{keyword}"
        for match in re.finditer(pattern_min, text):
            value = _clean_int(match.group(1))
            if value is not None:
                min_value = value if min_value is None else max(min_value, value)
        pattern_plus = rf"(\d[\d\s\.,]*)\s*(?:\+|o mas)\s*{keyword}"
        for match in re.finditer(pattern_plus, text):
            value = _clean_int(match.group(1))
            if value is not None:
                min_value = value if min_value is None else max(min_value, value)
        pattern_max = rf"(?:menos de|no mas de|maximo(?: de)?|hasta|menor a|inferior a|<=|<\s*)(\d[\d\s\.,]*)\s*{keyword}"
        for match in re.finditer(pattern_max, text):
            value = _clean_int(match.group(1))
            if value is not None:
                max_value = value if max_value is None else min(max_value, value)
    return min_value, max_value


def _tokenize_prompt_segment(segment: str) -> List[str]:
    normalized = _normalize_text(segment)
    if not normalized:
        return []
    tokens = normalized.split(" ")
    cleaned: List[str] = []
    for token in tokens:
        if not token or token in _PROMPT_STOPWORDS or token.isdigit():
            continue
        cleaned.append(token)
    return cleaned


def _expand_prompt_token(token: str) -> Set[str]:
    normalized = _normalize_text(token)
    if not normalized or normalized in _PROMPT_STOPWORDS:
        return set()
    expanded: Set[str] = {normalized}
    if normalized.endswith("es") and len(normalized) > 4:
        expanded.add(normalized[:-2])
    if normalized.endswith("s") and len(normalized) > 3:
        expanded.add(normalized[:-1])
    synonyms = _PROMPT_SYNONYMS.get(normalized)
    if synonyms:
        expanded.update(synonyms)
    for key, group in _PROMPT_SYNONYMS.items():
        if normalized in group:
            expanded.update(group)
            expanded.add(key)
    return {term for term in expanded if term and term not in _PROMPT_STOPWORDS}




def _term_in_haystack(term: str, haystack: str) -> bool:
    normalized_term = _normalize_text(term)
    if not normalized_term:
        return False
    if " " in normalized_term:
        return normalized_term in haystack
    pattern = rf"\b{re.escape(normalized_term)}\b"
    return bool(re.search(pattern, haystack))






def _format_user(user_info, position: int, limit: int) -> str:
    username = getattr(user_info, "username", "?")
    follower_count = int(getattr(user_info, "follower_count", 0) or 0)
    media_count = int(getattr(user_info, "media_count", 0) or 0)
    privacy = "privada" if getattr(user_info, "is_private", False) else "pública"
    return (
        f" {position:02d}/{limit:02d} → @{username} | "
        f"seguidores: {follower_count:,} | posteos: {media_count} | {privacy}"
    )


def _build_scraped_user(info) -> ScrapedUser:
    biography = (getattr(info, "biography", "") or "").strip()
    full_name = (getattr(info, "full_name", "") or "").strip()
    follower_count = int(getattr(info, "follower_count", 0) or 0)
    media_count = int(getattr(info, "media_count", 0) or 0)
    is_private = bool(getattr(info, "is_private", False))
    profile_pic_url = (getattr(info, "profile_pic_url", "") or "").strip()
    username = getattr(info, "username", "").strip()
    return ScrapedUser(
        username=username.lstrip("@"),
        biography=biography,
        full_name=full_name,
        follower_count=follower_count,
        media_count=media_count,
        is_private=is_private,
        profile_pic_url=profile_pic_url,
    )


# --- Lead filtering pipeline (Playwright + Texto inteligente + Imagen) ---

FILTER_STORAGE = BASE / "storage" / "lead_filters"
FILTER_LISTS = FILTER_STORAGE / "lists"
FILTER_CONFIG_PATH = FILTER_STORAGE / "filters_config.json"
FILTER_STORAGE.mkdir(parents=True, exist_ok=True)
FILTER_LISTS.mkdir(parents=True, exist_ok=True)


def filter_leads_pipeline() -> None:
    while True:
        banner()
        title("Filtrado de Leads")
        print("[1] Crear nuevo filtrado")
        print("[2] Ver listas de filtrado")
        print("[3] Configuración de filtros")
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
        warn("No hay filtros configurados. Usa 'Configuración de filtros' primero.")
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
        title("Configuración de filtros")
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
    print("- Clásicos:")
    print(f"  · Seguidores mínimos: {cfg.classic.min_followers}")
    print(f"  · Posts mínimos: {cfg.classic.min_posts}")
    print(f"  · Privacidad: {cfg.classic.privacy}")
    print(f"  · Link en bio: {cfg.classic.link_in_bio}")
    lang_label = {
        "es": "español",
        "pt": "portugués",
        "en": "inglés",
        "any": "indiferente",
    }.get(cfg.classic.language, "indiferente")
    print(f"  · Idioma: {lang_label}")
    includes = ", ".join(cfg.classic.include_keywords) if cfg.classic.include_keywords else "(ninguno)"
    excludes = ", ".join(cfg.classic.exclude_keywords) if cfg.classic.exclude_keywords else "(ninguno)"
    print(f"  · Palabras obligatorias: {includes}")
    print(f"  · Palabras prohibidas: {excludes}")
    print("- Texto inteligente:")
    print(f"  · Activado: {'sí' if cfg.text.enabled else 'no'}")
    if cfg.text.enabled:
        snippet = (cfg.text.criteria or "").strip()
        snippet = (snippet[:80] + "...") if len(snippet) > 80 else snippet
        print(f"  · Criterio: {snippet or '(vacío)'}")
    print("- Imagen:")
    print(f"  · Activado: {'sí' if cfg.image.enabled else 'no'}")
    if cfg.image.enabled:
        visual = (cfg.image.prompt or "").strip()
        visual = (visual[:80] + "...") if len(visual) > 80 else visual
        print(f"  · Prompt visual: {visual or '(vacío)'}")


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
    print("\nFiltrado detenido. ¿Qué querés hacer?")
    print("[1] Guardar leads calificados hasta ahora en el alias")
    print("[2] No guardar (mantener lista incompleta)")
    print("[3] Eliminar la lista incompleta")
    print("[4] Volver al menú (mantener lista incompleta)")
    choice = ask("Opcion: ").strip() or "2"
    if choice == "1":
        if qualified:
            _export_to_alias(alias, qualified)
        else:
            warn("No hay leads calificados aún para guardar.")
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
    users: List[str] = []
    with file_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle):
            if not row:
                continue
            users.append(row[0].strip())
    return users


def _load_usernames_from_txt(path: str) -> List[str]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        warn("TXT no encontrado.")
        return []
    return [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines()]


def _load_usernames_from_paste() -> List[str]:
    raw = ask_multiline("Pega usernames (uno por linea):")
    return [line.strip() for line in raw.splitlines()]


def _normalize_usernames(values: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    for value in values:
        if not value:
            continue
        cleaned = value.strip().lstrip("@")
        if not cleaned:
            continue
        normalized.append(cleaned)
    return _dedupe_preserve_order(normalized)


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
        default=min(1, len(accounts)),
    )
    concurrency = min(concurrency, len(accounts))
    delay_min = ask_int("Delay minimo (segundos): ", min_value=0, default=5)
    delay_max = ask_int("Delay maximo (segundos): ", min_value=0, default=max(delay_min, 8))
    if delay_max < delay_min:
        warn("El delay maximo era menor al minimo. Se invirtieron los valores.")
        delay_min, delay_max = delay_max, delay_min
    print("\nModo navegador:")
    print("[1] Segundo plano (headless)")
    print("[2] Visible (headful)")
    print("[3] Usar configuración del env (LEADS_HEADFUL/HUMAN_HEADFUL)")
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

    include_keywords: List[str] = existing.include_keywords if existing else []
    if ask("¿Agregar palabras clave obligatorias? (s/N): ").strip().lower() == "s":
        include_keywords = _prompt_keywords(
            "Palabras clave que DEBE contener (coma o salto de linea):",
            include_keywords,
        )

    exclude_keywords: List[str] = existing.exclude_keywords if existing else []
    if ask("¿Agregar palabras clave prohibidas? (s/N): ").strip().lower() == "s":
        exclude_keywords = _prompt_keywords(
            "Palabras clave que NO debe contener:",
            exclude_keywords,
        )
    print("\nIdioma del perfil:")
    print("[1] Español")
    print("[2] Portugués")
    print("[3] Inglés")
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
    return ClassicFilterConfig(
        min_followers=min_followers,
        min_posts=min_posts,
        privacy=privacy,
        link_in_bio=link_in_bio,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        language=language,
    )


def _prompt_text_filter(existing: Optional[TextFilterConfig]) -> Optional[TextFilterConfig]:
    default_enabled = "s" if existing and existing.enabled else "n"
    enabled = ask("Activar filtro de texto? (s/N): ").strip().lower() or default_enabled
    if enabled != "s":
        return TextFilterConfig(enabled=False, criteria="", model_path="")
    criteria = ask_multiline("Criterio de filtrado (texto):").strip()
    if not criteria:
        warn("El criterio de texto es obligatorio.")
        return None
    return TextFilterConfig(enabled=True, criteria=criteria, model_path="")


def _prompt_image_filter(existing: Optional[ImageFilterConfig]) -> Optional[ImageFilterConfig]:
    default_enabled = "s" if existing and existing.enabled else "n"
    enabled = ask("Activar filtro de imagen? (s/N): ").strip().lower() or default_enabled
    if enabled != "s":
        return ImageFilterConfig(
            enabled=False,
            prompt="",
        )
    prompt = ask_multiline("Prompt visual:").strip()
    if not prompt:
        warn("El prompt visual es obligatorio.")
        return None
    return ImageFilterConfig(
        enabled=True,
        prompt=prompt,
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
    list_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
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
    return {
        "saved_at": _now_iso(),
        "classic": {
            "min_followers": cfg.classic.min_followers,
            "min_posts": cfg.classic.min_posts,
            "privacy": cfg.classic.privacy,
            "link_in_bio": cfg.classic.link_in_bio,
            "include_keywords": cfg.classic.include_keywords,
            "exclude_keywords": cfg.classic.exclude_keywords,
            "language": cfg.classic.language,
        },
        "text": {
            "enabled": cfg.text.enabled,
            "criteria": cfg.text.criteria,
            "model_path": cfg.text.model_path,
        },
        "image": {
            "enabled": cfg.image.enabled,
            "prompt": cfg.image.prompt,
        },
    }


def _filter_config_from_dict(data: Dict[str, Any]) -> Optional[LeadFilterConfig]:
    if not data:
        return None
    classic_raw = data.get("classic") or {}
    text_raw = data.get("text") or {}
    image_raw = data.get("image") or {}
    classic = ClassicFilterConfig(
        min_followers=int(classic_raw.get("min_followers") or 0),
        min_posts=int(classic_raw.get("min_posts") or 0),
        privacy=str(classic_raw.get("privacy") or "any"),
        link_in_bio=str(classic_raw.get("link_in_bio") or "any"),
        include_keywords=list(classic_raw.get("include_keywords") or []),
        exclude_keywords=list(classic_raw.get("exclude_keywords") or []),
        language=str(classic_raw.get("language") or "any"),
    )
    text = TextFilterConfig(
        enabled=bool(text_raw.get("enabled")),
        criteria=str(text_raw.get("criteria") or ""),
        model_path="",
    )
    image = ImageFilterConfig(
        enabled=bool(image_raw.get("enabled")),
        prompt=str(image_raw.get("prompt") or ""),
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
    return LeadFilterRunConfig(
        alias=str(data.get("alias") or ""),
        accounts=list(data.get("accounts") or []),
        concurrency=int(data.get("concurrency") or 1),
        delay_min=float(data.get("delay_min") or 0),
        delay_max=float(data.get("delay_max") or 0),
        headless=headless,
    )


def _save_filter_config(cfg: LeadFilterConfig) -> None:
    payload = _filter_config_to_dict(cfg)
    FILTER_CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_filter_config() -> Optional[LeadFilterConfig]:
    if not FILTER_CONFIG_PATH.exists():
        return None
    try:
        payload = json.loads(FILTER_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return _filter_config_from_dict(payload)


def _save_filter_list(data: Dict[str, Any]) -> None:
    list_id = data.get("id") or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = FILTER_LISTS / f"{list_id}.json"
    data["id"] = list_id
    if "export_alias" not in data:
        data["export_alias"] = DEFAULT_EXPORT_ALIAS
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_filter_lists() -> List[Dict[str, Any]]:
    lists: List[Dict[str, Any]] = []
    for path in sorted(FILTER_LISTS.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["_path"] = str(path)
            if "export_alias" not in payload:
                payload["export_alias"] = DEFAULT_EXPORT_ALIAS
            lists.append(payload)
        except Exception:
            continue
    return lists


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
    path = list_data.get("_path")
    if path and Path(path).exists():
        Path(path).unlink()
        return
    list_id = list_data.get("id")
    if list_id:
        candidate = FILTER_LISTS / f"{list_id}.json"
        if candidate.exists():
            candidate.unlink()


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
    print("Ejecutando filtrado... (presioná Q para detener)\n")
    stopped = _run_async(_execute_filter_list_async(list_data, filter_cfg, run_cfg))
    _save_filter_list(list_data)

    if stopped:
        _handle_partial_export(list_data)
    else:
        _auto_export_on_complete(list_data)

    press_enter()


def _get_openai_api_key() -> str:
    env_values = read_env_local()
    return env_values.get("OPENAI_API_KEY") or SETTINGS.openai_api_key or ""


def _log_text_filter_omitted_once() -> None:
    global _TEXT_FILTER_OMITTED_LOGGED
    if _TEXT_FILTER_OMITTED_LOGGED:
        return
    _TEXT_FILTER_OMITTED_LOGGED = True
    logging.warning("Texto inteligente omitido: no hay API Key configurada")


def _log_text_filter_invalid_key_once() -> None:
    global _TEXT_FILTER_INVALID_KEY_LOGGED
    if _TEXT_FILTER_INVALID_KEY_LOGGED:
        return
    _TEXT_FILTER_INVALID_KEY_LOGGED = True
    logging.warning("Texto inteligente omitido: API Key invalida")


def _log_image_filter_omitted_once() -> None:
    global _IMAGE_FILTER_OMITTED_LOGGED
    if _IMAGE_FILTER_OMITTED_LOGGED:
        return
    _IMAGE_FILTER_OMITTED_LOGGED = True
    logging.warning("Filtro Imagen omitido: no hay API Key configurada")


def _is_invalid_api_key_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(token in msg for token in ("invalid_api_key", "incorrect api key", "invalid api key", "status: 401", "error code: 401"))


def _verify_text_ai_dependencies() -> None:
    try:
        from openai import OpenAI  # noqa: F401
    except Exception as exc:
        raise RuntimeError(
            "ERROR DE BUILD: faltan dependencias para Texto inteligente."
        ) from exc


def _verify_image_dependencies() -> None:
    try:
        from openai import OpenAI  # noqa: F401
    except Exception as exc:
        raise RuntimeError(
            "ERROR DE BUILD: faltan dependencias para Imagen (OpenAI)."
        ) from exc


def _verify_dependencies_for_run(cfg: LeadFilterConfig) -> None:
    if cfg.text.enabled:
        _verify_text_ai_dependencies()
    if cfg.image.enabled:
        _verify_image_dependencies()

def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("La operacion requiere contexto sync.")


def _resolve_accounts(usernames: List[str]) -> List[Dict[str, Any]]:
    accounts = list_all()
    wanted = {name.lower(): name for name in usernames if name}
    resolved: List[Dict[str, Any]] = []
    missing_password: List[str] = []
    for acct in accounts:
        uname = (acct.get("username") or "").strip()
        if not uname:
            continue
        if uname.lower() in wanted:
            if acct.get("password"):
                resolved.append(acct)
            else:
                missing_password.append(uname)
    if missing_password:
        warn("Cuentas sin password (omitidas): " + ", ".join(missing_password))
    return resolved


async def _execute_filter_list_async(
    list_data: Dict[str, Any],
    filter_cfg: LeadFilterConfig,
    run_cfg: LeadFilterRunConfig,
) -> bool:
    pending_indices = [
        idx for idx, item in enumerate(list_data.get("items") or [])
        if item.get("status") == "PENDING"
    ]
    if not pending_indices:
        warn("No quedan usernames pendientes.")
        return False

    accounts = _resolve_accounts(run_cfg.accounts)
    if not accounts:
        warn("No hay cuentas validas para ejecutar el filtrado.")
        return False

    worker_count = min(run_cfg.concurrency, len(accounts))
    accounts = accounts[:worker_count]

    queue: asyncio.Queue = asyncio.Queue()
    for idx in pending_indices:
        await queue.put(idx)
    for _ in range(worker_count):
        await queue.put(None)

    stop_event = asyncio.Event()

    list_lock = asyncio.Lock()
    text_lock = asyncio.Lock()
    image_lock = asyncio.Lock()

    tasks = []
    for account in accounts:
        task = asyncio.create_task(
            _filter_worker(
                account,
                queue,
                list_data,
                list_lock,
                filter_cfg,
                run_cfg,
                text_lock,
                image_lock,
                stop_event,
            )
        )
        tasks.append(task)

    async def _drain_queue() -> int:
        drained = 0
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            queue.task_done()
            drained += 1
        return drained

    # Evitar bloqueo infinito si todos los workers terminan antes de consumir la cola.
    while True:
        if all(task.done() for task in tasks):
            remaining = await _drain_queue()
            if remaining:
                warn(
                    "Filtrado detenido: quedaron pendientes sin procesar. "
                    "Podes reanudar luego."
                )
            break
        try:
            await asyncio.wait_for(queue.join(), timeout=1.0)
            break
        except asyncio.TimeoutError:
            continue

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            warn(f"Worker terminó con error: {result}")

    _refresh_list_stats(list_data)
    _save_filter_list(list_data)
    return stop_event.is_set()


async def _filter_worker(
    account: Dict[str, Any],
    queue: asyncio.Queue,
    list_data: Dict[str, Any],
    list_lock: asyncio.Lock,
    filter_cfg: LeadFilterConfig,
    run_cfg: LeadFilterRunConfig,
    text_lock: asyncio.Lock,
    image_lock: asyncio.Lock,
    stop_event: asyncio.Event,
) -> None:
    svc = None
    ctx = None
    page = None
    account_username = account.get("username", "")
    proxy_payload = _playwright_proxy_from_account(account)
    try:
        from src.auth.persistent_login import ensure_logged_in_async, ChallengeRequired
        from src.playwright_service import shutdown
    except Exception as exc:
        warn(f"Playwright no disponible: {exc}")
        return

    try:
        headless = run_cfg.headless
        if headless is None:
            headless = not _env_truthy(
                "LEADS_HEADFUL",
                _env_truthy("HUMAN_HEADFUL", False),
            )
        prev_overnight = os.getenv("IG_OVERNIGHT")
        if headless:
            # Evita prompts bloqueantes en modo headless.
            os.environ["IG_OVERNIGHT"] = "1"
        try:
            svc, ctx, page = await ensure_logged_in_async(
                account,
                headless=headless,
                proxy=proxy_payload,
            )
        finally:
            if headless:
                if prev_overnight is None:
                    os.environ.pop("IG_OVERNIGHT", None)
                else:
                    os.environ["IG_OVERNIGHT"] = prev_overnight
        while True:
            idx = await queue.get()
            if idx is None:
                queue.task_done()
                break
            if _should_stop(stop_event):
                queue.task_done()
                continue
            item = list_data["items"][idx]
            username = item.get("username", "")
            result_label = "NO CALIFICA"
            reason = "error_proceso"
            try:
                for attempt in range(2):
                    page = await _ensure_page(ctx, page)
                    try:
                        ok, reason = await _evaluate_username(
                            page,
                            username,
                            filter_cfg,
                            text_lock,
                            image_lock,
                        )
                        result_label = "CALIFICA" if ok else "NO CALIFICA"
                        if ok:
                            break
                        if reason in {"no_se_pudo_abrir", "perfil_sin_datos"} and attempt == 0:
                            continue
                        break
                    except Exception as exc:
                        if attempt == 0:
                            continue
                        raise
            except Exception as exc:
                warn(f"Error procesando @{username}: {exc}")
                reason = "error_proceso"
            await _update_list_item(
                list_data,
                idx,
                result_label,
                reason,
                account_username,
                list_lock,
            )
            _log_filter_result(username, account_username, result_label, reason)
            await _apply_delay(run_cfg.delay_min, run_cfg.delay_max)
            queue.task_done()
    except ChallengeRequired as exc:
        warn(f"Challenge requerido para @{account_username}: {exc}")
        if headless:
            warn(
                "Modo headless no permite resolver el challenge. "
                "Inicia sesion en modo visible una vez y reintenta."
            )
    finally:
        if svc or ctx:
            try:
                await shutdown(svc, ctx)
            except Exception:
                pass


async def _evaluate_username(
    page,
    username: str,
    filter_cfg: LeadFilterConfig,
    text_lock: asyncio.Lock,
    image_lock: asyncio.Lock,
) -> Tuple[bool, str]:
    user, reason = await _pw_fetch_profile_snapshot(page, username)
    if not user:
        return False, reason or "perfil_no_disponible"
    ok, reason = _passes_classic_filters(user, filter_cfg.classic)
    if not ok:
        return False, reason
    text_reason = ""
    if filter_cfg.text.enabled:
        api_key = _get_openai_api_key()
        if not api_key:
            _log_text_filter_omitted_once()
        else:
            async with text_lock:
                decision, reason = await asyncio.to_thread(
                    _text_ai_decision,
                    api_key,
                    user,
                    filter_cfg.text.criteria,
                )
            if decision is None:
                pass
            elif not decision:
                if reason:
                    logging.info("Texto inteligente NO_CALIFICA (%s): %s", user.username, reason)
                return False, reason or "texto_ia"
            else:
                if reason:
                    text_reason = reason
    if filter_cfg.image.enabled:
        image_bytes = await _get_profile_image_bytes(page, user.profile_pic_url)
        api_key = _get_openai_api_key()
        if not api_key:
            _log_image_filter_omitted_once()
        else:
            try:
                async with image_lock:
                    ok, reason = await asyncio.wait_for(
                        asyncio.to_thread(
                            _image_ai_decision,
                            api_key,
                            user,
                            filter_cfg.image.prompt,
                            image_bytes=image_bytes,
                        ),
                        timeout=30.0,
                    )
            except asyncio.TimeoutError:
                return False, "imagen_timeout"
            if not ok:
                return False, reason
    return True, text_reason or "ok"


async def _pw_fetch_profile_snapshot(page, username: str) -> Tuple[Optional[ScrapedUser], str]:
    username = (username or "").strip().lstrip("@")
    if not username:
        return None, "username_vacio"
    user = await _pw_fetch_profile_json(page, username)
    if user:
        return user, ""
    profile_url = f"https://www.instagram.com/{username}/"
    try:
        await page.goto(profile_url, wait_until="domcontentloaded", timeout=45_000)
    except Exception:
        try:
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=45_000)
        except Exception:
            return None, "no_se_pudo_abrir"
    # Retry API after navigation (cookies/session may be established now)
    try:
        user = await _pw_fetch_profile_json(page, username)
        if user:
            return user, ""
    except Exception:
        pass

    data: Dict[str, Any] = {}
    for _ in range(3):
        try:
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        data = await _pw_extract_profile_data(page)
        if data:
            break
        try:
            await page.wait_for_timeout(1000)
        except Exception:
            pass
    if not data:
        return None, "perfil_sin_datos"
    if data.get("not_found"):
        return None, "perfil_no_disponible"
    follower_count = data.get("follower_count", 0)
    media_count = data.get("media_count", 0)
    full_name = data.get("full_name", "")
    biography = data.get("biography", "")
    is_private = bool(data.get("is_private", False))
    profile_pic_url = data.get("profile_pic_url", "")
    user = ScrapedUser(
        username=username,
        biography=biography,
        full_name=full_name,
        follower_count=follower_count,
        media_count=media_count,
        is_private=is_private,
        profile_pic_url=profile_pic_url,
    )
    return user, ""


async def _pw_fetch_profile_json(page, username: str) -> Optional[ScrapedUser]:
    if page is None or not username:
        return None
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "X-IG-App-ID": "936619743392459",
        "X-ASBD-ID": "198387",
        "X-IG-WWW-Claim": "0",
        "Referer": f"https://www.instagram.com/{username}/",
    }
    try:
        resp = await page.context.request.get(url, headers=headers, timeout=20_000)
    except Exception:
        return None
    if not resp or not resp.ok:
        return None
    try:
        payload = await resp.json()
    except Exception:
        return None
    user = (payload or {}).get("data", {}).get("user")
    if not isinstance(user, dict):
        return None
    biography = (user.get("biography") or "").strip()
    full_name = (user.get("full_name") or "").strip()
    follower_count = int((user.get("edge_followed_by") or {}).get("count") or 0)
    media_count = int((user.get("edge_owner_to_timeline_media") or {}).get("count") or 0)
    is_private = bool(user.get("is_private", False))
    profile_pic_url = (user.get("profile_pic_url_hd") or user.get("profile_pic_url") or "").strip()
    username_val = (user.get("username") or username).strip().lstrip("@")
    return ScrapedUser(
        username=username_val,
        biography=biography,
        full_name=full_name,
        follower_count=follower_count,
        media_count=media_count,
        is_private=is_private,
        profile_pic_url=profile_pic_url,
    )


async def _pw_extract_profile_data(page) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    try:
        data = await page.evaluate(
            """
            () => {
                const ogDesc = document.querySelector("meta[property='og:description']")?.content || "";
                const ogTitle = document.querySelector("meta[property='og:title']")?.content || "";
                const metaDesc = document.querySelector("meta[name='description']")?.content || "";
                const bioNode =
                    document.querySelector("[data-testid='user-bio']") ||
                    document.querySelector("header section div.-vDIg span") ||
                    document.querySelector("header section div[dir='auto'] span") ||
                    document.querySelector("header section span");
                const bio = bioNode ? (bioNode.textContent || "").trim() : "";
                const ogImage =
                    document.querySelector("meta[property='og:image:secure_url']")?.content ||
                    document.querySelector("meta[property='og:image']")?.content ||
                    "";
                let profileImgHd = "";
                try {
                    const scripts = Array.from(document.scripts || []);
                    for (const s of scripts) {
                        const txt = s.textContent || "";
                        if (!txt.includes("profile_pic_url")) continue;
                        const m = txt.match(/\"profile_pic_url_hd\":\"([^\"]+)\"/);
                        if (m && m[1]) {
                            profileImgHd = m[1]
                                .replace(/\\u0026/g, "&")
                                .replace(/\\\\/g, "\\")
                                .replace(/\\\//g, "/");
                            break;
                        }
                    }
                } catch (e) {}
                let profileImg = "";
                const pickFromSrcset = (img) => {
                    const srcset = img?.getAttribute("srcset") || "";
                    if (!srcset) return "";
                    const parts = srcset.split(",").map(s => s.trim()).filter(Boolean);
                    if (!parts.length) return "";
                    return (parts[parts.length - 1].split(" ")[0] || "");
                };
                const candidates = Array.from(document.querySelectorAll("img"));
                const profileCandidates = candidates.filter(img => {
                    const alt = (img.getAttribute("alt") || "").toLowerCase();
                    if (alt.includes("perfil") || alt.includes("profile") || alt.includes("photo") || alt.includes("foto")) {
                        return true;
                    }
                    return Boolean(img.closest("header"));
                });
                const scored = (profileCandidates.length ? profileCandidates : candidates).map(img => {
                    const w = img.naturalWidth || img.width || 0;
                    const h = img.naturalHeight || img.height || 0;
                    return { img, area: w * h };
                }).sort((a, b) => b.area - a.area);
                const best = scored.length ? scored[0].img : null;
                if (best) {
                    profileImg =
                        best.getAttribute("src") ||
                        best.currentSrc ||
                        pickFromSrcset(best) ||
                        "";
                }
                if (!profileImg && ogImage) {
                    profileImg = ogImage;
                }
                const bodyText = document.body ? (document.body.innerText || "") : "";
                return { ogDesc, ogTitle, metaDesc, bio, profileImg, profileImgHd, ogImage, bodyText };
            }
            """
        )
    except Exception:
        data = {}
    if not data:
        data = await _pw_extract_profile_data_from_html(page)
    if not data:
        return {}

    body_text = (data.get("bodyText") or "")
    normalized_body = body_text.lower()
    not_found = any(
        token in normalized_body
        for token in (
            "sorry, this page isn't available",
            "page isn't available",
            "no disponible",
            "pagina no esta disponible",
        )
    )
    is_private = "this account is private" in normalized_body or "esta cuenta es privada" in normalized_body

    og_desc = data.get("ogDesc") or data.get("metaDesc") or ""
    og_title = data.get("ogTitle") or ""
    full_name, _ = _parse_og_title(og_title)
    bio = data.get("bio") or _extract_bio_from_meta(og_desc)
    follower_count, media_count = _parse_og_metrics(og_desc)
    profile_pic_url = data.get("profileImgHd") or data.get("profileImg") or ""

    return {
        "full_name": full_name,
        "biography": bio,
        "follower_count": follower_count,
        "media_count": media_count,
        "is_private": is_private,
        "profile_pic_url": profile_pic_url,
        "not_found": not_found,
    }


async def _pw_extract_profile_data_from_html(page) -> Dict[str, Any]:
    try:
        page_html = await page.content()
    except Exception:
        return {}
    if not page_html:
        return {}

    def _extract_meta_content(source: str, key: str) -> str:
        patterns = (
            rf'<meta[^>]+property="{re.escape(key)}"[^>]+content="([^"]*)"[^>]*>',
            rf"<meta[^>]+property='{re.escape(key)}'[^>]+content='([^']*)'[^>]*>",
            rf'<meta[^>]+content="([^"]*)"[^>]+property="{re.escape(key)}"[^>]*>',
            rf"<meta[^>]+content='([^']*)'[^>]+property='{re.escape(key)}'[^>]*>",
            rf'<meta[^>]+name="{re.escape(key)}"[^>]+content="([^"]*)"[^>]*>',
            rf"<meta[^>]+name='{re.escape(key)}'[^>]+content='([^']*)'[^>]*>",
            rf'<meta[^>]+content="([^"]*)"[^>]+name="{re.escape(key)}"[^>]*>',
            rf"<meta[^>]+content='([^']*)'[^>]+name='{re.escape(key)}'[^>]*>",
        )
        for pattern in patterns:
            match = re.search(pattern, source, flags=re.IGNORECASE)
            if match:
                return html.unescape(match.group(1))
        return ""

    og_desc = _extract_meta_content(page_html, "og:description")
    og_title = _extract_meta_content(page_html, "og:title")
    og_image = _extract_meta_content(page_html, "og:image:secure_url") or _extract_meta_content(page_html, "og:image")
    meta_desc = _extract_meta_content(page_html, "description")

    profile_img_hd = ""
    match_hd = re.search(r'"profile_pic_url_hd":"([^"]+)"', page_html)
    if match_hd:
        profile_img_hd = (
            match_hd.group(1)
            .replace("\u0026", "&")
            .replace("\\\\", "\\")
            .replace("\/", "/")
        )

    body_text = html.unescape(re.sub(r"<[^>]+>", " ", page_html))

    return {
        "ogDesc": og_desc,
        "ogTitle": og_title,
        "metaDesc": meta_desc,
        "bio": "",
        "profileImg": og_image,
        "profileImgHd": profile_img_hd,
        "ogImage": og_image,
        "bodyText": body_text,
    }



def _parse_og_title(title: str) -> Tuple[str, str]:
    if not title:
        return "", ""
    match = re.search(r"^(.*?)\s*\(@([^)]+)\)", title)
    if not match:
        return title.strip(), ""
    return match.group(1).strip(), match.group(2).strip().lstrip("@")


def _extract_bio_from_meta(meta_desc: str) -> str:
    if not meta_desc:
        return ""
    for quote in ("\u201c", "\u201d", "\""):
        if quote in meta_desc:
            parts = meta_desc.split(quote)
            if len(parts) >= 3:
                return parts[1].strip()
    return ""


def _parse_og_metrics(og_desc: str) -> Tuple[int, int]:
    if not og_desc:
        return 0, 0
    followers = _extract_metric(og_desc, ("followers", "seguidores"))
    posts = _extract_metric(og_desc, ("posts", "publicaciones", "posteos"))
    return followers, posts


def _extract_metric(text: str, labels: Tuple[str, ...]) -> int:
    for label in labels:
        pattern = rf"([\d\.,]+\s*[kmbmil]*)\s+{label}"
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return _parse_count(match.group(1))
    return 0


def _parse_count(raw: str) -> int:
    if not raw:
        return 0
    s = raw.strip().lower().replace(" ", "")
    multiplier = 1
    if s.endswith("k") or "mil" in s:
        multiplier = 1000
    elif s.endswith("m") or "mill" in s:
        multiplier = 1_000_000
    elif s.endswith("b"):
        multiplier = 1_000_000_000
    num = re.sub(r"[^0-9\.,]", "", s)
    if not num:
        return 0
    if multiplier != 1:
        num = num.replace(",", ".")
        try:
            return int(float(num) * multiplier)
        except Exception:
            return 0
    num = num.replace(".", "").replace(",", "")
    try:
        return int(num)
    except Exception:
        return 0


def _passes_classic_filters(user: ScrapedUser, cfg: ClassicFilterConfig) -> Tuple[bool, str]:
    if cfg.privacy == "public" and user.is_private:
        return False, "perfil_privado"
    if cfg.privacy == "private" and not user.is_private:
        return False, "perfil_publico"
    if cfg.min_followers and user.follower_count < cfg.min_followers:
        return False, "seguidores_min"
    if cfg.min_posts and user.media_count < cfg.min_posts:
        return False, "posts_min"
    link_present = _bio_has_link(user.biography)
    if cfg.link_in_bio == "yes" and not link_present:
        return False, "sin_link_bio"
    if cfg.link_in_bio == "no" and link_present:
        return False, "con_link_bio"
    haystack = _normalize_text(" ".join([user.username, user.full_name, user.biography]))
    if cfg.exclude_keywords:
        for term in cfg.exclude_keywords:
            if _normalize_text(term) in haystack:
                return False, "keyword_excluida"
    if cfg.include_keywords:
        if not any(_normalize_text(term) in haystack for term in cfg.include_keywords):
            return False, "keyword_faltante"
    if cfg.language and cfg.language != "any":
        detected = _detect_language(" ".join([user.username, user.full_name, user.biography]))
        if detected == "unknown":
            return False, "idioma_desconocido"
        if detected != cfg.language:
            return False, "idioma_no_coincide"
    return True, ""


def _bio_has_link(bio: str) -> bool:
    if not bio:
        return False
    return bool(re.search(r"(https?://|www\.)", bio, flags=re.IGNORECASE))


def _playwright_proxy_from_account(account: Dict) -> Optional[Dict[str, str]]:
    try:
        from src.auth.onboarding import build_proxy
    except Exception:
        return None
    proxy_raw = account.get("proxy_url") or account.get("proxy")
    if not proxy_raw:
        return None
    return build_proxy(proxy_raw)


def _parse_text_ai_output(raw_text: str) -> Tuple[Optional[bool], str]:
    if not raw_text:
        return None, ""
    first_line = raw_text.strip().splitlines()[0].strip()
    if not first_line:
        return None, ""
    upper = first_line.upper()
    label = None
    if upper.startswith("CALIFICA"):
        label = "CALIFICA"
        decision = True
    elif upper.startswith("NO_CALIFICA") or upper.startswith("NO CALIFICA") or upper.startswith("NO"):
        label = "NO_CALIFICA"
        decision = False
    else:
        return None, ""
    reason = first_line[len(label):].strip(" -:|	") if label else ""
    if len(reason) > 140:
        reason = reason[:140]
    return decision, reason


def _text_ai_decision(api_key: str, user: ScrapedUser, criteria: str) -> Tuple[Optional[bool], str]:
    criteria = (criteria or "").strip()
    if not criteria:
        return False, "criterio_vacio"
    try:
        from openai import OpenAI
    except Exception as exc:
        logging.error("Texto inteligente: no se pudo importar OpenAI: %s", exc)
        return False, "openai_no_disponible"
    try:
        client = OpenAI(api_key=api_key)
    except Exception as exc:
        logging.error("Texto inteligente: no se pudo inicializar OpenAI: %s", exc)
        return False, "openai_no_disponible"

    system = (
        "Sos un clasificador estricto de perfiles de Instagram. "
        "Responde SOLO con 'CALIFICA' o 'NO_CALIFICA'. "
        "Si queres agregar una razon corta, agregala en la misma linea tras un separador."
    )
    payload_lines = ["Criterio:", criteria, "", f"Username: {user.username}"]
    if user.full_name:
        payload_lines.append(f"Nombre: {user.full_name}")
    payload_lines.append(f"Biografia: {user.biography}")
    payload_lines.append("Respuesta:")
    user_content = "\n".join(payload_lines)

    raw_text = ""
    try:
        if hasattr(client, "responses"):
            response = client.responses.create(
                model="gpt-4o-mini",
                input=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_content},
                ],
                temperature=0.0,
                max_output_tokens=32,
            )
            raw_text = (getattr(response, "output_text", "") or "").strip()
        elif hasattr(client, "chat") and hasattr(client.chat, "completions"):
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_content},
                ],
                temperature=0.0,
                max_tokens=32,
            )
            choices = getattr(response, "choices", []) or []
            if choices:
                message = getattr(choices[0], "message", None)
                raw_text = (getattr(message, "content", "") or "").strip()
        if not raw_text:
            raise RuntimeError("respuesta_vacia")
    except Exception as exc:
        if _is_invalid_api_key_error(exc):
            _log_text_filter_invalid_key_once()
            return None, "api_key_invalida"
        logging.error("Texto inteligente: fallo la llamada a OpenAI: %s", exc)
        return False, "openai_error"

    decision, reason = _parse_text_ai_output(raw_text)
    if decision is None:
        logging.warning("Texto inteligente: respuesta invalida: %s", raw_text)
        return False, "respuesta_invalida"
    return decision, reason












def _image_ai_decision(
    api_key: str,
    user: ScrapedUser,
    prompt: str,
    *,
    image_bytes: Optional[bytes] = None,
) -> Tuple[bool, str]:
    if not user.profile_pic_url and not image_bytes:
        return False, "sin_foto"
    if image_bytes is None:
        image_bytes = _download_image_bytes(user.profile_pic_url)
    if not image_bytes:
        return False, "foto_no_disponible"

    prompt = (prompt or "").strip()
    if not prompt:
        return False, "prompt_vacio"
    try:
        from openai import OpenAI
    except Exception as exc:
        logging.error("Imagen: no se pudo importar OpenAI: %s", exc)
        return False, "openai_no_disponible"
    try:
        client = OpenAI(api_key=api_key)
    except Exception as exc:
        logging.error("Imagen: no se pudo inicializar OpenAI: %s", exc)
        return False, "openai_no_disponible"

    system = (
        "Sos un clasificador de imagenes de perfil de Instagram. "
        "Tu objetivo es evitar falsos negativos en fotos reales (baja resolucion, parciales, de perfil, pequenas). "
        "Considera presencia razonable del rasgo pedido aunque no sea dominante. "
        "Responde CALIFICA si hay evidencia visual plausible y no hay contradiccion clara. "
        "Responde NO_CALIFICA solo si el rasgo esta claramente ausente o la imagen no permite ver nada relevante. "
        "Responde SOLO con 'CALIFICA' o 'NO_CALIFICA'. "
        "Si queres agregar una razon corta, agregala en la misma linea tras un separador."
    )
    user_text = (
        "Criterio visual (interpreta de forma flexible y contextual):\n"
        f"{prompt}\n\n"
        "Recorda: fotos de IG suelen ser pequenas o parciales; si hay evidencia razonable, CALIFICA.\n"
        "Respuesta:"
    )
    image_b64 = base64.b64encode(image_bytes).decode("ascii")
    data_url = f"data:image/jpeg;base64,{image_b64}"
    raw_text = ""
    try:
        if hasattr(client, "responses"):
            response = client.responses.create(
                model="gpt-4o-mini",
                input=[
                    {"role": "system", "content": system},
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": user_text},
                            {"type": "input_image", "image_url": data_url},
                        ],
                    },
                ],
                temperature=0.0,
                max_output_tokens=32,
            )
            raw_text = (getattr(response, "output_text", "") or "").strip()
        elif hasattr(client, "chat") and hasattr(client.chat, "completions"):
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": user_text},
                            {"type": "image_url", "image_url": {"url": data_url}},
                        ],
                    },
                ],
                temperature=0.0,
                max_tokens=32,
            )
            choices = getattr(response, "choices", []) or []
            if choices:
                message = getattr(choices[0], "message", None)
                raw_text = (getattr(message, "content", "") or "").strip()
        if not raw_text:
            raise RuntimeError("respuesta_vacia")
    except Exception as exc:
        if _is_invalid_api_key_error(exc):
            logging.warning("Imagen: API Key invalida.")
            return False, "api_key_invalida"
        logging.error("Imagen: fallo la llamada a OpenAI: %s", exc)
        return False, "openai_error"

    decision, reason = _parse_text_ai_output(raw_text)
    if decision is None:
        logging.warning("Imagen: respuesta invalida: %s", raw_text)
        return False, "respuesta_invalida"
    return decision, reason


def _download_image_bytes(url: str) -> Optional[bytes]:
    if not url:
        return None
    if url.startswith("data:"):
        return None
    try:
        import requests
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Referer": "https://www.instagram.com/",
        }
        resp = requests.get(url, timeout=20, headers=headers)
        if resp.status_code != 200:
            return None
        return resp.content
    except Exception:
        return None


async def _update_list_item(
    list_data: Dict[str, Any],
    idx: int,
    result: str,
    reason: str,
    account: str,
    lock: asyncio.Lock,
) -> None:
    async with lock:
        item = list_data["items"][idx]
        item["status"] = "QUALIFIED" if result == "CALIFICA" else "DISCARDED"
        item["result"] = result
        item["reason"] = "" if result == "CALIFICA" else reason
        item["account"] = account
        item["updated_at"] = _now_iso()
        _refresh_list_stats(list_data)
        _save_filter_list(list_data)


def _log_filter_result(username: str, account: str, result: str, reason: str) -> None:
    if result == "CALIFICA":
        detail = reason or "-"
    else:
        detail = reason or "-"
    print(f"@{username} | @{account} | {result} | {detail}", flush=True)


async def _apply_delay(min_s: float, max_s: float) -> None:
    low = max(0.0, float(min_s))
    high = max(low, float(max_s))
    if high <= 0:
        return
    await asyncio.sleep(random.uniform(low, high))


def _clear_console() -> None:
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


def _ask_float(
    prompt: str,
    *,
    default: float,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    while True:
        raw = ask(prompt).strip()
        if not raw:
            return float(default)
        try:
            value = float(raw.replace(",", "."))
        except Exception:
            warn("Valor invalido. Ingresa un numero.")
            continue
        if min_value is not None and value < min_value:
            warn(f"El valor debe ser >= {min_value}.")
            continue
        if max_value is not None and value > max_value:
            warn(f"El valor debe ser <= {max_value}.")
            continue
        return value


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"
