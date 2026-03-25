from __future__ import annotations

import asyncio
import csv
import getpass
import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import Sniffer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import unquote, urlparse

from core.accounts_helpers.csv_utils import _TOTP_HEADER_ALIASES, extract_totp_secret_from_row
from core.totp_store import (
    _normalize_secret,
    generate_code as generate_totp_code,
    get_secret as get_totp_secret,
    save_secret as save_totp_secret,
)
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from paths import accounts_root
from src.auth.persistent_login import ChallengeRequired, ensure_logged_in_async
from src.browser_profile_paths import browser_storage_state_path
from src.instagram_adapter import BASE_URL, INBOX_URL, get_login_errors, is_logged_in
from src.playwright_service import BASE_PROFILES, PlaywrightService, shutdown
from src.proxy_payload import normalize_playwright_proxy, proxy_from_account
from src.runtime.playwright_runtime import run_coroutine_sync

logger = logging.getLogger(__name__)

_DEFAULT_PROFILE_ROOT = Path(BASE_PROFILES)
RESULTS_PATH = accounts_root(Path(__file__).resolve().parents[2]) / "onboarding_results.csv"

ProxyPayload = Optional[Dict[str, str]]
AccountPayload = Dict[str, Any]
OnboardingResult = Dict[str, Any]

ENCODINGS = ["utf-8-sig", "utf-8", "latin-1"]
DELIMS = [",", ";", "\t"]
HEADER_ALIASES: Dict[str, List[str]] = {
    "username": ["username", "user", "login"],
    "password": ["password", "pass"],
    "totp_secret": list(_TOTP_HEADER_ALIASES),
    "proxy_url": ["proxy_url", "proxy", "http_proxy", "https_proxy"],
    "proxy_ip": ["proxy_ip", "ip", "host"],
    "proxy_port": ["proxy_port", "port"],
    "proxy_user": ["proxy_user", "proxy_username", "user_proxy", "username_proxy", "user"],
    "proxy_pass": ["proxy_pass", "proxy_password", "pass_proxy", "password_proxy", "pass"],
    "sticky": ["sticky", "session", "persist"],
    "minutes": ["minutes", "session_minutes", "mins"],
}
EXPECTED_ORDER = [
    "username",
    "password",
    "totp_secret",
    "proxy_ip",
    "proxy_port",
    "proxy_user",
    "proxy_pass",
]


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return run_coroutine_sync(coro, ignore_stop=True)
    raise RuntimeError("login_account_playwright requiere contexto sync; no usar dentro de un loop activo.")


def smart_open_csv(csv_path: Union[str, Path]) -> str:
    """
    Intenta leer el CSV usando las codificaciones mÃ¡s comunes.
    Devuelve el contenido completo como texto.
    """
    path = Path(csv_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo CSV: {path}")

    last_exc: Optional[Exception] = None
    for encoding in ENCODINGS:
        try:
            return path.read_text(encoding=encoding)
        except Exception as exc:  # pragma: no cover - depende del archivo
            last_exc = exc
            continue
    raise RuntimeError(f"No se pudo leer CSV con encodings conocidos: {last_exc}")


def _detect_delimiter(sample: str) -> Tuple[str, Optional[csv.Dialect]]:
    snippet = sample[:4096]
    try:
        dialect = Sniffer().sniff(snippet, delimiters=",;\t")
        return dialect.delimiter, dialect
    except Exception:
        pass
    for candidate in DELIMS:
        if snippet.count(candidate):
            return candidate, None
    return ",", None


def smart_read_csv(csv_path: Union[str, Path]) -> Tuple[List[str], List[List[str]]]:
    data = smart_open_csv(csv_path)
    delimiter, dialect = _detect_delimiter(data)
    stream = io.StringIO(data)
    if dialect:
        reader = csv.reader(stream, dialect=dialect)
    else:
        reader = csv.reader(stream, delimiter=delimiter)

    rows: List[List[str]] = []
    for raw_row in reader:
        trimmed = [cell.strip() for cell in raw_row]
        if not any(trimmed):
            continue
        rows.append(trimmed)

    if not rows:
        return [], []

    headers = [(cell or "").strip() for cell in rows[0]]
    return headers, rows[1:]


def _row_from_headers(headers: List[str], row: List[str]) -> Dict[str, Any]:
    """
    Normaliza una fila usando headers detectados. Si los headers NO contienen
    username o password, asumimos formato posicional en EXPECTED_ORDER.
    """
    normalized = [h.strip().lower() for h in headers]
    has_username = any(alias in normalized for alias in ("username", "user", "login"))
    has_password = any(alias in normalized for alias in ("password", "pass"))

    if not has_username or not has_password:
        values = [cell.strip() for cell in row]
        while len(values) < len(EXPECTED_ORDER):
            values.append("")
        mapped = {EXPECTED_ORDER[i]: values[i] for i in range(len(EXPECTED_ORDER))}
        totp_secret = extract_totp_secret_from_row({"totp_secret": mapped.get("totp_secret", "")})
        proxy_fields: Dict[str, str] = {}
        if mapped.get("proxy_ip") or mapped.get("proxy_port"):
            proxy_fields = {
                "ip": mapped.get("proxy_ip", ""),
                "port": mapped.get("proxy_port", ""),
                "username": mapped.get("proxy_user", ""),
                "password": mapped.get("proxy_pass", ""),
            }
        return {
            "username": mapped.get("username", ""),
            "password": mapped.get("password", ""),
            "totp_secret": totp_secret,
            "proxy_fields": proxy_fields,
        }

    header_to_index = {header.strip().lower(): idx for idx, header in enumerate(headers)}

    def get_by_alias(aliases: List[str]) -> str:
        for alias in aliases:
            key = alias.strip().lower()
            idx = header_to_index.get(key)
            if idx is not None and idx < len(row):
                return row[idx].strip()
        return ""

    username = get_by_alias(["username", "user", "login"])
    password = get_by_alias(["password", "pass"])
    row_by_header = {
        header: row[idx].strip()
        for idx, header in enumerate(headers)
        if idx < len(row)
    }
    totp = extract_totp_secret_from_row(row_by_header)
    proxy_url = get_by_alias(["proxy_url", "proxy", "http_proxy", "https_proxy"])

    if proxy_url:
        proxy_fields: Dict[str, str] = {"url": proxy_url}
    else:
        proxy_fields = {
            "ip": get_by_alias(["proxy_ip", "ip", "host"]),
            "port": get_by_alias(["proxy_port", "port"]),
            "username": get_by_alias(
                ["proxy_user", "proxy_username", "user_proxy", "username_proxy", "user"]
            ),
            "password": get_by_alias(
                ["proxy_pass", "proxy_password", "pass_proxy", "password_proxy", "pass"]
            ),
        }

    proxy_fields["sticky"] = get_by_alias(["sticky", "session", "persist"])
    proxy_fields["minutes"] = get_by_alias(["minutes", "session_minutes", "mins"])

    return {
        "username": username,
        "password": password,
        "totp_secret": totp,
        "proxy_fields": proxy_fields,
    }


def build_proxy(payload: Union[str, Dict[str, Any], None]) -> ProxyPayload:
    """
    Normaliza los datos de proxy hacia el formato esperado por Playwright:
    {"server": "http://ip:port", "username": "...", "password": "..."}.
    Acepta:
      - str tipo http://user:pass@ip:port o ip:port
      - dict con claves url/ip/host, port, username, password, sticky, minutes.
    """
    if not payload:
        return None

    if isinstance(payload, str):
        raw = payload.strip()
        if not raw:
            return None
        has_scheme = "://" in raw
        candidate = raw if has_scheme else f"http://{raw}"
        parsed = urlparse(candidate)
        server = candidate if has_scheme else f"http://{parsed.netloc or parsed.path}"
        proxy: Dict[str, str] = {"server": server}
        if parsed.username and parsed.password:
            proxy["username"] = unquote(parsed.username)
            proxy["password"] = unquote(parsed.password)
        return proxy

    if isinstance(payload, dict):
        normalized = {str(k).lower(): v for k, v in payload.items() if v not in (None, "")}
        url_value = normalized.get("url") or normalized.get("server")
        ip = normalized.get("ip") or normalized.get("host") or normalized.get("hostname")
        port = normalized.get("port")
        scheme = normalized.get("scheme") or normalized.get("protocol") or "http"
        server: Optional[str] = None
        if url_value:
            server = str(url_value).strip()
        elif ip and port:
            server = f"{scheme}://{ip}:{port}"
        if not server:
            return None
        proxy = {"server": server}
        username = normalized.get("username") or normalized.get("user")
        password = normalized.get("password") or normalized.get("pass")
        if username:
            proxy["username"] = str(username)
        if password:
            proxy["password"] = str(password)
        return proxy

    return None


def _make_account(username: str, password: str, totp_secret: str | None, proxy_fields: dict | None):
    """
    Construye el dict 'account' en el formato que espera ensure_logged_in/login_and_persist.
    - username, password obligatorios
    - totp_secret opcional como valor de entrada; el login siempre se resuelve
      desde el store canÃ³nico si existe
    - proxy_fields puede venir como dict con ip/port/username/password o como {'url': ...}
    """
    acc = {"username": (username or "").strip(), "password": (password or "").strip()}

    if totp_secret:
        acc["totp_secret"] = str(totp_secret).strip()

    proxy = None
    if proxy_fields:
        proxy = build_proxy(proxy_fields)
    if proxy:
        acc["proxy"] = proxy

    return acc


def parse_accounts_csv(csv_path: Union[str, Path]) -> List[Dict[str, Any]]:
    headers, raw_rows = smart_read_csv(csv_path)
    if not headers:
        return []

    normalized_headers = [h.strip().lower() for h in headers]
    has_username = any(alias in normalized_headers for alias in ("username", "user", "login"))
    has_password = any(alias in normalized_headers for alias in ("password", "pass"))
    header_has_labels = has_username and has_password

    if header_has_labels:
        data_rows = raw_rows
        base_row_number = 2
        parser_headers = headers
    else:
        data_rows = [headers] + raw_rows if headers else raw_rows
        base_row_number = 1
        parser_headers = []

    parsed_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(data_rows):
        row_number = base_row_number + idx
        parsed = _row_from_headers(parser_headers, row)
        username = (parsed.get("username") or "").strip().lstrip("@")
        password = (parsed.get("password") or "").strip()
        totp_secret = (parsed.get("totp_secret") or "").strip()
        proxy_fields = parsed.get("proxy_fields") or {}

        account_data = {
            "row_number": row_number,
            "username": username,
            "password": password,
            "totp_secret": totp_secret,
            "proxy_url": proxy_fields.get("url") or (
                f"http://{proxy_fields.get('ip')}:{proxy_fields.get('port')}"
                if proxy_fields.get("ip") and proxy_fields.get("port")
                else ""
            ),
            "proxy_user": proxy_fields.get("username") or "",
            "proxy_pass": proxy_fields.get("password") or "",
            "proxy_sticky_minutes": proxy_fields.get("minutes") or proxy_fields.get("sticky"),
            "proxy_fields": proxy_fields,
        }
        parsed_rows.append(account_data)

    return parsed_rows


def code_provider_prompt(label: str = "Ingresa el cÃ³digo recibido (WhatsApp/SMS/email): ") -> str:
    """
    Solicita un cÃ³digo de verificaciÃ³n sin mostrarlo en consola.
    """
    try:
        return getpass.getpass(label).strip()
    except (EOFError, KeyboardInterrupt):
        return ""


def _profile_path_for(username: str, profile_root: Union[str, Path]) -> Path:
    return browser_storage_state_path(username, profiles_root=profile_root or _DEFAULT_PROFILE_ROOT)


def _persist_canonical_totp(username: str, raw_secret: str | None) -> Optional[str]:
    candidate = str(raw_secret or "").strip()
    if not candidate:
        return None

    normalized = _normalize_secret(candidate)
    current = get_totp_secret(username)
    if current != normalized:
        save_totp_secret(username, normalized)
    return normalized


def _canonical_totp_payload(username: str, account: AccountPayload) -> Dict[str, Any]:
    _persist_canonical_totp(username, account.get("totp_secret"))

    secret = get_totp_secret(username)
    if not secret:
        if callable(account.get("totp_callback")):
            logger.debug(
                "Se ignorÃ³ totp_callback en memoria para @%s porque no existe secreto canÃ³nico.",
                username,
            )
        return {}

    return {
        "totp_secret": secret,
        "totp_callback": lambda _ignored, target=username: generate_totp_code(target) or "",
    }


async def login_and_persist_async(
    account: AccountPayload,
    *,
    headless: bool = True,
    profile_root: Union[str, Path] = _DEFAULT_PROFILE_ROOT,
) -> Dict[str, str]:
    """
    Ejecuta el login humano mediante Playwright y persiste storage_state.json.
    Devuelve {"username", "status" (ok|need_code|failed), "message", "profile_path"}.
    """
    username = (account.get("username") or "").strip().lstrip("@")
    password = (account.get("password") or "").strip()
    if not username or not password:
        return {
            "username": username or "",
            "status": "failed",
            "message": "Faltan credenciales (username/password).",
            "profile_path": "",
        }

    proxy_payload = proxy_from_account(account)
    canonical_totp = _canonical_totp_payload(username, account)

    need_code = {"value": False}

    def _code_provider() -> str:
        label = f"CÃ³digo para @{username} (WhatsApp/SMS/email): "
        code = code_provider_prompt(label)
        if not code:
            need_code["value"] = True
        return code

    payload: AccountPayload = {
        "username": username,
        "password": password,
        "proxy": proxy_payload,
        "code_provider": _code_provider,
    }
    payload.update(canonical_totp)

    svc: Optional[PlaywrightService] = None
    ctx = page = None
    close_browser = True
    try:
        svc, ctx, page = await ensure_logged_in_async(
            payload,
            headless=headless,
            profile_root=profile_root,
            proxy=proxy_payload,
        )
        if await is_logged_in(page):
            storage_path = str(_profile_path_for(username, profile_root))
            return {
                "username": username,
                "status": "ok",
                "message": "Login completado.",
                "profile_path": storage_path,
            }
        status = "challenge_required" if need_code["value"] else "error"
        message = (
            "Se requiere verificacion por email."
            if status == "challenge_required"
            else "No se pudo confirmar la sesion."
        )
        if status == "challenge_required":
            close_browser = False
        return {
            "username": username,
            "status": status,
            "message": message,
            "profile_path": "",
        }
    except ChallengeRequired:  # pragma: no cover - depende de Playwright
        close_browser = False
        return {
            "username": username,
            "status": "challenge_required",
            "message": "Se requiere verificacion por email.",
            "profile_path": "",
        }
    except Exception as exc:  # pragma: no cover - depende de Playwright
        status = "challenge_required" if need_code["value"] else "error"
        message = (
            "Se requiere verificacion por email."
            if status == "challenge_required"
            else str(exc)
        )
        if status == "challenge_required":
            close_browser = False
        return {
            "username": username,
            "status": status,
            "message": message,
            "profile_path": "",
        }
    finally:
        if svc and close_browser:
            await shutdown(svc, ctx)


def login_and_persist(
    account: AccountPayload,
    *,
    headless: bool = True,
    profile_root: Union[str, Path] = _DEFAULT_PROFILE_ROOT,
) -> Dict[str, str]:
    """
    Wrapper sync para login_and_persist_async (evita usar Playwright sync API).
    """
    return _run_async(
        login_and_persist_async(
            account,
            headless=headless,
            profile_root=profile_root,
        )
    )


def _write_results_file(rows: List[OnboardingResult]) -> None:
    if not rows:
        return
    try:
        RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with RESULTS_PATH.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["username", "status", "message", "profile_path", "row_number"])
            for row in rows:
                writer.writerow(
                    [
                        row.get("username", ""),
                        row.get("status", ""),
                        row.get("message", ""),
                        row.get("profile_path", ""),
                        row.get("row_number", ""),
                    ]
                )
    except Exception as exc:  # pragma: no cover - operaciones de disco
        logger.warning("No se pudo escribir %s: %s", RESULTS_PATH, exc)


def write_onboarding_results(rows: List[OnboardingResult]) -> None:
    _write_results_file(rows)


def _login_trace(alias: str, username: str):
    prefix = f"[LOGIN] [ACCOUNT alias={alias} user=@{username}]"

    def _trace(message: str) -> None:
        print(f"{prefix} {message}", flush=True)

    return _trace


def _proxy_label(proxy_payload: Optional[Dict[str, str]]) -> str:
    if not proxy_payload:
        return "No proxy"
    server = (
        proxy_payload.get("server")
        or proxy_payload.get("url")
        or proxy_payload.get("proxy")
        or ""
    )
    if not server:
        return "Proxy enabled"
    candidate = server if "://" in server else f"http://{server}"
    try:
        parsed = urlparse(candidate)
        hostport = parsed.netloc or parsed.path
    except Exception:
        hostport = server
    return f"Proxy enabled {hostport}"


_INBOX_READY_SELECTORS = (
    "a[href='/direct/inbox/']",
    "a[href*='/direct/inbox/']",
    "a[href*='/direct/t/']",
    "div[role='navigation'] a[href*='/direct/']",
    "input[placeholder='Search']",
    "input[placeholder='Buscar']",
    "input[name='queryBox']",
    "svg[aria-label='Direct']",
    "svg[aria-label='Mensajes']",
)
_FEED_READY_SELECTORS = (
    "svg[aria-label='Home']",
    "svg[aria-label='Inicio']",
    "a[href='/']",
    "nav[role='navigation']",
    "a[href='/direct/inbox/']",
)
_LOGIN_FORM_SELECTORS = (
    "input[name='username']",
    "input[name='email']",
    "input[autocomplete='username']",
    "input[name='password']",
    "input[type='password']",
    "form[action*='login']",
)


async def _first_visible_selector(page, selectors: tuple[str, ...]) -> Optional[str]:
    for selector in selectors:
        try:
            if await page.locator(selector).count():
                return selector
        except Exception:
            continue
    return None


async def confirm_feed_logged_in(page, trace=None) -> tuple[bool, str]:
    if callable(trace):
        trace("Wait for feed confirmation")
        trace(f"Open {BASE_URL}")
    try:
        await page.goto(BASE_URL, wait_until="domcontentloaded")
    except Exception as exc:
        raise RuntimeError(f"feed_navigation_failed:{exc}") from exc

    url = (page.url or "").lower()
    login_selector = await _first_visible_selector(page, _LOGIN_FORM_SELECTORS)
    if "/accounts/login" in url or "/accounts/onetap" in url or login_selector:
        return False, login_selector or "login_required"
    if "/challenge/" in url:
        return False, "challenge_required"

    feed_selector = await _first_visible_selector(page, _FEED_READY_SELECTORS)
    normalized_base = BASE_URL.rstrip("/").lower()
    normalized_url = url.rstrip("/")
    if normalized_url == normalized_base or feed_selector:
        return True, feed_selector or "feed_url"
    return False, "feed_not_ready"


async def confirm_inbox_logged_in(page, trace=None) -> tuple[bool, str]:
    if callable(trace):
        trace("Wait for inbox confirmation")
        trace(f"Open {INBOX_URL}")
    try:
        await page.goto(INBOX_URL, wait_until="domcontentloaded")
    except Exception as exc:
        raise RuntimeError(f"inbox_navigation_failed:{exc}") from exc

    url = (page.url or "").lower()
    inbox_selector = await _first_visible_selector(page, _INBOX_READY_SELECTORS)
    if "/direct/inbox" in url or inbox_selector:
        return True, inbox_selector or "inbox_url"

    login_selector = await _first_visible_selector(page, _LOGIN_FORM_SELECTORS)
    if "/accounts/login" in url or "/accounts/onetap" in url or login_selector:
        return False, login_selector or "login_required"
    if "/challenge/" in url:
        return False, "challenge_required"
    raise RuntimeError("inbox_surface_unknown: no inbox DOM y no login form")


async def login_account_playwright_async(
    account: AccountPayload,
    alias: str,
    *,
    headful: bool = True,
) -> Dict[str, Any]:
    login_account = dict(account)
    username = (login_account.get("username") or "").strip().lstrip("@")
    password = (login_account.get("password") or "").strip()
    trace = _login_trace(alias, username or "unknown")

    if not username or not password:
        trace("FAIL reason=missing_username_or_password")
        return {
            "username": username or "",
            "status": "failed",
            "message": "missing_username_or_password",
            "profile_path": "",
            "row_number": login_account.get("row_number"),
        }

    proxy_payload = proxy_from_account(login_account)
    if not proxy_payload:
        proxy_payload = normalize_playwright_proxy(
            login_account.get("proxy"),
            proxy_user=login_account.get("proxy_user"),
            proxy_pass=login_account.get("proxy_pass"),
        )
    if proxy_payload:
        login_account["proxy"] = proxy_payload
    trace(_proxy_label(proxy_payload))

    login_account.pop("totp_secret", None)
    login_account.pop("totp_callback", None)
    login_account.update(_canonical_totp_payload(username, account))
    login_account["alias"] = alias
    login_account["trace"] = trace
    login_account.setdefault("strict_login", False)
    login_account.setdefault("force_login", False)
    login_account.setdefault("disable_safe_browser_recovery", True)

    headless = not headful
    svc = None
    ctx = None
    progress_callback = login_account.get("login_progress_callback")

    def _progress(state: str, message: str) -> None:
        if not callable(progress_callback):
            return
        try:
            progress_callback(state, message)
        except Exception:
            return

    try:
        _progress("running_login", "Resolviendo sesion")
        svc, ctx, page = await ensure_logged_in_async(
            login_account,
            headless=headless,
            profile_root=_DEFAULT_PROFILE_ROOT,
            proxy=proxy_payload,
        )

        _progress("confirming_feed", "Confirmando feed")
        ok_feed, feed_reason = await confirm_feed_logged_in(page, trace=trace)
        if not ok_feed:
            trace(f"FAIL reason={feed_reason}")
            return {
                "username": username,
                "status": "failed",
                "message": feed_reason,
                "profile_path": "",
                "row_number": login_account.get("row_number"),
            }

        _progress("confirming_inbox", "Confirmando inbox")
        ok_inbox, reason = await confirm_inbox_logged_in(page, trace=trace)
        if ok_inbox:
            profile_path = str(_profile_path_for(username, _DEFAULT_PROFILE_ROOT))
            try:
                await svc.save_storage_state(ctx, profile_path)
            except Exception:
                pass
            trace("SUCCESS login confirmed by inbox")
            return {
                "username": username,
                "status": "ok",
                "message": f"{feed_reason} -> {reason}",
                "profile_path": profile_path,
                "row_number": login_account.get("row_number"),
            }

        errors = []
        try:
            errors = await get_login_errors(page)
        except Exception:
            errors = []
        detail = reason
        if errors:
            detail = f"{detail} errors={'; '.join(errors)}"
        trace(f"FAIL reason={detail}")
        return {
            "username": username,
            "status": "failed",
            "message": detail,
            "profile_path": "",
            "row_number": login_account.get("row_number"),
        }
    except ChallengeRequired:
        trace("FAIL reason=challenge_required")
        return {
            "username": username,
            "status": "failed",
            "message": "challenge_required",
            "profile_path": "",
            "row_number": login_account.get("row_number"),
        }
    except PlaywrightTimeoutError as exc:
        detail = f"timeout:{exc}"
        trace(f"FAIL reason={detail}")
        return {
            "username": username,
            "status": "failed",
            "message": detail,
            "profile_path": "",
            "row_number": login_account.get("row_number"),
        }
    except Exception as exc:
        detail = str(exc)
        trace(f"FAIL reason={detail}")
        return {
            "username": username,
            "status": "failed",
            "message": detail,
            "profile_path": "",
            "row_number": login_account.get("row_number"),
        }
    finally:
        if svc:
            try:
                await shutdown(svc, ctx)
            except Exception:
                pass


def login_account_playwright(
    account: AccountPayload,
    alias: str,
    *,
    headful: bool = True,
) -> Dict[str, Any]:
    return _run_async(
        login_account_playwright_async(account, alias, headful=headful)
    )


def onboard_accounts_from_csv(
    csv_path: Union[str, Path],
    *,
    headless: bool = True,
    concurrency: int = 2,
) -> List[OnboardingResult]:
    """
    Procesa un CSV de cuentas (username,password,totp_secret,proxy_...) y
    ejecuta login headless por fila. Guarda los resultados en storage/accounts/onboarding_results.csv.
    """
    headers, raw_rows = smart_read_csv(csv_path)
    if not headers:
        return []

    normalized_headers = [h.strip().lower() for h in headers]
    has_username = any(alias in normalized_headers for alias in ("username", "user", "login"))
    has_password = any(alias in normalized_headers for alias in ("password", "pass"))
    header_has_labels = has_username and has_password

    if header_has_labels:
        data_rows = raw_rows
        base_row_number = 2
        parser_headers = headers
    else:
        data_rows = [headers] + raw_rows if headers else raw_rows
        base_row_number = 1
        parser_headers = []

    if not data_rows:
        return []

    results: List[OnboardingResult] = []

    def worker(row_number: int, row: List[str]) -> OnboardingResult:
        parsed: Dict[str, Any] = {}
        try:
            parsed = _row_from_headers(parser_headers, row)
            username = (parsed.get("username") or "").strip().lstrip("@")
            password = (parsed.get("password") or "").strip()
            totp_secret = (parsed.get("totp_secret") or "").strip()
            proxy_fields = parsed.get("proxy_fields") or {}

            account_data = {
                "username": username,
                "password": password,
                "totp_secret": totp_secret,
                "proxy_url": proxy_fields.get("url") or (
                    f"http://{proxy_fields.get('ip')}:{proxy_fields.get('port')}"
                    if proxy_fields.get("ip") and proxy_fields.get("port")
                    else ""
                ),
                "proxy_user": proxy_fields.get("username") or "",
                "proxy_pass": proxy_fields.get("password") or "",
                "proxy_sticky_minutes": proxy_fields.get("minutes") or proxy_fields.get("sticky"),
            }

            if not username or not password:
                result = {
                    "username": username,
                    "status": "failed",
                    "message": "Campos incompletos: username, password",
                    "profile_path": "",
                }
            else:
                _persist_canonical_totp(username, totp_secret)
                account_payload = _make_account(username, password, None, proxy_fields or {})
                result = login_and_persist(account_payload, headless=headless, profile_root=_DEFAULT_PROFILE_ROOT)

            result["row_number"] = row_number
            result["account_data"] = account_data
            print(f"[{row_number}] {username} -> {result.get('status')} {result.get('message', '')}")
            return result
        except Exception as exc:
            print(f"[{row_number}] error -> {exc}")
            return {
                "username": "",
                "status": "failed",
                "message": str(exc),
                "profile_path": "",
                "row_number": row_number,
                "account_data": parsed or {},
            }

    with ThreadPoolExecutor(max_workers=max(1, int(concurrency or 1))) as executor:
        futures = [
            executor.submit(worker, base_row_number + idx, row)
            for idx, row in enumerate(data_rows)
        ]
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda item: item.get("row_number") or 0)
    _write_results_file(results)
    return results


__all__ = [
    "build_proxy",
    "code_provider_prompt",
    "login_and_persist",
    "login_and_persist_async",
    "login_account_playwright",
    "login_account_playwright_async",
    "onboard_accounts_from_csv",
    "parse_accounts_csv",
    "confirm_inbox_logged_in",
    "write_onboarding_results",
]
