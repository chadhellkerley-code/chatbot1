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

from src.auth.persistent_login import ChallengeRequired, ensure_logged_in_async
from src.instagram_adapter import is_logged_in
from src.playwright_service import BASE_PROFILES, PlaywrightService, shutdown

logger = logging.getLogger(__name__)

_DEFAULT_PROFILE_ROOT = Path(BASE_PROFILES)
RESULTS_PATH = Path("data") / "onboarding_results.csv"

ProxyPayload = Optional[Dict[str, str]]
AccountPayload = Dict[str, Any]
OnboardingResult = Dict[str, Any]

ENCODINGS = ["utf-8-sig", "utf-8", "latin-1"]
DELIMS = [",", ";", "\t"]
HEADER_ALIASES: Dict[str, List[str]] = {
    "username": ["username", "user", "login"],
    "password": ["password", "pass"],
    "totp_secret": ["totp_secret", "totp", "2fa", "otp"],
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
        return asyncio.run(coro)
    raise RuntimeError("login_and_persist requiere contexto sync; usa login_and_persist_async.")


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
            "totp_secret": mapped.get("totp_secret", ""),
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
    totp = get_by_alias(["totp_secret", "totp", "2fa", "otp"])
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
    - totp_secret opcional
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


def code_provider_prompt(label: str = "Ingresa el cÃ³digo recibido (WhatsApp/SMS/email): ") -> str:
    """
    Solicita un cÃ³digo de verificaciÃ³n sin mostrarlo en consola.
    """
    try:
        return getpass.getpass(label).strip()
    except (EOFError, KeyboardInterrupt):
        return ""


def _profile_path_for(username: str, profile_root: Union[str, Path]) -> Path:
    root = Path(profile_root or _DEFAULT_PROFILE_ROOT)
    return root / username / "storage_state.json"


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

    proxy_payload = account.get("proxy")
    if not proxy_payload:
        proxy_payload = build_proxy(account.get("proxy_url"))

    totp_secret = (account.get("totp_secret") or "").replace(" ", "")
    totp_callback = account.get("totp_callback")
    if totp_secret and not totp_callback:
        try:
            import pyotp
        except Exception:  # pragma: no cover - pyotp opcional
            pyotp = None  # type: ignore
        if pyotp:
            def _totp(_: str, secret: str = totp_secret) -> str:
                return pyotp.TOTP(secret).now()
            totp_callback = _totp
            try:
                current_totp = pyotp.TOTP(totp_secret).now()
                logger.info("TOTP generado para @%s (cambia cada 30s): %s", username, current_totp)
            except Exception:
                pass

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
    if totp_secret:
        payload["totp_secret"] = totp_secret
    if totp_callback:
        payload["totp_callback"] = totp_callback

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


def onboard_accounts_from_csv(
    csv_path: Union[str, Path],
    *,
    headless: bool = True,
    concurrency: int = 2,
) -> List[OnboardingResult]:
    """
    Procesa un CSV de cuentas (username,password,totp_secret,proxy_...) y
    ejecuta login headless por fila. Guarda los resultados en data/onboarding_results.csv.
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
                account_payload = _make_account(username, password, totp_secret, proxy_fields or {})
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
    "onboard_accounts_from_csv",
]
