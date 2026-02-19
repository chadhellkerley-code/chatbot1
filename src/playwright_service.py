from __future__ import annotations

import os
import sys
from pathlib import Path
from paths import runtime_base
from typing import Optional, Tuple, Union

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

# Carpeta donde se guardan las sesiones persistentes (cookies, localStorage, etc.)
# IMPORTANTE: cuando la app se ejecuta desde un ejecutable o script empaquetado,
# el directorio de trabajo puede cambiar. Resolvemos las rutas de perfiles
# relativamente a un directorio base configurable (APP_DATA_ROOT) o, en su defecto,
# al directorio del proyecto para garantizar la persistencia.
_BASE_ROOT = runtime_base(Path(__file__).resolve().parent.parent)
if not os.getenv("PLAYWRIGHT_BROWSERS_PATH") and not os.getenv("PLAYWRIGHT_CHROME_EXECUTABLE"):
    local_browsers = _BASE_ROOT / "ms-playwright"
    if local_browsers.exists():
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(local_browsers)
_profiles_env = os.getenv("PROFILES_DIR")
if _profiles_env:
    _profiles_path = Path(_profiles_env).expanduser()
    BASE_PROFILES = _profiles_path if _profiles_path.is_absolute() else (_BASE_ROOT / _profiles_path)
else:
    BASE_PROFILES = _BASE_ROOT / "profiles"
DEFAULT_VIEWPORT = {"width": 1920, "height": 1080}
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/119 Safari/537.36"
)
DEFAULT_LOCALE = "es-ES"
DEFAULT_TIMEZONE = os.getenv("HUMAN_TZ", "America/New_York")
DEFAULT_ARGS = [
    "--no-sandbox",
    "--lang=en-US",
]
_PLAYWRIGHT_CHROMIUM_PREFIX = "chromium-"
_PLAYWRIGHT_HEADLESS_PREFIX = "chromium_headless_shell-"
_PLAYWRIGHT_EXECUTABLE_ENV_KEYS = (
    "PLAYWRIGHT_CHROME_EXECUTABLE",
    "PLAYWRIGHT_EXECUTABLE_PATH",
    "CHROME_EXECUTABLE",
)
_MIN_EXECUTABLE_BYTES = 1 * 1024 * 1024


def _parse_revision(name: str, prefix: str) -> int:
    if not name.startswith(prefix):
        return -1
    suffix = name[len(prefix) :]
    digits = "".join(ch for ch in suffix if ch.isdigit())
    return int(digits) if digits else -1


def _pick_latest_dir(root: Path, prefix: str) -> Optional[Path]:
    try:
        candidates = []
        for item in root.iterdir():
            if item.is_dir() and item.name.startswith(prefix):
                candidates.append((_parse_revision(item.name, prefix), item))
    except Exception:
        return None
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _chromium_exe_candidates(browser_dir: Path) -> list[Path]:
    if sys.platform.startswith("win"):
        return [
            browser_dir / "chrome-win64" / "chrome.exe",
            browser_dir / "chrome-win" / "chrome.exe",
        ]
    if sys.platform == "darwin":
        return [
            browser_dir / "chrome-mac" / "Chromium.app" / "Contents" / "MacOS" / "Chromium"
        ]
    return [browser_dir / "chrome-linux" / "chrome"]


def _headless_exe_candidates(browser_dir: Path) -> list[Path]:
    if sys.platform.startswith("win"):
        return [
            browser_dir / "chrome-headless-shell-win64" / "chrome-headless-shell.exe",
            browser_dir / "chrome-headless-shell-win32" / "chrome-headless-shell.exe",
            browser_dir / "chrome-headless-shell" / "chrome-headless-shell.exe",
            browser_dir / "headless_shell" / "headless_shell.exe",
        ]
    if sys.platform == "darwin":
        return [
            browser_dir
            / "chrome-headless-shell"
            / "Chromium.app"
            / "Contents"
            / "MacOS"
            / "Chromium"
        ]
    return [browser_dir / "chrome-headless-shell" / "chrome-headless-shell"]


def _standalone_chrome_candidates(root: Path) -> list[Path]:
    if sys.platform.startswith("win"):
        return [
            root / "chrome-win64" / "chrome.exe",
            root / "chrome-win" / "chrome.exe",
            root / "browsers" / "chrome-win64" / "chrome.exe",
            root / "browsers" / "chrome-win" / "chrome.exe",
        ]
    if sys.platform == "darwin":
        return [
            root / "chrome-mac" / "Chromium.app" / "Contents" / "MacOS" / "Chromium",
            root / "browsers" / "chrome-mac" / "Chromium.app" / "Contents" / "MacOS" / "Chromium",
        ]
    return [
        root / "chrome-linux" / "chrome",
        root / "browsers" / "chrome-linux" / "chrome",
    ]


def _is_valid_executable(path: Path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > _MIN_EXECUTABLE_BYTES
    except Exception:
        return False


def _resolve_executable_from_env() -> Optional[Path]:
    for key in _PLAYWRIGHT_EXECUTABLE_ENV_KEYS:
        value = (os.environ.get(key) or "").strip()
        if not value:
            continue
        candidate = Path(value).expanduser()
        if _is_valid_executable(candidate):
            return candidate
    return None


def _select_standalone_executable(root: Path) -> Optional[Path]:
    for exe_path in _standalone_chrome_candidates(root):
        if _is_valid_executable(exe_path):
            return exe_path
    return None


def _select_executable(root: Path, *, headless: bool) -> Optional[Path]:
    if not root.exists():
        return None

    prefixes = [_PLAYWRIGHT_HEADLESS_PREFIX, _PLAYWRIGHT_CHROMIUM_PREFIX] if headless else [
        _PLAYWRIGHT_CHROMIUM_PREFIX
    ]
    for prefix in prefixes:
        browser_dir = _pick_latest_dir(root, prefix)
        if not browser_dir:
            continue
        candidates = (
            _headless_exe_candidates(browser_dir)
            if prefix == _PLAYWRIGHT_HEADLESS_PREFIX
            else _chromium_exe_candidates(browser_dir)
        )
        for exe_path in candidates:
            if exe_path.exists():
                return exe_path
    return None


def resolve_playwright_executable(headless: bool) -> Optional[Path]:
    explicit = _resolve_executable_from_env()
    if explicit:
        return explicit

    env_root = (os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").strip()
    playwright_roots = []
    standalone_roots = []
    if env_root:
        env_path = Path(env_root).expanduser()
        playwright_roots.append(env_path)
        standalone_roots.extend([env_path, env_path.parent])
    standalone_roots.append(_BASE_ROOT)
    playwright_roots.append(_BASE_ROOT / "ms-playwright")

    seen: set[str] = set()
    for root in standalone_roots:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        executable = _select_standalone_executable(root)
        if executable:
            return executable

    seen.clear()
    for root in playwright_roots:
        for candidate in (root, root / "ms-playwright"):
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            executable = _select_executable(candidate, headless=headless)
            if executable:
                return executable
    return None


class AsyncBrowserHandle:
    """Wrapper que ofrece close() sobre el runtime async de Playwright."""

    def __init__(self, runtime):
        self._runtime = runtime

    async def close(self) -> None:
        await self._runtime.stop()


class PlaywrightService:
    """
    Servicio para administrar un navegador Chromium compartido y
    crear contextos aislados por cuenta con storage_state persistente.
    """

    def __init__(
        self,
        headless: bool = False,
        base_profiles: Optional[Path] = None,
    ) -> None:
        self._headless = headless
        self._base_profiles = Path(base_profiles or BASE_PROFILES)
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._launch_proxy: Optional[dict] = None

    @property
    def playwright(self) -> Optional[Playwright]:
        return self._playwright

    @staticmethod
    def _launch_proxy_payload(proxy: Optional[dict]) -> Optional[dict]:
        if not proxy or not isinstance(proxy, dict):
            return None
        server = (
            str(proxy.get("server") or proxy.get("url") or proxy.get("proxy") or "")
            .strip()
        )
        if not server:
            return None
        payload: dict = {"server": server}
        username = str(proxy.get("username") or "").strip()
        password = str(proxy.get("password") or "").strip()
        if username:
            payload["username"] = username
        if password:
            payload["password"] = password
        return payload

    async def start(self, launch_proxy: Optional[dict] = None) -> "PlaywrightService":
        if self._playwright is not None:
            return self

        self._base_profiles.mkdir(parents=True, exist_ok=True)
        self._playwright = await async_playwright().start()
        launch_kwargs = {
            "headless": self._headless,
            "slow_mo": 120,
            "args": DEFAULT_ARGS,
        }
        proxy_payload = self._launch_proxy_payload(launch_proxy)
        self._launch_proxy = proxy_payload
        if proxy_payload:
            launch_kwargs["proxy"] = proxy_payload
        executable = resolve_playwright_executable(headless=self._headless)
        if executable:
            launch_kwargs["executable_path"] = str(executable)
        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        return self

    async def new_context_for_account(
        self,
        profile_dir: Union[str, Path],
        storage_state: Optional[Union[str, Path]] = None,
        proxy: Optional[dict] = None,
    ) -> BrowserContext:
        if self._browser is None:
            raise RuntimeError("PlaywrightService no inicializado. Llama a start() primero.")

        profile_path = Path(profile_dir)
        profile_path.mkdir(parents=True, exist_ok=True)

        storage_state_path: Optional[str] = None
        if storage_state:
            storage_state_path = str(storage_state)

        # If browser already has a global proxy, do not override at context level.
        context_proxy = None if self._launch_proxy else self._launch_proxy_payload(proxy)

        ctx = await self._browser.new_context(
            storage_state=storage_state_path,
            proxy=context_proxy,
            viewport=DEFAULT_VIEWPORT,
            user_agent=DEFAULT_USER_AGENT,
            locale=DEFAULT_LOCALE,
            timezone_id=DEFAULT_TIMEZONE,
            permissions=[],
            accept_downloads=False,
        )
        ctx.set_default_timeout(30_000)
        return ctx

    async def save_storage_state(
        self,
        ctx: BrowserContext,
        destination: Union[str, Path],
    ) -> Path:
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
        await ctx.storage_state(path=str(dest))
        return dest

    async def close(self) -> None:
        browser, playwright = self._browser, self._playwright
        self._browser = None
        self._playwright = None
        try:
            if browser is not None:
                await browser.close()
        finally:
            if playwright is not None:
                await playwright.stop()


async def launch_persistent(
    account_id: str,
    proxy: Optional[dict] = None,
    headful: Optional[bool] = None,
) -> Tuple[Playwright, BrowserContext]:
    """
    Lanza un contexto de navegador PERSISTENTE por cuenta (API legado).
    - account_id: normalmente el username de IG.
    - proxy: dict opcional: {"server": "http://ip:port", "username": "...", "password": "..."}
    - headful: si None, usa env HUMAN_HEADFUL (default true).
    """
    BASE_PROFILES.mkdir(exist_ok=True)
    user_data_dir = BASE_PROFILES / account_id
    user_data_dir.mkdir(parents=True, exist_ok=True)

    if headful is None:
        headful = os.getenv("HUMAN_HEADFUL", "true").lower() == "true"

    pw = await async_playwright().start()
    executable = resolve_playwright_executable(headless=not headful)
    ctx = await pw.chromium.launch_persistent_context(
        user_data_dir=str(user_data_dir),
        headless=not headful,
        executable_path=str(executable) if executable else None,
        proxy=proxy or None,
        viewport=DEFAULT_VIEWPORT,
        user_agent=DEFAULT_USER_AGENT,
        locale=DEFAULT_LOCALE,
        timezone_id=DEFAULT_TIMEZONE,
        permissions=[],
        slow_mo=120,
        args=DEFAULT_ARGS,
    )
    return pw, ctx


async def get_page(ctx: BrowserContext) -> Page:
    """Devuelve la primera página abierta o crea una nueva."""
    return ctx.pages[0] if ctx.pages else await ctx.new_page()


async def ensure_context(
    *,
    account: str,
    headful: bool = True,
    lang: Optional[str] = None,
    proxy: Optional[dict] = None,
) -> Tuple[AsyncBrowserHandle, BrowserContext, Page]:
    """
    Crea (o reutiliza) un perfil persistente para la cuenta y devuelve browser/context/page async.
    """
    runtime = await async_playwright().start()
    profile_dir = BASE_PROFILES / account
    profile_dir.mkdir(parents=True, exist_ok=True)

    locale = (lang or DEFAULT_LOCALE) or "en-US"
    args = [arg for arg in DEFAULT_ARGS if not arg.startswith("--lang=")]
    args.append(f"--lang={locale}")

    executable = resolve_playwright_executable(headless=not headful)
    context: BrowserContext = await runtime.chromium.launch_persistent_context(
        user_data_dir=str(profile_dir),
        headless=not headful,
        executable_path=str(executable) if executable else None,
        proxy=proxy,
        viewport=DEFAULT_VIEWPORT,
        user_agent=DEFAULT_USER_AGENT,
        locale=locale,
        timezone_id=DEFAULT_TIMEZONE,
        args=args,
    )
    context.set_default_timeout(30_000)
    page: Page = context.pages[0] if context.pages else await context.new_page()
    return AsyncBrowserHandle(runtime), context, page


async def shutdown(pw_or_service: Union[Playwright, PlaywrightService], ctx: Optional[BrowserContext]):
    """
    Cierra el contexto y el runtime de Playwright con seguridad.
    Acepta tanto la API vieja (Playwright) como la nueva (PlaywrightService).
    """
    if ctx is not None:
        try:
            await ctx.close()
        except Exception:
            pass

    if isinstance(pw_or_service, PlaywrightService):
        await pw_or_service.close()
    else:
        await pw_or_service.stop()
