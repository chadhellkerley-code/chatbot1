from __future__ import annotations

import os
from pathlib import Path
from paths import runtime_base
from typing import Optional, Tuple, Union

from playwright.async_api import BrowserContext as AsyncBrowserContext, Page as AsyncPage, async_playwright
from playwright.sync_api import Browser, BrowserContext, Playwright, sync_playwright

# Carpeta donde se guardan las sesiones persistentes (cookies, localStorage, etc.)
# IMPORTANTE: cuando la app se ejecuta desde un ejecutable o script empaquetado,
# el directorio de trabajo puede cambiar. Resolvemos las rutas de perfiles
# relativamente a un directorio base configurable (APP_DATA_ROOT) o, en su defecto,
# al directorio del proyecto para garantizar la persistencia.
_BASE_ROOT = runtime_base(Path(__file__).resolve().parent.parent)
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

    @property
    def playwright(self) -> Optional[Playwright]:
        return self._playwright

    def start(self) -> "PlaywrightService":
        if self._playwright is not None:
            return self

        self._base_profiles.mkdir(parents=True, exist_ok=True)
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self._headless,
            slow_mo=120,
            args=DEFAULT_ARGS,
        )
        return self

    def new_context_for_account(
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

        ctx = self._browser.new_context(
            storage_state=storage_state_path,
            proxy=proxy or None,
            viewport=DEFAULT_VIEWPORT,
            user_agent=DEFAULT_USER_AGENT,
            locale=DEFAULT_LOCALE,
            timezone_id=DEFAULT_TIMEZONE,
            permissions=[],
            accept_downloads=False,
        )
        ctx.set_default_timeout(30_000)
        return ctx

    def save_storage_state(
        self,
        ctx: BrowserContext,
        destination: Union[str, Path],
    ) -> Path:
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
        ctx.storage_state(path=str(dest))
        return dest

    def close(self) -> None:
        browser, playwright = self._browser, self._playwright
        self._browser = None
        self._playwright = None
        try:
            if browser is not None:
                browser.close()
        finally:
            if playwright is not None:
                playwright.stop()


def launch_persistent(
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

    pw = sync_playwright().start()
    ctx = pw.chromium.launch_persistent_context(
        user_data_dir=str(user_data_dir),
        headless=not headful,
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


def get_page(ctx: BrowserContext):
    """Devuelve la primera página abierta o crea una nueva."""
    return ctx.pages[0] if ctx.pages else ctx.new_page()


async def ensure_context(
    *,
    account: str,
    headful: bool = True,
    lang: Optional[str] = None,
    proxy: Optional[dict] = None,
) -> Tuple[AsyncBrowserHandle, AsyncBrowserContext, AsyncPage]:
    """
    Crea (o reutiliza) un perfil persistente para la cuenta y devuelve browser/context/page async.
    """
    runtime = await async_playwright().start()
    profile_dir = BASE_PROFILES / account
    profile_dir.mkdir(parents=True, exist_ok=True)

    locale = (lang or DEFAULT_LOCALE) or "en-US"
    args = [arg for arg in DEFAULT_ARGS if not arg.startswith("--lang=")]
    args.append(f"--lang={locale}")

    context: AsyncBrowserContext = await runtime.chromium.launch_persistent_context(
        user_data_dir=str(profile_dir),
        headless=not headful,
        proxy=proxy,
        viewport=DEFAULT_VIEWPORT,
        user_agent=DEFAULT_USER_AGENT,
        locale=locale,
        timezone_id=DEFAULT_TIMEZONE,
        args=args,
    )
    context.set_default_timeout(30_000)
    page: AsyncPage = context.pages[0] if context.pages else await context.new_page()
    return AsyncBrowserHandle(runtime), context, page


def shutdown(pw_or_service: Union[Playwright, PlaywrightService], ctx: Optional[BrowserContext]):
    """
    Cierra el contexto y el runtime de Playwright con seguridad.
    Acepta tanto la API vieja (Playwright) como la nueva (PlaywrightService).
    """
    if ctx is not None:
        try:
            ctx.close()
        except Exception:
            pass

    if isinstance(pw_or_service, PlaywrightService):
        pw_or_service.close()
    else:
        pw_or_service.stop()
