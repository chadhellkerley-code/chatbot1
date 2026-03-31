from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import threading
import time
import traceback
import asyncio
import contextlib
import inspect
import uuid
import weakref
from concurrent.futures import CancelledError as FutureCancelledError
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Union

from playwright.async_api import Browser, BrowserContext, Playwright, async_playwright
from playwright.sync_api import sync_playwright

from core.log_rotation import rotate_daily_file
from core.storage_atomic import atomic_write_json
from paths import runtime_root, storage_root
from src.browser_profile_lifecycle import (
    mark_profile_closed_cleanly,
    mark_profile_closing,
    mark_profile_open,
    mark_profile_unclean_shutdown,
)
from src.browser_profile_paths import browser_profile_owner_key, canonical_browser_profile_path
from src.runtime.playwright_resolver import (
    resolve_bundled_google_chrome_executable,
    resolve_google_chrome_executable,
    resolve_playwright_chromium_executable,
)
from src.stealth.stealth_core import patch_context

PLAYWRIGHT_BASE_FLAGS = [
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-dev-shm-usage",
    "--disable-non-proxied-udp",
    "--force-webrtc-ip-handling-policy=default_public_interface_only",
    "--disable-blink-features=AutomationControlled",
    "--exclude-switches=enable-automation",
    "--disable-infobars",
    "--no-first-run",
    "--no-default-browser-check",
    "--test-type",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
    "--disable-features=RendererCodeIntegrity",
]

PLAYWRIGHT_IGNORE_DEFAULT_ARGS = [
    "--enable-automation",
    "--no-sandbox",
]

PLAYWRIGHT_SAFE_MODE_ARGS = PLAYWRIGHT_BASE_FLAGS + [
    "--enable-logging=stderr",
    "--v=1",
]

PLAYWRIGHT_BROWSER_MODE_DEFAULT = "default"
PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY = "chrome_only"
PLAYWRIGHT_BROWSER_MODE_MANAGED = "managed"


def normalize_browser_mode(browser_mode: str | None) -> str:
    normalized = str(browser_mode or "").strip().lower()
    if normalized in {"", PLAYWRIGHT_BROWSER_MODE_DEFAULT}:
        return PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY
    if normalized in {
        PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
        PLAYWRIGHT_BROWSER_MODE_MANAGED,
    }:
        return normalized
    return PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY

_DRIVER_CRASH_TOKENS = (
    "connection closed while reading from the driver",
    "target page, context or browser has been closed",
    "target closed",
    "browser has been closed",
    "protocol error",
    "econnreset",
    "driver closed",
    "socket.send() raised exception",
)

_BROWSER_LAYER_FALLBACK_TOKENS = (
    "connection closed",
    "target closed",
    "protocol error",
    "browsertype.launch",
    "browser has been closed",
    "socket.send() raised exception",
    "chromium distribution 'chrome' is not found",
    "failed to launch browser process",
)

_LOOP_THREAD_LOCK = threading.RLock()
_LOOP_THREAD: Optional[threading.Thread] = None
_LOOP: Optional[asyncio.AbstractEventLoop] = None
_LOOP_READY = threading.Event()

_SYNC_PLAYWRIGHT_LOCK = threading.RLock()
_SYNC_PLAYWRIGHT = None
_RUNTIME_OWNER_LOCK = threading.RLock()
_ASYNC_RUNTIME_INSTANCES: dict[str, weakref.ReferenceType["PlaywrightRuntime"]] = {}
_PERSISTENT_PROFILE_OWNERSHIP_LOCK = threading.RLock()


@dataclass
class RuntimeOwnerState:
    runtime_id: str
    owner_module: str
    active_contexts: int = 0


_ASYNC_RUNTIME_OWNERS: dict[str, RuntimeOwnerState] = {}
_SYNC_RUNTIME_OWNERS: dict[str, RuntimeOwnerState] = {}


@dataclass
class PersistentProfileOwnerState:
    profile_key: str
    profile_dir: str
    mode: str
    runtime_id: str
    owner_module: str
    hold_count: int = 0


_PERSISTENT_PROFILE_OWNERS: dict[str, PersistentProfileOwnerState] = {}


class PlaywrightRuntimeCancelledError(RuntimeError):
    pass


class PlaywrightRuntimeTimeoutError(TimeoutError):
    pass


class PersistentProfileOwnershipError(RuntimeError):
    conflict_code = "profile_mode_conflict"
    handoff_code = "profile_handoff_required"

    def __init__(
        self,
        *,
        profile_dir: Union[str, Path],
        requested_mode: str,
        active_mode: str,
        runtime_id: str,
        active_runtime_id: str,
        owner_module: str = "",
    ) -> None:
        self.profile_dir = str(canonical_browser_profile_path(profile_dir))
        self.requested_mode = str(requested_mode or "").strip().lower() or "unknown"
        self.active_mode = str(active_mode or "").strip().lower() or "unknown"
        self.runtime_id = str(runtime_id or "").strip()
        self.active_runtime_id = str(active_runtime_id or "").strip()
        self.owner_module = str(owner_module or "").strip()
        self.reason_code = (
            "profile_in_use_by_headful" if self.active_mode == "headful" else "profile_in_use_by_headless"
        )
        payload = self.to_payload()
        super().__init__(f"{self.conflict_code}: {json.dumps(payload, ensure_ascii=False)}")

    def to_payload(self) -> dict[str, str]:
        return {
            "reason_code": self.reason_code,
            "handoff_code": self.handoff_code,
            "profile_dir": self.profile_dir,
            "requested_mode": self.requested_mode,
            "active_mode": self.active_mode,
            "runtime_id": self.runtime_id,
            "active_runtime_id": self.active_runtime_id,
            "owner_module": self.owner_module,
        }


def _profile_mode_label(headless: bool) -> str:
    return "headless" if bool(headless) else "headful"


def _claim_persistent_profile_ownership(
    *,
    profile_dir: Union[str, Path],
    headless: bool,
    runtime_id: str,
    owner_module: str,
) -> Callable[[], None]:
    profile_path = canonical_browser_profile_path(profile_dir)
    profile_key = browser_profile_owner_key(profile_path)
    requested_mode = _profile_mode_label(headless)
    runtime_id_value = str(runtime_id or "").strip()
    owner_module_value = str(owner_module or "").strip()
    with _PERSISTENT_PROFILE_OWNERSHIP_LOCK:
        current = _PERSISTENT_PROFILE_OWNERS.get(profile_key)
        if current is not None:
            raise PersistentProfileOwnershipError(
                profile_dir=profile_path,
                requested_mode=requested_mode,
                active_mode=current.mode,
                runtime_id=runtime_id_value,
                active_runtime_id=current.runtime_id,
                owner_module=current.owner_module,
            )
        current = PersistentProfileOwnerState(
            profile_key=profile_key,
            profile_dir=str(profile_path),
            mode=requested_mode,
            runtime_id=runtime_id_value,
            owner_module=owner_module_value,
            hold_count=1,
        )
        _PERSISTENT_PROFILE_OWNERS[profile_key] = current

    released = False

    def _release() -> None:
        nonlocal released
        if released:
            return
        released = True
        with _PERSISTENT_PROFILE_OWNERSHIP_LOCK:
            current = _PERSISTENT_PROFILE_OWNERS.get(profile_key)
            if current is None:
                return
            current.hold_count = max(0, int(current.hold_count) - 1)
            if current.hold_count <= 0:
                _PERSISTENT_PROFILE_OWNERS.pop(profile_key, None)

    return _release


def _persistent_profile_hold_count(profile_dir: Union[str, Path]) -> int:
    profile_key = browser_profile_owner_key(profile_dir)
    with _PERSISTENT_PROFILE_OWNERSHIP_LOCK:
        current = _PERSISTENT_PROFILE_OWNERS.get(profile_key)
        return int(current.hold_count if current is not None else 0)


def _env_enabled(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "y", "on", "si"}


def _loop_worker() -> None:
    global _LOOP
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    with _LOOP_THREAD_LOCK:
        _LOOP = loop
        _LOOP_READY.set()
    try:
        loop.run_forever()
    finally:
        try:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        try:
            loop.close()
        except Exception:
            pass
        with _LOOP_THREAD_LOCK:
            _LOOP = None
            _LOOP_READY.clear()


def _ensure_runtime_loop() -> asyncio.AbstractEventLoop:
    global _LOOP_THREAD
    with _LOOP_THREAD_LOCK:
        if _LOOP is not None and _LOOP_THREAD is not None and _LOOP_THREAD.is_alive():
            return _LOOP
        _LOOP_READY.clear()
        _LOOP_THREAD = threading.Thread(
            target=_loop_worker,
            name="playwright-runtime-loop",
            daemon=True,
        )
        _LOOP_THREAD.start()
    _LOOP_READY.wait(timeout=10.0)
    with _LOOP_THREAD_LOCK:
        if _LOOP is None:
            raise RuntimeError("No se pudo inicializar el loop runtime de Playwright.")
        return _LOOP


def run_coroutine_sync(
    coro: Any,
    *,
    timeout: Optional[float] = None,
    poll_interval: float = 0.10,
    cancel_reason: str = "",
    on_cancel: Callable[[], None] | None = None,
    on_poll: Callable[[], None] | None = None,
    ignore_stop: bool = False,
) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("run_coroutine_sync requiere contexto sync; no usar dentro de un loop activo.")
    active_stop_token = None
    try:
        from runtime.runtime import STOP_EVENT

        active_stop_token = STOP_EVENT.current_token()
    except Exception:
        active_stop_token = None

    wrapped_coro = coro
    if active_stop_token is not None:
        from runtime.runtime import bind_stop_token, restore_stop_token

        async def _bound_coro() -> Any:
            previous = bind_stop_token(active_stop_token)
            try:
                return await coro
            finally:
                restore_stop_token(previous)

        wrapped_coro = _bound_coro()

    loop = _ensure_runtime_loop()
    future = asyncio.run_coroutine_threadsafe(wrapped_coro, loop)
    normalized_timeout = None if timeout is None else max(0.0, float(timeout))
    interval = max(0.02, float(poll_interval or 0.10))
    deadline = None if normalized_timeout is None else time.monotonic() + normalized_timeout

    def _invoke_on_cancel() -> None:
        if not callable(on_cancel):
            return
        try:
            on_cancel()
        except Exception:
            pass

    def _invoke_on_poll() -> None:
        if not callable(on_poll):
            return
        try:
            on_poll()
        except Exception:
            pass

    while True:
        _invoke_on_poll()
        if not ignore_stop:
            try:
                from runtime.runtime import STOP_EVENT

                if STOP_EVENT.is_set():
                    future.cancel()
                    _invoke_on_cancel()
                    raise PlaywrightRuntimeCancelledError(
                        cancel_reason or "playwright_operation_cancelled"
                    )
            except PlaywrightRuntimeCancelledError:
                raise
            except Exception:
                pass

        if deadline is not None and time.monotonic() >= deadline:
            future.cancel()
            _invoke_on_cancel()
            raise PlaywrightRuntimeTimeoutError(
                cancel_reason or "playwright_operation_timeout"
            )

        wait_timeout = interval
        if deadline is not None:
            remaining = max(0.0, deadline - time.monotonic())
            wait_timeout = max(0.02, min(interval, remaining))
        try:
            return future.result(timeout=wait_timeout)
        except FutureTimeoutError:
            continue
        except FutureCancelledError as exc:
            raise PlaywrightRuntimeCancelledError(
                cancel_reason or "playwright_operation_cancelled"
            ) from exc


def start_sync_playwright() -> Any:
    global _SYNC_PLAYWRIGHT
    with _SYNC_PLAYWRIGHT_LOCK:
        if _SYNC_PLAYWRIGHT is None:
            _SYNC_PLAYWRIGHT = sync_playwright().start()
        return _SYNC_PLAYWRIGHT


@contextlib.contextmanager
def sync_playwright_context() -> Any:
    yield start_sync_playwright()


def stop_sync_playwright() -> None:
    global _SYNC_PLAYWRIGHT
    with _SYNC_PLAYWRIGHT_LOCK:
        pw = _SYNC_PLAYWRIGHT
        _SYNC_PLAYWRIGHT = None
    if pw is not None:
        try:
            pw.stop()
        except Exception:
            pass


def _detect_owner_module(explicit_owner: str = "") -> str:
    owner = str(explicit_owner or "").strip()
    if owner:
        return owner
    try:
        frame = inspect.currentframe()
        while frame is not None:
            module_name = frame.f_globals.get("__name__", "")
            if module_name and module_name != __name__:
                return str(module_name)
            frame = frame.f_back
    except Exception:
        pass
    return __name__


def _register_owner(
    owners: dict[str, RuntimeOwnerState],
    *,
    runtime_id: str,
    owner_module: str,
) -> RuntimeOwnerState:
    with _RUNTIME_OWNER_LOCK:
        current = owners.get(runtime_id)
        if current is None:
            current = RuntimeOwnerState(runtime_id=runtime_id, owner_module=owner_module, active_contexts=0)
            owners[runtime_id] = current
        elif owner_module:
            current.owner_module = owner_module
        return current


def _owner_snapshot(
    owners: dict[str, RuntimeOwnerState],
    runtime_id: str,
) -> RuntimeOwnerState:
    with _RUNTIME_OWNER_LOCK:
        current = owners.get(runtime_id)
        if current is None:
            return RuntimeOwnerState(runtime_id=runtime_id, owner_module="", active_contexts=0)
        return RuntimeOwnerState(
            runtime_id=current.runtime_id,
            owner_module=current.owner_module,
            active_contexts=int(current.active_contexts),
        )


def _update_owner_active_contexts(
    owners: dict[str, RuntimeOwnerState],
    runtime_id: str,
    delta: int,
) -> RuntimeOwnerState:
    with _RUNTIME_OWNER_LOCK:
        current = owners.get(runtime_id)
        if current is None:
            current = RuntimeOwnerState(runtime_id=runtime_id, owner_module="", active_contexts=0)
            owners[runtime_id] = current
        current.active_contexts = max(0, int(current.active_contexts) + int(delta))
        return RuntimeOwnerState(
            runtime_id=current.runtime_id,
            owner_module=current.owner_module,
            active_contexts=int(current.active_contexts),
        )


def _total_active_contexts(owners: dict[str, RuntimeOwnerState]) -> int:
    with _RUNTIME_OWNER_LOCK:
        return sum(max(0, int(item.active_contexts)) for item in owners.values())


def _register_async_runtime_instance(runtime: "PlaywrightRuntime") -> None:
    with _RUNTIME_OWNER_LOCK:
        _ASYNC_RUNTIME_INSTANCES[runtime.runtime_id] = weakref.ref(runtime)


def _resolve_async_runtime_instance(runtime_id: str) -> Optional["PlaywrightRuntime"]:
    with _RUNTIME_OWNER_LOCK:
        ref = _ASYNC_RUNTIME_INSTANCES.get(str(runtime_id or "").strip())
    if ref is None:
        return None
    instance = ref()
    if instance is None:
        with _RUNTIME_OWNER_LOCK:
            _ASYNC_RUNTIME_INSTANCES.pop(str(runtime_id or "").strip(), None)
    return instance


def register_sync_runtime_owner(*, owner_module: str = "") -> RuntimeOwnerState:
    runtime_id = uuid.uuid4().hex
    owner = _detect_owner_module(owner_module)
    return _register_owner(_SYNC_RUNTIME_OWNERS, runtime_id=runtime_id, owner_module=owner)


def get_sync_runtime_owner(runtime_id: str) -> RuntimeOwnerState:
    return _owner_snapshot(_SYNC_RUNTIME_OWNERS, runtime_id)


def mark_sync_runtime_context_open(runtime_id: str) -> RuntimeOwnerState:
    return _update_owner_active_contexts(_SYNC_RUNTIME_OWNERS, runtime_id, 1)


def mark_sync_runtime_context_closed(runtime_id: str) -> RuntimeOwnerState:
    return _update_owner_active_contexts(_SYNC_RUNTIME_OWNERS, runtime_id, -1)


def safe_runtime_stop(*, runtime_id: str = "", playwright: Any = None) -> bool:
    if runtime_id:
        _register_owner(
            _SYNC_RUNTIME_OWNERS,
            runtime_id=runtime_id,
            owner_module=get_sync_runtime_owner(runtime_id).owner_module,
        )
    if _total_active_contexts(_SYNC_RUNTIME_OWNERS) > 0:
        return False
    if playwright is not None and _SYNC_PLAYWRIGHT is None:
        try:
            playwright.stop()
        except Exception:
            pass
        return True
    stop_sync_playwright()
    return True


async def safe_restart_runtime(runtime_id: str, *, reason: str = "") -> bool:
    normalized_runtime_id = str(runtime_id or "").strip()
    if not normalized_runtime_id:
        return False
    owner = _owner_snapshot(_ASYNC_RUNTIME_OWNERS, normalized_runtime_id)
    runtime = _resolve_async_runtime_instance(normalized_runtime_id)
    if runtime is None:
        return False
    runtime._append_debug(
        f"[safe_restart_runtime] runtime_id={normalized_runtime_id} reason={reason or '-'} "
        f"owner={owner.owner_module or '-'} active_contexts={owner.active_contexts}"
    )
    if owner.active_contexts > 0:
        runtime._append_debug(
            f"[safe_restart_runtime] denied runtime_id={normalized_runtime_id} "
            f"owner={owner.owner_module or '-'} active_contexts={owner.active_contexts}"
        )
        return False
    stopped = await runtime.safe_shutdown_if_unused(
        stop_shared_playwright=_total_active_contexts(_ASYNC_RUNTIME_OWNERS) == 0
    )
    runtime._append_debug(
        f"[safe_restart_runtime] completed runtime_id={normalized_runtime_id} stopped={stopped}"
    )
    return bool(stopped)


def _merge_launch_args(*arg_sets: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for arg_set in arg_sets:
        for raw in arg_set:
            value = str(raw or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            merged.append(value)
    return merged


def _context_extra_http_headers(
    locale: Optional[str],
    headers: Optional[Mapping[str, Any]] = None,
) -> dict[str, str] | None:
    merged: dict[str, str] = {}
    if isinstance(headers, Mapping):
        for raw_key, raw_value in headers.items():
            key = str(raw_key or "").strip()
            if not key:
                continue
            if key.lower() == "accept-language":
                continue
            merged[key] = str(raw_value or "")
    locale_value = str(locale or "").strip()
    if locale_value:
        merged["Accept-Language"] = locale_value
    return merged or None


def _should_fallback_to_embedded(exc: BaseException) -> bool:
    message = str(exc or "").strip().lower()
    if not message:
        return False
    return any(token in message for token in _BROWSER_LAYER_FALLBACK_TOKENS)


def _emit_browser_layer_log(message: str, *, log_fn: Optional[Callable[[str], None]] = None) -> None:
    line = f"[Browser Layer] {message}"
    if log_fn is not None:
        try:
            log_fn(line)
        except Exception:
            pass
    try:
        print(line, flush=True)
    except Exception:
        pass


def _normalized_executable(executable_path: Optional[Union[str, Path]]) -> Optional[str]:
    if not executable_path:
        return None
    try:
        candidate = Path(str(executable_path)).expanduser()
    except Exception:
        return None
    if candidate.exists() and candidate.is_file():
        return str(candidate)
    return None


def _chrome_only_launch_candidates(
    executable_path: Optional[Union[str, Path]],
    *,
    headless: bool,
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, Optional[str]]] = [
        ("Configured browser", _normalized_executable(executable_path)),
        ("Google Chrome", _normalized_executable(resolve_google_chrome_executable())),
        ("Bundled Chrome", _normalized_executable(resolve_bundled_google_chrome_executable())),
        ("Playwright Chromium", _normalized_executable(resolve_playwright_chromium_executable(headless=headless))),
    ]
    ordered: list[tuple[str, str]] = []
    seen: set[str] = set()
    for label, candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        ordered.append((label, candidate))
    return ordered


def _managed_launch_executable(
    executable_path: Optional[Union[str, Path]],
    *,
    headless: bool,
) -> Optional[str]:
    explicit = _normalized_executable(executable_path)
    if explicit:
        return explicit
    return _normalized_executable(resolve_playwright_chromium_executable(headless=headless))


def _browser_layer_failure(
    *,
    code: str,
    headless: bool,
    visible_reason: Optional[str],
    executable_path: Optional[str],
    chrome_error: BaseException,
    embedded_error: Optional[BaseException] = None,
    compat_error: Optional[BaseException] = None,
) -> RuntimeError:
    payload = {
        "headless": bool(headless),
        "visible_reason": visible_reason or "",
        "embedded_executable": executable_path or "",
        "chrome_channel_error": str(chrome_error),
        "embedded_error": str(embedded_error) if embedded_error else "",
        "embedded_compat_error": str(compat_error) if compat_error else "",
    }
    return RuntimeError(f"{code}: {json.dumps(payload, ensure_ascii=False)}")


def _chrome_only_launch_failure(
    *,
    code: str,
    headless: bool,
    visible_reason: Optional[str],
    executable_path: Optional[str],
    launch_error: BaseException,
    attempts: Optional[list[dict[str, str]]] = None,
) -> RuntimeError:
    payload = {
        "headless": bool(headless),
        "visible_reason": visible_reason or "",
        "chrome_executable": executable_path or "",
        "launch_error": str(launch_error),
        "attempts": list(attempts or []),
    }
    return RuntimeError(f"{code}: {json.dumps(payload, ensure_ascii=False)}")


async def _launch_browser(
    playwright: Playwright,
    headless: bool,
    visible_reason: str | None = None,
    *,
    args: Optional[list[str]] = None,
    slow_mo: int = 0,
    proxy: Optional[dict[str, str]] = None,
    executable_path: Optional[Union[str, Path]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    browser_mode: str = PLAYWRIGHT_BROWSER_MODE_DEFAULT,
) -> Browser:
    normalized_args = _merge_launch_args(list(args or []), PLAYWRIGHT_BASE_FLAGS)
    common_kwargs: dict[str, Any] = {
        "headless": False,
        "slow_mo": int(max(0, slow_mo)),
        "args": normalized_args,
        "ignore_default_args": list(PLAYWRIGHT_IGNORE_DEFAULT_ARGS),
    }
    if proxy:
        common_kwargs["proxy"] = dict(proxy)

    normalized_mode = normalize_browser_mode(browser_mode)
    if normalized_mode == PLAYWRIGHT_BROWSER_MODE_MANAGED:
        managed_kwargs = dict(common_kwargs)
        managed_executable = _managed_launch_executable(executable_path, headless=headless)
        if managed_executable:
            managed_kwargs["executable_path"] = managed_executable
        browser = await playwright.chromium.launch(**managed_kwargs)
        _emit_browser_layer_log(
            f"Managed Playwright launch ok -> {managed_executable or 'default'}",
            log_fn=log_fn,
        )
        return browser
    if normalized_mode == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY:
        chrome_only_candidates = _chrome_only_launch_candidates(executable_path, headless=headless)
        if not chrome_only_candidates:
            raise RuntimeError("PW-BROWSER-CHROME-ONLY: browser_launch_candidates_missing")
        attempts: list[dict[str, str]] = []
        last_error: BaseException | None = None
        for label, candidate in chrome_only_candidates:
            chrome_kwargs = dict(common_kwargs)
            chrome_kwargs["executable_path"] = candidate
            try:
                browser = await playwright.chromium.launch(**chrome_kwargs)
                _emit_browser_layer_log(f"{label} launch ok -> {candidate}", log_fn=log_fn)
                return browser
            except Exception as chrome_exc:
                last_error = chrome_exc
                attempts.append(
                    {
                        "label": label,
                        "executable_path": candidate,
                        "launch_error": str(chrome_exc),
                    }
                )
                _emit_browser_layer_log(f"{label} launch failed -> {candidate}: {chrome_exc}", log_fn=log_fn)
        assert last_error is not None
        raise _chrome_only_launch_failure(
            code="PW-BROWSER-CHROME-ONLY-FAILED",
            headless=headless,
            visible_reason=visible_reason,
            executable_path=attempts[-1]["executable_path"],
            launch_error=last_error,
            attempts=attempts,
        ) from last_error

    try:
        browser = await playwright.chromium.launch(channel="chrome", **common_kwargs)
        _emit_browser_layer_log("Chrome channel launch ok", log_fn=log_fn)
        return browser
    except Exception as chrome_exc:
        embedded_executable = _normalized_executable(executable_path)
        if not embedded_executable:
            embedded_executable = _normalized_executable(
                resolve_playwright_chromium_executable(headless=headless)
            )
        if not (_should_fallback_to_embedded(chrome_exc) or embedded_executable):
            raise
        _emit_browser_layer_log(
            "Chrome channel failed -> fallback to bundled Playwright Chromium",
            log_fn=log_fn,
        )
        if not embedded_executable:
            raise _browser_layer_failure(
                code="PW-BROWSER-LAYER-FAILED",
                headless=headless,
                visible_reason=visible_reason,
                executable_path=None,
                chrome_error=chrome_exc,
            ) from chrome_exc

        embedded_kwargs = dict(common_kwargs)
        embedded_kwargs["executable_path"] = embedded_executable
        try:
            browser = await playwright.chromium.launch(**embedded_kwargs)
            _emit_browser_layer_log("Bundled Playwright Chromium launch ok", log_fn=log_fn)
            return browser
        except Exception as embedded_exc:
            compat_kwargs = dict(embedded_kwargs)
            compat_kwargs["args"] = _merge_launch_args(normalized_args, [])
            _emit_browser_layer_log(
                "Bundled Playwright Chromium retry with compat flags",
                log_fn=log_fn,
            )
            try:
                browser = await playwright.chromium.launch(**compat_kwargs)
                _emit_browser_layer_log(
                    "Bundled Playwright Chromium launch ok (compat)",
                    log_fn=log_fn,
                )
                return browser
            except Exception as compat_exc:
                raise _browser_layer_failure(
                    code="PW-BROWSER-LAYER-FAILED",
                    headless=headless,
                    visible_reason=visible_reason,
                    executable_path=embedded_executable,
                    chrome_error=chrome_exc,
                    embedded_error=embedded_exc,
                    compat_error=compat_exc,
                ) from compat_exc


async def launch_async_browser(
    playwright: Playwright,
    *,
    headless: bool,
    executable_path: Optional[Union[str, Path]] = None,
    proxy: Optional[dict[str, str]] = None,
    args: Optional[list[str]] = None,
    slow_mo: int = 0,
    visible_reason: str | None = None,
    log_fn: Optional[Callable[[str], None]] = None,
    browser_mode: str = PLAYWRIGHT_BROWSER_MODE_DEFAULT,
) -> Browser:
    """Public async launcher aligned with the browser-layer fallback strategy."""
    return await _launch_browser(
        playwright,
        headless=headless,
        visible_reason=visible_reason,
        args=args,
        slow_mo=slow_mo,
        proxy=proxy,
        executable_path=executable_path,
        log_fn=log_fn,
        browser_mode=browser_mode,
    )


async def _launch_persistent_context(
    playwright: Playwright,
    *,
    user_data_dir: Union[str, Path],
    username: str,
    headless: bool,
    args: Optional[list[str]] = None,
    proxy: Optional[dict[str, str]] = None,
    executable_path: Optional[Union[str, Path]] = None,
    visible_reason: str | None = None,
    log_fn: Optional[Callable[[str], None]] = None,
    browser_mode: str = PLAYWRIGHT_BROWSER_MODE_DEFAULT,
    **kwargs: Any,
) -> BrowserContext:
    persistent_dir = str(user_data_dir)
    _emit_browser_layer_log(
        f"Persistent user-data-dir -> {persistent_dir}",
        log_fn=log_fn,
    )
    normalized_args = _merge_launch_args(list(args or []), PLAYWRIGHT_BASE_FLAGS)
    common_kwargs: dict[str, Any] = {
        "user_data_dir": persistent_dir,
        "headless": False,
        "args": normalized_args,
        "ignore_default_args": list(PLAYWRIGHT_IGNORE_DEFAULT_ARGS),
    }
    if proxy:
        common_kwargs["proxy"] = dict(proxy)
    if kwargs:
        common_kwargs.update(kwargs)
    persistent_headers = _context_extra_http_headers(
        kwargs.get("locale"),
        kwargs.get("extra_http_headers"),
    )
    if persistent_headers:
        common_kwargs["extra_http_headers"] = persistent_headers

    normalized_mode = normalize_browser_mode(browser_mode)
    if normalized_mode == PLAYWRIGHT_BROWSER_MODE_MANAGED:
        managed_kwargs = dict(common_kwargs)
        managed_executable = _managed_launch_executable(executable_path, headless=headless)
        if managed_executable:
            managed_kwargs["executable_path"] = managed_executable
        context = await playwright.chromium.launch_persistent_context(**managed_kwargs)
        await patch_context(context, username)
        _emit_browser_layer_log(
            f"Managed Playwright persistent launch ok -> {managed_executable or 'default'}",
            log_fn=log_fn,
        )
        return context
    if normalized_mode == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY:
        chrome_only_candidates = _chrome_only_launch_candidates(executable_path, headless=headless)
        if not chrome_only_candidates:
            raise RuntimeError("PW-PERSISTENT-CHROME-ONLY: browser_launch_candidates_missing")
        attempts: list[dict[str, str]] = []
        last_error: BaseException | None = None
        for label, candidate in chrome_only_candidates:
            chrome_kwargs = dict(common_kwargs)
            chrome_kwargs["executable_path"] = candidate
            try:
                context = await playwright.chromium.launch_persistent_context(**chrome_kwargs)
                await patch_context(context, username)
                _emit_browser_layer_log(
                    f"{label} persistent launch ok -> {candidate}",
                    log_fn=log_fn,
                )
                return context
            except Exception as chrome_exc:
                last_error = chrome_exc
                attempts.append(
                    {
                        "label": label,
                        "executable_path": candidate,
                        "launch_error": str(chrome_exc),
                    }
                )
                _emit_browser_layer_log(
                    f"{label} persistent launch failed -> {candidate}: {chrome_exc}",
                    log_fn=log_fn,
                )
        assert last_error is not None
        raise _chrome_only_launch_failure(
            code="PW-PERSISTENT-CHROME-ONLY-FAILED",
            headless=headless,
            visible_reason=visible_reason,
            executable_path=attempts[-1]["executable_path"],
            launch_error=last_error,
            attempts=attempts,
        ) from last_error

    try:
        context = await playwright.chromium.launch_persistent_context(
            channel="chrome",
            **common_kwargs,
        )
        await patch_context(context, username)
        _emit_browser_layer_log("Chrome channel launch ok", log_fn=log_fn)
        return context
    except Exception as chrome_exc:
        embedded_executable = _normalized_executable(executable_path)
        if not embedded_executable:
            embedded_executable = _normalized_executable(
                resolve_playwright_chromium_executable(headless=headless)
            )
        if not (_should_fallback_to_embedded(chrome_exc) or embedded_executable):
            raise
        _emit_browser_layer_log(
            "Chrome channel failed -> fallback to bundled Playwright Chromium",
            log_fn=log_fn,
        )
        if not embedded_executable:
            raise _browser_layer_failure(
                code="PW-PERSISTENT-LAYER-FAILED",
                headless=headless,
                visible_reason=visible_reason,
                executable_path=None,
                chrome_error=chrome_exc,
            ) from chrome_exc

        embedded_kwargs = dict(common_kwargs)
        embedded_kwargs["executable_path"] = embedded_executable
        try:
            context = await playwright.chromium.launch_persistent_context(**embedded_kwargs)
            await patch_context(context, username)
            _emit_browser_layer_log("Bundled Playwright Chromium launch ok", log_fn=log_fn)
            return context
        except Exception as embedded_exc:
            compat_kwargs = dict(embedded_kwargs)
            compat_kwargs["args"] = _merge_launch_args(normalized_args, [])
            _emit_browser_layer_log(
                "Bundled Playwright Chromium retry with compat flags",
                log_fn=log_fn,
            )
            try:
                context = await playwright.chromium.launch_persistent_context(**compat_kwargs)
                await patch_context(context, username)
                _emit_browser_layer_log(
                    "Bundled Playwright Chromium launch ok (compat)",
                    log_fn=log_fn,
                )
                return context
            except Exception as compat_exc:
                raise _browser_layer_failure(
                    code="PW-PERSISTENT-LAYER-FAILED",
                    headless=headless,
                    visible_reason=visible_reason,
                    executable_path=embedded_executable,
                    chrome_error=chrome_exc,
                    embedded_error=embedded_exc,
                    compat_error=compat_exc,
                ) from compat_exc


def launch_sync_browser(
    *,
    headless: bool,
    executable_path: Optional[Union[str, Path]] = None,
    proxy: Optional[dict[str, str]] = None,
    args: Optional[list[str]] = None,
    slow_mo: int = 0,
    visible_reason: str | None = None,
    browser_mode: str = PLAYWRIGHT_BROWSER_MODE_DEFAULT,
) -> Any:
    """Public sync launcher aligned with the browser-layer fallback strategy."""
    launch_kwargs: dict[str, Any] = {
        "headless": False,
        "slow_mo": int(max(0, slow_mo)),
        "args": _merge_launch_args(list(args or []), PLAYWRIGHT_BASE_FLAGS),
        "ignore_default_args": list(PLAYWRIGHT_IGNORE_DEFAULT_ARGS),
    }
    if proxy:
        launch_kwargs["proxy"] = dict(proxy)
    playwright = start_sync_playwright()
    normalized_mode = normalize_browser_mode(browser_mode)
    if normalized_mode == PLAYWRIGHT_BROWSER_MODE_MANAGED:
        managed_kwargs = dict(launch_kwargs)
        managed_executable = _managed_launch_executable(executable_path, headless=headless)
        if managed_executable:
            managed_kwargs["executable_path"] = managed_executable
        browser = playwright.chromium.launch(**managed_kwargs)
        _emit_browser_layer_log(f"Managed Playwright launch ok -> {managed_executable or 'default'}")
        return browser
    if normalized_mode == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY:
        chrome_only_candidates = _chrome_only_launch_candidates(executable_path, headless=headless)
        if not chrome_only_candidates:
            raise RuntimeError("PW-SYNC-BROWSER-CHROME-ONLY: browser_launch_candidates_missing")
        attempts: list[dict[str, str]] = []
        last_error: BaseException | None = None
        for label, candidate in chrome_only_candidates:
            chrome_kwargs = dict(launch_kwargs)
            chrome_kwargs["executable_path"] = candidate
            try:
                browser = playwright.chromium.launch(**chrome_kwargs)
                _emit_browser_layer_log(f"{label} launch ok -> {candidate}")
                return browser
            except Exception as chrome_exc:
                last_error = chrome_exc
                attempts.append(
                    {
                        "label": label,
                        "executable_path": candidate,
                        "launch_error": str(chrome_exc),
                    }
                )
                _emit_browser_layer_log(f"{label} launch failed -> {candidate}: {chrome_exc}")
        assert last_error is not None
        raise _chrome_only_launch_failure(
            code="PW-SYNC-BROWSER-CHROME-ONLY-FAILED",
            headless=headless,
            visible_reason=visible_reason,
            executable_path=attempts[-1]["executable_path"],
            launch_error=last_error,
            attempts=attempts,
        ) from last_error
    try:
        browser = playwright.chromium.launch(channel="chrome", **launch_kwargs)
        _emit_browser_layer_log("Chrome channel launch ok")
        return browser
    except Exception as chrome_exc:
        embedded_executable = _normalized_executable(executable_path)
        if not embedded_executable:
            embedded_executable = _normalized_executable(
                resolve_playwright_chromium_executable(headless=headless)
            )
        if not (_should_fallback_to_embedded(chrome_exc) or embedded_executable):
            raise
        _emit_browser_layer_log("Chrome channel failed -> fallback to bundled Playwright Chromium")
        if not embedded_executable:
            raise _browser_layer_failure(
                code="PW-SYNC-BROWSER-LAYER-FAILED",
                headless=headless,
                visible_reason=visible_reason,
                executable_path=None,
                chrome_error=chrome_exc,
            ) from chrome_exc

        embedded_kwargs = dict(launch_kwargs)
        embedded_kwargs["executable_path"] = embedded_executable
        try:
            browser = playwright.chromium.launch(**embedded_kwargs)
            _emit_browser_layer_log("Bundled Playwright Chromium launch ok")
            return browser
        except Exception as embedded_exc:
            compat_kwargs = dict(embedded_kwargs)
            compat_kwargs["args"] = _merge_launch_args(
                list(launch_kwargs.get("args") or []),
                [],
            )
            _emit_browser_layer_log("Bundled Playwright Chromium retry with compat flags")
            try:
                browser = playwright.chromium.launch(**compat_kwargs)
                _emit_browser_layer_log("Bundled Playwright Chromium launch ok (compat)")
                return browser
            except Exception as compat_exc:
                raise _browser_layer_failure(
                    code="PW-SYNC-BROWSER-LAYER-FAILED",
                    headless=headless,
                    visible_reason=visible_reason,
                    executable_path=embedded_executable,
                    chrome_error=chrome_exc,
                    embedded_error=embedded_exc,
                    compat_error=compat_exc,
                ) from compat_exc


def launch_sync_persistent_context(
    *,
    user_data_dir: Union[str, Path],
    username: str | None = None,
    headless: bool,
    executable_path: Optional[Union[str, Path]] = None,
    proxy: Optional[dict[str, str]] = None,
    args: Optional[list[str]] = None,
    browser_mode: str = PLAYWRIGHT_BROWSER_MODE_DEFAULT,
    **kwargs: Any,
) -> Any:
    persistent_dir = str(user_data_dir)
    username_value = str(username or Path(persistent_dir).name or "default")
    _emit_browser_layer_log(f"Persistent user-data-dir -> {persistent_dir}")
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": persistent_dir,
        "headless": False,
        "args": _merge_launch_args(list(args or []), PLAYWRIGHT_BASE_FLAGS),
        "ignore_default_args": list(PLAYWRIGHT_IGNORE_DEFAULT_ARGS),
    }
    if proxy:
        launch_kwargs["proxy"] = dict(proxy)
    if kwargs:
        launch_kwargs.update(kwargs)
    persistent_headers = _context_extra_http_headers(
        kwargs.get("locale"),
        kwargs.get("extra_http_headers"),
    )
    if persistent_headers:
        launch_kwargs["extra_http_headers"] = persistent_headers
    playwright = start_sync_playwright()
    normalized_mode = normalize_browser_mode(browser_mode)
    if normalized_mode == PLAYWRIGHT_BROWSER_MODE_MANAGED:
        managed_kwargs = dict(launch_kwargs)
        managed_executable = _managed_launch_executable(executable_path, headless=headless)
        if managed_executable:
            managed_kwargs["executable_path"] = managed_executable
        context = playwright.chromium.launch_persistent_context(**managed_kwargs)
        patch_context(context, username_value)
        _emit_browser_layer_log(
            f"Managed Playwright persistent launch ok -> {managed_executable or 'default'}"
        )
        return context
    if normalized_mode == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY:
        chrome_only_candidates = _chrome_only_launch_candidates(executable_path, headless=headless)
        if not chrome_only_candidates:
            raise RuntimeError("PW-SYNC-PERSISTENT-CHROME-ONLY: browser_launch_candidates_missing")
        attempts: list[dict[str, str]] = []
        last_error: BaseException | None = None
        for label, candidate in chrome_only_candidates:
            chrome_kwargs = dict(launch_kwargs)
            chrome_kwargs["executable_path"] = candidate
            try:
                context = playwright.chromium.launch_persistent_context(**chrome_kwargs)
                patch_context(context, username_value)
                _emit_browser_layer_log(f"{label} persistent launch ok -> {candidate}")
                return context
            except Exception as chrome_exc:
                last_error = chrome_exc
                attempts.append(
                    {
                        "label": label,
                        "executable_path": candidate,
                        "launch_error": str(chrome_exc),
                    }
                )
                _emit_browser_layer_log(f"{label} persistent launch failed -> {candidate}: {chrome_exc}")
        assert last_error is not None
        raise _chrome_only_launch_failure(
            code="PW-SYNC-PERSISTENT-CHROME-ONLY-FAILED",
            headless=headless,
            visible_reason="sync_persistent",
            executable_path=attempts[-1]["executable_path"],
            launch_error=last_error,
            attempts=attempts,
        ) from last_error
    try:
        context = playwright.chromium.launch_persistent_context(channel="chrome", **launch_kwargs)
        patch_context(context, username_value)
        _emit_browser_layer_log("Chrome channel launch ok")
        return context
    except Exception as chrome_exc:
        embedded_executable = _normalized_executable(executable_path)
        if not embedded_executable:
            embedded_executable = _normalized_executable(
                resolve_playwright_chromium_executable(headless=headless)
            )
        if not (_should_fallback_to_embedded(chrome_exc) or embedded_executable):
            raise
        _emit_browser_layer_log("Chrome channel failed -> fallback to bundled Playwright Chromium")
        if not embedded_executable:
            raise _browser_layer_failure(
                code="PW-SYNC-PERSISTENT-LAYER-FAILED",
                headless=headless,
                visible_reason="sync_persistent",
                executable_path=None,
                chrome_error=chrome_exc,
            ) from chrome_exc
        embedded_kwargs = dict(launch_kwargs)
        embedded_kwargs["executable_path"] = embedded_executable
        try:
            context = playwright.chromium.launch_persistent_context(**embedded_kwargs)
            patch_context(context, username_value)
            _emit_browser_layer_log("Bundled Playwright Chromium launch ok")
            return context
        except Exception as embedded_exc:
            compat_kwargs = dict(embedded_kwargs)
            compat_kwargs["args"] = _merge_launch_args(
                list(launch_kwargs.get("args") or []),
                [],
            )
            _emit_browser_layer_log("Bundled Playwright Chromium retry with compat flags")
            try:
                context = playwright.chromium.launch_persistent_context(**compat_kwargs)
                patch_context(context, username_value)
                _emit_browser_layer_log("Bundled Playwright Chromium launch ok (compat)")
                return context
            except Exception as compat_exc:
                raise _browser_layer_failure(
                    code="PW-SYNC-PERSISTENT-LAYER-FAILED",
                    headless=headless,
                    visible_reason="sync_persistent",
                    executable_path=embedded_executable,
                    chrome_error=chrome_exc,
                    embedded_error=embedded_exc,
                    compat_error=compat_exc,
                ) from compat_exc


def shutdown_runtime_loop() -> None:
    global _LOOP_THREAD
    with _LOOP_THREAD_LOCK:
        loop = _LOOP
        thread = _LOOP_THREAD
    if loop is not None:
        try:
            loop.call_soon_threadsafe(loop.stop)
        except Exception:
            pass
    if thread is not None:
        try:
            thread.join(timeout=5.0)
        except Exception:
            pass
    with _LOOP_THREAD_LOCK:
        _LOOP_THREAD = None


def is_driver_crash_error(exc: BaseException) -> bool:
    message = str(exc or "").strip().lower()
    if not message:
        return False
    return any(token in message for token in _DRIVER_CRASH_TOKENS)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _runtime_root() -> Path:
    default = Path(__file__).resolve().parents[2]
    return runtime_root(default)


class PlaywrightRuntime:
    """Single Playwright runtime with diagnostics + recovery."""

    _SHARED_PLAYWRIGHT: Optional[Playwright] = None
    _ASYNC_START_LOCK: Optional[asyncio.Lock] = None

    def __init__(self, *, headless: bool = False, owner_module: str = "") -> None:
        self.headless = bool(headless)
        self._root = _runtime_root()
        self._storage_root = storage_root(Path(__file__).resolve().parents[2])
        self._storage_root.mkdir(parents=True, exist_ok=True)
        self._diagnostic_path = self._storage_root / "diagnostic_bundle.json"
        self._debug_log_path = self._storage_root / "playwright_debug.log"
        self.runtime_id = uuid.uuid4().hex
        self._owner_module = _detect_owner_module(owner_module)
        _register_owner(
            _ASYNC_RUNTIME_OWNERS,
            runtime_id=self.runtime_id,
            owner_module=self._owner_module,
        )
        _register_async_runtime_instance(self)

        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._launch_proxy: Optional[dict[str, str]] = None
        self._launch_executable: Optional[str] = None
        self._launch_args: list[str] = []
        self._safe_mode = False

    @property
    def playwright(self) -> Optional[Playwright]:
        return self._playwright

    @property
    def browser(self) -> Optional[Browser]:
        return self._browser

    @property
    def executable_path(self) -> Optional[str]:
        return self._launch_executable

    @property
    def owner_module(self) -> str:
        return self._owner_module

    @property
    def ownership(self) -> RuntimeOwnerState:
        return _owner_snapshot(_ASYNC_RUNTIME_OWNERS, self.runtime_id)

    @property
    def active_contexts(self) -> int:
        return int(self.ownership.active_contexts)

    def _append_debug(self, message: str) -> None:
        try:
            line = f"{_utc_now_iso()} {message}\n"
            self._debug_log_path.parent.mkdir(parents=True, exist_ok=True)
            rotate_daily_file(self._debug_log_path)
            with self._debug_log_path.open("a", encoding="utf-8") as handle:
                handle.write(line)
        except Exception:
            pass

    def _mark_context_open(self) -> RuntimeOwnerState:
        state = _update_owner_active_contexts(_ASYNC_RUNTIME_OWNERS, self.runtime_id, 1)
        self._append_debug(
            f"[ownership] runtime_id={state.runtime_id} owner={state.owner_module or '-'} active_contexts={state.active_contexts}"
        )
        return state

    def _mark_context_closed(self) -> RuntimeOwnerState:
        state = _update_owner_active_contexts(_ASYNC_RUNTIME_OWNERS, self.runtime_id, -1)
        self._append_debug(
            f"[ownership] runtime_id={state.runtime_id} owner={state.owner_module or '-'} active_contexts={state.active_contexts}"
        )
        return state

    def _track_context(
        self,
        context: BrowserContext,
        *,
        before_close: Callable[[], None] | None = None,
        on_close: Callable[[], None] | None = None,
        on_close_error: Callable[[BaseException], None] | None = None,
    ) -> BrowserContext:
        self._mark_context_open()
        runtime = self
        release_state = {"done": False}
        closing_state = {"started": False}

        def _before_close() -> None:
            if closing_state["started"]:
                return
            closing_state["started"] = True
            if callable(before_close):
                try:
                    before_close()
                except Exception:
                    pass

        def _release_context(*_args: Any, **_kwargs: Any) -> None:
            if release_state["done"]:
                return
            release_state["done"] = True
            if callable(on_close):
                try:
                    on_close()
                except Exception:
                    pass
            runtime._mark_context_closed()

        original_close = getattr(context, "close", None)
        if callable(original_close):
            async def _tracked_close(*args: Any, **kwargs: Any) -> Any:
                _before_close()
                try:
                    result = await original_close(*args, **kwargs)
                except Exception as exc:
                    if callable(on_close_error):
                        try:
                            on_close_error(exc)
                        except Exception:
                            pass
                    raise
                _release_context()
                return result

            with contextlib.suppress(Exception):
                setattr(context, "close", _tracked_close)
        else:
            self._append_debug("[ownership] context close hook unavailable")
        try:
            context.on("close", _release_context)
        except Exception:
            if not callable(original_close):
                self._append_debug("[ownership] context release hook not installed")
        return context

    async def safe_shutdown_if_unused(self, *, stop_shared_playwright: bool = False) -> bool:
        owner = self.ownership
        if owner.active_contexts > 0:
            self._append_debug(
                f"[safe_shutdown_if_unused] skip runtime_id={owner.runtime_id} owner={owner.owner_module or '-'} "
                f"active_contexts={owner.active_contexts}"
            )
            return False

        browser = self._browser
        self._browser = None
        self._playwright = None
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass

        if stop_shared_playwright and _total_active_contexts(_ASYNC_RUNTIME_OWNERS) == 0:
            await self._stop_shared_playwright()
        self._append_debug(
            f"[safe_shutdown_if_unused] runtime_id={owner.runtime_id} owner={owner.owner_module or '-'} "
            f"stop_shared={stop_shared_playwright}"
        )
        return True

    def _node_candidates(self) -> list[Path]:
        candidates: list[Path] = []
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            base = Path(meipass)
            candidates.extend(
                [
                    base / "playwright" / "driver" / "node.exe",
                    base / "_internal" / "playwright" / "driver" / "node.exe",
                ]
            )
        exe_path = getattr(sys, "executable", "") or ""
        if exe_path:
            try:
                exe_dir = Path(exe_path).resolve().parent
                candidates.append(exe_dir / "_internal" / "playwright" / "driver" / "node.exe")
            except Exception:
                pass
        candidates.extend(
            [
                self._root / "_internal" / "playwright" / "driver" / "node.exe",
                self._root / "playwright" / "driver" / "node.exe",
            ]
        )
        try:
            import playwright  # type: ignore

            candidates.append(Path(playwright.__file__).resolve().parent / "driver" / "node.exe")
        except Exception:
            pass
        unique: list[Path] = []
        seen: set[str] = set()
        for candidate in candidates:
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            unique.append(candidate)
        return unique

    def _resolve_node_path(self) -> Optional[Path]:
        for candidate in self._node_candidates():
            if candidate.exists() and candidate.is_file():
                return candidate
        return None

    def _resolve_chrome_path(self, explicit_executable: Optional[Union[str, Path]]) -> Optional[Path]:
        if explicit_executable:
            candidate = Path(str(explicit_executable)).expanduser()
            if candidate.exists() and candidate.is_file():
                return candidate
        env_value = (os.environ.get("PLAYWRIGHT_CHROME_EXECUTABLE") or "").strip()
        if env_value:
            candidate = Path(env_value).expanduser()
            if candidate.exists() and candidate.is_file():
                return candidate
        for resolver in (
            resolve_google_chrome_executable,
            resolve_bundled_google_chrome_executable,
        ):
            candidate = resolver()
            if candidate is not None:
                return candidate
        return resolve_playwright_chromium_executable(headless=self.headless)

    @staticmethod
    def _run_version(path: Optional[Path], args: list[str]) -> dict[str, Any]:
        if path is None:
            return {"ok": False, "exit_code": None, "stdout": "", "stderr": "path_not_found"}
        cmd = [str(path)] + list(args)
        try:
            run_kwargs: dict[str, Any] = {
                "capture_output": True,
                "text": True,
                "timeout": 20,
            }
            if os.name == "nt":
                create_no_window = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= int(getattr(subprocess, "STARTF_USESHOWWINDOW", 0))
                startupinfo.wShowWindow = 0
                run_kwargs["creationflags"] = create_no_window
                run_kwargs["startupinfo"] = startupinfo
            result = subprocess.run(
                cmd,
                **run_kwargs,
            )
            return {
                "ok": result.returncode == 0,
                "exit_code": int(result.returncode),
                "stdout": (result.stdout or "").strip(),
                "stderr": (result.stderr or "").strip(),
            }
        except Exception as exc:
            return {
                "ok": False,
                "exit_code": None,
                "stdout": "",
                "stderr": str(exc),
            }

    def _build_hash(self) -> str:
        env_hash = (os.environ.get("APP_BUILD_HASH") or "").strip()
        if env_hash:
            return env_hash
        exe = getattr(sys, "executable", "") or ""
        if not exe:
            return "dev"
        try:
            path = Path(exe).resolve()
            stat = path.stat()
            raw = f"{path}:{int(stat.st_size)}:{int(stat.st_mtime)}"
            return str(abs(hash(raw)))
        except Exception:
            return "unknown"

    async def _smoke_launch(self, executable_path: Optional[Union[str, Path]]) -> dict[str, Any]:
        if self._playwright is None:
            return {"ok": False, "error": "playwright_not_started"}
        browser: Optional[Browser] = None
        try:
            browser = await launch_async_browser(
                self._playwright,
                headless=False,
                args=list(PLAYWRIGHT_SAFE_MODE_ARGS),
                executable_path=executable_path,
                visible_reason="diagnostic_smoke",
                log_fn=self._append_debug,
            )
            context = await browser.new_context(
                extra_http_headers=_context_extra_http_headers("en-US"),
            )
            await patch_context(context, "diagnostic_smoke")
            page = await context.new_page()
            await page.goto("about:blank", wait_until="domcontentloaded", timeout=10_000)
            await context.close()
            await browser.close()
            return {"ok": True}
        except Exception as exc:
            if browser is not None:
                try:
                    await browser.close()
                except Exception:
                    pass
            return {"ok": False, "error": str(exc)}

    async def _write_diagnostic_bundle(
        self,
        *,
        executable_path: Optional[Union[str, Path]],
        error: Optional[str] = None,
        stack: Optional[str] = None,
        code: str = "",
        smoke: Optional[dict[str, Any]] = None,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        node_path = self._resolve_node_path()
        chrome_path = self._resolve_chrome_path(executable_path)
        node_version = self._run_version(node_path, ["--version"])
        chrome_version = self._run_version(chrome_path, ["--version"])
        if smoke is None:
            smoke = await self._smoke_launch(executable_path)

        bundle: dict[str, Any] = {
            "timestamp": _utc_now_iso(),
            "app_version": self._build_hash(),
            "build_hash": self._build_hash(),
            "os": {
                "platform": platform.platform(),
                "release": platform.release(),
                "version": platform.version(),
                "machine": platform.machine(),
                "python": platform.python_version(),
            },
            "paths": {
                "app_root": str(self._root),
                "node": str(node_path) if node_path else "",
                "chrome": str(chrome_path) if chrome_path else "",
                "diagnostic_bundle": str(self._diagnostic_path),
                "playwright_debug_log": str(self._debug_log_path),
            },
            "commands": {
                "node_version": node_version,
                "chrome_version": chrome_version,
            },
            "smoke_launch": smoke,
            "error": {
                "code": code,
                "message": error or "",
                "stack": stack or "",
            },
            "runtime": {
                "safe_mode": bool(self._safe_mode),
                "headless": bool(self.headless),
                "launch_proxy": bool(self._launch_proxy),
                "launch_executable": self._launch_executable or "",
                "launch_args": list(self._launch_args),
            },
        }
        if extra:
            bundle["extra"] = dict(extra)

        try:
            self._diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_json(self._diagnostic_path, bundle)
        except Exception:
            pass

        self._append_debug(
            f"[diag] code={code or '-'} error={error or '-'} "
            f"node_ok={node_version.get('ok')} chrome_ok={chrome_version.get('ok')} "
            f"smoke_ok={smoke.get('ok') if isinstance(smoke, dict) else False}"
        )

    async def record_failure(
        self,
        *,
        code: str,
        error: BaseException,
        executable_path: Optional[Union[str, Path]] = None,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        resolved_executable = executable_path
        if not resolved_executable and self._launch_executable:
            resolved_executable = self._launch_executable
        await self._write_diagnostic_bundle(
            executable_path=resolved_executable,
            error=str(error),
            stack=traceback.format_exc(),
            code=code,
            extra=extra,
        )

    def _configure_debug_env(self) -> None:
        debug_enabled = _env_enabled("PLAYWRIGHT_RUNTIME_DEBUG") or _env_enabled("PLAYWRIGHT_DEBUG")
        if not debug_enabled:
            os.environ.pop("PLAYWRIGHT_DEBUG_LOG", None)
            return

        current = (os.environ.get("DEBUG") or "").strip()
        needed = {"pw:api", "pw:browser"}
        parts = {item.strip() for item in current.split(",") if item.strip()}
        for token in needed:
            parts.add(token)
        os.environ["DEBUG"] = ",".join(sorted(parts))
        os.environ["PLAYWRIGHT_DEBUG_LOG"] = str(self._debug_log_path)

    async def _ensure_playwright(self) -> None:
        shared = self.__class__._SHARED_PLAYWRIGHT
        if shared is not None:
            self._playwright = shared
            return
        if self.__class__._ASYNC_START_LOCK is None:
            self.__class__._ASYNC_START_LOCK = asyncio.Lock()
        async with self.__class__._ASYNC_START_LOCK:
            shared = self.__class__._SHARED_PLAYWRIGHT
            if shared is None:
                shared = await async_playwright().start()
                self.__class__._SHARED_PLAYWRIGHT = shared
            self._playwright = shared

    async def _stop_shared_playwright(self) -> None:
        shared = self.__class__._SHARED_PLAYWRIGHT
        self.__class__._SHARED_PLAYWRIGHT = None
        self.__class__._ASYNC_START_LOCK = None
        if shared is None:
            return
        try:
            await shared.stop()
        except Exception:
            pass

    async def start(
        self,
        *,
        launch_proxy: Optional[dict[str, str]] = None,
        executable_path: Optional[Union[str, Path]] = None,
        launch_args: Optional[list[str]] = None,
        slow_mo: int = 120,
        safe_mode: bool = False,
        launch_browser: bool = True,
        force_headless: Optional[bool] = None,
        browser_mode: str = PLAYWRIGHT_BROWSER_MODE_DEFAULT,
    ) -> None:
        self._configure_debug_env()
        await self._ensure_playwright()
        self._launch_proxy = dict(launch_proxy or {}) or None
        self._launch_executable = str(executable_path) if executable_path else None
        self._launch_args = list(launch_args or [])
        self._safe_mode = bool(safe_mode)
        self._append_debug(
            f"[start] launch_browser={launch_browser} safe_mode={safe_mode} "
            f"headless={self.headless} proxy={bool(self._launch_proxy)} "
            f"browser_mode={browser_mode} executable={self._launch_executable or '-'}"
        )
        if not launch_browser:
            # Persistent mode manages browser launch in get_context(); avoid
            # extra diagnostics/work on successful starts to keep startup clean.
            return
        if self._browser is not None:
            return

        effective_headless = bool(force_headless) if force_headless is not None else bool(self.headless)
        if safe_mode:
            effective_headless = True

        args = _merge_launch_args(
            list(self._launch_args),
            PLAYWRIGHT_SAFE_MODE_ARGS if safe_mode else PLAYWRIGHT_BASE_FLAGS,
        )
        active_proxy = dict(launch_proxy) if (launch_proxy and not safe_mode) else None

        try:
            self._browser = await _launch_browser(
                self._playwright,
                headless=effective_headless,
                visible_reason=None if effective_headless else "shared_runtime",
                args=args,
                slow_mo=int(max(0, slow_mo)),
                proxy=active_proxy,
                executable_path=executable_path,
                log_fn=self._append_debug,
                browser_mode=browser_mode,
            )
        except Exception as exc:
            await self._write_diagnostic_bundle(
                executable_path=executable_path,
                error=str(exc),
                stack=traceback.format_exc(),
                code="start_failed",
            )
            raise

    async def stop(self) -> None:
        stopped = await self.safe_shutdown_if_unused(stop_shared_playwright=False)
        self._append_debug(f"[stop] runtime closed={stopped}")

    async def restart(self, *, reason: str = "") -> bool:
        self._append_debug(f"[restart] reason={reason or '-'}")
        return await safe_restart_runtime(self.runtime_id, reason=reason)

    async def get_context(
        self,
        *,
        account: str,
        profile_dir: Union[str, Path],
        storage_state: Optional[Union[str, Path]] = None,
        proxy: Optional[dict[str, str]] = None,
        mode: str = "shared",
        executable_path: Optional[Union[str, Path]] = None,
        launch_args: Optional[list[str]] = None,
        user_agent: Optional[str] = "",
        locale: Optional[str] = "",
        timezone_id: Optional[str] = "",
        viewport_kwargs: Optional[dict[str, Any]] = None,
        permissions: Optional[list[str]] = None,
        launch_proxy: Optional[dict[str, str]] = None,
        force_headless: Optional[bool] = None,
        safe_mode: bool = False,
        browser_mode: str = PLAYWRIGHT_BROWSER_MODE_DEFAULT,
        subsystem: str = "default",
        _retried: bool = False,
    ) -> BrowserContext:
        profile_path = Path(profile_dir)
        profile_path.mkdir(parents=True, exist_ok=True)
        viewport_payload = dict(viewport_kwargs or {})
        perms = list(permissions or [])
        normalized_mode = str(mode or "shared").strip().lower()
        active_safe_mode = bool(_retried or safe_mode)
        locale_value = str(locale or "").strip() or None
        timezone_value = str(timezone_id or "").strip() or None
        user_agent_value = str(user_agent or "").strip() or None
        persistent_release: Callable[[], None] | None = None

        try:
            if normalized_mode == "persistent":
                persistent_headless = (
                    True
                    if active_safe_mode
                    else bool(force_headless)
                    if force_headless is not None
                    else self.headless
                )
                persistent_release = _claim_persistent_profile_ownership(
                    profile_dir=profile_path,
                    headless=persistent_headless,
                    runtime_id=self.runtime_id,
                    owner_module=self.owner_module,
                )
                release_callback = persistent_release
                owner_token = f"{self.runtime_id}:{uuid.uuid4().hex}"
                lifecycle_mode = _profile_mode_label(persistent_headless)
                lifecycle_pid = os.getpid()

                def _mark_persistent_profile_closing() -> None:
                    mark_profile_closing(
                        account=account,
                        profile_dir=profile_path,
                        subsystem=subsystem,
                        mode=lifecycle_mode,
                        pid=lifecycle_pid,
                        owner_token=owner_token,
                    )

                def _mark_persistent_profile_closed() -> None:
                    try:
                        mark_profile_closed_cleanly(
                            account=account,
                            profile_dir=profile_path,
                            subsystem=subsystem,
                            mode=lifecycle_mode,
                            pid=lifecycle_pid,
                            owner_token=owner_token,
                        )
                    finally:
                        if release_callback is not None:
                            release_callback()

                def _mark_persistent_profile_close_failed(error: BaseException) -> None:
                    mark_profile_unclean_shutdown(
                        account=account,
                        profile_dir=profile_path,
                        subsystem=subsystem,
                        mode=lifecycle_mode,
                        pid=lifecycle_pid,
                        owner_token=owner_token,
                        reason_code="browser_close_failed",
                        payload={
                            "error": str(error) or type(error).__name__,
                            "error_type": type(error).__name__,
                        },
                    )

                await self.start(
                    launch_proxy=launch_proxy,
                    executable_path=executable_path,
                    launch_args=launch_args,
                    safe_mode=active_safe_mode,
                    launch_browser=False,
                    force_headless=force_headless,
                    browser_mode=browser_mode,
                )
                if self._playwright is None:
                    raise RuntimeError("playwright_not_started")
                args = _merge_launch_args(
                    list(launch_args or []),
                    PLAYWRIGHT_SAFE_MODE_ARGS if active_safe_mode else PLAYWRIGHT_BASE_FLAGS,
                )
                persistent_proxy = None if active_safe_mode else (proxy or launch_proxy or None)
                persistent_kwargs: dict[str, Any] = {
                    **viewport_payload,
                    "permissions": perms,
                }
                if user_agent_value:
                    persistent_kwargs["user_agent"] = user_agent_value
                if locale_value:
                    persistent_kwargs["locale"] = locale_value
                    persistent_headers = _context_extra_http_headers(locale_value)
                    if persistent_headers:
                        persistent_kwargs["extra_http_headers"] = persistent_headers
                if timezone_value:
                    persistent_kwargs["timezone_id"] = timezone_value
                context = await _launch_persistent_context(
                    self._playwright,
                    user_data_dir=str(profile_path),
                    username=account,
                    headless=persistent_headless,
                    executable_path=executable_path,
                    proxy=persistent_proxy,
                    args=args,
                    visible_reason=None if persistent_headless else f"persistent:{account}",
                    log_fn=self._append_debug,
                    browser_mode=browser_mode,
                    **persistent_kwargs,
                )
                context.set_default_timeout(30_000)
                tracked_context = self._track_context(
                    context,
                    before_close=_mark_persistent_profile_closing,
                    on_close=_mark_persistent_profile_closed,
                    on_close_error=_mark_persistent_profile_close_failed,
                )
                mark_profile_open(
                    account=account,
                    profile_dir=profile_path,
                    subsystem=subsystem,
                    mode=lifecycle_mode,
                    pid=lifecycle_pid,
                    owner_token=owner_token,
                    owner_hold_count=_persistent_profile_hold_count(profile_path),
                )
                with contextlib.suppress(Exception):
                    setattr(tracked_context, "_profile_lifecycle_owner_token", owner_token)
                with contextlib.suppress(Exception):
                    setattr(tracked_context, "_profile_lifecycle_profile_dir", str(profile_path))
                with contextlib.suppress(Exception):
                    setattr(tracked_context, "_profile_lifecycle_subsystem", str(subsystem or "default"))
                with contextlib.suppress(Exception):
                    setattr(tracked_context, "_profile_lifecycle_mode", lifecycle_mode)
                persistent_release = None
                return tracked_context

            await self.start(
                launch_proxy=launch_proxy,
                executable_path=executable_path,
                launch_args=launch_args,
                safe_mode=active_safe_mode,
                launch_browser=True,
                force_headless=force_headless,
                browser_mode=browser_mode,
            )
            if self._browser is None:
                raise RuntimeError("browser_not_started")
            context_proxy = None if self._launch_proxy else (None if active_safe_mode else proxy)
            context_kwargs: dict[str, Any] = {
                "storage_state": str(storage_state) if storage_state else None,
                "proxy": context_proxy,
                "permissions": perms,
                "accept_downloads": False,
                **viewport_payload,
            }
            if user_agent_value:
                context_kwargs["user_agent"] = user_agent_value
            if locale_value:
                context_kwargs["locale"] = locale_value
                context_headers = _context_extra_http_headers(locale_value)
                if context_headers:
                    context_kwargs["extra_http_headers"] = context_headers
            if timezone_value:
                context_kwargs["timezone_id"] = timezone_value
            context = await self._browser.new_context(**context_kwargs)
            await patch_context(context, account)
            context.set_default_timeout(30_000)
            return self._track_context(context)
        except PersistentProfileOwnershipError:
            raise
        except Exception as exc:
            if persistent_release is not None:
                persistent_release()
                persistent_release = None
            if is_driver_crash_error(exc) and not _retried:
                if normalize_browser_mode(browser_mode) in {
                    PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
                    PLAYWRIGHT_BROWSER_MODE_MANAGED,
                }:
                    await self._write_diagnostic_bundle(
                        executable_path=executable_path,
                        error=str(exc),
                        stack=traceback.format_exc(),
                        code="driver_crash_no_fallback",
                        extra={
                            "account": account,
                            "mode": normalized_mode,
                            "browser_mode": browser_mode,
                        },
                    )
                    raise
                fallback_mode = "shared" if normalized_mode == "persistent" else normalized_mode
                fallback_message = f"mode={normalized_mode} failed -> fallback to {fallback_mode}"
                self._append_debug(f"[fallback] {fallback_message}")
                if normalized_mode == "persistent" and fallback_mode == "shared":
                    try:
                        print("mode=persistent failed -> fallback to shared", flush=True)
                    except Exception:
                        pass
                recovery_executable: Optional[Union[str, Path]] = executable_path
                # If caller forced an executable path, let Playwright choose default binary on recovery.
                if recovery_executable:
                    recovery_executable = None
                fallback_proxy = None if active_safe_mode else proxy
                fallback_launch_proxy = None if active_safe_mode else launch_proxy
                fallback_force_headless = True if active_safe_mode else force_headless
                await self._write_diagnostic_bundle(
                    executable_path=executable_path,
                    error=str(exc),
                    stack=traceback.format_exc(),
                    code="driver_crash_retry",
                    extra={
                        "account": account,
                        "mode": normalized_mode,
                        "fallback_mode": fallback_mode,
                        "fallback_message": fallback_message,
                    },
                )
                restarted = await self.restart(reason=f"{normalized_mode}_driver_crash")
                if not restarted:
                    raise RuntimeError(
                        f"PW-RESTART-BLOCKED: runtime_id={self.runtime_id} active_contexts={self.active_contexts}"
                    ) from exc
                return await self.get_context(
                    account=account,
                    profile_dir=profile_path,
                    storage_state=storage_state,
                    proxy=fallback_proxy,
                    mode=fallback_mode,
                    executable_path=recovery_executable,
                    launch_args=launch_args,
                    user_agent=user_agent,
                    locale=locale,
                    timezone_id=timezone_id,
                    viewport_kwargs=viewport_payload,
                    permissions=perms,
                    launch_proxy=fallback_launch_proxy,
                    force_headless=fallback_force_headless,
                    safe_mode=active_safe_mode,
                    browser_mode=browser_mode,
                    subsystem=subsystem,
                    _retried=True,
                )
            await self._write_diagnostic_bundle(
                executable_path=executable_path,
                error=str(exc),
                stack=traceback.format_exc(),
                code="context_failed",
                extra={"account": account, "mode": normalized_mode},
            )
            short_code = f"PW-{normalized_mode.upper()}-FAILED"
            raise RuntimeError(f"{short_code}: {exc}") from exc
