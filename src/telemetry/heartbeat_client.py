from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any, Callable

from src.licensing.license_client import SupabaseLicenseClient, get_runtime_context
from src.telemetry.event_client import runtime_health_snapshot


logger = logging.getLogger(__name__)


class HeartbeatClient:
    def __init__(
        self,
        snapshot_provider: Callable[[], dict[str, Any]] | None = None,
        *,
        interval_seconds: int = 300,
    ) -> None:
        self._snapshot_provider = snapshot_provider
        self._interval_seconds = max(30, int(interval_seconds or 300))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="instacrm-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout_seconds: float = 2.0) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(0.1, float(timeout_seconds)))
        self._thread = None

    def beat_once(self) -> bool:
        context = get_runtime_context()
        if context is None:
            return False
        payload = self._build_payload(context)
        client = SupabaseLicenseClient(admin=False)
        try:
            client.rest.insert("client_heartbeats", payload, returning="minimal")
            return True
        except Exception:
            logger.exception("Could not send client heartbeat")
            return False

    def _run_loop(self) -> None:
        self.beat_once()
        while not self._stop_event.wait(self._interval_seconds):
            self.beat_once()

    def _build_payload(self, context) -> dict[str, Any]:
        provider_payload = {}
        if callable(self._snapshot_provider):
            try:
                provider_payload = dict(self._snapshot_provider() or {})
            except Exception:
                logger.exception("Could not collect heartbeat snapshot")
                provider_payload = {}
        runtime_state = runtime_health_snapshot()
        return {
            "license_key": context.license_key,
            "device_id": context.device_id,
            "machine_name": context.machine_name,
            "app_version": str(
                provider_payload.get("app_version")
                or context.app_version
                or os.environ.get("APP_VERSION")
                or "unknown"
            ).strip(),
            "accounts_count": int(provider_payload.get("accounts_count") or 0),
            "active_workers": int(provider_payload.get("active_workers") or 0),
            "startup_ok": bool(provider_payload.get("startup_ok", True)),
            "db_ok": bool(provider_payload.get("db_ok", True)),
            "runtime_ok": bool(
                provider_payload.get("runtime_ok", runtime_state.get("runtime_ok", True))
            ),
            "last_error_code": str(
                provider_payload.get("last_error_code")
                or runtime_state.get("last_error_code")
                or ""
            ).strip(),
            "last_error_message": str(
                provider_payload.get("last_error_message")
                or runtime_state.get("last_error_message")
                or ""
            ).strip(),
            "timestamp": provider_payload.get("timestamp")
            or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
