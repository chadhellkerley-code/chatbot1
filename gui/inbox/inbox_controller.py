from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from PySide6.QtCore import QObject, QTimer, Signal
from src.runtime.ownership_router import OwnershipRouter


class InboxController(QObject):
    snapshot_changed = Signal(object)
<<<<<<< HEAD
    runtime_status_changed = Signal(object)
=======
>>>>>>> origin/main

    def __init__(
        self,
        service: Any,
        *,
        on_thread_selected: Callable[[str], None] | None = None,
        snapshot_poll_ms: int = 0,
<<<<<<< HEAD
        runtime_poll_ms: int = 1000,
=======
>>>>>>> origin/main
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        del snapshot_poll_ms
        self._service = service
        self._on_thread_selected = on_thread_selected
        self._current_filter = "all"
        self._current_thread_key = ""
        self._active = False
        self._refresh_scheduled = False
        self._pending_force_scroll = False
        self._loading_thread_key = ""
        self._requested_thread_key = ""
        self._auto_selected_thread_key = ""
<<<<<<< HEAD
        self._projection_updated_at: float | None = None
        self._projection_reason = ""
        self._ownership_router = OwnershipRouter()
        self._runtime_poll_ms = max(0, int(runtime_poll_ms or 0))
        self._runtime_timer = QTimer(self)
        self._runtime_timer.setInterval(max(250, self._runtime_poll_ms) if self._runtime_poll_ms else 0)
        self._runtime_timer.timeout.connect(self.refresh_runtime_status)
        self._last_runtime_token = ""
        self._last_runtime_running: bool | None = None
=======
        self._sync_label = "Cache local"
        self._ownership_router = OwnershipRouter()
>>>>>>> origin/main
        runtime_aliases = list(getattr(self._service, "list_runtime_aliases", lambda: [])() or [])
        self._runtime_alias = str(runtime_aliases[0] or "").strip() if runtime_aliases else ""

        events = getattr(self._service, "events", None)
        cache_signal = getattr(events, "cache_updated", None)
        if cache_signal is not None:
            cache_signal.connect(self._on_cache_updated)
        snapshot_signal = getattr(events, "snapshot_updated", None)
        if snapshot_signal is not None:
            snapshot_signal.connect(self._on_state_updated)
        thread_signal = getattr(events, "thread_updated", None)
        if thread_signal is not None:
            thread_signal.connect(self._on_state_updated)

    def activate(self, *, initial_thread_key: str = "") -> None:
        self._active = True
        clean_key = str(initial_thread_key or "").strip()
        if clean_key:
            self._auto_selected_thread_key = ""
            self._current_thread_key = clean_key
            self._loading_thread_key = clean_key
            self._pending_force_scroll = True
            self._persist_selected_thread(clean_key)
        self._service.ensure_started()
        set_ui_active = getattr(self._service, "set_ui_active", None)
        if callable(set_ui_active):
            set_ui_active(True)
<<<<<<< HEAD
        self._start_runtime_refresh()
        self.refresh_runtime_status(force=True)
=======
>>>>>>> origin/main
        self._schedule_refresh(force=True)
        if clean_key:
            self._requested_thread_key = clean_key
            self._request_thread_open(clean_key)

    def deactivate(self) -> None:
        self._active = False
        self._refresh_scheduled = False
        self._requested_thread_key = ""
<<<<<<< HEAD
        self._stop_runtime_refresh()
=======
>>>>>>> origin/main
        set_ui_active = getattr(self._service, "set_ui_active", None)
        if callable(set_ui_active):
            set_ui_active(False)

    def set_filter(self, filter_mode: str) -> None:
        next_filter = str(filter_mode or "all").strip().lower() or "all"
        if next_filter == self._current_filter:
            return
        self._current_filter = next_filter
        self._schedule_refresh(force=True)

    def force_refresh(self) -> None:
<<<<<<< HEAD
=======
        self._sync_label = "Sincronizando..."
>>>>>>> origin/main
        self._service.refresh()
        self._schedule_refresh(force=True)

    def select_thread(self, thread_key: str) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        if clean_key == self._current_thread_key and clean_key != self._auto_selected_thread_key:
            return
        self._auto_selected_thread_key = ""
        self._current_thread_key = clean_key
        self._loading_thread_key = clean_key
        self._pending_force_scroll = True
        self._persist_selected_thread(clean_key)
        self._requested_thread_key = clean_key
        self._request_thread_open(clean_key)
        self._schedule_refresh(force=True)

    def send_message(self, text: str) -> None:
        if not self._current_thread_key:
            return
        content = str(text or "").strip()
        if not content:
            return
        self._pending_force_scroll = True
        self._service.send_message(self._current_thread_key, content)
        self._schedule_refresh(force=True)

    def send_pack(self, pack_id: str) -> None:
        if not self._current_thread_key:
            return
        if self._service.send_pack(self._current_thread_key, pack_id):
            self._schedule_refresh(force=True)

    def add_tag(self, tag: str = "Etiqueta manual") -> None:
        if not self._current_thread_key:
            return
        adder = getattr(self._service, "add_tag", None)
        if callable(adder) and adder(self._current_thread_key, str(tag or "").strip() or "Etiqueta manual"):
            self._schedule_refresh(force=True)

    def mark_follow_up(self) -> None:
        if not self._current_thread_key:
            return
        marker = getattr(self._service, "mark_follow_up", None)
        if callable(marker) and marker(self._current_thread_key):
            self._schedule_refresh(force=True)

    def delete_message(self, message: Any) -> None:
        if not self._current_thread_key or not isinstance(message, dict):
            return
        deleter = getattr(self._service, "delete_local_message", None)
        if callable(deleter) and deleter(self._current_thread_key, message):
            self._schedule_refresh(force=True)

    def take_thread_manual(self) -> None:
        if not self._current_thread_key:
            return
        taker = getattr(self._service, "take_thread_manual", None)
        if callable(taker) and taker(self._current_thread_key):
            self._schedule_refresh(force=True)

    def release_thread_manual(self) -> None:
        if not self._current_thread_key:
            return
        releaser = getattr(self._service, "release_thread_manual", None)
        if callable(releaser) and releaser(self._current_thread_key):
            self._schedule_refresh(force=True)

    def mark_thread_qualified(self) -> None:
        if not self._current_thread_key:
            return
        marker = getattr(self._service, "mark_thread_qualified", None)
        if callable(marker) and marker(self._current_thread_key):
            self._schedule_refresh(force=True)

    def mark_thread_disqualified(self) -> None:
        if not self._current_thread_key:
            return
        marker = getattr(self._service, "mark_thread_disqualified", None)
        if callable(marker) and marker(self._current_thread_key):
            self._schedule_refresh(force=True)

    def clear_thread_classification(self) -> None:
        if not self._current_thread_key:
            return
        clearer = getattr(self._service, "clear_thread_classification", None)
        if callable(clearer) and clearer(self._current_thread_key):
            self._schedule_refresh(force=True)

    def request_ai_suggestion(self) -> None:
        if not self._current_thread_key:
            return
        if self._service.request_ai_suggestion(self._current_thread_key):
            self._schedule_refresh(force=True)

    def set_runtime_alias(self, alias_id: str) -> None:
        self._runtime_alias = str(alias_id or "").strip()
<<<<<<< HEAD
        self.refresh_runtime_status(force=True)
=======
>>>>>>> origin/main
        self._schedule_refresh(force=True)

    def start_runtime(self, config: dict[str, Any]) -> None:
        alias = str(config.get("alias_id") or self._runtime_alias or "").strip()
        if not alias:
            return
        self._runtime_alias = alias
        starter = getattr(self._service, "start_alias_runtime", None)
        if callable(starter):
            starter(alias, config)
<<<<<<< HEAD
            self.refresh_runtime_status(force=True)
=======
>>>>>>> origin/main
            self._schedule_refresh(force=True)

    def stop_runtime(self) -> None:
        alias = str(self._runtime_alias or "").strip()
        if not alias:
            return
        stopper = getattr(self._service, "stop_alias_runtime", None)
        if callable(stopper):
            stopper(alias)
<<<<<<< HEAD
            self.refresh_runtime_status(force=True)
            self._schedule_refresh(force=True)

    def _start_runtime_refresh(self) -> None:
        if not self._runtime_poll_ms or self._runtime_timer.isActive():
            return
        self._runtime_timer.start()

    def _stop_runtime_refresh(self) -> None:
        if self._runtime_timer.isActive():
            self._runtime_timer.stop()

    def refresh_runtime_status(self, *, force: bool = False) -> None:
        if not self._active and not force:
            return
        runtime_status = {}
        runtime_getter = getattr(self._service, "alias_runtime_status", None)
        if callable(runtime_getter) and self._runtime_alias:
            try:
                runtime_status = dict(runtime_getter(self._runtime_alias) or {})
            except Exception:
                runtime_status = {}
        ui_status = _runtime_status_ui(runtime_status)
        token = _runtime_status_token(ui_status)
        running = bool(ui_status.get("is_running"))
        running_changed = self._last_runtime_running is not None and running != self._last_runtime_running
        self._last_runtime_running = running
        if force or token != self._last_runtime_token:
            self._last_runtime_token = token
            self.runtime_status_changed.emit(ui_status)
        if running_changed:
=======
>>>>>>> origin/main
            self._schedule_refresh(force=True)

    def _on_cache_updated(self, payload: Any) -> None:
        updated_at = None
<<<<<<< HEAD
        reason = ""
        if isinstance(payload, dict):
            updated_at = payload.get("updated_at")
            reason = str(payload.get("reason") or "").strip()
        stamp = _coerce_timestamp(updated_at)
        if stamp is not None:
            self._projection_updated_at = stamp
        if reason:
            self._projection_reason = reason
=======
        if isinstance(payload, dict):
            updated_at = payload.get("updated_at")
        try:
            stamp = float(updated_at) if updated_at is not None else None
        except Exception:
            stamp = None
        if stamp is not None:
            self._sync_label = f"Actualizado {datetime.fromtimestamp(stamp).strftime('%H:%M:%S')}"
>>>>>>> origin/main
        self._schedule_refresh()

    def _on_state_updated(self, _payload: Any) -> None:
        self._schedule_refresh()

    def _schedule_refresh(self, *, force: bool = False) -> None:
        if not self._active and not force:
            return
        if self._refresh_scheduled:
            return
        self._refresh_scheduled = True
        QTimer.singleShot(0, self._emit_snapshot)

    def _request_thread_open(self, thread_key: str) -> None:
        opener = getattr(self._service, "request_open_thread", None)
        if callable(opener):
            opener(thread_key)
            return
        self._service.open_thread(thread_key)

    def _emit_snapshot(self) -> None:
        self._refresh_scheduled = False
        if not self._active:
            return

        list_threads_cached = getattr(self._service, "list_threads_cached", None)
        projection_ready = bool(getattr(self._service, "projection_ready", lambda: True)())
        if callable(list_threads_cached):
            all_rows = list(list_threads_cached("all") or [])
        else:
            all_rows = list(self._service.list_threads("all") or [])
        rows = [row for row in all_rows if _matches_filter(row, self._current_filter)]
        unread_count = sum(1 for row in all_rows if _matches_filter(row, "unread"))
        pending_count = sum(1 for row in all_rows if _matches_filter(row, "pending"))
        qualified_count = sum(1 for row in all_rows if _matches_filter(row, "qualified"))
        disqualified_count = sum(1 for row in all_rows if _matches_filter(row, "disqualified"))

        if projection_ready and self._current_thread_key and not any(
            str(row.get("thread_key") or "").strip() == self._current_thread_key for row in all_rows
        ):
            self._current_thread_key = ""
            self._loading_thread_key = ""
            self._requested_thread_key = ""
            self._auto_selected_thread_key = ""
            self._persist_selected_thread("")

        if not self._current_thread_key and rows:
            auto_key = str(rows[0].get("thread_key") or "").strip()
            if auto_key:
                self._auto_selected_thread_key = auto_key
                self._current_thread_key = auto_key
                self._persist_selected_thread(auto_key)

        selected_row = _find_thread_row(all_rows, self._current_thread_key)
        getter = getattr(self._service, "get_thread_cached", None)
        if callable(getter):
            service_thread = getter(self._current_thread_key) if self._current_thread_key else None
        else:
            service_thread = self._service.get_thread(self._current_thread_key) if self._current_thread_key else None
        messages = list((service_thread or {}).get("messages") or []) if isinstance(service_thread, dict) else []
        thread = _merge_thread_snapshot(selected_row, service_thread, messages=messages)
        thread_status = str((thread or {}).get("thread_status") or "").strip().lower() if isinstance(thread, dict) else ""
        thread_error = str((thread or {}).get("thread_error") or "").strip() if isinstance(thread, dict) else ""
        if (
            self._current_thread_key
            and not messages
            and self._requested_thread_key != self._current_thread_key
            and self._auto_selected_thread_key != self._current_thread_key
        ):
            self._requested_thread_key = self._current_thread_key
            self._request_thread_open(self._current_thread_key)
        if self._current_thread_key and (messages or thread_error or thread_status in {"ready", "failed"}):
            self._loading_thread_key = ""
            self._requested_thread_key = ""

        packs = list(self._service.list_packs() or [])
        runtime_status = {}
        runtime_getter = getattr(self._service, "alias_runtime_status", None)
        if callable(runtime_getter) and self._runtime_alias:
<<<<<<< HEAD
            try:
                runtime_status = dict(runtime_getter(self._runtime_alias) or {})
            except Exception:
                runtime_status = {}
        runtime_status = _runtime_status_ui(runtime_status)
=======
            runtime_status = dict(runtime_getter(self._runtime_alias) or {})
>>>>>>> origin/main
        runtime_aliases = list(getattr(self._service, "list_runtime_aliases", lambda: [])() or [])
        thread_runtime_status = _thread_runtime_status(
            self._service,
            thread,
            selected_alias=self._runtime_alias,
            selected_status=runtime_status,
        )
        thread_permissions = _thread_permissions(
            thread,
            runtime_status=thread_runtime_status,
            router=self._ownership_router,
        )
<<<<<<< HEAD
        thread_alias_id = _thread_alias_id(thread)
        selected_runtime_alias = str(self._runtime_alias or "").strip()
        thread_permissions.update(
            {
                "thread_alias_id": thread_alias_id,
                "selected_runtime_alias": selected_runtime_alias,
                "selected_runtime_matches_thread": bool(
                    thread_alias_id
                    and selected_runtime_alias
                    and thread_alias_id.lower() == selected_runtime_alias.lower()
                ),
            }
        )
        projection_status = _projection_status_ui(
            updated_at=self._projection_updated_at,
            reason=self._projection_reason,
            ready=projection_ready,
        )
        remote_sync_status = _remote_sync_status_ui(thread)
        thread_truth = _thread_truth_ui(
            thread,
            messages=messages,
            runtime_status=thread_runtime_status,
            selected_runtime_alias=selected_runtime_alias,
        )
=======
>>>>>>> origin/main
        payload = {
            "rows": rows,
            "total_count": len(all_rows),
            "current_thread_key": self._current_thread_key,
            "metrics": {
                "threads": len(all_rows),
                "unread": unread_count,
                "pending": pending_count,
            },
            "bucket_counts": {
                "qualified": qualified_count,
                "disqualified": disqualified_count,
            },
<<<<<<< HEAD
            "sync_label": projection_status.get("label") or "Proyeccion local",
            "projection_status": projection_status,
            "remote_sync_status": remote_sync_status,
=======
            "sync_label": self._sync_label,
>>>>>>> origin/main
            "thread": thread,
            "messages": messages,
            "seen_text": str((thread or {}).get("last_seen_text") or "").strip() if isinstance(thread, dict) else "",
            "thread_status": thread_status,
            "thread_error": thread_error,
            "packs": packs,
            "loading": bool(
                self._current_thread_key
                and self._loading_thread_key == self._current_thread_key
                and thread_status not in {"ready", "failed"}
                and not thread_error
                and not messages
            ),
            "force_scroll_to_bottom": bool(self._pending_force_scroll),
<<<<<<< HEAD
            "actions_status": _actions_status(thread, thread_permissions, truth=thread_truth),
            "runtime_aliases": runtime_aliases,
            "runtime_status": runtime_status,
            "selected_runtime_alias": selected_runtime_alias,
            "thread_runtime_status": thread_runtime_status,
            "thread_permissions": thread_permissions,
            "thread_truth": thread_truth,
=======
            "actions_status": _actions_status(thread, thread_permissions),
            "runtime_aliases": runtime_aliases,
            "runtime_status": runtime_status,
            "thread_runtime_status": thread_runtime_status,
            "thread_permissions": thread_permissions,
>>>>>>> origin/main
        }
        self.snapshot_changed.emit(payload)
        if messages:
            self._pending_force_scroll = False

    def _persist_selected_thread(self, thread_key: str) -> None:
        if self._on_thread_selected is not None:
            self._on_thread_selected(str(thread_key or "").strip())


def _matches_filter(row: dict[str, Any], mode: str) -> bool:
    filter_mode = str(mode or "all").strip().lower()
    if filter_mode == "qualified":
        return str(row.get("bucket") or "").strip().lower() == "qualified"
    if filter_mode == "disqualified":
        return str(row.get("bucket") or "").strip().lower() == "disqualified"
    if filter_mode == "unread":
        try:
            return int(row.get("unread_count") or 0) > 0
        except Exception:
            return False
    if filter_mode == "pending":
        if "needs_reply" in row:
            return bool(row.get("needs_reply"))
        return str(row.get("last_message_direction") or "").strip().lower() == "inbound"
    return True


<<<<<<< HEAD
def _runtime_status_ui(status: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(status, dict) or not status:
        return {}
    payload = dict(status)
    payload.setdefault("scheduler_current_account_id", str(payload.get("current_account_id") or "").strip())
    payload.setdefault("scheduler_next_account_id", str(payload.get("next_account_id") or "").strip())
    payload.setdefault("sender_attached_account_id", str(payload.get("sender_attached_account_id") or "").strip())
    payload.setdefault("sender_attached_thread_key", str(payload.get("sender_attached_thread_key") or "").strip())
    payload.setdefault("last_send_attempt_account_id", str(payload.get("last_send_attempt_account_id") or "").strip())
    payload.setdefault("last_send_attempt_thread_key", str(payload.get("last_send_attempt_thread_key") or "").strip())
    payload.setdefault("last_send_attempt_job_id", int(payload.get("last_send_attempt_job_id") or 0))
    payload.setdefault("last_send_attempt_job_type", str(payload.get("last_send_attempt_job_type") or "").strip())
    payload.setdefault("last_send_attempt_at", payload.get("last_send_attempt_at"))
    payload.setdefault("last_send_attempt_outcome", str(payload.get("last_send_attempt_outcome") or "").strip())
    payload.setdefault("last_send_attempt_reason_code", str(payload.get("last_send_attempt_reason_code") or "").strip())
    payload.setdefault("last_send_outcome", str(payload.get("last_send_outcome") or "").strip())
    payload.setdefault("last_send_reason_code", str(payload.get("last_send_reason_code") or "").strip())
    payload.setdefault("last_send_reason", str(payload.get("last_send_reason") or "").strip())
    payload.setdefault("last_send_account_id", str(payload.get("last_send_account_id") or "").strip())
    payload.setdefault("last_send_thread_key", str(payload.get("last_send_thread_key") or "").strip())
    payload.setdefault("last_send_job_id", int(payload.get("last_send_job_id") or 0))
    payload.setdefault("last_send_job_type", str(payload.get("last_send_job_type") or "").strip())
    payload.setdefault("last_send_at", payload.get("last_send_at"))
    payload.setdefault("last_send_exception_type", str(payload.get("last_send_exception_type") or "").strip())
    payload.setdefault("last_send_exception_message", str(payload.get("last_send_exception_message") or "").strip())
    payload.setdefault("last_heartbeat_at", payload.get("last_heartbeat_at"))
    payload.setdefault("updated_at", payload.get("updated_at"))
    keep_keys = {
        "alias_id",
        "is_running",
        "worker_state",
        "mode",
        "current_turn_count",
        "max_turns_per_account",
        "last_error",
        "current_account_id",
        "next_account_id",
        "scheduler_current_account_id",
        "scheduler_next_account_id",
        "sender_attached_account_id",
        "sender_attached_thread_key",
        "last_send_attempt_account_id",
        "last_send_attempt_thread_key",
        "last_send_attempt_job_id",
        "last_send_attempt_job_type",
        "last_send_attempt_at",
        "last_send_attempt_outcome",
        "last_send_attempt_reason_code",
        "last_send_outcome",
        "last_send_reason_code",
        "last_send_reason",
        "last_send_account_id",
        "last_send_thread_key",
        "last_send_job_id",
        "last_send_job_type",
        "last_send_at",
        "last_send_exception_type",
        "last_send_exception_message",
        "last_heartbeat_at",
        "updated_at",
    }
    return {key: payload.get(key) for key in keep_keys if key in payload}


def _runtime_status_token(status: dict[str, Any]) -> str:
    if not isinstance(status, dict) or not status:
        return ""
    parts: list[tuple[str, str]] = []
    for key in sorted(status.keys()):
        value = status.get(key)
        parts.append((str(key), str(value)))
    return "|".join(f"{key}={value}" for key, value in parts)


def _actions_status(
    thread: dict[str, Any] | None,
    permissions: dict[str, Any] | None = None,
    *,
    truth: dict[str, Any] | None = None,
) -> str:
    if not isinstance(thread, dict):
        return "Selecciona una conversacion para ver estado operativo, acciones y runtime."
    permissions = dict(permissions or {})
    truth_payload = dict(truth or {})
    truth_label = str(truth_payload.get("label") or "").strip()
    alias_note = str(truth_payload.get("alias_note") or "").strip()
=======
def _actions_status(thread: dict[str, Any] | None, permissions: dict[str, Any] | None = None) -> str:
    if not isinstance(thread, dict):
        return "Selecciona una conversacion para habilitar IA y packs."
    permissions = dict(permissions or {})
>>>>>>> origin/main
    health = str(thread.get("account_health") or "healthy").strip().lower()
    if health != "healthy":
        label = {
            "login_required": "Requiere login",
            "checkpoint": "Checkpoint",
            "suspended": "Suspendida",
            "banned": "Bloqueada",
            "proxy_error": "Error de proxy",
            "unknown": "Estado desconocido",
        }.get(health, "Estado desconocido")
        detail = str(thread.get("account_health_reason") or "").strip()
<<<<<<< HEAD
        prefix = truth_label or "Cuenta con error"
        message = f"{prefix}\nCuenta con error: {label}"
        if detail:
            message = f"{message}\n{detail}"
        if alias_note:
            message = f"{message}\n{alias_note}"
        return message
    suggestion_status = str(thread.get("suggestion_status") or "").strip().lower()
    if suggestion_status == "queued":
        return "\n".join(part for part in (truth_label, "Sugerencia IA en cola.", alias_note) if part)
    if suggestion_status == "failed":
        return "\n".join(
            part
            for part in (
                truth_label,
                str(thread.get("suggestion_error") or "No se pudo generar sugerencia."),
                alias_note,
            )
            if part
        )
    if not bool(permissions.get("can_request_ai", True)) or not bool(permissions.get("can_send_pack", True)):
        manual_status = _manual_action_status(thread, permissions)
        return "\n".join(part for part in (truth_label, manual_status, alias_note) if part)
    return (
        "\n".join(
            part
            for part in (
                truth_label,
                (
                    f"Cuenta emisora: @{str(thread.get('account_id') or '-').strip()}\n"
                    f"Cliente: @{str(thread.get('recipient_username') or '-').strip() or '-'}"
                ),
                alias_note,
            )
            if part
        )
=======
        return f"Cuenta con error: {label}" if not detail else f"Cuenta con error: {label}\n{detail}"
    suggestion_status = str(thread.get("suggestion_status") or "").strip().lower()
    if suggestion_status == "queued":
        return "Generando sugerencia IA..."
    if suggestion_status == "failed":
        return str(thread.get("suggestion_error") or "No se pudo generar sugerencia.")
    if not bool(permissions.get("can_request_ai", True)) or not bool(permissions.get("can_send_pack", True)):
        return _manual_action_status(thread, permissions)
    return (
        f"Cuenta emisora: @{str(thread.get('account_id') or '-').strip()}\n"
        f"Cliente: @{str(thread.get('recipient_username') or '-').strip() or '-'}"
>>>>>>> origin/main
    )


def _thread_alias_id(thread: dict[str, Any] | None) -> str:
    if not isinstance(thread, dict):
        return ""
    return str(thread.get("alias_id") or thread.get("account_alias") or "").strip()


def _thread_runtime_status(
    service: Any,
    thread: dict[str, Any] | None,
    *,
    selected_alias: str,
    selected_status: dict[str, Any] | None,
) -> dict[str, Any]:
    alias_id = _thread_alias_id(thread)
    if not alias_id:
        return {}
    if alias_id == str(selected_alias or "").strip():
        return dict(selected_status or {})
    runtime_getter = getattr(service, "alias_runtime_status", None)
    if not callable(runtime_getter):
        return {}
    return dict(runtime_getter(alias_id) or {})


def _thread_permissions(
    thread: dict[str, Any] | None,
    *,
    runtime_status: dict[str, Any] | None,
    router: OwnershipRouter,
) -> dict[str, Any]:
    if not isinstance(thread, dict):
        return {
            "has_thread": False,
            "runtime_active": False,
            "owner": "none",
            "health": "unknown",
            "can_manual_send": False,
            "can_send_pack": False,
            "can_request_ai": False,
            "can_takeover_manual": False,
            "can_release_manual": False,
            "can_mark_follow_up": False,
            "can_add_tag": False,
            "can_mark_qualified": False,
            "can_mark_disqualified": False,
            "can_clear_classification": False,
            "composer_mode": "disabled",
            "manual_send_reason": "no_thread",
<<<<<<< HEAD
            "thread_alias_id": "",
            "selected_runtime_alias": "",
            "selected_runtime_matches_thread": False,
=======
>>>>>>> origin/main
        }
    row = dict(thread)
    health = str(row.get("account_health") or "healthy").strip().lower() or "healthy"
    owner = str(row.get("owner") or "none").strip().lower() or "none"
    runtime_active = bool((runtime_status or {}).get("is_running"))
    can_manual_send = health == "healthy" and router.can_manual_send(row, runtime_active=runtime_active)
    block_reason = _manual_send_block_reason(row, runtime_active=runtime_active, health=health, router=router)
    return {
        "has_thread": True,
        "runtime_active": runtime_active,
        "owner": owner,
        "health": health,
        "can_manual_send": can_manual_send,
        "can_send_pack": can_manual_send,
        "can_request_ai": can_manual_send,
        "can_takeover_manual": (
            health == "healthy"
            and owner != "manual"
            and router.can_manual_takeover(row, runtime_active=runtime_active)
        ),
        "can_release_manual": router.can_manual_release(row),
        "can_mark_follow_up": owner != "manual",
        "can_add_tag": True,
        "can_mark_qualified": not (owner == "manual" and str(row.get("bucket") or "").strip().lower() == "qualified"),
        "can_mark_disqualified": str(row.get("bucket") or "").strip().lower() != "disqualified",
        "can_clear_classification": (
            str(row.get("bucket") or "").strip().lower() != "all"
            or owner == "manual"
        ),
        "composer_mode": "editable" if can_manual_send else ("readonly" if block_reason == "runtime_auto_owner" else "disabled"),
        "manual_send_reason": block_reason,
    }


def _manual_send_block_reason(
    thread: dict[str, Any],
    *,
    runtime_active: bool,
    health: str,
    router: OwnershipRouter,
) -> str:
    if health != "healthy":
        return "account_unhealthy"
    bucket = str(thread.get("bucket") or "all").strip().lower() or "all"
    if bucket == "disqualified":
        return "disqualified"
    owner = str(thread.get("owner") or "none").strip().lower() or "none"
    if runtime_active and owner != "manual":
        return "runtime_auto_owner"
    if router.can_manual_send(thread, runtime_active=runtime_active):
        return ""
    status = str(thread.get("status") or thread.get("operational_status") or "open").strip().lower() or "open"
    if runtime_active and status == "closed":
        return "runtime_closed"
    if runtime_active and owner == "manual":
        return "runtime_manual_blocked"
    return "manual_blocked"


def _manual_action_status(thread: dict[str, Any], permissions: dict[str, Any]) -> str:
    reason = str(permissions.get("manual_send_reason") or "").strip().lower()
<<<<<<< HEAD
    thread_alias_id = str(permissions.get("thread_alias_id") or "").strip()
    selected_runtime_alias = str(permissions.get("selected_runtime_alias") or "").strip()
    alias_suffix = ""
    if thread_alias_id:
        alias_suffix = f" Alias del thread: @{thread_alias_id}."
        if selected_runtime_alias and selected_runtime_alias.lower() != thread_alias_id.lower():
            alias_suffix = (
                f" Alias del thread: @{thread_alias_id}. Runtime seleccionado: @{selected_runtime_alias}."
            )
    if reason == "runtime_auto_owner":
        return (
            "Runtime activo para el alias real del thread."
            " Toma manual o frena ese runtime para usar IA y packs."
            f"{alias_suffix}"
        )
    if reason == "disqualified":
        return "Thread descalificado. El backend no acepta acciones manuales sobre este contacto."
    if reason == "runtime_closed":
        return (
            "El thread esta cerrado mientras el runtime sigue activo."
            " Frena el runtime o retoma manual antes de actuar."
            f"{alias_suffix}"
        )
=======
    if reason == "runtime_auto_owner":
        return "Runtime activo para este alias. Toma el thread manual o frena el runtime para usar IA y packs."
    if reason == "disqualified":
        return "Thread descalificado. El backend no acepta acciones manuales sobre este contacto."
    if reason == "runtime_closed":
        return "El thread esta cerrado mientras el runtime sigue activo. Frena el runtime o retoma manual antes de actuar."
>>>>>>> origin/main
    if reason == "runtime_manual_blocked":
        return "El thread es manual pero no cumple las reglas actuales para responder con el runtime activo."
    return (
        f"Cuenta emisora: @{str(thread.get('account_id') or '-').strip()}\n"
        f"Cliente: @{str(thread.get('recipient_username') or '-').strip() or '-'}"
    )


def _find_thread_row(rows: list[dict[str, Any]], thread_key: str) -> dict[str, Any] | None:
    clean_key = str(thread_key or "").strip()
    if not clean_key:
        return None
    for row in rows:
        if str(row.get("thread_key") or "").strip() == clean_key:
            return dict(row)
    return None


def _merge_thread_snapshot(
    selected_row: dict[str, Any] | None,
    service_thread: dict[str, Any] | None,
    *,
    messages: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if not isinstance(selected_row, dict) and not isinstance(service_thread, dict):
        return None

    payload: dict[str, Any] = {}
    if isinstance(selected_row, dict):
        payload.update(selected_row)
    if isinstance(service_thread, dict):
        for key, value in service_thread.items():
            if value in (None, "", [], ()):
                payload.setdefault(key, value)
                continue
            payload[key] = value

    recipient_username = str(
        payload.get("recipient_username")
        or payload.get("username")
        or ""
    ).strip().lstrip("@")
    if recipient_username:
        payload["recipient_username"] = recipient_username
        payload.setdefault("username", recipient_username)
    if not str(payload.get("account_id") or "").strip():
        payload["account_id"] = str(payload.get("account") or "").strip()
    if not str(payload.get("display_name") or "").strip():
        payload["display_name"] = (
            recipient_username
            or str(payload.get("thread_id") or "").strip()
            or "Conversacion"
        )
    payload["messages"] = [dict(row) for row in messages or [] if isinstance(row, dict)]
    return payload if payload else None
<<<<<<< HEAD


def _coerce_timestamp(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _format_clock(stamp: Any) -> str:
    resolved = _coerce_timestamp(stamp)
    if resolved is None or resolved <= 0:
        return "-"
    return datetime.fromtimestamp(resolved).strftime("%H:%M:%S")


def _projection_status_ui(*, updated_at: float | None, reason: str, ready: bool) -> dict[str, Any]:
    if updated_at is not None:
        label = f"Proyeccion local {_format_clock(updated_at)}"
    elif ready:
        label = "Proyeccion local lista"
    else:
        label = "Proyeccion local cargando"
    return {
        "label": label,
        "updated_at": updated_at,
        "reason": str(reason or "").strip(),
        "ready": bool(ready),
    }


def _remote_sync_status_ui(thread: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(thread, dict):
        return {
            "label": "Sync remota: selecciona un thread",
            "account_id": "",
            "account_last_sync_at": None,
            "thread_last_synced_at": None,
        }
    account_id = str(thread.get("account_id") or "").strip()
    account_last_sync_at = _coerce_timestamp(thread.get("account_last_sync_at"))
    thread_last_synced_at = _coerce_timestamp(thread.get("last_synced_at"))
    if account_last_sync_at is not None:
        label = (
            f"Sync remota @{account_id} {_format_clock(account_last_sync_at)}"
            if account_id
            else f"Sync remota {_format_clock(account_last_sync_at)}"
        )
    elif thread_last_synced_at is not None:
        label = f"Thread persistido {_format_clock(thread_last_synced_at)}"
    else:
        label = "Sync remota sin dato"
    return {
        "label": label,
        "account_id": account_id,
        "account_last_sync_at": account_last_sync_at,
        "thread_last_synced_at": thread_last_synced_at,
    }


def _latest_outbound_message(messages: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in reversed(messages):
        if str(row.get("direction") or "").strip().lower() == "outbound":
            return dict(row)
    return None


def _delivery_truth(message: dict[str, Any] | None) -> str:
    if not isinstance(message, dict):
        return ""
    status = str(message.get("delivery_status") or "").strip().lower()
    sent_status = str(message.get("sent_status") or "").strip().lower()
    if status in {"pending", "sending", "error"}:
        return status
    if sent_status in {"cancelled", "failed"}:
        return sent_status
    if sent_status in {"confirmed", "sent"} or status == "sent":
        return "confirmed"
    return ""


def _is_cancelled_reason(value: Any) -> bool:
    reason = str(value or "").strip().lower()
    if not reason:
        return False
    return any(
        token in reason
        for token in (
            "cancel",
            "runtime_stop",
            "runtime_stopping",
            "runtime_inactive",
            "takeover",
        )
    )


def _thread_truth_ui(
    thread: dict[str, Any] | None,
    *,
    messages: list[dict[str, Any]],
    runtime_status: dict[str, Any] | None,
    selected_runtime_alias: str,
) -> dict[str, Any]:
    if not isinstance(thread, dict):
        return {"code": "none", "label": "Sin seleccion", "detail": "", "alias_note": ""}
    sender_status = str(thread.get("sender_status") or "").strip().lower()
    pack_status = str(thread.get("pack_status") or "").strip().lower()
    thread_status = str(thread.get("thread_status") or "").strip().lower()
    thread_error = str(thread.get("thread_error") or "").strip()
    sender_error = str(thread.get("sender_error") or "").strip()
    pack_error = str(thread.get("pack_error") or "").strip()
    health = str(thread.get("account_health") or "healthy").strip().lower()
    needs_reply = bool(thread.get("needs_reply"))
    last_seen_text = str(thread.get("last_seen_text") or "").strip()
    last_direction = str(thread.get("last_message_direction") or "").strip().lower()
    runtime_payload = dict(runtime_status or {})
    latest_outbound = _latest_outbound_message(messages)
    delivery_truth = _delivery_truth(latest_outbound)
    thread_key = str(thread.get("thread_key") or "").strip()
    runtime_last_send_thread_key = str(runtime_payload.get("last_send_thread_key") or "").strip()
    runtime_last_send_outcome = str(runtime_payload.get("last_send_outcome") or "").strip().lower()
    runtime_last_send_reason = str(
        runtime_payload.get("last_send_reason") or runtime_payload.get("last_send_reason_code") or ""
    ).strip()
    thread_alias_id = _thread_alias_id(thread)
    clean_selected_alias = str(selected_runtime_alias or "").strip()
    alias_note = ""
    if thread_alias_id:
        if clean_selected_alias and clean_selected_alias.lower() != thread_alias_id.lower():
            alias_note = f"Runtime seleccionado @{clean_selected_alias} | alias del thread @{thread_alias_id}"
        elif clean_selected_alias:
            alias_note = f"Runtime y thread en @{thread_alias_id}"

    label = "Thread listo"
    code = "ready"
    detail = ""
    if health != "healthy":
        code = "account_error"
        label = "Cuenta con error"
        detail = str(thread.get("account_health_reason") or "").strip()
    elif thread_status == "opening":
        code = "opening"
        label = "Abriendo thread"
        detail = "Preparando apertura real del thread."
    elif sender_status == "preparing":
        code = "preparing"
        label = "Preparando thread"
        detail = "El sender esta preparando contexto antes de enviar."
    elif pack_status == "running":
        code = "pack_running"
        label = "Enviando pack"
        detail = "Hay un pack en curso."
    elif sender_status == "sending":
        code = "sending"
        label = "Enviando mensaje"
        detail = "El sender esta en envio activo."
    elif pack_status == "queued":
        code = "pack_queued"
        label = "Pack en cola"
        detail = "Hay un pack esperando turno."
    elif sender_status == "queued":
        code = "queued"
        label = "Mensaje en cola"
        detail = "Hay un mensaje local encolado."
    elif (
        runtime_last_send_thread_key == thread_key
        and runtime_last_send_outcome == "cancelled"
    ) or _is_cancelled_reason(sender_error) or _is_cancelled_reason(pack_error):
        code = "cancelled"
        label = "Envio cancelado"
        detail = runtime_last_send_reason or sender_error or pack_error
    elif thread_status == "failed" or sender_status == "failed" or pack_status == "failed":
        code = "failed"
        label = "Envio fallido"
        detail = thread_error or sender_error or pack_error
    elif thread_error:
        code = "thread_error"
        label = "Error del thread"
        detail = thread_error
    elif delivery_truth == "sending":
        code = "sending"
        label = "Enviando mensaje"
        detail = "El ultimo mensaje sigue en envio."
    elif delivery_truth == "pending":
        code = "queued"
        label = "Mensaje en cola"
        detail = "El ultimo mensaje todavia no salio del storage local."
    elif delivery_truth == "cancelled":
        code = "cancelled"
        label = "Envio cancelado"
        detail = runtime_last_send_reason or "cancelled"
    elif delivery_truth == "failed":
        code = "failed"
        label = "Envio fallido"
        detail = sender_error or pack_error or "failed"
    elif pack_status == "done":
        code = "confirmed"
        label = "Pack confirmado"
        detail = "El ultimo pack quedo confirmado en storage."
    elif delivery_truth == "confirmed" and last_direction == "outbound":
        code = "confirmed"
        label = "Envio confirmado"
        detail = "El ultimo outbound quedo confirmado en storage."
    elif needs_reply:
        code = "needs_reply"
        label = "Pendiente de respuesta"
        detail = "El ultimo inbound sigue sin respuesta confirmada."
    elif last_seen_text:
        code = "seen"
        label = "Sin pendiente"
        detail = last_seen_text
    elif last_direction == "outbound":
        code = "confirmed"
        label = "Sin pendiente"
        detail = "Ultimo movimiento confirmado: outbound."
    return {
        "code": code,
        "label": label,
        "detail": detail,
        "alias_note": alias_note,
    }
=======
>>>>>>> origin/main
