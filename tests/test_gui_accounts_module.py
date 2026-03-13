from __future__ import annotations
import base64
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QDialog, QPushButton

from gui.page_base import GuiState, PageContext
import gui.pages_accounts as pages_accounts_module
from gui.pages_accounts import (
    AccountSelectionDialog,
    AccountsActionsPage,
    AccountsPage,
    ImportAccountsDialog,
    ProxiesPage,
    WarmupStageEditorDialog,
)
from gui.query_runner import QueryManager
from gui.task_runner import LogStore, TaskManager
from runtime.runtime import STOP_EVENT
from src.content_publisher.content_library_service import ContentLibraryService


ROOT = Path(__file__).resolve().parents[1]
PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"
)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _pump_events(iterations: int = 4) -> None:
    app = _app()
    for _ in range(max(1, iterations)):
        app.processEvents()


def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.02) -> bool:
    deadline = time.time() + max(0.1, float(timeout or 0.1))
    while time.time() < deadline:
        if predicate():
            return True
        _pump_events(2)
        time.sleep(max(0.005, float(interval or 0.005)))
    _pump_events(4)
    return bool(predicate())


def _write_png(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(PNG_BYTES)
    return path


def _normalize_test_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@")


def test_import_accounts_dialog_uses_sequential_login_queue() -> None:
    _app()
    dialog = ImportAccountsDialog(8)
    try:
        assert dialog._concurrency.isReadOnly() is True
        assert dialog._concurrency.text() == "Secuencial"
        assert dialog.concurrency() == 1
    finally:
        dialog.close()


class _FakeAccountsService:
    def __init__(self) -> None:
        self.rows_by_alias: dict[str, list[dict[str, Any]]] = {
            "default": [
                {"username": "uno", "assigned_proxy_id": "px-1", "messages_per_account": 20, "alias": "default"},
                {"username": "dos", "assigned_proxy_id": "", "messages_per_account": 15, "alias": "default"},
            ],
            "matias": [
                {"username": "matias_a", "assigned_proxy_id": "px-2", "messages_per_account": 10, "alias": "matias"},
            ],
        }
        self.proxies = [
            {"id": "px-1", "server": "http://127.0.0.1:8080", "active": True},
            {"id": "px-2", "server": "http://127.0.0.1:8081", "active": True},
        ]
        self.manual_open_calls: list[dict[str, Any]] = []
        self.profile_open_calls: list[dict[str, Any]] = []
        self.rename_calls: list[tuple[str, str]] = []
        self.reels_runs: list[dict[str, Any]] = []
        self.add_account_calls: list[dict[str, Any]] = []
        self.manual_close_requests: list[str] = []
        self.manual_shutdown_count = 0
        self._block_manual_session = False
        self._manual_session_released = threading.Event()

    def list_aliases(self) -> list[str]:
        return ["default", "matias"]

    def list_accounts(self, alias: str | None = None) -> list[dict[str, Any]]:
        time.sleep(0.12)
        if alias is None:
            combined: list[dict[str, Any]] = []
            for rows in self.rows_by_alias.values():
                combined.extend(dict(row) for row in rows)
            return combined
        return [dict(row) for row in self.rows_by_alias.get(str(alias or ""), [])]

    def connected_status(self, record: dict[str, Any]) -> bool:
        return str(record.get("username") or "").strip() != "dos"

    def health_badge(self, record: dict[str, Any]) -> str:
        return "VIVA" if self.connected_status(record) else "NO ACTIVA"

    def manual_action_eligibility(self, record: dict[str, Any]) -> dict[str, Any]:
        connected = self.connected_status(record)
        return {
            "allowed": connected,
            "connected": connected,
            "badge": self.health_badge(record),
            "message": "" if connected else "Necesitas re-login en esta cuenta",
        }

    def remove_accounts(self, usernames: list[str]) -> int:
        selected = {str(item or "").strip().lower() for item in usernames}
        removed = 0
        for alias, rows in list(self.rows_by_alias.items()):
            kept = []
            for row in rows:
                if str(row.get("username") or "").strip().lower() in selected:
                    removed += 1
                    continue
                kept.append(row)
            self.rows_by_alias[alias] = kept
        return removed

    def set_message_limit(self, usernames: list[str], limit: int) -> int:
        selected = {str(item or "").strip().lower() for item in usernames}
        updated = 0
        for rows in self.rows_by_alias.values():
            for row in rows:
                if str(row.get("username") or "").strip().lower() in selected:
                    row["messages_per_account"] = int(limit)
                    updated += 1
        return updated

    def login(
        self,
        alias: str,
        usernames: list[str] | None = None,
        *,
        concurrency: int = 1,
    ) -> list[dict[str, Any]]:
        del alias, usernames, concurrency
        return [{"ok": True}]

    def relogin(
        self,
        alias: str,
        usernames: list[str] | None = None,
        *,
        concurrency: int = 1,
    ) -> list[dict[str, Any]]:
        del alias, usernames, concurrency
        return [{"ok": True}]

    def add_account(
        self,
        username: str,
        alias: str,
        *,
        password: str = "",
        proxy: dict[str, Any] | None = None,
        totp_secret: str = "",
    ) -> bool:
        self.add_account_calls.append(
            {
                "username": username,
                "alias": alias,
                "password": password,
                "proxy": proxy,
                "totp_secret": totp_secret,
            }
        )
        return True

    def list_proxy_records(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in self.proxies:
            payload = dict(item)
            payload.setdefault("last_public_ip", "127.0.0.1")
            payload.setdefault("last_latency_ms", 120.0)
            rows.append(payload)
        return rows

    def proxy_integrity_summary(self) -> dict[str, Any]:
        return {
            "total": len(self.proxies),
            "active": sum(1 for item in self.proxies if bool(item.get("active", True))),
            "assigned_accounts": 2,
            "invalid_assignments": 0,
        }

    def proxy_health_label(self, record: dict[str, Any]) -> str:
        if not bool(record.get("active", True)):
            return "Error"
        latency = float(record.get("last_latency_ms") or 120.0)
        return f"OK {latency:.0f} ms"

    def proxy_display_for_account(self, record: dict[str, Any]) -> dict[str, str]:
        assigned = str(record.get("assigned_proxy_id") or "").strip()
        if assigned:
            return {"status": "ok", "label": assigned}
        return {"status": "none", "label": "-"}

    def proxy_preflight_for_accounts(self, accounts: list[dict[str, Any]], **_kwargs) -> dict[str, Any]:
        return {
            "ready_accounts": [dict(item) for item in accounts],
            "blocked_accounts": [],
            "ready": len(accounts),
            "blocked": 0,
            "status_counts": {},
            "blocked_status_counts": {},
        }

    def upsert_proxy(self, record: dict[str, Any]) -> dict[str, Any]:
        return dict(record)

    def import_proxies_csv(self, path: str) -> dict[str, Any]:
        del path
        return {"imported": 0}

    def delete_proxy(self, proxy_id: str) -> int:
        del proxy_id
        return 1

    def toggle_proxy_active(self, proxy_id: str, *, active: bool) -> dict[str, Any]:
        return {"id": proxy_id, "active": active}

    def test_proxy(self, proxy_id: str) -> dict[str, Any]:
        return {"proxy_id": proxy_id, "public_ip": "127.0.0.1", "latency": 10, "health_label": "OK 10 ms"}

    def sweep_proxy_health(self, **_kwargs) -> dict[str, Any]:
        return {"checked": 0, "succeeded": 0, "failed": 0, "results": []}

    def assign_proxy(self, usernames: list[str], proxy_id: str) -> int:
        selected = {str(item or "").strip().lower() for item in usernames}
        updated = 0
        for rows in self.rows_by_alias.values():
            for row in rows:
                if str(row.get("username") or "").strip().lower() in selected:
                    row["assigned_proxy_id"] = proxy_id
                    updated += 1
        return updated

    def open_manual_sessions(
        self,
        alias: str,
        usernames: list[str],
        *,
        start_url: str,
        action_label: str,
        max_minutes: int = 0,
        restore_page_if_closed: bool = False,
    ) -> dict[str, Any]:
        time.sleep(0.03)
        payload = {
            "alias": alias,
            "usernames": list(usernames),
            "start_url": start_url,
            "action_label": action_label,
            "max_minutes": max_minutes,
            "restore_page_if_closed": restore_page_if_closed,
        }
        self.manual_open_calls.append(payload)
        if self._block_manual_session:
            self._manual_session_released.wait(timeout=2.0)
        return payload

    def open_profile_sessions(
        self,
        alias: str,
        usernames: list[str],
        *,
        action_label: str = "Otros cambios",
        max_minutes: int = 0,
    ) -> dict[str, Any]:
        time.sleep(0.03)
        payload = {
            "alias": alias,
            "usernames": list(usernames),
            "action_label": action_label,
            "max_minutes": max_minutes,
        }
        self.profile_open_calls.append(payload)
        if self._block_manual_session:
            self._manual_session_released.wait(timeout=2.0)
        return payload

    def clear_manual_session_close_request(self, username: str) -> None:
        del username

    def close_manual_session(self, username: str) -> bool:
        self.manual_close_requests.append(_normalize_test_username(username))
        self._manual_session_released.set()
        return True

    def shutdown_manual_sessions(self) -> None:
        self.manual_shutdown_count += 1
        self._manual_session_released.set()

    def rename_account_username(self, old_username: str, new_username: str) -> str:
        old_clean = str(old_username or "").strip().lstrip("@")
        new_clean = str(new_username or "").strip().lstrip("@")
        for alias, rows in self.rows_by_alias.items():
            for row in rows:
                if str(row.get("username") or "").strip().lower() == old_clean.lower():
                    row["username"] = new_clean
                    row["alias"] = alias
                    self.rename_calls.append((old_clean, new_clean))
                    return new_clean
        raise RuntimeError(f"No existe @{old_clean}")

    def run_reels_playwright(
        self,
        alias: str,
        usernames: list[str],
        *,
        minutes: int = 10,
        likes_target: int = 0,
    ) -> list[dict[str, Any]]:
        selected = list(usernames)
        self.reels_runs.append(
            {
                "alias": alias,
                "usernames": selected,
                "minutes": minutes,
                "likes_target": likes_target,
            }
        )
        viewed = 0
        liked = 0
        while viewed < 6 and not STOP_EVENT.is_set():
            viewed += 1
            print(f"{selected[0]} viendo reel")
            if likes_target > liked:
                liked += 1
                print(f"{selected[0]} dio like")
            time.sleep(0.04)
        return [
            {
                "username": selected[0],
                "viewed": viewed,
                "liked": liked,
                "errors": 0,
                "messages": [],
            }
        ]


class _FakeWarmupService:
    def __init__(self) -> None:
        self._flows: dict[int, dict[str, Any]] = {}
        self._next_flow_id = 1
        self._next_stage_id = 10
        self._next_action_id = 100
        self._next_log_id = 1
        self._seed_flow("default", "Warm Up Default", ["uno", "dos"])
        self._seed_flow("matias", "Warm Up Matias", ["matias_a"])

    def ensure_default_flow(self, alias: str | None) -> dict[str, Any]:
        clean_alias = str(alias or "default").strip() or "default"
        existing = next(
            (
                flow
                for flow in self._flows.values()
                if str(flow.get("alias") or "").strip().lower() == clean_alias.lower()
            ),
            None,
        )
        if existing is None:
            existing = self.create_flow(alias=clean_alias, usernames=["uno"], name="Warm Up")
        return self.get_flow(int(existing.get("id") or 0))

    def create_flow(
        self,
        *,
        alias: str | None,
        usernames: list[str] | tuple[str, ...],
        name: str | None = None,
    ) -> dict[str, Any]:
        clean_alias = str(alias or "default").strip() or "default"
        selected = [
            _normalize_test_username(item)
            for item in usernames
            if _normalize_test_username(item)
        ]
        desired_name = str(name or "").strip() or "Flujo Warm Up"
        existing_names = {
            str(flow.get("name") or "").strip().lower()
            for flow in self._flows.values()
            if str(flow.get("alias") or "").strip().lower() == clean_alias.lower()
        }
        final_name = desired_name
        if final_name.lower() in existing_names:
            index = 2
            while f"{desired_name} {index}".lower() in existing_names:
                index += 1
            final_name = f"{desired_name} {index}"
        flow_id = self._next_flow_id
        self._next_flow_id += 1
        self._flows[flow_id] = {
            "id": flow_id,
            "alias": clean_alias,
            "name": final_name,
            "status": "paused",
            "has_started": False,
            "selected_accounts": [
                {"flow_id": flow_id, "account_order": index, "username": username}
                for index, username in enumerate(selected, start=1)
            ],
            "stages": [],
            "account_states": [],
            "resume": {
                "status": "paused",
                "has_started": False,
                "current_stage_order": 1,
                "current_action_order": 1,
                "last_account": "",
                "last_action_type": "",
            },
            "logs": [],
        }
        self.append_log(flow_id, f"Flujo creado para alias {clean_alias} con {len(selected)} cuenta(s).")
        return self.get_flow(flow_id)

    def list_flows(self, alias: str | None = None) -> list[dict[str, Any]]:
        clean_alias = str(alias or "").strip()
        flows = []
        for flow in self._flows.values():
            if clean_alias and str(flow.get("alias") or "").strip().lower() != clean_alias.lower():
                continue
            flows.append(
                {
                    "id": int(flow.get("id") or 0),
                    "alias": str(flow.get("alias") or "").strip(),
                    "name": str(flow.get("name") or "").strip(),
                    "status": str(flow.get("status") or "paused").strip() or "paused",
                    "has_started": bool(flow.get("has_started")),
                    "current_stage_order": int(flow.get("resume", {}).get("current_stage_order") or 1),
                    "current_action_order": int(flow.get("resume", {}).get("current_action_order") or 1),
                    "last_account": str(flow.get("resume", {}).get("last_account") or "").strip(),
                    "stages_count": len(flow.get("stages") or []),
                    "account_count": len(flow.get("selected_accounts") or []),
                    "log_count": len(flow.get("logs") or []),
                    "created_at": "",
                    "updated_at": "",
                }
            )
        return flows

    def get_flow(self, flow_id: int) -> dict[str, Any]:
        flow = self._flow_by_id(flow_id)
        return self._clone(flow) if flow is not None else {}

    def save_stage(
        self,
        flow_id: int,
        *,
        stage_id: int | None = None,
        title: str,
        settings: dict[str, Any] | None = None,
        actions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        flow = self._flow_by_id(flow_id)
        if flow is None:
            raise RuntimeError("flow_not_found")
        clean_stage_id = int(stage_id or 0)
        stage_rows = flow["stages"]
        target = next((item for item in stage_rows if int(item.get("id") or 0) == clean_stage_id), None)
        if target is None:
            target = {
                "id": self._next_stage_id,
                "stage_order": len(stage_rows) + 1,
                "title": "",
                "settings": {},
                "actions": [],
            }
            self._next_stage_id += 1
            stage_rows.append(target)
        target["title"] = str(title or "").strip() or f"Dia {int(target.get('stage_order') or 1)}"
        target["settings"] = dict(settings or {})
        target["actions"] = []
        for index, action in enumerate(actions or [], start=1):
            payload = dict(action)
            target["actions"].append(
                {
                    "id": self._next_action_id,
                    "stage_id": int(target["id"]),
                    "action_order": index,
                    "action_type": str(payload.get("action_type") or "watch_reels").strip(),
                    "payload": {
                        "target": str(payload.get("target") or "").strip(),
                        "text": str(payload.get("text") or "").strip(),
                        "quantity": int(payload.get("quantity") or 1),
                    },
                    "target": str(payload.get("target") or "").strip(),
                    "text": str(payload.get("text") or "").strip(),
                    "quantity": int(payload.get("quantity") or 1),
                }
            )
            self._next_action_id += 1
        return self.get_flow(int(flow.get("id") or 0))

    def delete_stage(self, stage_id: int) -> dict[str, Any]:
        clean_stage_id = int(stage_id or 0)
        for flow in self._flows.values():
            kept = [stage for stage in flow["stages"] if int(stage.get("id") or 0) != clean_stage_id]
            if len(kept) == len(flow["stages"]):
                continue
            for index, stage in enumerate(kept, start=1):
                stage["stage_order"] = index
            flow["stages"] = kept
            return self.get_flow(int(flow.get("id") or 0))
        return {}

    def delete_flow(self, flow_id: int) -> bool:
        return self._flows.pop(int(flow_id or 0), None) is not None

    def append_log(self, flow_id: int, message: str, *, level: str = "info") -> dict[str, Any]:
        flow = self._flow_by_id(flow_id)
        if flow is None:
            return {}
        row = {
            "id": self._next_log_id,
            "flow_id": int(flow_id or 0),
            "level": str(level or "info").strip() or "info",
            "message": str(message or "").strip(),
            "created_at": f"2026-03-10 02:{self._next_log_id:02d}:00",
        }
        self._next_log_id += 1
        flow.setdefault("logs", []).append(row)
        return dict(row)

    def list_logs(self, flow_id: int) -> list[dict[str, Any]]:
        flow = self._flow_by_id(flow_id)
        if flow is None:
            return []
        return [dict(item) for item in flow.get("logs") or []]

    def read_logs_after(self, flow_id: int, last_log_id: int = 0) -> tuple[int, list[dict[str, Any]]]:
        rows = [row for row in self.list_logs(flow_id) if int(row.get("id") or 0) > int(last_log_id or 0)]
        next_id = int(last_log_id or 0)
        if rows:
            next_id = int(rows[-1].get("id") or next_id)
        return next_id, rows

    def mark_flow_running(
        self,
        flow_id: int,
        *,
        stage_order: int | None = None,
        action_order: int | None = None,
        last_account: str = "",
    ) -> dict[str, Any]:
        flow = self._flow_by_id(flow_id)
        if flow is None:
            return {}
        flow["status"] = "running"
        flow["has_started"] = True
        flow["resume"]["status"] = "running"
        flow["resume"]["has_started"] = True
        flow["resume"]["current_stage_order"] = int(stage_order or flow["resume"].get("current_stage_order") or 1)
        flow["resume"]["current_action_order"] = int(action_order or flow["resume"].get("current_action_order") or 1)
        if last_account:
            flow["resume"]["last_account"] = str(last_account or "").strip()
        return self.get_flow(int(flow.get("id") or 0))

    def record_account_state(
        self,
        flow_id: int,
        username: str,
        *,
        stage_order: int,
        action_order: int,
        last_action_type: str = "",
        status: str = "paused",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        flow = self._flow_by_id(flow_id)
        if flow is None:
            return {}
        clean_username = _normalize_test_username(username)
        states = flow.setdefault("account_states", [])
        state = next((item for item in states if str(item.get("username") or "").strip().lower() == clean_username.lower()), None)
        if state is None:
            state = {"username": clean_username}
            states.append(state)
        state.update(
            {
                "flow_id": int(flow_id or 0),
                "username": clean_username,
                "status": str(status or "paused").strip() or "paused",
                "current_stage_order": int(stage_order or 1),
                "current_action_order": int(action_order or 1),
                "last_action_type": str(last_action_type or "").strip(),
                "payload": dict(payload or {}),
                "updated_at": "2026-03-10 02:00:00",
            }
        )
        flow["status"] = str(status or "paused").strip() or "paused"
        flow["has_started"] = True
        flow["resume"] = {
            "status": flow["status"],
            "has_started": True,
            "current_stage_order": int(stage_order or 1),
            "current_action_order": int(action_order or 1),
            "last_account": clean_username,
            "last_action_type": str(last_action_type or "").strip(),
        }
        return self.get_flow(int(flow.get("id") or 0))

    def pause_flow(self, flow_id: int, *, reason: str = "paused") -> dict[str, Any]:
        flow = self._flow_by_id(flow_id)
        if flow is None:
            return {}
        flow["status"] = "paused"
        flow["has_started"] = True
        flow["resume"]["status"] = "paused"
        flow["resume"]["has_started"] = True
        self.append_log(flow_id, f"Flujo pausado: {reason}.", level="warning")
        return self.get_flow(int(flow.get("id") or 0))

    def pause_active_flows(self, reason: str = "application closing") -> int:
        paused = 0
        for flow in self._flows.values():
            if str(flow.get("status") or "").strip().lower() != "running":
                continue
            flow["status"] = "paused"
            flow["has_started"] = True
            flow["resume"]["status"] = "paused"
            flow["resume"]["has_started"] = True
            self.append_log(int(flow.get("id") or 0), f"Flujo pausado: {reason}.", level="warning")
            paused += 1
        return paused

    def _seed_flow(self, alias: str, name: str, usernames: list[str]) -> None:
        flow = self.create_flow(alias=alias, usernames=usernames, name=name)
        flow = self.save_stage(
            flow["id"],
            title="Dia 1",
            settings={"base_delay_minutes": 20},
            actions=[{"action_type": "watch_reels", "target": "", "text": "", "quantity": 8}],
        )
        self.save_stage(
            flow["id"],
            title="Dia 2",
            settings={"base_delay_minutes": 30},
            actions=[{"action_type": "like_posts", "target": "", "text": "", "quantity": 2}],
        )

    def _flow_by_id(self, flow_id: int) -> dict[str, Any] | None:
        return self._flows.get(int(flow_id or 0))

    def _clone(self, flow: dict[str, Any]) -> dict[str, Any]:
        import copy

        data = copy.deepcopy(flow)
        data["selected_accounts"] = [
            dict(item)
            for item in data.get("selected_accounts") or []
            if isinstance(item, dict)
        ]
        data["selected_usernames"] = [item["username"] for item in data["selected_accounts"]]
        data["account_count"] = len(data["selected_accounts"])
        data["accounts"] = [dict(item) for item in data.get("account_states") or [] if isinstance(item, dict)]
        data["account_states"] = [dict(item) for item in data.get("account_states") or [] if isinstance(item, dict)]
        data["latest_log_id"] = int(data.get("logs", [{}])[-1].get("id") or 0) if data.get("logs") else 0
        return data


def _build_ctx():
    STOP_EVENT.clear()
    services = SimpleNamespace(accounts=_FakeAccountsService(), warmup=_FakeWarmupService())
    logs = LogStore()
    tasks = TaskManager(logs)
    queries = QueryManager()
    ctx = PageContext(
        services=services,
        tasks=tasks,
        logs=logs,
        queries=queries,
        state=GuiState(),
        open_route=lambda route, payload=None: None,
        go_back=lambda: None,
        can_go_back=lambda: False,
    )
    return services, logs, tasks, queries, ctx


def test_accounts_page_applies_latest_alias_after_inflight_refresh() -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = AccountsPage(ctx)
    try:
        page.on_navigate_to()
        ctx.state.active_alias = "matias"
        page.refresh_table()

        assert _wait_until(
            lambda: page._table.rowCount() == 1
            and page._table.item(0, 0) is not None
            and page._table.item(0, 0).text() == "@matias_a"
        )
        assert "Alias activo: matias" in page._summary.text()
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_page_rejects_manual_add_without_password(monkeypatch) -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = AccountsPage(ctx)

    class _FakeDialog:
        def __init__(self, *_args, **_kwargs) -> None:
            return None

        def refresh_aliases(self, aliases: list[str], current_alias: str) -> None:
            del aliases, current_alias

        def exec(self) -> int:
            return QDialog.Accepted

        def username(self) -> str:
            return "nuevo"

        def password(self) -> str:
            return ""

        def totp_secret(self) -> str:
            return ""

        def alias(self) -> str:
            return "default"

    try:
        monkeypatch.setattr(pages_accounts_module, "AddAccountDialog", _FakeDialog)
        page._show_modal_message = lambda *args, **kwargs: None  # type: ignore[method-assign]

        page._open_add_account_dialog()

        assert services.accounts.add_account_calls == []
        assert page._status_label.text() == "Username, password y alias son obligatorios."
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_page_summarizes_login_completion_with_invalid_accounts() -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = AccountsPage(ctx)
    try:
        summary = page._summarize_login_completion(
            "accounts_login",
            ok=True,
            message="",
            result=[
                {"username": "ok_one", "status": "ok"},
                {"username": "missing", "status": "failed", "message": "missing_password"},
            ],
        )

        assert summary == "Login finalizado: 1 correctas, 1 con error. Sin password guardado: 1."
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_actions_manual_username_flow_updates_internal_record() -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = AccountsActionsPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: len(page._records) == 2)

        responses = iter([("matias_a_nuevo", True)])
        page._open_account_selector = lambda **kwargs: {  # type: ignore[method-assign]
            "alias": "matias",
            "usernames": ["matias_a"],
        }
        page.prompt_text = lambda **kwargs: next(responses)  # type: ignore[method-assign]

        page._start_manual_sequence("username")

        assert _wait_until(
            lambda: len(services.accounts.manual_open_calls) == 1
            and not tasks.is_running("accounts_manual_action")
            and not page._manual_queue
        )
        assert services.accounts.manual_open_calls[0]["alias"] == "matias"
        assert services.accounts.rename_calls == [("matias_a", "matias_a_nuevo")]
        assert "@matias_a_nuevo" in page._manual_log.toPlainText()
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_account_selection_dialog_blocks_non_operable_manual_accounts() -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    dialog = AccountSelectionDialog(ctx, require_manual_action_ready=True)
    try:
        dialog.refresh_aliases(["default", "matias"], "default")
        assert _wait_until(lambda: dialog._accounts.count() == 2)

        items = [dialog._accounts.item(index) for index in range(dialog._accounts.count())]
        enabled_items = [item for item in items if item is not None and bool(item.flags() & Qt.ItemIsEnabled)]
        disabled_items = [item for item in items if item is not None and not bool(item.flags() & Qt.ItemIsEnabled)]
        assert len(enabled_items) == 1
        assert len(disabled_items) == 1

        dialog._mark_all()

        assert "Necesitas re-login en esta cuenta" in disabled_items[0].text()
        assert enabled_items[0].checkState() == Qt.Checked
        assert disabled_items[0].checkState() == Qt.Unchecked
    finally:
        dialog.close()
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_actions_navigate_away_closes_manual_session_and_cancels_sequence() -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = AccountsActionsPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: len(page._records) == 2)

        services.accounts._block_manual_session = True
        page._open_account_selector = lambda **kwargs: {  # type: ignore[method-assign]
            "alias": "matias",
            "usernames": ["matias_a"],
        }
        page.prompt_text = lambda **kwargs: (_ for _ in ()).throw(AssertionError("prompt no esperado"))  # type: ignore[method-assign]

        page._start_manual_sequence("username")

        assert _wait_until(
            lambda: len(services.accounts.manual_open_calls) == 1
            and tasks.is_running("accounts_manual_action")
        )

        page.on_navigate_from()

        assert _wait_until(
            lambda: not tasks.is_running("accounts_manual_action")
            and page._manual_abort_requested is False
            and page._manual_current_username == ""
        )
        assert services.accounts.manual_close_requests == ["matias_a"]
        assert page._manual_current_username == ""
        assert page._manual_queue == []
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_actions_view_content_streams_logs_and_stops_cleanly() -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = AccountsActionsPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: len(page._records) == 2)
        page._show_accounts_module("view_content")

        page._open_account_selector = lambda **kwargs: {  # type: ignore[method-assign]
            "alias": "matias",
            "usernames": ["matias_a"],
        }
        page._select_view_accounts()
        page._start_view_content()

        assert _wait_until(lambda: "viendo reel" in page._view_log.toPlainText(), timeout=2.5)
        page._stop_view_content()
        assert _wait_until(lambda: not tasks.is_running("accounts_view_content"), timeout=2.5)
        assert _wait_until(lambda: page._view_stop_button.isEnabled() is False)
        assert services.accounts.reels_runs[0]["alias"] == "matias"
        assert _wait_until(lambda: "Resumen @matias_a" in page._view_log.toPlainText(), timeout=2.5)
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_actions_content_module_extracts_and_publishes_via_controller(tmp_path: Path) -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = AccountsActionsPage(ctx)
    library = ContentLibraryService(root_dir=tmp_path)

    class _FakeContentApi:
        def __init__(self) -> None:
            self.extract_calls: list[dict[str, Any]] = []
            self.publish_calls: list[dict[str, Any]] = []

        def extract(
            self,
            *,
            alias: str,
            account_ids: list[str],
            profile_urls: list[str],
            posts_per_profile: int,
        ) -> dict[str, Any]:
            self.extract_calls.append(
                {
                    "alias": alias,
                    "account_ids": list(account_ids),
                    "profile_urls": list(profile_urls),
                    "posts_per_profile": posts_per_profile,
                }
            )
            entry = library.store_media_entry(
                source_profile="profile_demo",
                media_type="image",
                media_files=[_write_png(tmp_path / "fixtures" / "extract_image.png")],
                caption="Caption demo",
                entry_key="extract_demo",
            )
            return {
                "summary": "Se guardaron 1 publicaciones de 1/1 perfiles.",
                "logs": [f"Guardado @profile_demo / image en {entry['media_path']}."],
                "stored_count": 1,
            }

        def publish(self, *, account_id: str, media_path: str, caption: str) -> dict[str, Any]:
            self.publish_calls.append(
                {
                    "account_id": account_id,
                    "media_path": media_path,
                    "caption": caption,
                }
            )
            return {
                "summary": f"Contenido publicado correctamente en @{account_id}.",
                "logs": [
                    f"Preparando publicacion en @{account_id}...",
                    "Carga completada.",
                    "Contenido publicado correctamente.",
                ],
            }

    fake_api = _FakeContentApi()
    page._content_controller._library_service = library
    page._content_controller._api_client = fake_api
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: len(page._records) == 2)
        page._content_controller.refresh_account_context(
            active_alias="default",
            aliases=["default", "matias"],
            rows=page._records,
        )

        page._open_content_module()
        assert page._content_stack.currentIndex() == 1

        page._content_controller.show_extract()
        page._content_controller._extract_accounts_list.item(0).setCheckState(Qt.Checked)
        page._content_controller._extract_urls.setPlainText("https://instagram.com/profile_demo")
        page._content_controller._start_extract()

        assert _wait_until(
            lambda: not tasks.is_running("content_extract")
            and "Se guardaron 1 publicaciones" in page._content_controller._extract_result.text(),
            timeout=2.5,
        )
        assert fake_api.extract_calls[0]["alias"] == "default"

        page._content_controller.show_publish()
        assert _wait_until(lambda: page._content_controller._publish_gallery.count() == 1)
        page._content_controller._publish_gallery.setCurrentRow(0)
        page._content_controller._start_publish()

        assert _wait_until(
            lambda: not tasks.is_running("content_publish")
            and "Contenido publicado correctamente." in page._content_controller._publish_log.toPlainText(),
            timeout=2.5,
        )
        assert fake_api.publish_calls[0]["account_id"] == str(
            page._content_controller._publish_account_combo.currentData() or ""
        )
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_actions_warmup_loads_selected_flow_and_swaps_log_panel() -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = AccountsActionsPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: len(page._records) == 2)

        first_flow = services.warmup.create_flow(
            alias="matias",
            usernames=["matias_a"],
            name="Flujo Matias A",
        )
        first_flow = services.warmup.save_stage(
            first_flow["id"],
            title="Dia 1",
            settings={"base_delay_minutes": 20},
            actions=[{"action_type": "watch_reels", "target": "", "text": "", "quantity": 5}],
        )
        services.warmup.append_log(first_flow["id"], "Linea flujo 1")

        second_flow = services.warmup.create_flow(
            alias="default",
            usernames=["uno"],
            name="Flujo Default B",
        )
        second_flow = services.warmup.save_stage(
            second_flow["id"],
            title="Dia 1",
            settings={"base_delay_minutes": 20},
            actions=[{"action_type": "like_posts", "target": "", "text": "", "quantity": 2}],
        )
        services.warmup.append_log(second_flow["id"], "Linea flujo 2")

        page._load_warmup_flow(first_flow["id"])
        page._show_accounts_module("warmup")

        assert page._modules_stack.currentIndex() == 2
        assert "Flujo Matias A" in page._warmup_flow_summary_label.text()
        assert "Linea flujo 1" in page._warmup_log.toPlainText()
        assert "Comenzar" == page._warmup_start_button.text()

        page._load_warmup_flow(second_flow["id"])

        assert "Flujo Default B" in page._warmup_flow_summary_label.text()
        assert "Linea flujo 2" in page._warmup_log.toPlainText()
        assert "Linea flujo 1" not in page._warmup_log.toPlainText()
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_actions_warmup_switches_start_button_to_resume_after_activity() -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = AccountsActionsPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: len(page._records) == 2)

        flow = services.warmup.create_flow(
            alias="matias",
            usernames=["matias_a"],
            name="Flujo Reanudar",
        )
        flow = services.warmup.save_stage(
            flow["id"],
            title="Dia 1",
            settings={"base_delay_minutes": 20},
            actions=[{"action_type": "watch_reels", "target": "", "text": "", "quantity": 5}],
        )
        page._load_warmup_flow(flow["id"])
        page._show_accounts_module("warmup")

        assert page._warmup_start_button.text() == "Comenzar"

        services.warmup.mark_flow_running(flow["id"], stage_order=1, action_order=1, last_account="matias_a")
        services.warmup.pause_flow(flow["id"], reason="manual")
        page._load_warmup_flow(flow["id"])

        assert page._warmup_start_button.text() == "Reanudar"
        assert "Flujo pausado" in page._warmup_log.toPlainText()
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_warmup_stage_editor_uses_action_cards_with_readable_defaults() -> None:
    _app()
    dialog = WarmupStageEditorDialog(
        {
            "title": "Dia 4",
            "settings": {"base_delay_minutes": 20},
            "actions": [
                {
                    "action_type": "watch_reels",
                    "target": "",
                    "text": "",
                    "quantity": 5,
                }
            ],
        }
    )
    try:
        assert dialog.minimumWidth() >= 820
        assert len(dialog._action_cards) == 1
        assert dialog._action_cards[0].action_payload()["action_type"] == "watch_reels"
        assert dialog._action_cards[0]._text_input.minimumHeight() >= 88
    finally:
        dialog.close()


def test_proxies_page_uses_account_assignment_wording() -> None:
    _app()
    services, logs, tasks, queries, ctx = _build_ctx()
    page = ProxiesPage(ctx)
    try:
        page.refresh_page()
        buttons = {button.text() for button in page.findChildren(QPushButton)}
        assert "Asignar proxy a cuentas" in buttons
        headers = [
            str(page._table.horizontalHeaderItem(index).text())
            for index in range(page._table.columnCount())
        ]
        assert headers == ["Proxy", "Endpoint", "Salida", "Estado", "Salud", "Cuentas asignadas"]
        assert "Invalidas: 0" in page._integrity_label.text()
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_module_avoids_default_qt_dialogs() -> None:
    source = (ROOT / "gui" / "pages_accounts.py").read_text(encoding="utf-8")
    assert "QInputDialog" not in source
    assert "QMessageBox" not in source
    assert "getOpenFileName(" not in source
    assert "AccountsAlertDialog" in source
