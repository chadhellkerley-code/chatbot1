from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from application.services import AutomationService, ServiceContext
from core import responder


def test_start_autoresponder_returns_inbox_only_snapshot_without_activating_runtime(monkeypatch) -> None:
    service = AutomationService(ServiceContext.default(Path.cwd()))
    monkeypatch.setattr(
        service,
        "alias_account_rows",
        lambda alias: [{"username": "acct-1", "proxy": "", "connected": True}],
    )
    monkeypatch.setattr(
        responder,
        "_activate_bot",
        lambda: (_ for _ in ()).throw(AssertionError("legacy activation should stay disconnected")),
    )

    snapshot = service.start_autoresponder({"alias": "alias-demo"})

    assert snapshot["status"] == "Idle"
    assert snapshot["message"] == "El runtime de autoresponder/follow-up ahora se administra solo desde Inbox."
    assert snapshot["task_active"] is False


def test_start_autoresponder_preserves_blocked_accounts_in_wrapper_snapshot(monkeypatch) -> None:
    service = AutomationService(ServiceContext.default(Path.cwd()))
    monkeypatch.setattr(
        service,
        "alias_account_rows",
        lambda alias: [
            {"username": "acct-1", "proxy": "proxy-a", "connected": True},
            {
                "username": "acct-2",
                "proxy": "proxy-b",
                "connected": True,
                "blocked": True,
                "blocked_reason": "Proxy en cuarentena",
                "safety_state": "blocked",
                "safety_message": "Proxy en cuarentena",
            },
        ],
    )

    snapshot = service.start_autoresponder({"alias": "alias-demo"})

    blocked_row = next(row for row in snapshot["account_rows"] if row["account"] == "acct-2")
    assert snapshot["accounts_total"] == 2
    assert snapshot["accounts_active"] == 1
    assert snapshot["accounts_blocked"] == 1
    assert blocked_row["blocked"] is True
    assert blocked_row["blocked_reason"] == "Proxy en cuarentena"
    assert blocked_row["safety_state"] == "blocked"
    assert blocked_row["safety_message"] == "Proxy en cuarentena"


def test_start_autoresponder_never_uses_legacy_activation_even_when_accounts_are_safe(monkeypatch) -> None:
    service = AutomationService(ServiceContext.default(Path.cwd()))
    monkeypatch.setattr(
        service,
        "alias_account_rows",
        lambda alias: [
            {
                "username": "acct-1",
                "proxy": "proxy-a",
                "connected": True,
                "blocked": False,
                "blocked_reason": "",
                "safety_state": "usable",
                "safety_message": "Lista",
            }
        ],
    )
    monkeypatch.setattr(
        responder,
        "_activate_bot",
        lambda: (_ for _ in ()).throw(AssertionError("legacy activation should not start")),
    )

    snapshot = service.start_autoresponder({"alias": "alias-demo"})

    assert snapshot["status"] == "Idle"
    assert snapshot["task_active"] is False
    assert snapshot["message"] == "El runtime de autoresponder/follow-up ahora se administra solo desde Inbox."
    assert snapshot["accounts_active"] == 1
    assert snapshot["accounts_blocked"] == 0


def test_autoresponder_snapshot_preserves_preflight_blocked_rows_without_runtime(monkeypatch) -> None:
    service = AutomationService(ServiceContext.default(Path.cwd()))
    monkeypatch.setattr(service, "_autoresponder_targets", lambda alias: ["acct-1", "acct-2"])
    monkeypatch.setattr(
        service,
        "alias_account_rows",
        lambda alias: [
            {"username": "acct-1", "proxy": "proxy-a", "connected": True},
            {"username": "acct-2", "proxy": "proxy-b", "connected": True},
        ],
    )
    monkeypatch.setattr(responder, "_get_autoresponder_runtime_controller", lambda: None)
    service._update_autoresponder_state(
        {
            "alias": "alias-demo",
            "status": "Starting",
            "task_active": True,
            "account_rows": [
                {"account": "acct-1", "proxy": "proxy-a", "blocked": False},
                {
                    "account": "acct-2",
                    "proxy": "proxy-b",
                    "blocked": True,
                    "blocked_reason": "proxy quarantined",
                },
            ],
        },
        replace=True,
    )

    snapshot = service.autoresponder_snapshot("alias-demo")

    blocked_row = next(row for row in snapshot["account_rows"] if row["account"] == "acct-2")
    assert snapshot["accounts_total"] == 2
    assert snapshot["accounts_active"] == 1
    assert snapshot["accounts_blocked"] == 1
    assert blocked_row["blocked"] is True
    assert blocked_row["blocked_reason"] == "proxy quarantined"


def test_alias_account_rows_marks_proxy_preflight_blocked_accounts(monkeypatch) -> None:
    service = AutomationService(ServiceContext.default(Path.cwd()))
    monkeypatch.setattr(
        service,
        "_all_account_records",
        lambda: [
            {"username": "acct-1", "alias": "alias-demo", "active": True, "assigned_proxy_id": "proxy-a"},
            {"username": "acct-2", "alias": "alias-demo", "active": True, "assigned_proxy_id": "proxy-b"},
        ],
    )
    monkeypatch.setattr(
        responder,
        "_inspect_startable_accounts",
        lambda targets, log_skipped=False: {
            "startable_accounts": ["acct-1"],
            "account_statuses": [
                {
                    "username": "acct-1",
                    "blocked": False,
                    "safety_state": "usable",
                    "message": "Lista",
                },
                {
                    "username": "acct-2",
                    "blocked": True,
                    "safety_state": "blocked",
                    "reason": "proxy_quarantined",
                    "message": "Proxy en cuarentena",
                }
            ],
            "skipped_accounts": [
                {
                    "username": "acct-2",
                    "source": "proxy",
                    "status": "blocked",
                    "reason": "proxy_quarantined",
                    "message": "Proxy en cuarentena",
                }
            ],
        },
    )

    rows = service.alias_account_rows("alias-demo")

    assert rows[0]["blocked"] is False
    assert rows[1]["blocked"] is True
    assert rows[1]["blocked_reason"] == "Proxy en cuarentena"
    assert rows[1]["safety_state"] == "blocked"
    assert rows[1]["safety_message"] == "Proxy en cuarentena"


def test_alias_account_rows_excludes_usage_deactivated_accounts(monkeypatch) -> None:
    service = AutomationService(ServiceContext.default(Path.cwd()))
    monkeypatch.setattr(
        service,
        "_all_account_records",
        lambda: [
            {"username": "acct-1", "alias": "alias-demo", "active": True, "usage_state": "active"},
            {"username": "acct-2", "alias": "alias-demo", "active": True, "usage_state": "deactivated"},
        ],
    )
    monkeypatch.setattr(
        responder,
        "_inspect_startable_accounts",
        lambda targets, log_skipped=False: {
            "startable_accounts": list(targets),
            "account_statuses": [
                {
                    "username": target,
                    "blocked": False,
                    "safety_state": "usable",
                    "message": "Lista",
                }
                for target in targets
            ],
            "skipped_accounts": [],
        },
    )

    rows = service.alias_account_rows("alias-demo")

    assert [str(row.get("username") or "") for row in rows] == ["acct-1"]


def test_max_alias_concurrency_ignores_proxy_preflight_blocked_accounts(monkeypatch) -> None:
    service = AutomationService(ServiceContext.default(Path.cwd()))
    monkeypatch.setattr(
        service,
        "_all_account_records",
        lambda: [
            {"username": "acct-1", "alias": "alias-demo", "active": True, "assigned_proxy_id": "proxy-a"},
            {"username": "acct-2", "alias": "alias-demo", "active": True, "assigned_proxy_id": "proxy-b"},
        ],
    )
    monkeypatch.setattr(
        responder,
        "_inspect_startable_accounts",
        lambda targets, log_skipped=False: {
            "startable_accounts": ["acct-1"],
            "account_statuses": [
                {
                    "username": "acct-1",
                    "blocked": False,
                    "safety_state": "usable",
                    "message": "Lista",
                },
                {
                    "username": "acct-2",
                    "blocked": True,
                    "safety_state": "blocked",
                    "reason": "proxy_inactive",
                    "message": "Proxy inactivo",
                }
            ],
            "skipped_accounts": [
                {
                    "username": "acct-2",
                    "source": "proxy",
                    "status": "blocked",
                    "reason": "proxy_inactive",
                    "message": "Proxy inactivo",
                }
            ],
        },
    )

    capacity = service.max_alias_concurrency("alias-demo")

    assert capacity == 1


def test_activate_bot_enters_main_loop_and_closes_clients_on_stop(monkeypatch) -> None:
    calls: list[str] = []

    class _FakeClient:
        def __init__(self) -> None:
            self.closed = 0

        def close(self) -> None:
            self.closed += 1

    class _FakeRuntime:
        def begin_cycle(self, account: str, *, now_ts: float | None = None) -> None:
            calls.append(f"begin:{account}")

        def is_account_blocked(self, account: str, *, now_ts: float | None = None):
            return False, 0.0, ""

        def mark_rate_signal(self, account: str, *, reason: str = "") -> None:
            return None

        def snapshot(self, account: str) -> dict[str, float]:
            return {}

    fake_client = _FakeClient()
    prompt_entry = {
        "alias": "alias-demo",
        "objection_prompt": "",
        "objection_strategy_name": "OBJECION",
        "flow_config": {
            "version": 1,
            "entry_stage_id": "stage_1",
            "stages": [
                {
                    "id": "stage_1",
                    "action_type": "PACK_A",
                    "transitions": {
                        "positive": "stage_1",
                        "negative": "stage_1",
                        "doubt": "stage_1",
                        "neutral": "stage_1",
                    },
                    "followups": [{"delay_hours": 4, "action_type": "PACK_FU"}],
                    "post_objection": {
                        "enabled": True,
                        "action_type": "OBJECION",
                        "max_steps": 2,
                        "resolved_transition": "positive",
                        "unresolved_transition": "negative",
                    },
                }
            ],
        },
    }

    monkeypatch.setattr(responder, "_refresh_autoresponder_storage_caches", lambda: None)
    monkeypatch.setattr(responder, "_load_preferences", lambda alias=None: ("test-key", ""))
    monkeypatch.setattr(responder, "_probe_ai_runtime", lambda api_key: (True, ""))
    monkeypatch.setattr(responder, "_prompt_alias_selection", lambda: "alias-demo")
    monkeypatch.setattr(responder, "_choose_targets", lambda alias: ["acct-1"])
    monkeypatch.setattr(responder, "_filter_startable_accounts", lambda targets: ["acct-1"])
    monkeypatch.setattr(responder, "_get_prompt_entry", lambda alias: dict(prompt_entry))
    monkeypatch.setattr(
        responder,
        "_resolve_prompt_entry_for_user",
        lambda username, active_alias=None, fallback_entry=None: dict(prompt_entry),
    )
    monkeypatch.setattr(
        responder,
        "_list_packs",
        lambda: [
            {
                "id": "pack-a-1",
                "name": "Pack A",
                "type": "PACK_A",
                "active": True,
                "actions": [{"type": "text_fixed", "content": "hola"}],
            },
            {
                "id": "pack-fu-1",
                "name": "Pack FU",
                "type": "PACK_FU",
                "active": True,
                "actions": [{"type": "text_fixed", "content": "seguimiento"}],
            },
        ],
    )
    monkeypatch.setattr(
        responder,
        "refresh_settings",
        lambda: SimpleNamespace(
            quiet=True,
            log_dir=None,
            log_file="app.log",
            autoresponder_delay=1,
        ),
    )
    monkeypatch.setattr(
        responder,
        "ask_int",
        lambda prompt, min_value=0, default=None: int(default if default is not None else min_value),
    )
    monkeypatch.setattr(responder, "ask", lambda prompt="": "")
    monkeypatch.setattr(responder, "press_enter", lambda _msg="": None)
    monkeypatch.setattr(responder, "ensure_logging", lambda **kwargs: None)
    monkeypatch.setattr(responder, "_q_listener_enabled_for_autoresponder", lambda: False)
    monkeypatch.setattr(responder, "_reset_autoresponder_runtime_controller", lambda: None)
    monkeypatch.setattr(responder, "_get_autoresponder_runtime_controller", lambda: _FakeRuntime())
    monkeypatch.setattr(responder, "mark_connected", lambda username, connected: None)
    monkeypatch.setattr(responder, "_client_for", lambda username: fake_client)
    monkeypatch.setattr(responder, "_autoresponder_health_check_client", lambda client: (True, ""))
    monkeypatch.setattr(responder, "_safezone_quarantine_status", lambda user: (False, 0.0, ""))
    monkeypatch.setattr(responder, "_is_playwright_client_invalid", lambda client: False)
    monkeypatch.setattr(responder, "_print_bot_summary", lambda stats: None)
    monkeypatch.setattr(responder, "_sleep_cycle_delay_from_message_delay", lambda delay_min, delay_max: None)

    @contextmanager
    def _quiet_console():
        yield

    def _full_discovery_initial(client, user: str, threads_target: int):
        calls.append("discovery")
        return ["thread-1"]

    def _build_cycle_workset(user: str, **kwargs):
        calls.append("memory")
        return ["thread-1"], 1, 1

    def _decision_cycle_from_memory(*args, **kwargs):
        calls.append("decision")

    def _incremental_discovery_sync(client, user: str, page_limit: int):
        calls.append("incremental")
        return 0, 0, []

    def _process_followups_math(*args, **kwargs):
        calls.append("followups")
        responder.request_stop("test stop")

    monkeypatch.setattr(responder, "_suppress_console_noise", _quiet_console)
    monkeypatch.setattr(responder, "full_discovery_initial", _full_discovery_initial)
    monkeypatch.setattr(responder, "_build_cycle_workset", _build_cycle_workset)
    monkeypatch.setattr(responder, "decision_cycle_from_memory", _decision_cycle_from_memory)
    monkeypatch.setattr(responder, "incremental_discovery_sync", _incremental_discovery_sync)
    monkeypatch.setattr(responder, "_process_followups_math", _process_followups_math)
    monkeypatch.setattr(responder, "_emit_autoresponder_event", lambda event, **payload: calls.append(event))

    result = responder._activate_bot()

    assert result["status"] == "stopped"
    assert result["loop_started"] is True
    assert "START" in calls
    assert "discovery" in calls
    assert "memory" in calls
    assert "decision" in calls
    assert "incremental" in calls
    assert "followups" in calls
    assert "STOP" in calls
    assert fake_client.closed == 1


def test_filter_startable_accounts_uses_saved_state_without_session_probe(monkeypatch, tmp_path: Path) -> None:
    storage_state = tmp_path / "acct-1" / "storage_state.json"
    storage_state.parent.mkdir(parents=True, exist_ok=True)
    storage_state.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        responder,
        "get_account",
        lambda username: {"username": username} if username in {"acct-1", "acct-2"} else None,
    )
    monkeypatch.setattr(
        responder,
        "has_playwright_storage_state",
        lambda username: username == "acct-1" and storage_state.exists(),
    )
    monkeypatch.setattr(
        responder,
        "_ensure_session",
        lambda username: (_ for _ in ()).throw(AssertionError("session probe should not run on startup")),
    )

    result = responder._filter_startable_accounts(["acct-1", "acct-2", "acct-3"])

    assert result == ["acct-1"]


def test_filter_startable_accounts_excludes_proxy_preflight_blocked_accounts(monkeypatch) -> None:
    monkeypatch.setattr(
        responder,
        "get_account",
        lambda username: {"username": username} if username in {"acct-1", "acct-2"} else None,
    )
    monkeypatch.setattr(responder, "has_playwright_storage_state", lambda username: True)
    monkeypatch.setattr(
        responder,
        "preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [dict(item) for item in accounts if item.get("username") == "acct-1"],
            "blocked_accounts": [
                {
                    "username": "acct-2",
                    "status": "quarantined",
                    "message": "proxy quarantined",
                }
            ],
            "blocked_status_counts": {"quarantined": 1},
        },
    )

    result = responder._filter_startable_accounts(["acct-1", "acct-2", "acct-3"])

    assert result == ["acct-1"]


def test_inspect_startable_accounts_applies_account_safety_contract(monkeypatch) -> None:
    records = {
        "acct-ok": {"username": "acct-ok"},
        "acct-login": {"username": "acct-login"},
        "acct-low": {
            "username": "acct-low",
            "low_profile": True,
            "low_profile_reason": "Actividad reciente insuficiente",
        },
        "acct-runtime": {"username": "acct-runtime"},
        "acct-health": {"username": "acct-health"},
    }

    class _FakeRuntime:
        def is_account_blocked(self, account: str, *, now_ts: float | None = None):
            if account == "acct-runtime":
                return True, 900.0, "pause:checkpoint"
            return False, 0.0, ""

    monkeypatch.setattr(responder, "get_account", lambda username: dict(records.get(username, {})) if username in records else None)
    monkeypatch.setattr(responder, "has_playwright_storage_state", lambda username: username != "acct-login")
    monkeypatch.setattr(
        responder,
        "preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [dict(item) for item in accounts],
            "blocked_accounts": [],
            "blocked_status_counts": {},
        },
    )
    monkeypatch.setattr(responder, "_get_autoresponder_runtime_controller", lambda: _FakeRuntime())
    monkeypatch.setattr(responder, "_safezone_quarantine_status", lambda user: (False, 0.0, ""))
    monkeypatch.setattr(
        responder.health_store,
        "get_record",
        lambda username: (
            SimpleNamespace(state="MUERTA", reason="challenge"),
            False,
        )
        if username == "acct-health"
        else (None, True),
    )

    inspection = responder._inspect_startable_accounts(
        ["acct-ok", "acct-login", "acct-low", "acct-runtime", "acct-health"],
        log_skipped=False,
    )
    account_statuses = {
        str(item.get("username") or ""): dict(item)
        for item in (inspection.get("account_statuses") or [])
        if isinstance(item, dict)
    }

    assert inspection["startable_accounts"] == ["acct-ok"]
    assert account_statuses["acct-login"]["safety_state"] == "needs_login"
    assert account_statuses["acct-login"]["message"] == "Re-login requerido"
    assert account_statuses["acct-low"]["safety_state"] == "low_profile"
    assert account_statuses["acct-low"]["message"] == "Bajo perfil: Actividad reciente insuficiente"
    assert account_statuses["acct-runtime"]["safety_state"] == "blocked"
    assert account_statuses["acct-runtime"]["message"] == "Checkpoint pendiente"
    assert account_statuses["acct-health"]["safety_state"] == "blocked"
    assert account_statuses["acct-health"]["message"] == "Challenge pendiente"


def test_pause_autoresponder_account_for_safety_uses_explicit_runtime_pause() -> None:
    calls: list[tuple[str, str, float]] = []

    class _FakeRuntime:
        def pause_account(self, account: str, *, reason: str, duration_seconds: float) -> None:
            calls.append((account, reason, duration_seconds))

    result = responder._pause_autoresponder_account_for_safety(
        "acct-1",
        "activate_cycle_exception:login_required",
        runtime=_FakeRuntime(),
    )

    assert result is not None
    assert result[0] == "needs_login"
    assert calls == [("acct-1", "needs_login", float(responder._AUTORESPONDER_LOGIN_REQUIRED_PAUSE_SECONDS))]


def test_autoresponder_health_check_client_is_lightweight(monkeypatch) -> None:
    class _FakePage:
        def is_closed(self) -> bool:
            return False

    class _FakeBrowser:
        def is_connected(self) -> bool:
            return True

    class _FakeContext:
        @property
        def pages(self) -> list[object]:
            return []

    class _FakeClient:
        _page = _FakePage()
        _browser = _FakeBrowser()
        _context = _FakeContext()

        def _ensure_page(self):
            raise AssertionError("health check should not ensure the page")

        def _ensure_inbox_workspace_fast(self) -> None:
            raise AssertionError("health check should not probe the inbox workspace")

        def _open_inbox(self, *args, **kwargs) -> None:
            raise AssertionError("health check should not reopen the inbox")

    ok, reason = responder._autoresponder_health_check_client(_FakeClient())

    assert ok is True
    assert reason == "ok"
