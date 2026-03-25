from __future__ import annotations

from types import SimpleNamespace

from application.services.account_service import AccountService
from application.services.base import ServiceContext
from application.services.system_service import SystemService
from gui.snapshot_queries import build_accounts_table_snapshot
import application.services.account_service as account_service_module
import application.services.system_service as system_service_module


def _build_service(tmp_path) -> AccountService:
    return AccountService(ServiceContext.default(tmp_path))


def test_account_service_connected_status_uses_session_state_only(monkeypatch, tmp_path) -> None:
    service = _build_service(tmp_path)
    record = {"username": "acc-1", "connected": False}

    monkeypatch.setattr(
        account_service_module.health_store,
        "get_badge",
        lambda _username: ("VIVA", False),
    )
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "connected_status",
        lambda *_args, **_kwargs: False,
    )

    assert service.connected_status(record) is False


def test_account_service_neutralizes_stale_health_badge(monkeypatch, tmp_path) -> None:
    service = _build_service(tmp_path)

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "_badge_for_display",
        lambda _record: ("NO ACTIVA", True),
    )

    assert service.health_badge({"username": "acc-1"}) == "NO VERIFICADA"


def test_refresh_connected_health_updates_stale_connected_account_to_viva(monkeypatch, tmp_path) -> None:
    service = _build_service(tmp_path)
    state_by_username: dict[str, str] = {"acc-1": "NO ACTIVA"}
    rows = [{"username": "acc-1", "alias": "mati", "connected": True}]

    monkeypatch.setattr(service, "list_accounts", lambda _alias=None: rows)
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "connected_status",
        lambda record, **_kwargs: bool(record.get("connected")),
    )
    monkeypatch.setattr(
        account_service_module.health_store,
        "get_badge",
        lambda username: (state_by_username.get(username), False),
    )

    def _fake_probe(record: dict[str, object]) -> tuple[bool, str]:
        username = str(record.get("username") or "")
        state_by_username[username] = "VIVA"
        return True, "instagram_ui_ready"

    monkeypatch.setattr(account_service_module, "_check_connected_account_health", _fake_probe)

    result = service.refresh_connected_health("mati")

    assert result["eligible"] == 1
    assert result["alive"] == 1
    assert result["inactive"] == 0
    assert result["dead"] == 0
    assert result["errors"] == 0
    assert result["results"][0]["health"] == "VIVA"


def test_refresh_connected_health_marks_login_redirect_as_no_activa(monkeypatch, tmp_path) -> None:
    service = _build_service(tmp_path)
    state_by_username: dict[str, str] = {"acc-1": "VIVA"}
    rows = [{"username": "acc-1", "alias": "mati", "connected": True}]

    monkeypatch.setattr(service, "list_accounts", lambda _alias=None: rows)
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "connected_status",
        lambda record, **_kwargs: bool(record.get("connected")),
    )
    monkeypatch.setattr(
        account_service_module.health_store,
        "get_badge",
        lambda username: (state_by_username.get(username), False),
    )

    def _fake_probe(record: dict[str, object]) -> tuple[bool, str]:
        username = str(record.get("username") or "")
        state_by_username[username] = "NO ACTIVA"
        return False, "redirected_to_login"

    monkeypatch.setattr(account_service_module, "_check_connected_account_health", _fake_probe)

    result = service.refresh_connected_health("mati")

    assert result["eligible"] == 1
    assert result["alive"] == 0
    assert result["inactive"] == 1
    assert result["dead"] == 0
    assert result["results"][0]["health"] == "NO ACTIVA"
    assert result["results"][0]["reason"] == "redirected_to_login"


def test_refresh_connected_health_marks_challenge_as_muerta(monkeypatch, tmp_path) -> None:
    service = _build_service(tmp_path)
    state_by_username: dict[str, str] = {"acc-1": "NO ACTIVA"}
    rows = [{"username": "acc-1", "alias": "mati", "connected": True}]

    monkeypatch.setattr(service, "list_accounts", lambda _alias=None: rows)
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "connected_status",
        lambda record, **_kwargs: bool(record.get("connected")),
    )
    monkeypatch.setattr(
        account_service_module.health_store,
        "get_badge",
        lambda username: (state_by_username.get(username), False),
    )

    def _fake_probe(record: dict[str, object]) -> tuple[bool, str]:
        username = str(record.get("username") or "")
        state_by_username[username] = "MUERTA"
        return False, "challenge"

    monkeypatch.setattr(account_service_module, "_check_connected_account_health", _fake_probe)

    result = service.refresh_connected_health("mati")

    assert result["eligible"] == 1
    assert result["alive"] == 0
    assert result["inactive"] == 0
    assert result["dead"] == 1
    assert result["results"][0]["health"] == "MUERTA"
    assert result["results"][0]["reason"] == "challenge"


def test_refresh_connected_health_does_not_change_connected_for_stale_health(monkeypatch, tmp_path) -> None:
    service = _build_service(tmp_path)
    state_by_username: dict[str, str] = {"acc-1": "NO VERIFICADA"}
    rows = [{"username": "acc-1", "alias": "mati", "connected": True}]

    monkeypatch.setattr(service, "list_accounts", lambda _alias=None: rows)
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "connected_status",
        lambda record, **_kwargs: bool(record.get("connected")),
    )
    monkeypatch.setattr(
        account_service_module.health_store,
        "get_badge",
        lambda username: (state_by_username.get(username), False),
    )

    def _fake_probe(record: dict[str, object]) -> tuple[bool, str]:
        username = str(record.get("username") or "")
        state_by_username[username] = "NO ACTIVA"
        return False, "redirected_to_login"

    monkeypatch.setattr(account_service_module, "_check_connected_account_health", _fake_probe)

    result = service.refresh_connected_health("mati")

    assert rows[0]["connected"] is True
    assert result["results"][0]["health"] == "NO ACTIVA"
    assert result["results"][0]["reason"] == "redirected_to_login"


class _FakeAccounts:
    def list_aliases(self) -> list[str]:
        return ["default"]

    def list_accounts(self, alias: str) -> list[dict[str, object]]:
        assert alias == "default"
        return [
            {"username": "stale_live"},
            {"username": "fresh_alive"},
            {"username": "fresh_inactive"},
            {"username": "stale_inactive"},
        ]

    def connected_status(self, record: dict[str, object]) -> bool:
        username = str(record.get("username") or "")
        return username in {"stale_live", "fresh_alive"}

    def health_badge(self, record: dict[str, object]) -> str:
        username = str(record.get("username") or "")
        if username == "stale_live":
            return "NO VERIFICADA"
        if username == "fresh_alive":
            return "VIVA"
        if username == "fresh_inactive":
            return "NO ACTIVA"
        return "NO VERIFICADA"

    def manual_action_eligibility(self, record: dict[str, object]) -> dict[str, object]:
        connected = bool(record.get("connected"))
        badge = str(record.get("health_badge") or "").strip().upper()
        allowed = connected and badge == "VIVA"
        return {
            "allowed": allowed,
            "message": "" if allowed else "Necesitas re-login en esta cuenta",
        }

    def proxy_display_for_account(self, record: dict[str, object]) -> dict[str, str]:
        del record
        return {"label": "-", "status": "unknown"}

    def login_progress_for_account(self, record: dict[str, object]) -> dict[str, object]:
        del record
        return {"active": False, "state": "", "message": "", "label": "", "updated_at": ""}


def test_build_accounts_table_snapshot_decouples_connected_from_health() -> None:
    services = SimpleNamespace(accounts=_FakeAccounts())

    snapshot = build_accounts_table_snapshot(services, active_alias="default")
    rows = {str(row.get("username")): row for row in snapshot["rows"]}

    assert rows["stale_live"]["health_badge"] == "NO VERIFICADA"
    assert rows["stale_live"]["connected"] is True
    assert rows["stale_live"]["connected_label"] == "Si"
    assert rows["stale_live"]["manual_action_allowed"] is False

    assert rows["fresh_alive"]["health_badge"] == "VIVA"
    assert rows["fresh_alive"]["connected"] is True
    assert rows["fresh_alive"]["connected_label"] == "Si"

    assert rows["fresh_inactive"]["health_badge"] == "NO ACTIVA"
    assert rows["fresh_inactive"]["connected"] is False
    assert rows["fresh_inactive"]["connected_label"] == "No"

    assert rows["stale_inactive"]["health_badge"] == "NO VERIFICADA"
    assert rows["stale_inactive"]["connected"] is False
    assert rows["stale_inactive"]["connected_label"] == "No"


def test_table_alias_summary_and_dashboard_keep_connected_counts_aligned_with_stale_health(
    monkeypatch,
    tmp_path,
) -> None:
    accounts_service = _build_service(tmp_path)
    system_service = SystemService(ServiceContext.default(tmp_path))

    mati_rows = [
        {"username": f"mati_{index}", "alias": "MATI", "connected": True}
        for index in range(1, 11)
    ]
    mati_rows.extend(
        [
            {"username": "mati_off_1", "alias": "MATI", "connected": False},
            {"username": "mati_off_2", "alias": "MATI", "connected": False},
        ]
    )
    all_rows = mati_rows + [{"username": "nuevas_1", "alias": "nuevas", "connected": True}]

    def _connected(record: dict[str, object], **_kwargs) -> bool:
        return bool(record.get("connected"))

    def _badge_for_display(record: dict[str, object]) -> tuple[str, bool]:
        if bool(record.get("connected")):
            return "VIVA", True
        return "NO ACTIVA", False

    def _health_badge(username: str) -> tuple[str, bool]:
        for row in all_rows:
            if str(row.get("username")) == username:
                if bool(row.get("connected")):
                    return "VIVA", True
                return "NO ACTIVA", False
        return "", False

    monkeypatch.setattr(account_service_module.accounts_module, "list_all", lambda: [dict(row) for row in all_rows])
    monkeypatch.setattr(account_service_module.accounts_module, "connected_status", _connected)
    monkeypatch.setattr(account_service_module.accounts_module, "_badge_for_display", _badge_for_display)
    monkeypatch.setattr(system_service_module.accounts_module, "list_all", lambda: [dict(row) for row in all_rows])
    monkeypatch.setattr(system_service_module.accounts_module, "connected_status", _connected)
    monkeypatch.setattr(system_service_module.health_store, "get_badge", _health_badge)
    monkeypatch.setattr(system_service_module.storage_module, "sent_totals_today", lambda: (0, 0, "-", "UTC"))
    monkeypatch.setattr(system_service_module.storage_module, "conversation_rows", lambda **_kwargs: [])

    table_snapshot = build_accounts_table_snapshot(SimpleNamespace(accounts=accounts_service), active_alias="MATI")
    table_connected = sum(1 for row in table_snapshot["rows"] if bool(row.get("connected")))
    stale_rows = [
        row
        for row in table_snapshot["rows"]
        if str(row.get("health_badge") or "").strip().upper() == "NO VERIFICADA"
    ]
    mati_alias = accounts_service.get_alias_snapshot("MATI")
    nuevas_alias = accounts_service.get_alias_snapshot("nuevas")
    dashboard = system_service.dashboard_snapshot()

    assert len(table_snapshot["rows"]) == 12
    assert table_connected == 10
    assert len(stale_rows) == 10
    assert all(bool(row.get("connected")) for row in stale_rows)
    assert all(str(row.get("connected_label") or "") == "Si" for row in stale_rows)
    assert mati_alias["accounts_connected"] == 10
    assert nuevas_alias["accounts_connected"] == 1
    assert dashboard["metrics"]["connected_accounts"] == 11
