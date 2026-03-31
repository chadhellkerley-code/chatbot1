from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

import application.services as services_module
from application.services.alias_lifecycle_service import AliasLifecycleService
from application.services.base import ServiceContext
from gui.main_window import MainWindow


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


class _FakeStateStore:
    def __init__(self) -> None:
        self.sync_foundation_calls = 0

    def get_active_alias(self) -> str:
        return "default"

    def sync_foundation(self) -> dict[str, int]:
        self.sync_foundation_calls += 1
        return {"accounts": 0, "lead_status": 0, "conversation_engine": 0}


def test_main_window_startup_keeps_noncritical_services_out_of_pre_show(monkeypatch, tmp_path: Path) -> None:
    _app()
    counts = {
        "accounts": 0,
        "inbox": 0,
        "system": 0,
        "warmup": 0,
        "leads": 0,
        "automation": 0,
        "campaigns": 0,
        "aliases": 0,
    }
    state_store = _FakeStateStore()

    class _FakeAccountsService:
        def __init__(self, context: ServiceContext) -> None:
            del context
            counts["accounts"] += 1

        def get_alias_record(self, alias: str) -> dict[str, str]:
            return {"display_name": str(alias or "default") or "default"}

        def shutdown_manual_sessions(self) -> None:
            return None

    class _FakeInboxService:
        def __init__(self, context: ServiceContext) -> None:
            del context
            counts["inbox"] += 1

        def set_ui_active(self, _active: bool) -> None:
            return None

        def shutdown(self) -> None:
            return None

        def diagnostics(self) -> dict[str, int]:
            return {"worker_count": 0, "queued_tasks": 0, "dedupe_pending": 0}

    class _FakeSystemService:
        def __init__(self, context: ServiceContext) -> None:
            del context
            counts["system"] += 1

    class _FakeWarmupService:
        def __init__(self, context: ServiceContext) -> None:
            del context
            counts["warmup"] += 1

        def pause_active_flows(self, _reason: str) -> None:
            return None

    class _FakeLeadsService:
        def __init__(self, context: ServiceContext) -> None:
            del context
            counts["leads"] += 1

    class _FakeAutomationService:
        def __init__(self, context: ServiceContext, inbox_service=None) -> None:
            del context, inbox_service
            counts["automation"] += 1

    class _FakeCampaignService:
        def __init__(self, context: ServiceContext) -> None:
            del context
            counts["campaigns"] += 1

    class _FakeAliasLifecycleService:
        def __init__(self, context: ServiceContext, **kwargs) -> None:
            del context, kwargs
            counts["aliases"] += 1

        def get_active_alias(self) -> str:
            return "default"

    monkeypatch.setattr(services_module, "get_app_state_store", lambda _root: state_store)
    monkeypatch.setattr("application.services.account_service.AccountService", _FakeAccountsService)
    monkeypatch.setattr("application.services.inbox_service.InboxService", _FakeInboxService)
    monkeypatch.setattr("application.services.system_service.SystemService", _FakeSystemService)
    monkeypatch.setattr("application.services.warmup_service.WarmupService", _FakeWarmupService)
    monkeypatch.setattr("application.services.leads_service.LeadsService", _FakeLeadsService)
    monkeypatch.setattr("application.services.automation_service.AutomationService", _FakeAutomationService)
    monkeypatch.setattr("application.services.campaign_service.CampaignService", _FakeCampaignService)
    monkeypatch.setattr("application.services.alias_lifecycle_service.AliasLifecycleService", _FakeAliasLifecycleService)

    services = services_module.build_application_services(tmp_path)
    window = MainWindow(mode="owner", services=services)
    try:
        assert counts == {
            "accounts": 1,
            "inbox": 1,
            "system": 1,
            "warmup": 0,
            "leads": 0,
            "automation": 0,
            "campaigns": 0,
            "aliases": 0,
        }
        assert state_store.sync_foundation_calls == 0

        window.start_startup_housekeeping()
        assert services.wait_for_post_show_hydration(timeout=1.0) is True

        assert counts["warmup"] == 1
        assert counts["leads"] == 0
        assert counts["automation"] == 0
        assert counts["campaigns"] == 0
        assert counts["aliases"] == 0
        assert state_store.sync_foundation_calls == 1

        services.leads
        services.automation
        services.campaigns
        services.aliases

        assert counts["leads"] == 1
        assert counts["automation"] == 1
        assert counts["campaigns"] == 1
        assert counts["aliases"] == 1
    finally:
        window.close()


def test_alias_lifecycle_service_defers_optional_dependencies_for_active_alias(monkeypatch, tmp_path: Path) -> None:
    automation_provider_calls = 0
    warmup_provider_calls = 0
    state_store = _FakeStateStore()

    class _FakeAccountsService:
        def resolve_alias_id(self, alias: str | None, *, default: str = "default") -> str:
            clean_alias = str(alias or "").strip()
            return clean_alias or default

        def list_alias_records(self) -> list[dict[str, str]]:
            return [{"alias_id": "default", "display_name": "default"}]

    def _automation_provider():
        nonlocal automation_provider_calls
        automation_provider_calls += 1
        return object()

    def _warmup_provider():
        nonlocal warmup_provider_calls
        warmup_provider_calls += 1
        return object()

    monkeypatch.setattr(
        "application.services.alias_lifecycle_service.get_app_state_store",
        lambda _root: state_store,
    )

    service = AliasLifecycleService(
        ServiceContext(root_dir=tmp_path),
        accounts=_FakeAccountsService(),
        automation_provider=_automation_provider,
        warmup_provider=_warmup_provider,
    )

    assert service.get_active_alias() == "default"
    assert automation_provider_calls == 0
    assert warmup_provider_calls == 0
