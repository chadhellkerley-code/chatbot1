from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from application.services.base import ServiceContext
from application.services.leads_service import LeadsService
from gui.snapshot_queries import (
<<<<<<< HEAD
    build_campaign_capacity_snapshot,
=======
>>>>>>> origin/main
    build_campaign_create_snapshot,
    build_leads_import_snapshot,
    build_leads_lists_snapshot,
)


class _FakeAccounts:
    def list_aliases(self) -> list[str]:
        return ["matias"]


class _FakeCampaigns:
    def list_templates(self) -> list[dict[str, str]]:
        return [{"name": "saludo", "text": "hola"}]

<<<<<<< HEAD
    def get_capacity(
        self,
        alias: str,
        *,
        leads_alias: str = "",
        workers_requested: int = 0,
    ) -> dict[str, object]:
        return {
            "alias": alias,
            "leads_alias": leads_alias,
            "workers_capacity": 2,
            "workers_requested": workers_requested,
            "workers_effective": min(2, max(0, int(workers_requested or 0))) if workers_requested else 2,
            "proxies": ["p1"],
            "has_none_accounts": False,
            "remaining_slots_total": 7,
            "planned_eligible_leads": 0,
            "planned_runnable_leads": 0,
            "account_remaining": [],
        }
=======
    def get_capacity(self, alias: str) -> dict[str, object]:
        return {"alias": alias, "workers_capacity": 2, "proxies": ["p1"], "has_none_accounts": False}
>>>>>>> origin/main


class _HugeSummaryLeads:
    def list_list_summaries(self) -> list[dict[str, object]]:
        return [
            {"name": f"lista_{index:04d}", "count": index + 1}
            for index in range(1000)
        ]

    def load_list(self, _name: str) -> list[str]:
        raise AssertionError("build_campaign_create_snapshot should not load full leads lists for huge summaries")


def test_campaign_create_snapshot_reads_real_lead_lists(tmp_path: Path) -> None:
    leads = LeadsService(ServiceContext(root_dir=tmp_path))
    leads.save_list("demo", ["uno", "uno", "dos"])
    services = SimpleNamespace(
        accounts=_FakeAccounts(),
        leads=leads,
        campaigns=_FakeCampaigns(),
    )

    snapshot = build_campaign_create_snapshot(services, active_alias="matias")

    assert snapshot["lead_lists"] == ["demo"]
    assert snapshot["lead_counts"] == {"demo": 2}
    assert snapshot["capacity"]["workers_capacity"] == 2


<<<<<<< HEAD
def test_campaign_capacity_snapshot_exposes_real_quota_and_plan_fields() -> None:
    services = SimpleNamespace(
        campaigns=_FakeCampaigns(),
    )

    snapshot = build_campaign_capacity_snapshot(
        services,
        alias="matias",
        leads_alias="demo",
        workers_requested=1,
    )

    assert snapshot["alias"] == "matias"
    assert snapshot["leads_alias"] == "demo"
    assert snapshot["workers_requested"] == 1
    assert snapshot["remaining_slots_total"] == 7


=======
>>>>>>> origin/main
def test_campaign_create_snapshot_uses_list_summaries_without_loading_full_lists(
    tmp_path: Path,
    monkeypatch,
) -> None:
    leads = LeadsService(ServiceContext(root_dir=tmp_path))
    leads.save_list("demo", ["uno", "dos"])
    services = SimpleNamespace(
        accounts=_FakeAccounts(),
        leads=leads,
        campaigns=_FakeCampaigns(),
    )

    def _fail_load(_name: str) -> list[str]:
        raise AssertionError("build_campaign_create_snapshot should not load full leads lists")

    monkeypatch.setattr(leads, "load_list", _fail_load)

    snapshot = build_campaign_create_snapshot(services, active_alias="matias")

    assert snapshot["lead_lists"] == ["demo"]
    assert snapshot["lead_counts"] == {"demo": 2}


def test_leads_lists_snapshot_exposes_clean_rows(tmp_path: Path) -> None:
    leads = LeadsService(ServiceContext(root_dir=tmp_path))
    leads.save_list("lista_a", ["@uno", "dos", "uno"])
    services = SimpleNamespace(leads=leads)

    snapshot = build_leads_lists_snapshot(services)

    assert snapshot["rows"] == [{"name": "lista_a", "usernames": ["uno", "dos"], "count": 2}]


def test_leads_import_snapshot_includes_latest_import_activity(tmp_path: Path) -> None:
    leads = LeadsService(ServiceContext(root_dir=tmp_path))
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text("username\nuno\ndos\n", encoding="utf-8")
    leads.import_csv(csv_path, "demo")
    services = SimpleNamespace(leads=leads)

    snapshot = build_leads_import_snapshot(services)

    assert snapshot["lists"] == ["demo"]
    assert "Ultimo import: demo" in snapshot["summary"]
    assert "7d: ok 1" in snapshot["summary"]


def test_campaign_create_snapshot_excludes_list_after_full_rollback(tmp_path: Path) -> None:
    leads = LeadsService(ServiceContext(root_dir=tmp_path))
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text("username\nuno\ndos\n", encoding="utf-8")
    leads.import_csv(csv_path, "demo")
    leads.rollback_last_import("demo")
    services = SimpleNamespace(
        accounts=_FakeAccounts(),
        leads=leads,
        campaigns=_FakeCampaigns(),
    )

    snapshot = build_campaign_create_snapshot(services, active_alias="matias")

    assert snapshot["lead_lists"] == []
    assert snapshot["lead_counts"] == {}


def test_campaign_create_snapshot_handles_huge_summary_sets_without_loading_lists() -> None:
    services = SimpleNamespace(
        accounts=_FakeAccounts(),
        leads=_HugeSummaryLeads(),
        campaigns=_FakeCampaigns(),
    )

    snapshot = build_campaign_create_snapshot(services, active_alias="matias")

    assert len(snapshot["lead_lists"]) == 1000
    assert snapshot["lead_lists"][0] == "lista_0000"
    assert snapshot["lead_lists"][-1] == "lista_0999"
    assert snapshot["lead_counts"]["lista_0000"] == 1
    assert snapshot["lead_counts"]["lista_0999"] == 1000
