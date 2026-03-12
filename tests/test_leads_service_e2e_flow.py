from __future__ import annotations

import asyncio
from pathlib import Path

from application.services.base import ServiceContext
from application.services.leads_service import LeadsService
from core import leads as leads_module


def _mark_result(
    item: dict[str, object],
    *,
    passed: bool,
    account: str,
    updated_at: str,
    reason: str = "",
) -> None:
    item["status"] = "QUALIFIED" if passed else "DISCARDED"
    item["result"] = "CALIFICA" if passed else "NO CALIFICA"
    item["reason"] = "" if passed else str(reason or "descartado")
    item["account"] = str(account or "")
    item["updated_at"] = str(updated_at or "")


class _ScriptedFilterRuntime:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, list_data, filter_cfg, run_cfg):  # noqa: ANN001
        del filter_cfg, run_cfg
        self.calls += 1
        items = list_data.get("items") or []
        pending_items = [item for item in items if str(item.get("status") or "") == "PENDING"]

        if self.calls == 1:
            assert [str(item.get("username") or "") for item in pending_items] == ["uno", "dos", "tres"]
            _mark_result(
                pending_items[0],
                passed=True,
                account="CuentaA",
                updated_at="2026-03-12T10:00:00Z",
            )
            leads_module._refresh_list_stats(list_data)
            return True

        if self.calls == 2:
            assert [str(item.get("username") or "") for item in pending_items] == ["dos", "tres"]
            _mark_result(
                pending_items[0],
                passed=True,
                account="CuentaB",
                updated_at="2026-03-12T10:05:00Z",
            )
            _mark_result(
                pending_items[1],
                passed=False,
                account="CuentaB",
                updated_at="2026-03-12T10:05:30Z",
                reason="keyword_faltante",
            )
            leads_module._refresh_list_stats(list_data)
            return False

        raise AssertionError(f"unexpected scripted runtime call #{self.calls}")


def test_leads_service_create_run_stop_resume_and_export_flow(tmp_path: Path, monkeypatch) -> None:
    service = LeadsService(ServiceContext(root_dir=tmp_path))
    scripted_runtime = _ScriptedFilterRuntime()
    export_alias = "clientes_export"

    monkeypatch.setattr(leads_module, "_verify_dependencies_for_run", lambda cfg: None)
    monkeypatch.setattr(leads_module, "_execute_filter_list_async", scripted_runtime)
    monkeypatch.setattr(leads_module, "_run_async", lambda coro: asyncio.run(coro))

    created = service.create_filter_list(
        ["uno", "dos", "tres"],
        export_alias=export_alias,
        filters=service.default_filter_config(),
        run={
            "alias": "ventas",
            "accounts": ["cuenta_a"],
            "concurrency": 1,
            "delay_min": 5,
            "delay_max": 10,
            "headless": True,
            "max_runtime_seconds": 120,
        },
        source_list="seed-list",
    )
    list_id = str(created.get("id") or "")

    assert list_id
    assert service.load_list(export_alias) == []

    first_run = service.execute_filter_list(list_id)

    assert first_run["stopped"] is True
    assert first_run["processed"] == 1
    assert first_run["qualified"] == 1
    assert first_run["discarded"] == 0
    assert first_run["pending"] == 2
    assert [row["id"] for row in service.list_filter_list_summaries(status="incomplete")] == [list_id]

    stopped_row = service.find_filter_list(list_id)
    assert stopped_row["status"] == "pending"
    assert stopped_row["processed"] == 1
    assert stopped_row["qualified"] == 1
    assert stopped_row["discarded"] == 0
    assert stopped_row["pending"] == 2

    partial_export = service.finalize_stopped_filter_list(list_id, action="export")

    assert partial_export["action"] == "export"
    assert partial_export["exported"] == 1
    assert service.load_list(export_alias) == ["uno"]

    second_run = service.execute_filter_list(list_id)

    assert second_run["stopped"] is False
    assert second_run["processed"] == 3
    assert second_run["qualified"] == 2
    assert second_run["discarded"] == 1
    assert second_run["pending"] == 0
    assert [row["id"] for row in service.list_filter_list_summaries(status="completed")] == [list_id]

    completed_row = service.find_filter_list(list_id)
    assert completed_row["status"] == "done"
    assert completed_row["processed"] == 3
    assert completed_row["qualified"] == 2
    assert completed_row["discarded"] == 1
    assert completed_row["pending"] == 0
    assert service.load_list(export_alias) == ["uno", "dos"]
    assert scripted_runtime.calls == 2
