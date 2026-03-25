from __future__ import annotations

from types import SimpleNamespace

from gui.snapshot_queries import build_automation_flow_snapshot


class _FakeAccountsService:
    def list_aliases(self) -> list[str]:
        return ["default"]


class _FakeAutomationService:
    def alias_reference_snapshot(self) -> dict[str, object]:
        return {"prompt_aliases": ["legacy_alias", "default"]}

    def list_packs(self) -> list[dict[str, object]]:
        return [{"id": "pack_1", "type": "PACK_A", "name": "Pack A"}]

    def get_flow_config(self, alias: str) -> dict[str, object]:
        return {"alias": alias, "version": 1, "entry_stage_id": "", "stages": []}


def test_build_automation_flow_snapshot_ignores_prompt_aliases_not_present_in_accounts() -> None:
    services = SimpleNamespace(accounts=_FakeAccountsService(), automation=_FakeAutomationService())

    snapshot = build_automation_flow_snapshot(
        services,
        active_alias="default",
        selected_alias="legacy_alias",
    )

    assert snapshot["aliases"] == ["default"]
    assert snapshot["selected_alias"] == "default"
    assert snapshot["flow_config"]["alias"] == "default"
