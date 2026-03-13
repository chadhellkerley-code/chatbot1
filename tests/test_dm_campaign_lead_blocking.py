from __future__ import annotations

import json

from core.storage import successful_contacts_index
from src.dm_campaign import lead_status_store
from src.dm_campaign.proxy_workers_runner import _filter_pending_leads_for_campaign


def _configure_lead_status_store(monkeypatch, tmp_path) -> None:
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(lead_status_store, "_STORAGE", storage_dir)
    monkeypatch.setattr(lead_status_store, "_FILE", storage_dir / "lead_status.json")


def test_lead_status_store_is_alias_scoped_and_preserves_legacy_entries(monkeypatch, tmp_path) -> None:
    _configure_lead_status_store(monkeypatch, tmp_path)
    lead_status_store._FILE.write_text(
        json.dumps(
            {
                "version": 1,
                "leads": {
                    "lead-1": {"status": "sent", "sent_by": "acct-1"},
                    "lead-2": {"status": "skipped", "last_error": "already_contacted"},
                },
            }
        ),
        encoding="utf-8",
    )

    assert lead_status_store.is_terminal_lead_status("lead-1", alias="matias") is False
    assert lead_status_store.get_legacy_lead_status("lead-1") == {"status": "sent", "sent_by": "acct-1"}

    lead_status_store.mark_lead_sent("lead-1", sent_by="acct-1", alias="matias")

    assert lead_status_store.is_terminal_lead_status("lead-1", alias="matias") is True
    assert lead_status_store.is_terminal_lead_status("lead-1", alias="otro") is False

    payload = json.loads(lead_status_store._FILE.read_text(encoding="utf-8"))
    assert payload["version"] == 2
    assert payload["aliases"]["matias"]["leads"]["lead-1"]["status"] == "sent"
    assert payload["legacy_global_leads"]["lead-1"]["status"] == "sent"


def test_campaign_prefilter_uses_alias_scope_and_keeps_shared_registry_as_advisory(monkeypatch) -> None:
    migrated_terminal: set[str] = {"scoped-terminal"}
    terminal_updates: list[tuple[str, list[tuple[str, str]], list[tuple[str, str]]]] = []

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda accounts, *, campaign_alias="": {
            "daily_counts": {account: 0 for account in accounts},
            "campaign_registry": {"campaign-sent"},
            "shared_registry": {"shared-only"},
        },
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.get_prefilter_snapshot",
        lambda alias: (
            set(migrated_terminal),
            {
                "legacy-sent": {"status": "sent", "sent_by": "acct-1"},
                "legacy-already-contacted": {"status": "skipped", "last_error": "already_contacted"},
            },
        ),
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_account_usernames",
        lambda alias: {"acct-1"},
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.apply_terminal_status_updates",
        lambda *, alias="", sent_updates=(), skipped_updates=(): terminal_updates.append(
            (alias, list(sent_updates), list(skipped_updates))
        ),
    )

    pending, stats = _filter_pending_leads_for_campaign(
        [
            "scoped-terminal",
            "campaign-sent",
            "legacy-sent",
            "shared-only",
            "legacy-already-contacted",
            "fresh",
        ],
        alias="matias",
    )

    assert pending == ["shared-only", "legacy-already-contacted", "fresh"]
    assert stats["blocked_total"] == 3
    assert stats["blocked_by_alias_status"] == 1
    assert stats["blocked_by_campaign_registry"] == 1
    assert stats["blocked_by_legacy_campaign_status"] == 1
    assert stats["advisory_shared_registry_hits"] == 1
    assert stats["advisory_legacy_status_ignored"] == 1
    assert terminal_updates == [("matias", [("legacy-sent", "acct-1")], [])]


def test_get_prefilter_snapshot_reuses_cached_file_snapshot(monkeypatch, tmp_path) -> None:
    _configure_lead_status_store(monkeypatch, tmp_path)
    lead_status_store._FILE.write_text(
        json.dumps(
            {
                "version": 2,
                "aliases": {
                    "matias": {
                        "leads": {
                            "lead-a": {"status": "sent"},
                        }
                    }
                },
                "legacy_global_leads": {
                    "lead-b": {"status": "sent", "sent_by": "acct-1"},
                },
            }
        ),
        encoding="utf-8",
    )
    lead_status_store._PREFILTER_SNAPSHOT_CACHE.clear()

    original_load_json_file = lead_status_store.load_json_file
    calls = {"count": 0}

    def _counting_load_json_file(*args, **kwargs):
        calls["count"] += 1
        return original_load_json_file(*args, **kwargs)

    monkeypatch.setattr(lead_status_store, "load_json_file", _counting_load_json_file)

    first_terminal, first_legacy = lead_status_store.get_prefilter_snapshot("matias")
    second_terminal, second_legacy = lead_status_store.get_prefilter_snapshot("matias")

    assert first_terminal == {"lead-a"}
    assert second_terminal == {"lead-a"}
    assert first_legacy == {"lead-b": {"status": "sent", "sent_by": "acct-1"}}
    assert second_legacy == {"lead-b": {"status": "sent", "sent_by": "acct-1"}}
    assert calls["count"] == 1


def test_successful_contacts_index_can_filter_campaign_records_by_alias(monkeypatch, tmp_path) -> None:
    sent_file = tmp_path / "sent_log.jsonl"
    sent_file.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts": 1,
                        "account": "acct-1",
                        "to": "lead-a",
                        "ok": True,
                        "detail": "sent_verified",
                        "source_engine": "campaign",
                        "campaign_alias": "matias",
                    }
                ),
                json.dumps(
                    {
                        "ts": 2,
                        "account": "acct-2",
                        "to": "lead-b",
                        "ok": True,
                        "detail": "sent_verified",
                        "source_engine": "campaign",
                        "campaign_alias": "otro",
                    }
                ),
                json.dumps(
                    {
                        "ts": 3,
                        "account": "acct-3",
                        "to": "lead-c",
                        "ok": True,
                        "detail": "sent_verified",
                        "source_engine": "responder",
                    }
                ),
                json.dumps(
                    {
                        "ts": 4,
                        "account": "acct-4",
                        "to": "lead-d",
                        "ok": True,
                        "detail": "sent_verified",
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("core.storage.SENT", sent_file)

    scoped = successful_contacts_index(
        source_engine="campaign",
        campaign_alias="matias",
        include_legacy=False,
    )
    global_index = successful_contacts_index()

    assert scoped == {"lead-a": {"acct-1"}}
    assert set(global_index) == {"lead-a", "lead-b", "lead-c", "lead-d"}
