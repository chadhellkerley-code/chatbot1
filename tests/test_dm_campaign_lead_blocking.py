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
    lead_status_store._PREFILTER_SNAPSHOT_CACHE.clear()


def test_mark_lead_sent_updates_global_contact_store_with_real_ttl(monkeypatch, tmp_path) -> None:
    _configure_lead_status_store(monkeypatch, tmp_path)
    lead_status_store._FILE.write_text(
        json.dumps(
            {
                "version": 1,
                "leads": {
                    "lead-1": {"status": "sent", "sent_by": "acct-legacy"},
                },
            }
        ),
        encoding="utf-8",
    )

    lead_status_store.mark_lead_sent("lead-1", sent_by="acct-1", alias="matias")

    payload = json.loads(lead_status_store._FILE.read_text(encoding="utf-8"))
    global_entry = payload["global_contacted_leads"]["lead-1"]

    assert payload["version"] == 3
    assert payload["aliases"]["matias"]["leads"]["lead-1"]["status"] == "sent"
    assert payload["legacy_global_leads"]["lead-1"]["status"] == "sent"
    assert global_entry["last_status"] == "sent"
    assert global_entry["last_alias"] == "matias"
    assert global_entry["last_campaign"] == "matias"
    assert global_entry["last_account"] == "acct-1"
    assert lead_status_store.is_terminal_lead_status("lead-1", alias="matias") is True
    assert lead_status_store.is_globally_contact_blocked("lead-1", now=global_entry["last_contacted_at"] + 10) is True
    assert (
        lead_status_store.is_globally_contact_blocked(
            "lead-1",
            now=global_entry["last_contacted_at"] + lead_status_store.GLOBAL_CONTACT_TTL_SECONDS,
        )
        is False
    )


def test_failed_and_skipped_statuses_do_not_create_global_contact(monkeypatch, tmp_path) -> None:
    _configure_lead_status_store(monkeypatch, tmp_path)

    lead_status_store.mark_lead_failed("lead-failed", reason="FLOW_TIMEOUT", alias="matias")
    lead_status_store.mark_lead_skipped("lead-skipped", reason="username_not_found", alias="matias")

    assert lead_status_store.get_global_contact_record("lead-failed") is None
    assert lead_status_store.get_global_contact_record("lead-skipped") is None


def test_campaign_prefilter_blocks_recent_global_contacts_across_aliases_and_allows_expired_ones(monkeypatch) -> None:
    now_ts = 1_700_000_000
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.time.time", lambda: now_ts)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda accounts, *, campaign_alias="": {
            "daily_counts": {account: 0 for account in accounts},
            "campaign_registry": {"campaign-history"},
            "shared_registry": {"shared-history"},
        },
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_account_usernames",
        lambda alias: {"acct-1"},
    )

    pending, stats = _filter_pending_leads_for_campaign(
        [
            "pepito",
<<<<<<< HEAD
            "already-selected",
            "alias-recent-sent",
=======
>>>>>>> origin/main
            "old-alias-sent",
            "alias-skipped",
            "expired-global",
            "campaign-history",
            "shared-history",
            "fresh",
        ],
        alias="nuevo1",
<<<<<<< HEAD
        run_id="run-123",
        alias_status_map={
            "already-selected": {"status": "pending", "updated_at": now_ts - 5, "pending_run_id": "run-123"},
            "alias-recent-sent": {"status": "sent", "sent_timestamp": now_ts - 30},
=======
        alias_status_map={
>>>>>>> origin/main
            "old-alias-sent": {"status": "sent", "sent_timestamp": now_ts - lead_status_store.GLOBAL_CONTACT_TTL_SECONDS - 1},
            "alias-skipped": {"status": "skipped", "skipped_timestamp": now_ts - 30},
        },
        global_contact_map={
            "pepito": {"last_contacted_at": now_ts - 60, "last_status": "sent", "last_alias": "nuevo"},
<<<<<<< HEAD
            "already-selected": {
                "last_contacted_at": now_ts - 10,
                "last_status": "sent",
                "last_alias": "externa",
            },
=======
>>>>>>> origin/main
            "expired-global": {
                "last_contacted_at": now_ts - lead_status_store.GLOBAL_CONTACT_TTL_SECONDS - 1,
                "last_status": "sent",
                "last_alias": "viejo",
            },
        },
    )

    assert pending == [
<<<<<<< HEAD
        "already-selected",
=======
>>>>>>> origin/main
        "old-alias-sent",
        "expired-global",
        "campaign-history",
        "shared-history",
        "fresh",
    ]
<<<<<<< HEAD
    assert stats["blocked_total"] == 3
    assert stats["blocked_by_global_contact"] == 1
    assert stats["blocked_by_alias_sent_status"] == 1
    assert stats["blocked_by_alias_skipped_status"] == 1
    assert stats["preserved_pending"] == 1
=======
    assert stats["blocked_total"] == 2
    assert stats["blocked_by_global_contact"] == 1
    assert stats["blocked_by_alias_skipped_status"] == 1
>>>>>>> origin/main
    assert stats["advisory_alias_sent_ignored"] == 1
    assert stats["advisory_campaign_registry_hits"] == 1
    assert stats["advisory_shared_registry_hits"] == 1


def test_get_prefilter_snapshot_bootstraps_global_contacts_from_sent_log_and_reuses_cache(monkeypatch, tmp_path) -> None:
    _configure_lead_status_store(monkeypatch, tmp_path)
    lead_status_store._FILE.write_text(
        json.dumps(
            {
                "version": 3,
                "aliases": {
                    "matias": {
                        "leads": {
                            "lead-a": {"status": "sent", "sent_timestamp": 100},
<<<<<<< HEAD
                            "lead-pending": {"status": "pending", "updated_at": 250},
=======
>>>>>>> origin/main
                        }
                    }
                },
                "legacy_global_leads": {
                    "lead-b": {"status": "sent", "sent_by": "acct-1", "sent_timestamp": 200},
                },
                "global_contacted_leads": {},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "storage" / "sent_log.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts": 300,
                        "account": "acct-2",
                        "to": "lead-c",
                        "ok": True,
                        "detail": "sent_verified",
                        "source_engine": "campaign",
                        "campaign_alias": "otro",
                    }
                ),
                json.dumps(
                    {
                        "ts": 400,
                        "account": "acct-3",
                        "to": "lead-d",
                        "ok": True,
                        "detail": "sent_unverified",
                        "source_engine": "campaign",
                        "campaign_alias": "otro",
                        "sent_unverified": True,
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    original_load_json_file = lead_status_store.load_json_file
    calls = {"count": 0}

    def _counting_load_json_file(*args, **kwargs):
        calls["count"] += 1
        return original_load_json_file(*args, **kwargs)

    monkeypatch.setattr(lead_status_store, "load_json_file", _counting_load_json_file)

    first_alias_status, first_global = lead_status_store.get_prefilter_snapshot("matias")
    second_alias_status, second_global = lead_status_store.get_prefilter_snapshot("matias")

    assert first_alias_status["lead-a"]["status"] == "sent"
    assert second_alias_status["lead-a"]["status"] == "sent"
<<<<<<< HEAD
    assert first_alias_status["lead-pending"]["status"] == "pending"
    assert second_alias_status["lead-pending"]["status"] == "pending"
    assert first_global["lead-a"]["last_contacted_at"] == 100
    assert first_global["lead-b"]["last_contacted_at"] == 200
    assert first_global["lead-c"]["last_contacted_at"] == 300
    assert first_global["lead-d"]["last_contacted_at"] == 400
=======
    assert first_global["lead-a"]["last_contacted_at"] == 100
    assert first_global["lead-b"]["last_contacted_at"] == 200
    assert first_global["lead-c"]["last_contacted_at"] == 300
    assert "lead-d" not in first_global
>>>>>>> origin/main
    assert second_global == first_global
    assert calls["count"] == 1


<<<<<<< HEAD
def test_pending_preselection_stays_prioritized_and_overrides_later_global_block(monkeypatch, tmp_path) -> None:
    _configure_lead_status_store(monkeypatch, tmp_path)
    now_ts = 1_700_000_000
    monkeypatch.setattr("src.dm_campaign.lead_status_store.time.time", lambda: now_ts)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.time.time", lambda: now_ts)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda accounts, *, campaign_alias="": {
            "daily_counts": {account: 0 for account in accounts},
            "campaign_registry": set(),
            "shared_registry": set(),
        },
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_account_usernames",
        lambda alias: {"acct-1"},
    )

    marked = lead_status_store.mark_leads_pending(["fresh-2", "fresh-1"], alias="nuevo1", run_id="run-keep")
    alias_status_map, _global_contact_map = lead_status_store.get_prefilter_snapshot("nuevo1")

    pending, stats = _filter_pending_leads_for_campaign(
        ["fresh-3", "fresh-2", "fresh-1"],
        alias="nuevo1",
        run_id="run-keep",
        alias_status_map=alias_status_map,
        global_contact_map={
            "fresh-2": {
                "last_contacted_at": now_ts - 30,
                "last_status": "sent",
                "last_alias": "externa",
            }
        },
    )

    assert marked == 2
    assert pending == ["fresh-2", "fresh-1", "fresh-3"]
    assert stats["preserved_pending"] == 2
    assert stats["blocked_total"] == 0


def test_stale_pending_from_other_run_does_not_override_recent_contact_block(monkeypatch) -> None:
    now_ts = 1_700_000_000
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.time.time", lambda: now_ts)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda accounts, *, campaign_alias="": {
            "daily_counts": {account: 0 for account in accounts},
            "campaign_registry": set(),
            "shared_registry": set(),
        },
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_account_usernames",
        lambda alias: {"acct-1"},
    )

    pending, stats = _filter_pending_leads_for_campaign(
        ["stale-pending", "fresh"],
        alias="nuevo1",
        run_id="run-current",
        alias_status_map={
            "stale-pending": {"status": "pending", "updated_at": now_ts - 5, "pending_run_id": "run-old"},
        },
        global_contact_map={
            "stale-pending": {
                "last_contacted_at": now_ts - 20,
                "last_status": "sent",
                "last_alias": "externa",
            }
        },
    )

    assert pending == ["fresh"]
    assert stats["preserved_pending"] == 0
    assert stats["stale_pending_ignored"] == 1
    assert stats["blocked_total"] == 1


=======
>>>>>>> origin/main
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
<<<<<<< HEAD


def test_get_prefilter_snapshot_bootstraps_recent_non_campaign_sent_log_as_global_contact(monkeypatch, tmp_path) -> None:
    _configure_lead_status_store(monkeypatch, tmp_path)
    lead_status_store._FILE.write_text(
        json.dumps(
            {
                "version": 3,
                "aliases": {"matias": {"leads": {}}},
                "legacy_global_leads": {},
                "global_contacted_leads": {},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "storage" / "sent_log.jsonl").write_text(
        json.dumps(
            {
                "ts": 1_700_000_000,
                "account": "acct-1",
                "to": "lead-non-campaign",
                "ok": True,
                "detail": "sent_verified",
                "source_engine": "inbox",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    _alias_status_map, global_contact_map = lead_status_store.get_prefilter_snapshot("matias")

    assert global_contact_map["lead-non-campaign"]["last_account"] == "acct-1"
=======
>>>>>>> origin/main
