from __future__ import annotations

from datetime import timedelta

from core import account_limits
from core import storage


def test_sent_counts_today_by_account_counts_only_successful_records_for_today(monkeypatch) -> None:
    now = storage._now_local()
    records = [
        {"account": "AcctA", "ok": True, "local_dt": now},
        {"account": "accta", "ok": True, "local_dt": now},
        {"account": "AcctA", "ok": False, "local_dt": now},
        {"account": "AcctA", "ok": True, "skip_reason": "SKIPPED_NO_DM", "local_dt": now},
        {"account": "AcctA", "ok": True, "local_dt": now - timedelta(days=1)},
        {"account": "AcctB", "ok": True, "local_dt": now},
    ]
    monkeypatch.setattr(storage, "_iter_records", lambda: iter(records))

    counts = storage.sent_counts_today_by_account(["AcctA", "AcctB", "missing"])

    assert counts == {"accta": 2, "acctb": 1, "missing": 0}
    assert storage.sent_count_today_for_account("@AcctA") == 2


def test_sent_counts_today_by_account_can_scope_campaign_records(monkeypatch) -> None:
    now = storage._now_local()
    records = [
        {
            "account": "AcctA",
            "ok": True,
            "local_dt": now,
            "source_engine": "campaign",
            "campaign_alias": "matias",
            "run_id": "run-1",
        },
        {
            "account": "AcctA",
            "ok": True,
            "local_dt": now,
            "source_engine": "campaign",
            "campaign_alias": "matias",
            "run_id": "run-2",
        },
        {"account": "AcctA", "ok": True, "local_dt": now, "source_engine": "campaign", "campaign_alias": "otro"},
        {"account": "AcctA", "ok": True, "local_dt": now, "source_engine": "responder"},
        {"account": "AcctA", "ok": True, "local_dt": now},
        {"account": "AcctA", "ok": False, "local_dt": now, "source_engine": "campaign", "campaign_alias": "matias"},
    ]
    monkeypatch.setattr(storage, "_iter_records", lambda: iter(records))

    counts = storage.sent_counts_today_by_account(
        ["AcctA"],
        source_engine="campaign",
        campaign_alias="matias",
        run_id="run-1",
        include_legacy=False,
    )

    assert counts == {"accta": 1}
    assert storage.sent_count_today_for_account(
        "@AcctA",
        source_engine="campaign",
        campaign_alias="matias",
        run_id="run-1",
        include_legacy=False,
    ) == 1


def test_can_send_message_for_account_uses_campaign_quota_hints(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_sent_count_today_for_account(
        account: str,
        *,
        source_engine: str | None = None,
        campaign_alias: str | None = None,
        run_id: str | None = None,
        include_legacy: bool = True,
    ) -> int:
        captured["account"] = account
        captured["source_engine"] = source_engine
        captured["campaign_alias"] = campaign_alias
        captured["run_id"] = run_id
        captured["include_legacy"] = include_legacy
        return 2

    monkeypatch.setattr(account_limits, "sent_count_today_for_account", _fake_sent_count_today_for_account)

    can_send, sent_today, limit = account_limits.can_send_message_for_account(
        account={
            "username": "AcctA",
            "messages_per_account": 3,
            "quota_source_engine": "campaign",
            "quota_campaign_alias": "matias",
            "quota_run_id": "run-1",
            "quota_include_legacy": False,
        }
    )

    assert can_send is True
    assert sent_today == 2
    assert limit == 3
    assert captured == {
        "account": "AcctA",
        "source_engine": "campaign",
        "campaign_alias": "matias",
        "run_id": "run-1",
        "include_legacy": False,
    }
