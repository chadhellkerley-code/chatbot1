from __future__ import annotations

from src.dm_campaign.proxy_workers_runner import _apply_sent_today_counts, load_accounts


def test_load_accounts_uses_daily_capacity_snapshot(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {
                "username": "acct-1",
                "alias": alias,
                "active": True,
                "connected": True,
                "sent_today": 1,
            },
            {
                "username": "acct-2",
                "alias": "otro",
                "active": True,
                "connected": True,
            },
            {
                "username": "acct-3",
                "alias": alias,
                "active": False,
                "connected": True,
            },
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {"ready_accounts": [dict(accounts[0])], "blocked_accounts": []},
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda accounts, *, campaign_alias="": captured.update(
            {"accounts": set(accounts), "campaign_alias": campaign_alias}
        )
        or {"daily_counts": {"acct-1": 4}, "campaign_registry": set(), "shared_registry": set()},
    )

    accounts = load_accounts("matias")

    assert len(accounts) == 1
    assert accounts[0]["username"] == "acct-1"
    assert accounts[0]["sent_today"] == 4
    assert captured == {
        "accounts": {"acct-1"},
        "campaign_alias": "matias",
    }


def test_load_accounts_reuses_preloaded_daily_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {
                "username": "acct-1",
                "alias": alias,
                "active": True,
                "connected": False,
                "sent_today": 0,
            },
            {
                "username": "acct-2",
                "alias": alias,
                "active": True,
                "connected": False,
                "sent_today": 0,
            },
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {"ready_accounts": [dict(accounts[0])], "blocked_accounts": []},
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("snapshot should not be called")),
    )

    accounts = load_accounts("matias", sent_today_counts={"acct-1": 2})

    assert [item["username"] for item in accounts] == ["acct-1"]
    assert accounts[0]["sent_today"] == 2


def test_load_accounts_reconciles_daily_counts_with_live_sent_log(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {
                "username": "acct-1",
                "alias": alias,
                "active": True,
                "connected": True,
                "sent_today": 0,
            },
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {"ready_accounts": [dict(accounts[0])], "blocked_accounts": []},
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda accounts, *, campaign_alias="": {
            "daily_counts": {"acct-1": 0},
            "campaign_registry": set(),
            "shared_registry": set(),
        },
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.can_send_message_for_account",
        lambda **_kwargs: (True, 3, 10),
    )

    accounts = load_accounts("matias")

    assert accounts[0]["sent_today"] == 3


def test_apply_sent_today_counts_never_reduces_live_count() -> None:
    accounts = _apply_sent_today_counts(
        [{"username": "acct-1", "sent_today": 5}],
        sent_today_counts={"acct-1": 2},
    )

    assert accounts[0]["sent_today"] == 5
