from __future__ import annotations

<<<<<<< HEAD
from src.dm_campaign.proxy_workers_runner import _apply_sent_today_counts, load_accounts
=======
from src.dm_campaign.proxy_workers_runner import load_accounts
>>>>>>> origin/main


def test_load_accounts_uses_daily_capacity_snapshot(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
<<<<<<< HEAD
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {
                "username": "acct-1",
                "alias": alias,
=======
        "src.dm_campaign.proxy_workers_runner.list_all",
        lambda: [
            {
                "username": "acct-1",
                "alias": "matias",
>>>>>>> origin/main
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
<<<<<<< HEAD
                "alias": alias,
=======
                "alias": "matias",
>>>>>>> origin/main
                "active": False,
                "connected": True,
            },
        ],
    )
    monkeypatch.setattr(
<<<<<<< HEAD
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {"ready_accounts": [dict(accounts[0])], "blocked_accounts": []},
=======
        "src.dm_campaign.proxy_workers_runner.connected_status",
        lambda account, **_kwargs: bool(account.get("connected")),
>>>>>>> origin/main
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
<<<<<<< HEAD
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {
                "username": "acct-1",
                "alias": alias,
=======
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.list_all",
        lambda: [
            {
                "username": "acct-1",
                "alias": "matias",
>>>>>>> origin/main
                "active": True,
                "connected": False,
                "sent_today": 0,
            },
            {
                "username": "acct-2",
<<<<<<< HEAD
                "alias": alias,
=======
                "alias": "matias",
>>>>>>> origin/main
                "active": True,
                "connected": False,
                "sent_today": 0,
            },
        ],
    )
<<<<<<< HEAD
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {"ready_accounts": [dict(accounts[0])], "blocked_accounts": []},
=======

    def _fake_connected_status(account, **kwargs):
        captured.setdefault("checks", []).append(
            {
                "username": str(account.get("username") or ""),
                "fast": kwargs.get("fast"),
                "persist": kwargs.get("persist"),
                "reason": kwargs.get("reason"),
            }
        )
        return str(account.get("username") or "") == "acct-1"

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.connected_status",
        _fake_connected_status,
>>>>>>> origin/main
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("snapshot should not be called")),
    )

    accounts = load_accounts("matias", sent_today_counts={"acct-1": 2})

    assert [item["username"] for item in accounts] == ["acct-1"]
    assert accounts[0]["sent_today"] == 2
<<<<<<< HEAD


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
=======
    assert captured["checks"] == [
        {"username": "acct-1", "fast": True, "persist": False, "reason": "campaign-load"},
        {"username": "acct-2", "fast": True, "persist": False, "reason": "campaign-load"},
    ]
>>>>>>> origin/main
