from __future__ import annotations

from src.dm_campaign.proxy_workers_runner import load_accounts


def test_load_accounts_uses_daily_capacity_snapshot(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.list_all",
        lambda: [
            {
                "username": "acct-1",
                "alias": "matias",
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
                "alias": "matias",
                "active": False,
                "connected": True,
            },
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.connected_status",
        lambda account, **_kwargs: bool(account.get("connected")),
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
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.list_all",
        lambda: [
            {
                "username": "acct-1",
                "alias": "matias",
                "active": True,
                "connected": False,
                "sent_today": 0,
            },
            {
                "username": "acct-2",
                "alias": "matias",
                "active": True,
                "connected": False,
                "sent_today": 0,
            },
        ],
    )

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
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("snapshot should not be called")),
    )

    accounts = load_accounts("matias", sent_today_counts={"acct-1": 2})

    assert [item["username"] for item in accounts] == ["acct-1"]
    assert accounts[0]["sent_today"] == 2
    assert captured["checks"] == [
        {"username": "acct-1", "fast": True, "persist": False, "reason": "campaign-load"},
        {"username": "acct-2", "fast": True, "persist": False, "reason": "campaign-load"},
    ]
