<<<<<<< HEAD
import asyncio
=======
>>>>>>> origin/main
from pathlib import Path

from automation.actions import interactions


def test_select_accounts_playwright_skips_proxy_preflight_blocked_accounts(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    warnings: list[str] = []

    monkeypatch.setattr(
        interactions,
        "list_all",
        lambda: [
            {"username": "ready", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-ready"},
            {"username": "blocked", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-blocked"},
            {"username": "other", "alias": "alias-b", "active": True},
        ],
    )
    monkeypatch.setattr(
        interactions,
        "preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [dict(item) for item in accounts if item.get("username") == "ready"],
            "blocked_accounts": [
                {
                    "username": "blocked",
                    "status": "quarantined",
                    "message": "proxy quarantined",
                }
            ],
        },
    )
    monkeypatch.setattr(interactions, "_profiles_root", lambda: tmp_path)
    monkeypatch.setattr(interactions, "ask", lambda prompt="": "*")
    monkeypatch.setattr(interactions, "warn", lambda message: warnings.append(str(message)))
    monkeypatch.setattr(interactions, "press_enter", lambda _msg="": None)

    result = interactions._select_accounts_playwright("alias-a")

    captured = capsys.readouterr().out
    assert [str(item.get("username") or "") for item in result] == ["ready"]
    assert "@ready" in captured
    assert "@blocked" not in captured
    assert any("@blocked" in message and "proxy quarantined" in message for message in warnings)


def test_select_accounts_playwright_returns_empty_when_preflight_blocks_everything(monkeypatch) -> None:
    warnings: list[str] = []
    pressed = {"count": 0}

    monkeypatch.setattr(
        interactions,
        "list_all",
        lambda: [{"username": "blocked", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-blocked"}],
    )
    monkeypatch.setattr(
        interactions,
        "preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [],
            "blocked_accounts": [
                {
                    "username": "blocked",
                    "status": "inactive",
                    "message": "proxy inactive",
                }
            ],
        },
    )
    monkeypatch.setattr(interactions, "warn", lambda message: warnings.append(str(message)))
    monkeypatch.setattr(interactions, "press_enter", lambda _msg="": pressed.__setitem__("count", pressed["count"] + 1))
    monkeypatch.setattr(
        interactions,
        "ask",
        lambda prompt="": (_ for _ in ()).throw(AssertionError("selection should not be requested")),
    )

    result = interactions._select_accounts_playwright("alias-a")

    assert result == []
    assert pressed["count"] == 1
    assert any("proxy inactive" in message for message in warnings)
    assert any("No hay cuentas utilizables tras el preflight de proxy." in message for message in warnings)
<<<<<<< HEAD


def test_build_like_progress_targets_returns_sorted_spread_marks(monkeypatch) -> None:
    monkeypatch.setattr(interactions.random, "uniform", lambda a, b: (a + b) / 2)

    targets = interactions._build_like_progress_targets(5)

    assert len(targets) == 5
    assert targets == sorted(targets)
    assert all(0.12 <= value <= 0.92 for value in targets)
    assert all(right > left for left, right in zip(targets, targets[1:]))


def test_build_follow_progress_targets_returns_sorted_spread_marks(monkeypatch) -> None:
    monkeypatch.setattr(interactions.random, "uniform", lambda a, b: (a + b) / 2)

    targets = interactions._build_follow_progress_targets(3)

    assert len(targets) == 3
    assert targets == sorted(targets)
    assert all(0.2 <= value <= 0.9 for value in targets)
    assert all(right > left for left, right in zip(targets, targets[1:]))


def test_run_reels_for_account_spreads_likes_follows_and_respects_session_time(monkeypatch) -> None:
    class _FakeStopEvent:
        @staticmethod
        def is_set() -> bool:
            return False

    class _Clock:
        def __init__(self) -> None:
            self.now = 0.0

        def monotonic(self) -> float:
            return self.now

        async def sleep(self, seconds: float) -> bool:
            self.now += max(0.0, float(seconds or 0.0))
            return True

    clock = _Clock()
    liked_views: list[int] = []
    followed_views: list[int] = []
    advanced_views: list[int] = []

    async def _noop(*_args, **_kwargs) -> None:
        return None

    async def _fake_like(_page) -> bool:
        liked_views.append(summary.viewed)
        return True

    async def _fake_next(_page) -> None:
        advanced_views.append(summary.viewed)

    async def _fake_follow(_page, _attempted_profiles) -> tuple[bool, str]:
        followed_views.append(summary.viewed)
        return True, ""

    monkeypatch.setattr(interactions, "STOP_EVENT", _FakeStopEvent())
    monkeypatch.setattr(interactions.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(interactions, "_sleep_with_stop_async", clock.sleep)
    monkeypatch.setattr(interactions, "_dismiss_popups_async", _noop)
    monkeypatch.setattr(interactions, "_try_like_current_reel", _fake_like)
    monkeypatch.setattr(interactions, "_try_follow_current_reel_author", _fake_follow)
    monkeypatch.setattr(interactions, "_next_reel", _fake_next)
    monkeypatch.setattr(interactions.random, "uniform", lambda a, b: (a + b) / 2)

    summary = interactions.ReelsPlaywrightSummary(username="tester")

    asyncio.run(
        interactions._run_reels_for_account(
            page=object(),
            summary=summary,
            duration_s=160,
            likes_target=3,
            follows_target=2,
        )
    )

    assert summary.liked == 3
    assert summary.followed == 2
    assert summary.viewed >= 5
    assert all((right - left) > 1 for left, right in zip(liked_views, liked_views[1:]))
    assert all((right - left) > 1 for left, right in zip(followed_views, followed_views[1:]))
    assert advanced_views
    assert clock.now <= 160.0
=======
>>>>>>> origin/main
