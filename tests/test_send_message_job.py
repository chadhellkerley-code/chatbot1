from __future__ import annotations

from src.jobs.send_message_job import send_dm


class _FakeStateManager:
    def is_rate_limited(self, username: str) -> bool:
        return False

    def increment_daily_counter(self, username: str, counter_name: str = "messages_sent") -> int:
        raise AssertionError("No deberia incrementar el contador cuando la cuota ya fue alcanzada")

    def save_account_state(self, username: str, state: dict) -> bool:
        return True

    def set_rate_limit(self, username: str, seconds: int) -> None:
        return None


def test_send_dm_skips_when_account_quota_is_reached(monkeypatch) -> None:
    logged: list[tuple[tuple, dict]] = []

    monkeypatch.setattr("src.jobs.send_message_job.get_state_manager", lambda: _FakeStateManager())
    monkeypatch.setattr("src.jobs.send_message_job.get_account", lambda username: {"username": username, "messages_per_account": 1})
    monkeypatch.setattr(
        "src.jobs.send_message_job.can_send_message_for_account",
        lambda **_kwargs: (False, 1, 1),
    )
    monkeypatch.setattr(
        "src.jobs.send_message_job.log_sent",
        lambda *args, **kwargs: logged.append((args, kwargs)),
    )

    result = send_dm.run(
        username="acct-1",
        password="secret",
        proxy=None,
        target_user="lead-1",
        message_text="hola",
        human_delay=False,
    )

    assert result["success"] is False
    assert result["skipped"] is True
    assert result["reason"] == "ACCOUNT_QUOTA_REACHED"
    assert result["daily_count"] == 1
    assert result["daily_limit"] == 1
    assert logged == [
        (
            ("acct-1", "lead-1", False, "account_quota_reached"),
            {"skip": True, "skip_reason": "ACCOUNT_QUOTA_REACHED"},
        )
    ]
