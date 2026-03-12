from __future__ import annotations

from src.autoresponder_runtime import (
    AutoresponderRuntimeController,
    AutoresponderRuntimeLimits,
)


def _limits(**overrides):
    base = AutoresponderRuntimeLimits(
        requests_per_minute=120,
        hydrations_per_cycle=3,
        hydrations_per_thread_per_hour=4,
        thread_cooldown_seconds=300,
        pending_queue_limit=10,
        backoff_base_seconds=30,
        backoff_max_seconds=180,
        temporary_disable_seconds=60,
        jitter_min_seconds=0.0,
        jitter_max_seconds=0.0,
    )
    values = dict(base.__dict__)
    values.update(overrides)
    return AutoresponderRuntimeLimits(**values)


def test_runtime_respects_cycle_limit_and_thread_cooldown():
    runtime = AutoresponderRuntimeController(limits=_limits(hydrations_per_cycle=2, thread_cooldown_seconds=600))
    account = "acct"

    runtime.begin_cycle(account, now_ts=1000.0)
    can, reason = runtime.should_hydrate(
        account,
        "thread-1",
        last_activity_at=900.0,
        critical=False,
        now_ts=1000.0,
    )
    assert can is True
    assert reason == "ok"

    runtime.record_hydration_attempt(
        account,
        "thread-1",
        success=True,
        complete=True,
        last_activity_at=900.0,
        now_ts=1000.0,
    )
    can, reason = runtime.should_hydrate(
        account,
        "thread-1",
        last_activity_at=900.0,
        critical=False,
        now_ts=1100.0,
    )
    assert can is False
    assert reason == "thread_cooldown"

    runtime.record_hydration_attempt(
        account,
        "thread-2",
        success=True,
        complete=True,
        last_activity_at=901.0,
        now_ts=1101.0,
    )
    can, reason = runtime.should_hydrate(
        account,
        "thread-3",
        last_activity_at=902.0,
        critical=False,
        now_ts=1102.0,
    )
    assert can is False
    assert reason == "cycle_limit"


def test_runtime_pending_queue_deduplicates_and_prioritizes():
    runtime = AutoresponderRuntimeController(limits=_limits(pending_queue_limit=2))
    account = "acct"

    runtime.enqueue_pending(account, "t-low", reason="low", priority=1, now_ts=10.0)
    runtime.enqueue_pending(account, "t-high", reason="high", priority=9, now_ts=11.0)
    runtime.enqueue_pending(account, "t-low", reason="low-refresh", priority=5, now_ts=12.0)
    runtime.enqueue_pending(account, "t-drop", reason="drop", priority=0, now_ts=13.0)

    items = runtime.dequeue_pending(account, limit=5)
    ids = [item.thread_id for item in items]
    assert ids == ["t-high", "t-low"]


def test_runtime_rate_signal_opens_and_closes_block_window():
    runtime = AutoresponderRuntimeController(
        limits=_limits(
            backoff_base_seconds=20,
            backoff_max_seconds=60,
            temporary_disable_seconds=15,
        )
    )
    account = "acct"

    runtime.mark_rate_signal(account, reason="429", now_ts=1000.0)
    blocked, remaining, reason = runtime.is_account_blocked(account, now_ts=1001.0)
    assert blocked is True
    assert remaining > 0.0
    assert reason in {"backoff", "hydration_disabled"}

    blocked, remaining, _reason = runtime.is_account_blocked(account, now_ts=1065.0)
    assert blocked is False
    assert remaining == 0.0


def test_runtime_explicit_pause_takes_priority_until_window_expires():
    runtime = AutoresponderRuntimeController(limits=_limits())
    account = "acct"

    runtime.mark_rate_signal(account, reason="429", now_ts=1000.0)
    runtime.pause_account(account, reason="checkpoint", duration_seconds=600.0, now_ts=1000.0)

    blocked, remaining, reason = runtime.is_account_blocked(account, now_ts=1001.0)

    assert blocked is True
    assert remaining > 0.0
    assert reason == "pause:checkpoint"

    blocked, remaining, reason = runtime.is_account_blocked(account, now_ts=1705.0)

    assert blocked is False
    assert remaining == 0.0
    assert reason == ""
