from __future__ import annotations

import asyncio
from types import SimpleNamespace

import src.leads_filter_pipeline as leads_filter_pipeline


def test_v2_init_runtime_skips_profile_preflight_and_uses_storage_cookies(monkeypatch) -> None:
    fake_client = object()
    warned: list[str] = []

    monkeypatch.setattr(
        leads_filter_pipeline,
        "create_authenticated_client",
        lambda account, reason="": fake_client,
    )

    async def _fake_capture(runtime) -> None:
        runtime.request_timeout_min_ms = 8_000

    async def _fail_preflight(*_args, **_kwargs):
        raise AssertionError("profile preflight should not run for filtering runtimes")

    monkeypatch.setattr(leads_filter_pipeline, "capture_runtime_http_meta", _fake_capture)
    monkeypatch.setattr(leads_filter_pipeline, "profile_endpoint_preflight", _fail_preflight)

    runtime = asyncio.run(
        leads_filter_pipeline._v2_init_runtime(
            {"username": "worker_one", "alias": "default"},
            run_cfg=SimpleNamespace(),
            per_account_concurrency=1,
            image_concurrency_per_account=1,
            profile_daily_budget=10,
            profile_delay_min_seconds=1.0,
            profile_delay_max_seconds=2.0,
            profile_retry_max=1,
            image_retry_max=1,
            rate_limit_retry_max=1,
            profile_circuit_breaker_threshold=1,
            profile_circuit_breaker_seconds=1.0,
            image_circuit_breaker_threshold=1,
            image_circuit_breaker_seconds=1.0,
            warn=warned.append,
        )
    )

    assert runtime is not None
    assert runtime.http_client is fake_client
    assert warned == []
