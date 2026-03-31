from __future__ import annotations

import pytest

import core.storage as storage_module
import src.campaign_timezone_policy as policy


def test_no_proxy_campaign_browser_timezone_uses_system_timezone(monkeypatch) -> None:
    monkeypatch.setattr(
        policy,
        "account_proxy_preflight",
        lambda *_args, **_kwargs: {
            "network_mode": "direct",
            "proxy_id": "",
            "proxy_label": "direct",
        },
    )
    monkeypatch.setattr(policy, "resolve_system_timezone_id", lambda: "America/Montevideo")

    resolved = policy.resolve_campaign_browser_timezone({"username": "tester", "assigned_proxy_id": ""})

    assert resolved.timezone_id == "America/Montevideo"
    assert resolved.browser_timezone_source == "system"
    assert resolved.has_proxy is False
    assert resolved.business_timezone_id == storage_module.TZ_LABEL


def test_proxy_campaign_browser_timezone_uses_explicit_proxy_timezone(monkeypatch) -> None:
    monkeypatch.setattr(
        policy,
        "account_proxy_preflight",
        lambda *_args, **_kwargs: {
            "network_mode": "proxy",
            "proxy_id": "proxy-1",
            "proxy_label": "proxy-1",
            "record": {"id": "proxy-1", "timezone_id": "Europe/Madrid"},
        },
    )
    monkeypatch.setattr(policy, "resolve_system_timezone_id", lambda: "America/Montevideo")

    resolved = policy.resolve_campaign_browser_timezone({"username": "tester", "assigned_proxy_id": "proxy-1"})

    assert resolved.timezone_id == "Europe/Madrid"
    assert resolved.browser_timezone_source == "proxy"
    assert resolved.has_proxy is True
    assert resolved.proxy_id == "proxy-1"
    assert resolved.business_timezone_id == storage_module.TZ_LABEL


def test_proxy_campaign_browser_timezone_requires_explicit_proxy_timezone(monkeypatch) -> None:
    monkeypatch.setattr(
        policy,
        "account_proxy_preflight",
        lambda *_args, **_kwargs: {
            "network_mode": "proxy",
            "proxy_id": "proxy-1",
            "proxy_label": "proxy-1",
            "record": {"id": "proxy-1"},
        },
    )

    with pytest.raises(policy.CampaignTimezoneResolutionError) as exc_info:
        policy.resolve_campaign_browser_timezone({"username": "tester", "assigned_proxy_id": "proxy-1"})

    assert exc_info.value.reason_code == "PROXY_TIMEZONE_MISSING"
    assert exc_info.value.browser_timezone_source == "proxy"
    assert exc_info.value.business_timezone_id == storage_module.TZ_LABEL


def test_business_timezone_id_stays_bound_to_storage_policy() -> None:
    assert policy.business_timezone_id() == storage_module.TZ_LABEL
