from __future__ import annotations

import src.auth.persistent_login as persistent_login


def test_accounts_login_timeouts_prefer_accounts_specific_env(monkeypatch) -> None:
    monkeypatch.setenv(persistent_login.LEGACY_LEADS_LOGIN_NAV_TIMEOUT_ENV, "28000")
    monkeypatch.setenv(persistent_login.ACCOUNTS_LOGIN_NAV_TIMEOUT_ENV, "9000")
    monkeypatch.setenv(persistent_login.LEGACY_LEADS_LOGIN_INIT_TIMEOUT_ENV, "220")
    monkeypatch.setenv(persistent_login.ACCOUNTS_LOGIN_INIT_TIMEOUT_ENV, "150")

    assert persistent_login._bootstrap_nav_timeout_ms() == 9_000
    assert persistent_login._accounts_login_init_timeout_seconds(headless=False) == 150.0


def test_accounts_login_timeouts_fall_back_to_legacy_env_names(monkeypatch) -> None:
    monkeypatch.delenv(persistent_login.ACCOUNTS_LOGIN_NAV_TIMEOUT_ENV, raising=False)
    monkeypatch.delenv(persistent_login.ACCOUNTS_LOGIN_INIT_TIMEOUT_ENV, raising=False)
    monkeypatch.setenv(persistent_login.LEGACY_LEADS_LOGIN_NAV_TIMEOUT_ENV, "23000")
    monkeypatch.setenv(persistent_login.LEGACY_LEADS_LOGIN_INIT_TIMEOUT_ENV, "145")

    assert persistent_login._bootstrap_nav_timeout_ms() == 23_000
    assert persistent_login._accounts_login_init_timeout_seconds(headless=True) == 145.0
