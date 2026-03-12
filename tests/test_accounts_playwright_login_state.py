from __future__ import annotations

from pathlib import Path

from core import accounts
import src.auth.onboarding as onboarding


def test_relogin_clears_stale_storage_state_on_failed_result(
    monkeypatch,
    tmp_path: Path,
) -> None:
    profile_dir = tmp_path / "tester"
    profile_dir.mkdir(parents=True, exist_ok=True)
    storage_state = profile_dir / "storage_state.json"
    storage_state.write_text('{"cookies":[],"origins":[]}', encoding="utf-8")

    mark_calls: list[tuple[str, bool, bool]] = []
    expired_calls: list[tuple[str, str]] = []
    captured_payloads: list[dict[str, object]] = []

    monkeypatch.setattr(accounts, "BASE_PROFILES", tmp_path)
    monkeypatch.setattr(accounts, "_refresh_totp_export_cache", lambda force=True: None)
    monkeypatch.setattr(
        accounts,
        "_playwright_account_payload",
        lambda username, password, _account, force_totp_refresh=False: {
            "username": username,
            "password": password,
            "force_totp_refresh": force_totp_refresh,
        },
    )
    monkeypatch.setattr(
        onboarding,
        "login_account_playwright",
        lambda payload, alias, headful=True: captured_payloads.append(dict(payload)) or {
            "username": payload.get("username") or "",
            "status": "failed",
            "message": "challenge_required",
            "profile_path": "",
            "row_number": payload.get("row_number"),
        },
    )
    monkeypatch.setattr(onboarding, "write_onboarding_results", lambda _rows: None)
    monkeypatch.setattr(
        accounts,
        "mark_connected",
        lambda username, connected, *, invalidate_health=True: mark_calls.append(
            (username, connected, invalidate_health)
        ),
    )
    monkeypatch.setattr(accounts.health_store, "get_badge", lambda _username: (None, True))
    monkeypatch.setattr(
        accounts.health_store,
        "mark_session_expired",
        lambda username, *, reason="": expired_calls.append((username, reason)) or "NO ACTIVA",
    )

    results = accounts.relogin_accounts_with_playwright(
        "matias",
        [{"username": "tester", "password": "secret"}],
        concurrency=1,
    )

    assert results == [
        {
            "username": "tester",
            "status": "failed",
            "message": "challenge_required",
            "profile_path": "",
            "row_number": None,
        }
    ]
    assert storage_state.exists() is False
    assert mark_calls == [("tester", False, False)]
    assert expired_calls == [("tester", "login_failed")]
    assert len(captured_payloads) == 1
    payload = captured_payloads[0]
    assert payload["username"] == "tester"
    assert payload["password"] == "secret"
    assert payload["force_totp_refresh"] is True
    assert payload["alias"] == "matias"
    assert payload["strict_login"] is True
    assert payload["force_login"] is True
    assert payload["disable_safe_browser_recovery"] is True
    assert callable(payload["login_progress_callback"])
