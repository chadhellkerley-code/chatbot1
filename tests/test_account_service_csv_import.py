from __future__ import annotations

import csv
from pathlib import Path

import pytest

import application.services.account_service as account_service_module
from application.services.account_service import AccountService
from application.services.base import ServiceContext, ServiceError


def _build_service(tmp_path: Path) -> AccountService:
    return AccountService(ServiceContext.default(tmp_path))


def test_add_account_requires_password(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    add_calls: list[tuple[str, str, dict | None]] = []

    def _fake_add_account(username: str, alias: str, proxy: dict | None) -> bool:
        add_calls.append((username, alias, proxy))
        return True

    monkeypatch.setattr(account_service_module.accounts_module, "add_account", _fake_add_account)

    with pytest.raises(ServiceError, match="Password invalida"):
        service.add_account("ok_one", "alias-a", password="")

    assert add_calls == []


def test_import_accounts_csv_discards_rows_missing_username_or_password(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = _build_service(tmp_path)
    csv_path = tmp_path / "accounts.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "username",
                "password",
                "2FA Code",
                "proxy id",
                "proxy port",
                "proxy username",
                "proxy password",
                "TOTP Secret",
            ],
        )
        writer.writeheader()
        writer.writerow({"username": "ok_one", "password": "secret-one", "TOTP Secret": "BASE32ONE"})
        writer.writerow({"username": "missing_pass", "password": "", "TOTP Secret": "BASE32SKIP"})
        writer.writerow({"username": "", "password": "secret-skip", "TOTP Secret": "BASE32SKIP2"})
        writer.writerow({"username": "ok_two", "password": "secret-two"})

    added_accounts: list[tuple[str, str, dict | None]] = []
    stored_passwords: list[tuple[str, str]] = []
    saved_totp: list[tuple[str, str]] = []
    built_payloads: list[dict] = []
    login_calls: list[dict] = []

    monkeypatch.setattr(account_service_module.accounts_module, "list_all", lambda: [])

    def _fake_add_account(username: str, alias: str, proxy: dict | None) -> bool:
        added_accounts.append((username, alias, proxy))
        return True

    def _fake_store_password(username: str, password: str) -> None:
        stored_passwords.append((username, password))

    def _fake_save_totp_secret(username: str, secret: str) -> None:
        saved_totp.append((username, secret))

    def _fake_build_login_payload(
        username: str,
        password: str,
        proxy: dict,
        *,
        alias: str,
        totp_secret: str = "",
    ) -> dict:
        payload = {
            "username": username,
            "password": password,
            "proxy": dict(proxy),
            "alias": alias,
            "totp_secret": totp_secret,
        }
        built_payloads.append(payload)
        return payload

    def _fake_login_accounts(alias: str, payloads: list[dict], *, concurrency: int = 1) -> list[dict]:
        login_calls.append(
            {
                "alias": alias,
                "payloads": [dict(item) for item in payloads],
                "concurrency": concurrency,
            }
        )
        return [{"status": "ok", "username": item["username"]} for item in payloads]

    monkeypatch.setattr(account_service_module.accounts_module, "add_account", _fake_add_account)
    monkeypatch.setattr(account_service_module.accounts_module, "_store_account_password", _fake_store_password)
    monkeypatch.setattr(account_service_module, "save_totp_secret", _fake_save_totp_secret)
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "_build_playwright_login_payload",
        _fake_build_login_payload,
    )
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "login_accounts_with_playwright",
        _fake_login_accounts,
    )

    result = service.import_accounts_csv(
        "alias-a",
        csv_path,
        login_after_import=True,
        concurrency=3,
    )

    assert result == {
        "alias": "alias-a",
        "added": 2,
        "skipped": 2,
        "imported_usernames": ["ok_one", "ok_two"],
        "login_usernames": ["ok_one", "ok_two"],
        "login_results": [
            {"status": "ok", "username": "ok_one"},
            {"status": "ok", "username": "ok_two"},
        ],
    }
    assert added_accounts == [
        ("ok_one", "alias-a", None),
        ("ok_two", "alias-a", None),
    ]
    assert stored_passwords == [
        ("ok_one", "secret-one"),
        ("ok_two", "secret-two"),
    ]
    assert saved_totp == [("ok_one", "BASE32ONE")]
    assert built_payloads == [
        {
            "username": "ok_one",
            "password": "secret-one",
            "proxy": {},
            "alias": "alias-a",
            "totp_secret": "BASE32ONE",
        },
        {
            "username": "ok_two",
            "password": "secret-two",
            "proxy": {},
            "alias": "alias-a",
            "totp_secret": "",
        },
    ]
    assert login_calls == [
        {
            "alias": "alias-a",
            "payloads": built_payloads,
            "concurrency": 1,
        }
    ]


@pytest.mark.parametrize(
    ("method_name", "backend_name"),
    [
        ("login", "login_accounts_with_playwright"),
        ("relogin", "relogin_accounts_with_playwright"),
    ],
)
def test_account_service_login_queue_is_always_sequential(
    monkeypatch,
    tmp_path: Path,
    method_name: str,
    backend_name: str,
) -> None:
    service = _build_service(tmp_path)
    captured_calls: list[dict] = []

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [{"username": "ok_one", "alias": "alias-a", "password": "secret-one"}],
    )
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "playwright_login_queue_concurrency",
        lambda: 1,
    )
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "_account_password",
        lambda record: record.get("password"),
    )

    if method_name == "login":
        monkeypatch.setattr(
            account_service_module.accounts_module,
            "_build_playwright_login_payload",
            lambda username, password, proxy_settings, *, alias, totp_secret=None, row_number=None: {
                "username": username,
                "password": password,
                "alias": alias,
            },
        )

    def _fake_backend(alias: str, payloads: list[dict], *, concurrency: int = 1) -> list[dict]:
        captured_calls.append(
            {
                "alias": alias,
                "payloads": [dict(item) for item in payloads],
                "concurrency": concurrency,
            }
        )
        return [{"status": "ok", "username": "ok_one"}]

    monkeypatch.setattr(account_service_module.accounts_module, backend_name, _fake_backend)

    result = getattr(service, method_name)("alias-a", ["ok_one"], concurrency=9)

    assert result == [{"status": "ok", "username": "ok_one"}]
    assert captured_calls == [
        {
            "alias": "alias-a",
            "payloads": captured_calls[0]["payloads"],
            "concurrency": 1,
        }
    ]


def test_login_skips_accounts_without_password_and_processes_valid_ones(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = _build_service(tmp_path)
    backend_calls: list[dict] = []

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [
            {"username": "missing", "alias": "alias-a"},
            {"username": "ok_one", "alias": "alias-a", "password": "secret-one"},
        ],
    )
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "_account_password",
        lambda record: record.get("password"),
    )
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "_build_playwright_login_payload",
        lambda username, password, proxy_settings, *, alias, totp_secret=None, row_number=None: {
            "username": username,
            "password": password,
            "alias": alias,
        },
    )

    def _fake_backend(alias: str, payloads: list[dict], *, concurrency: int = 1) -> list[dict]:
        backend_calls.append(
            {
                "alias": alias,
                "payloads": [dict(item) for item in payloads],
                "concurrency": concurrency,
            }
        )
        return [{"status": "ok", "username": "ok_one"}]

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "login_accounts_with_playwright",
        _fake_backend,
    )

    result = service.login("alias-a", ["missing", "ok_one"])

    assert result == [
        {
            "username": "missing",
            "status": "failed",
            "message": "missing_password",
            "profile_path": "",
            "row_number": None,
        },
        {
            "status": "ok",
            "username": "ok_one",
        },
    ]
    assert backend_calls == [
        {
            "alias": "alias-a",
            "payloads": [
                {
                    "username": "ok_one",
                    "password": "secret-one",
                    "alias": "alias-a",
                }
            ],
            "concurrency": 1,
        }
    ]


def test_relogin_uses_password_store_and_skips_only_missing_passwords(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = _build_service(tmp_path)
    backend_calls: list[dict] = []

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [
            {"username": "stored_only", "alias": "alias-a"},
            {"username": "missing", "alias": "alias-a"},
        ],
    )
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "_account_password",
        lambda record: "secret-one" if record.get("username") == "stored_only" else "",
    )

    def _fake_backend(alias: str, records: list[dict], *, concurrency: int = 1) -> list[dict]:
        backend_calls.append(
            {
                "alias": alias,
                "records": [dict(item) for item in records],
                "concurrency": concurrency,
            }
        )
        return [{"status": "ok", "username": "stored_only"}]

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "relogin_accounts_with_playwright",
        _fake_backend,
    )

    result = service.relogin("alias-a", ["stored_only", "missing"])

    assert result == [
        {
            "status": "ok",
            "username": "stored_only",
        },
        {
            "username": "missing",
            "status": "failed",
            "message": "missing_password",
            "profile_path": "",
            "row_number": None,
        },
    ]
    assert backend_calls == [
        {
            "alias": "alias-a",
            "records": [
                {
                    "username": "stored_only",
                    "alias": "alias-a",
                    "alias_id": "alias-a",
                    "alias_display_name": "alias-a",
                }
            ],
            "concurrency": 1,
        }
    ]
