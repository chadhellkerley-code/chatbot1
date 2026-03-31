from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import core.accounts as accounts_module


def _configure_accounts_storage(monkeypatch, tmp_path: Path) -> Path:
    accounts_dir = tmp_path / "storage" / "accounts"
    accounts_dir.mkdir(parents=True, exist_ok=True)
    accounts_file = accounts_dir / "accounts.json"
    monkeypatch.setattr(accounts_module, "DATA", accounts_dir)
    monkeypatch.setattr(accounts_module, "FILE", accounts_file)
    monkeypatch.setattr(accounts_module, "_PASSWORD_FILE", accounts_dir / "passwords.json")
    monkeypatch.setattr(accounts_module, "_PASSWORD_CACHE", {})
    return accounts_file


def test_list_all_migrates_legacy_alias_field_to_canonical_alias_storage(monkeypatch, tmp_path: Path) -> None:
    accounts_file = _configure_accounts_storage(monkeypatch, tmp_path)
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "username": "demo.account",
                    "alias": "Ventas Norte",
                    "active": True,
                    "connected": False,
                }
            ]
        ),
        encoding="utf-8",
    )

    rows = accounts_module.list_all()

    assert rows[0]["alias_id"] == "ventas-norte"
    assert rows[0]["alias_display_name"] == "Ventas Norte"
    assert rows[0]["alias"] == "Ventas Norte"

    stored_rows = json.loads(accounts_file.read_text(encoding="utf-8"))
    assert stored_rows[0]["alias_id"] == "ventas-norte"
    assert stored_rows[0]["alias_display_name"] == "Ventas Norte"
    assert "alias" not in stored_rows[0]


def test_sync_alias_metadata_relabels_all_matching_accounts(monkeypatch, tmp_path: Path) -> None:
    accounts_file = _configure_accounts_storage(monkeypatch, tmp_path)
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "username": "uno",
                    "alias_id": "ventas-norte",
                    "alias_display_name": "Ventas Norte",
                    "active": True,
                },
                {
                    "username": "dos",
                    "alias": "VENTAS   NORTE",
                    "active": True,
                },
                {
                    "username": "tres",
                    "alias": "Otro",
                    "active": True,
                },
            ]
        ),
        encoding="utf-8",
    )

    updated = accounts_module.sync_alias_metadata(
        "ventas norte",
        alias_id="ventas-norte",
        display_name="VENTAS norte",
    )

    assert updated == 2

    rows = accounts_module.list_all()
    ventas_rows = [row for row in rows if row["alias_id"] == "ventas-norte"]
    assert len(ventas_rows) == 2
    assert all(row["alias_display_name"] == "VENTAS norte" for row in ventas_rows)
    assert all(row["alias"] == "VENTAS norte" for row in ventas_rows)


<<<<<<< HEAD
def test_list_all_defaults_missing_usage_state_to_active_and_persists_it(monkeypatch, tmp_path: Path) -> None:
    accounts_file = _configure_accounts_storage(monkeypatch, tmp_path)
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "username": "legacy.account",
                    "alias": "Ventas Norte",
                    "active": True,
                    "connected": False,
                }
            ]
        ),
        encoding="utf-8",
    )

    rows = accounts_module.list_all()

    assert rows[0]["usage_state"] == "active"

    stored_rows = json.loads(accounts_file.read_text(encoding="utf-8"))
    assert stored_rows[0]["usage_state"] == "active"


def test_update_account_persists_usage_state_changes(monkeypatch, tmp_path: Path) -> None:
    accounts_file = _configure_accounts_storage(monkeypatch, tmp_path)
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "username": "tester",
                    "alias": "Ventas Norte",
                    "active": True,
                    "usage_state": "active",
                }
            ]
        ),
        encoding="utf-8",
    )

    assert accounts_module.update_account("tester", {"usage_state": "deactivated"}) is True

    stored_rows = json.loads(accounts_file.read_text(encoding="utf-8"))
    assert stored_rows[0]["usage_state"] == "deactivated"


=======
>>>>>>> origin/main
def test_list_all_migrates_plaintext_passwords_to_encrypted_sqlite(
    monkeypatch,
    tmp_path: Path,
) -> None:
    accounts_file = _configure_accounts_storage(monkeypatch, tmp_path)
    passwords_file = accounts_file.parent / "passwords.json"
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "username": "demo.account",
                    "alias": "Ventas Norte",
                    "password": "secret-inline",
                    "active": True,
                },
                {
                    "username": "legacy.account",
                    "alias": "Ventas Norte",
                    "active": True,
                },
            ]
        ),
        encoding="utf-8",
    )
    passwords_file.write_text(
        json.dumps({"legacy.account": "secret-legacy"}),
        encoding="utf-8",
    )

    rows = accounts_module.list_all()

    assert accounts_module._account_password({"username": "demo.account"}) == "secret-inline"
    assert accounts_module._account_password({"username": "legacy.account"}) == "secret-legacy"
    assert {row["username"]: row["password"] for row in rows} == {
        "demo.account": "secret-inline",
        "legacy.account": "secret-legacy",
    }

    stored_rows = json.loads(accounts_file.read_text(encoding="utf-8"))
    assert all("password" not in row for row in stored_rows)
    assert json.loads(passwords_file.read_text(encoding="utf-8")) == {}

    credentials_db = accounts_file.parent / "credentials.sqlite3"
    assert credentials_db.exists()
    assert (accounts_file.parent / ".credentials_key").exists()

    conn = sqlite3.connect(credentials_db)
    try:
        encrypted_rows = dict(
            conn.execute("SELECT username, password_enc FROM account_credentials").fetchall()
        )
    finally:
        conn.close()

    assert set(encrypted_rows) == {"demo.account", "legacy.account"}
    assert all(value.startswith("enc:v1:") for value in encrypted_rows.values())
