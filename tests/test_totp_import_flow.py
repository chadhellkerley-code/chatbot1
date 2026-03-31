from __future__ import annotations

import asyncio
import csv
import importlib
import json
import sys
from pathlib import Path

import pytest

from application.services.base import ServiceContext, ServiceError

TOTP_SECRET = "JBSWY3DPEHPK3PXP"
OTHER_TOTP_SECRET = "JBSWY3DPEHPK3PXQ"


def _reload_totp_modules(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("TOTP_MASTER_KEY", "test-master-key")
    for module_name in (
        "core.totp_store",
        "core.accounts",
        "application.services.account_service",
        "src.auth.onboarding",
    ):
        sys.modules.pop(module_name, None)

    import core.totp_store as totp_store  # type: ignore
    import core.accounts as accounts_module  # type: ignore
    import application.services.account_service as account_service_module  # type: ignore
    import src.auth.onboarding as onboarding  # type: ignore

    totp_store = importlib.reload(totp_store)
    accounts_module = importlib.reload(accounts_module)
    account_service_module = importlib.reload(account_service_module)
    onboarding = importlib.reload(onboarding)
    return totp_store, accounts_module, account_service_module, onboarding


def _build_service(account_service_module, tmp_path: Path):
    return account_service_module.AccountService(ServiceContext.default(tmp_path))


def _write_accounts_csv(path: Path, header: str, secret: str = TOTP_SECRET) -> None:
    fieldnames = [
        "username",
        "password",
        "2fa code",
        "proxy id",
        "proxy port",
        "proxy username",
        "proxy password",
    ]
    if header not in fieldnames:
        fieldnames.append(header)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        row = {
            "username": "tester",
            "password": "secret-pass",
            "2fa code": secret if header == "2fa code" else "",
            "proxy id": "",
            "proxy port": "",
            "proxy username": "",
            "proxy password": "",
        }
        if header != "2fa code":
            row[header] = secret
        writer.writerow(row)


def _write_minimal_accounts_csv(path: Path, header: str, secret: str = TOTP_SECRET) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["username", "password", header])
        writer.writerow(["tester", "secret-pass", secret])


def _write_headerless_accounts_csv(path: Path, secret: str) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["tester", "secret-pass", secret])


def _stub_successful_onboarding_login(monkeypatch, onboarding, tmp_path: Path) -> dict[str, object]:
    observed: dict[str, object] = {}

    async def _fake_ensure_logged_in_async(account, *, headless, profile_root, proxy):
        observed["account"] = dict(account)
        observed["headless"] = headless
        observed["profile_root"] = profile_root
        observed["proxy"] = proxy
        return object(), object(), object()

    async def _fake_is_logged_in(_page) -> bool:
        return True

    async def _fake_shutdown(_svc, _ctx) -> None:
        observed["shutdown_called"] = True

    monkeypatch.setattr(onboarding, "ensure_logged_in_async", _fake_ensure_logged_in_async)
    monkeypatch.setattr(onboarding, "is_logged_in", _fake_is_logged_in)
    monkeypatch.setattr(onboarding, "shutdown", _fake_shutdown)
    monkeypatch.setattr(onboarding, "_profile_path_for", lambda username, root: tmp_path / f"{username}.json")
    return observed


def _write_legacy_totp_record(totp_store, root: Path, username: str, secret: str) -> Path:
    legacy_dir = root / "data" / "totp"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    master_key = "legacy-master-key"
    (legacy_dir / ".master_key").write_text(master_key, encoding="utf-8")
    salt = b"0123456789abcdef"
    ciphertext = totp_store._fernet(salt, master_key.encode("utf-8")).encrypt(secret.encode("utf-8"))
    record = totp_store.SecretRecord(salt=salt, ciphertext=ciphertext)
    path = legacy_dir / f"{username}.json"
    path.write_text(totp_store._encode(record), encoding="utf-8")
    return path


def test_manual_add_account_persists_totp_only_in_canonical_store(monkeypatch, tmp_path: Path) -> None:
    totp_store, accounts_module, account_service_module, _ = _reload_totp_modules(monkeypatch, tmp_path)
    service = _build_service(account_service_module, tmp_path)

    assert service.add_account("tester", "Ventas", password="secret-pass", totp_secret=TOTP_SECRET) is True

    assert totp_store.has_secret("tester") is True
    assert totp_store.get_secret("tester") == TOTP_SECRET
    assert totp_store._path_for("tester").is_file() is True

    stored_rows = json.loads(accounts_module.FILE.read_text(encoding="utf-8"))
    assert stored_rows[0]["username"] == "tester"
    assert "totp_secret" not in stored_rows[0]
    assert TOTP_SECRET not in accounts_module.FILE.read_text(encoding="utf-8")

    credentials_db = accounts_module.DATA / "credentials.sqlite3"
    assert credentials_db.exists() is True
    assert TOTP_SECRET.encode("utf-8") not in credentials_db.read_bytes()

    payload = accounts_module._playwright_account_payload("tester", "secret-pass", None)
    assert payload["totp_secret"] == TOTP_SECRET
    assert callable(payload["totp_callback"])


def test_add_account_rolls_back_when_totp_persistence_fails(monkeypatch, tmp_path: Path) -> None:
    totp_store, accounts_module, account_service_module, _ = _reload_totp_modules(monkeypatch, tmp_path)
    service = _build_service(account_service_module, tmp_path)

    def _fail_save(_username: str, _secret: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(account_service_module, "save_totp_secret", _fail_save)

    with pytest.raises(ServiceError, match="No se pudo persistir TOTP"):
        service.add_account("tester", "Ventas", password="secret-pass", totp_secret=TOTP_SECRET)

    assert accounts_module.get_account("tester") is None
    assert totp_store.has_secret("tester") is False


@pytest.mark.parametrize("header", ["2fa code", "totp_secret", "otp"])
def test_import_accounts_csv_persists_totp_for_supported_aliases(
    monkeypatch,
    tmp_path: Path,
    header: str,
) -> None:
    totp_store, accounts_module, account_service_module, _ = _reload_totp_modules(monkeypatch, tmp_path)
    service = _build_service(account_service_module, tmp_path)
    csv_path = tmp_path / f"accounts_{header.replace(' ', '_')}.csv"
    _write_accounts_csv(csv_path, header)

    result = service.import_accounts_csv("Ventas", csv_path)

    assert result["added"] == 1
    assert result["skipped"] == 0
    assert result["imported_usernames"] == ["tester"]
    assert totp_store.get_secret("tester") == TOTP_SECRET
    assert TOTP_SECRET not in accounts_module.FILE.read_text(encoding="utf-8")


@pytest.mark.parametrize("header", ["2fa code", "secret", "totp_secret"])
def test_import_accounts_csv_with_minimal_header_persists_totp(
    monkeypatch,
    tmp_path: Path,
    header: str,
) -> None:
    totp_store, accounts_module, account_service_module, _ = _reload_totp_modules(monkeypatch, tmp_path)
    service = _build_service(account_service_module, tmp_path)
    csv_path = tmp_path / f"accounts_minimal_{header.replace(' ', '_')}.csv"
    _write_minimal_accounts_csv(csv_path, header)

    result = service.import_accounts_csv("Ventas", csv_path)

    assert result["added"] == 1
    assert result["skipped"] == 0
    assert result["imported_usernames"] == ["tester"]
    assert totp_store._path_for("tester").is_file() is True
    assert totp_store.get_secret("tester") == TOTP_SECRET
    assert TOTP_SECRET not in accounts_module.FILE.read_text(encoding="utf-8")


def test_import_accounts_csv_without_header_persists_totp_only_in_canonical_store(
    monkeypatch,
    tmp_path: Path,
) -> None:
    totp_store, accounts_module, account_service_module, _ = _reload_totp_modules(monkeypatch, tmp_path)
    service = _build_service(account_service_module, tmp_path)
    csv_path = tmp_path / "accounts_no_header.csv"
    _write_headerless_accounts_csv(csv_path, TOTP_SECRET)

    result = service.import_accounts_csv("Ventas", csv_path)

    assert result["added"] == 1
    assert result["skipped"] == 0
    assert result["imported_usernames"] == ["tester"]
    assert totp_store._path_for("tester").is_file() is True
    assert totp_store.get_secret("tester") == TOTP_SECRET

    stored_rows = json.loads(accounts_module.FILE.read_text(encoding="utf-8"))
    assert stored_rows[0]["username"] == "tester"
    assert "totp_secret" not in stored_rows[0]
    assert TOTP_SECRET not in accounts_module.FILE.read_text(encoding="utf-8")

    credentials_db = accounts_module.DATA / "credentials.sqlite3"
    assert credentials_db.exists() is True
    assert TOTP_SECRET.encode("utf-8") not in credentials_db.read_bytes()


def test_import_accounts_csv_without_header_invalid_totp_does_not_persist_secret(
    monkeypatch,
    tmp_path: Path,
) -> None:
    invalid_secret = "otpauth://bad"
    totp_store, accounts_module, account_service_module, _ = _reload_totp_modules(monkeypatch, tmp_path)
    service = _build_service(account_service_module, tmp_path)
    csv_path = tmp_path / "accounts_no_header_invalid_totp.csv"
    _write_headerless_accounts_csv(csv_path, invalid_secret)

    result = service.import_accounts_csv("Ventas", csv_path)

    assert result["added"] == 1
    assert result["skipped"] == 0
    assert result["imported_usernames"] == ["tester"]
    assert totp_store.has_secret("tester") is False
    assert totp_store._path_for("tester").exists() is False

    stored_rows = json.loads(accounts_module.FILE.read_text(encoding="utf-8"))
    assert stored_rows[0]["username"] == "tester"
    assert "totp_secret" not in stored_rows[0]
    assert invalid_secret not in accounts_module.FILE.read_text(encoding="utf-8")

    credentials_db = accounts_module.DATA / "credentials.sqlite3"
    assert credentials_db.exists() is True
    assert invalid_secret.encode("utf-8") not in credentials_db.read_bytes()


def test_import_accounts_csv_raises_and_rolls_back_when_totp_persistence_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    totp_store, accounts_module, account_service_module, _ = _reload_totp_modules(monkeypatch, tmp_path)
    service = _build_service(account_service_module, tmp_path)
    csv_path = tmp_path / "accounts.csv"
    _write_accounts_csv(csv_path, "2fa code")

    def _fail_save(_username: str, _secret: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(account_service_module, "save_totp_secret", _fail_save)

    with pytest.raises(ServiceError, match=r"Fila 1: no se pudo importar @tester"):
        service.import_accounts_csv("Ventas", csv_path)

    assert accounts_module.get_account("tester") is None
    assert totp_store.has_secret("tester") is False


@pytest.mark.parametrize("header", ["2fa code", "authenticator", "secret"])
def test_onboarding_parse_accounts_csv_uses_canonical_totp_aliases(
    monkeypatch,
    tmp_path: Path,
    header: str,
) -> None:
    _, _, _, onboarding = _reload_totp_modules(monkeypatch, tmp_path)
    csv_path = tmp_path / f"onboarding_{header.replace(' ', '_')}.csv"
    _write_accounts_csv(csv_path, header)

    rows = onboarding.parse_accounts_csv(csv_path)

    assert len(rows) == 1
    assert rows[0]["username"] == "tester"
    assert rows[0]["password"] == "secret-pass"
    assert rows[0]["totp_secret"] == TOTP_SECRET


def test_onboarding_csv_persists_totp_in_canonical_store_before_login(
    monkeypatch,
    tmp_path: Path,
) -> None:
    totp_store, accounts_module, _, onboarding = _reload_totp_modules(monkeypatch, tmp_path)
    observed = _stub_successful_onboarding_login(monkeypatch, onboarding, tmp_path)
    csv_path = tmp_path / "onboarding.csv"
    _write_accounts_csv(csv_path, "2fa code")

    results = onboarding.onboard_accounts_from_csv(csv_path, headless=True, concurrency=1)

    assert results[0]["status"] == "ok"
    assert totp_store._path_for("tester").is_file() is True
    assert totp_store.get_secret("tester") == TOTP_SECRET
    assert observed["account"]["totp_secret"] == TOTP_SECRET
    assert callable(observed["account"]["totp_callback"])
    if accounts_module.FILE.exists():
        assert TOTP_SECRET not in accounts_module.FILE.read_text(encoding="utf-8")
    credentials_db = accounts_module.DATA / "credentials.sqlite3"
    if credentials_db.exists():
        assert TOTP_SECRET.encode("utf-8") not in credentials_db.read_bytes()


def test_relogin_uses_canonical_totp_store_without_in_memory_secret(
    monkeypatch,
    tmp_path: Path,
) -> None:
    totp_store, _, _, onboarding = _reload_totp_modules(monkeypatch, tmp_path)
    observed = _stub_successful_onboarding_login(monkeypatch, onboarding, tmp_path)
    totp_store.save_secret("tester", TOTP_SECRET)

    result = asyncio.run(
        onboarding.login_and_persist_async(
            {"username": "tester", "password": "secret-pass"},
            headless=True,
            profile_root=tmp_path / "profiles",
        )
    )

    assert result["status"] == "ok"
    assert observed["account"]["totp_secret"] == TOTP_SECRET
    assert callable(observed["account"]["totp_callback"])


def test_ensure_totp_for_playwright_uses_only_canonical_store(monkeypatch, tmp_path: Path) -> None:
    _, accounts_module, _, _ = _reload_totp_modules(monkeypatch, tmp_path)

    def _unexpected_refresh(*_args, **_kwargs) -> None:
        raise AssertionError("legacy CSV refresh should not run during login")

    monkeypatch.setattr(accounts_module, "_refresh_totp_export_cache", _unexpected_refresh)

    assert accounts_module._ensure_totp_for_playwright("tester", force_refresh=True) is False


def test_playwright_payload_ignores_legacy_csv_when_canonical_totp_is_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    totp_store, accounts_module, _, _ = _reload_totp_modules(monkeypatch, tmp_path)
    _write_accounts_csv(tmp_path / "accounts.csv", "2fa code")

    payload = accounts_module._playwright_account_payload(
        "tester",
        "secret-pass",
        None,
        force_totp_refresh=True,
    )

    assert "totp_secret" not in payload
    assert "totp_callback" not in payload
    assert totp_store.has_secret("tester") is False


def test_totp_normalization_helpers_delegate_to_canonical_normalizer(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _, accounts_module, _, _ = _reload_totp_modules(monkeypatch, tmp_path)
    import core.accounts_helpers.csv_utils as csv_utils  # type: ignore

    seen: list[str] = []

    def _fake_normalize(username: str) -> str:
        seen.append(username)
        return "canonical-user"

    monkeypatch.setattr(accounts_module, "normalize_totp_username", _fake_normalize)
    monkeypatch.setattr(csv_utils, "normalize_totp_username", _fake_normalize)

    assert csv_utils._safe_username_key("@Some.User") == "canonical-user"
    assert accounts_module._totp_record_path("@Some.User").name == "canonical-user.json"
    assert seen == ["@Some.User", "@Some.User"]


def test_legacy_totp_store_migrates_into_canonical_storage(
    monkeypatch,
    tmp_path: Path,
) -> None:
    totp_store, _, _, _ = _reload_totp_modules(monkeypatch, tmp_path)
    _write_legacy_totp_record(totp_store, tmp_path, "Legacy.User", TOTP_SECRET)
    monkeypatch.delenv("TOTP_MASTER_KEY", raising=False)

    summary = totp_store.migrate_legacy_store()

    assert summary == {"migrated": 1, "skipped_existing": 0, "skipped_invalid": 0}
    assert totp_store.get_secret("legacy.user") == TOTP_SECRET
    assert totp_store._path_for("legacy.user").is_file() is True


def test_legacy_totp_migration_does_not_overwrite_existing_canonical_secret(
    monkeypatch,
    tmp_path: Path,
) -> None:
    totp_store, _, _, _ = _reload_totp_modules(monkeypatch, tmp_path)
    totp_store.save_secret("Legacy.User", OTHER_TOTP_SECRET)
    _write_legacy_totp_record(totp_store, tmp_path, "Legacy.User", TOTP_SECRET)
    monkeypatch.delenv("TOTP_MASTER_KEY", raising=False)

    summary = totp_store.migrate_legacy_store()

    assert summary == {"migrated": 0, "skipped_existing": 1, "skipped_invalid": 0}
    assert totp_store.get_secret("legacy.user") == OTHER_TOTP_SECRET
