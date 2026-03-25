from __future__ import annotations

import license_identity
from license_identity import clear_client_identity_env, client_id_from_license_key
from paths import runtime_root, sessions_root, storage_root


def _clear_root_env(monkeypatch) -> None:
    for name in (
        "APP_DATA_ROOT",
        "INSTACRM_APP_ROOT",
        "INSTACRM_DATA_ROOT",
        "INSTACRM_RUNTIME_ROOT",
        "INSTACRM_LICENSE_FILE",
        "INSTACRM_ENABLE_CLIENT_ISOLATION",
        "INSTACRM_CLIENT_ID",
        "LICENSE_FILE",
    ):
        monkeypatch.delenv(name, raising=False)


def test_client_paths_use_hashed_license_scope(tmp_path, monkeypatch) -> None:
    _clear_root_env(monkeypatch)
    license_key = "ABCD-EFGH-IJKL-MNOP"
    license_path = tmp_path / "license.key"
    license_path.write_text(license_key + "\n", encoding="utf-8")

    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTACRM_LICENSE_FILE", str(license_path))
    monkeypatch.setenv("INSTACRM_ENABLE_CLIENT_ISOLATION", "1")
    clear_client_identity_env()

    client_id = client_id_from_license_key(license_key)

    assert storage_root(tmp_path) == tmp_path / "storage" / client_id
    assert runtime_root(tmp_path) == tmp_path / "runtime" / client_id
    assert sessions_root(tmp_path) == tmp_path / "sessions" / client_id


def test_existing_client_claims_legacy_data_once_without_cross_client_mix(tmp_path, monkeypatch) -> None:
    _clear_root_env(monkeypatch)
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTACRM_ENABLE_CLIENT_ISOLATION", "1")

    legacy_storage = tmp_path / "storage"
    legacy_runtime = tmp_path / "runtime"
    legacy_sessions = legacy_runtime / "sessions"
    (legacy_storage / "accounts").mkdir(parents=True, exist_ok=True)
    (legacy_runtime / "browser_profiles" / "demo").mkdir(parents=True, exist_ok=True)
    legacy_sessions.mkdir(parents=True, exist_ok=True)
    (legacy_storage / "accounts" / "accounts.json").write_text('{"ok": true}\n', encoding="utf-8")
    (legacy_runtime / "browser_profiles" / "demo" / "state.json").write_text("{}", encoding="utf-8")
    (legacy_sessions / "session_demo.json").write_text("{}", encoding="utf-8")

    first_license = tmp_path / "license-one.key"
    first_license.write_text("CLIENT-ONE-0001\n", encoding="utf-8")
    monkeypatch.setenv("INSTACRM_LICENSE_FILE", str(first_license))
    clear_client_identity_env()

    client_one = client_id_from_license_key("CLIENT-ONE-0001")
    first_storage = storage_root(tmp_path)
    first_runtime = runtime_root(tmp_path)
    first_sessions = sessions_root(tmp_path)

    assert first_storage == tmp_path / "storage" / client_one
    assert (first_storage / "accounts" / "accounts.json").is_file()
    assert first_runtime == tmp_path / "runtime" / client_one
    assert (first_runtime / "browser_profiles" / "demo" / "state.json").is_file()
    assert first_sessions == tmp_path / "sessions" / client_one
    assert (first_sessions / "session_demo.json").is_file()

    second_license = tmp_path / "license-two.key"
    second_license.write_text("CLIENT-TWO-0002\n", encoding="utf-8")
    monkeypatch.setenv("INSTACRM_LICENSE_FILE", str(second_license))
    clear_client_identity_env()

    client_two = client_id_from_license_key("CLIENT-TWO-0002")
    second_storage = storage_root(tmp_path)
    second_runtime = runtime_root(tmp_path)
    second_sessions = sessions_root(tmp_path)

    assert second_storage == tmp_path / "storage" / client_two
    assert not (second_storage / "accounts" / "accounts.json").exists()
    assert second_runtime == tmp_path / "runtime" / client_two
    assert not (second_runtime / "browser_profiles" / "demo" / "state.json").exists()
    assert second_sessions == tmp_path / "sessions" / client_two
    assert not (second_sessions / "session_demo.json").exists()


def test_paths_fall_back_to_legacy_roots_when_license_is_unavailable(tmp_path, monkeypatch) -> None:
    _clear_root_env(monkeypatch)
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTACRM_ENABLE_CLIENT_ISOLATION", "1")
    monkeypatch.setattr(license_identity, "resolve_license_key", lambda: "")
    clear_client_identity_env()

    assert storage_root(tmp_path) == tmp_path / "storage"
    assert runtime_root(tmp_path) == tmp_path / "runtime"
    assert sessions_root(tmp_path) == tmp_path / "sessions"
