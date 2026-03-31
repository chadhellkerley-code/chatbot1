from __future__ import annotations

from pathlib import Path


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_has_real_data_ignores_empty_json_placeholders(tmp_path: Path) -> None:
    from core.storage_migration import has_real_data

    root = tmp_path / "storage"
    root.mkdir(parents=True, exist_ok=True)

    _write(root / "accounts" / "accounts.json", "[]\n")
    _write(root / "accounts" / "aliases.json", "{\"schema_version\": 1, \"aliases\": []}\n")
    _write(root / "accounts" / "proxies.json", "{}\n")

    assert has_real_data(root) is False


def test_migration_runs_when_new_storage_dir_exists_but_has_no_real_data(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from core.storage_migration import migrate_old_storage_if_needed

    # OLD storage is discovered via ./data relative to CWD (OLD_STORAGE_PATH).
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INSTACRM_APP_MODE", "client")

    old_root = tmp_path / "data"
    _write(old_root / "accounts" / "accounts.json", '[{"username": "u"}]\n')

    new_root = tmp_path / "new_storage"
    new_root.mkdir(parents=True, exist_ok=True)  # simulate build-created folder
    monkeypatch.setenv("INSTACRM_DATA_ROOT", str(new_root))

    assert migrate_old_storage_if_needed() is True
    assert (new_root / "accounts" / "accounts.json").exists()

