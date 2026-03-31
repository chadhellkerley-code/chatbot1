from __future__ import annotations

from pathlib import Path

from src.browser_profile_paths import (
    browser_profile_dir,
    browser_storage_state_path,
    normalize_browser_profile_username,
)


def test_browser_profile_paths_normalize_username_and_root(tmp_path: Path) -> None:
<<<<<<< HEAD
    assert normalize_browser_profile_username(" @Worker_One ") == "worker_one"
    assert browser_profile_dir("@Worker_One", profiles_root=tmp_path) == tmp_path / "worker_one"
    assert browser_storage_state_path(" Worker_One ", profiles_root=tmp_path) == (
=======
    assert normalize_browser_profile_username(" @worker_one ") == "worker_one"
    assert browser_profile_dir("@worker_one", profiles_root=tmp_path) == tmp_path / "worker_one"
    assert browser_storage_state_path(" worker_one ", profiles_root=tmp_path) == (
>>>>>>> origin/main
        tmp_path / "worker_one" / "storage_state.json"
    )
