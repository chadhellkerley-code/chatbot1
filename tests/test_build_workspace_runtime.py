from __future__ import annotations

from pathlib import Path
import sys

from build import build_client as build_client_module
from build import helpers as build_helpers
from tools.build_executable import _copy_project, _sanitize_tree


def _write(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_copy_project_keeps_runtime_code_but_skips_transient_payloads(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"

    _write(source / "runtime" / "__init__.py", "from .runtime import *\n")
    _write(source / "runtime" / "runtime.py", "RUNTIME = 1\n")
    _write(source / "runtime" / "runtime_parity.py", "PARITY = 1\n")
    _write(source / "runtime" / "browser_profiles" / "profile.txt")
    _write(source / "runtime" / "browsers" / "chrome.exe")
    _write(source / "runtime" / "playwright" / "chromium.exe")
    _write(source / "tools" / "build_artifacts" / "junk.bin")

    _copy_project(source, destination)

    assert (destination / "runtime" / "__init__.py").exists()
    assert (destination / "runtime" / "runtime.py").exists()
    assert (destination / "runtime" / "runtime_parity.py").exists()
    assert not (destination / "runtime" / "browser_profiles").exists()
    assert not (destination / "runtime" / "browsers").exists()
    assert not (destination / "runtime" / "playwright").exists()
    assert not (destination / "tools" / "build_artifacts").exists()


def test_copy_project_skips_workspace_only_assets(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"

    _write(source / "launchers" / "client_launcher.py", "print('ok')\n")
    _write(source / "cloudflare" / "license-worker" / "node_modules" / "sharp.node")
    _write(source / "docs" / "BUILD.md", "# build\n")
    _write(source / "logs" / "build.log", "log\n")
    _write(source / ".vscode" / "settings.json", "{}\n")

    _copy_project(source, destination)

    assert (destination / "launchers" / "client_launcher.py").exists()
    assert not (destination / "cloudflare").exists()
    assert not (destination / "docs").exists()
    assert not (destination / "logs").exists()
    assert not (destination / ".vscode").exists()


def test_prepare_workspace_keeps_runtime_package_code(monkeypatch, tmp_path: Path) -> None:
    project = tmp_path / "project"
    temp_build_root = tmp_path / "tmp"
    temp_build_root.mkdir(parents=True, exist_ok=True)

    _write(project / "launchers" / "client_launcher.py", "print('ok')\n")
    _write(project / "runtime" / "__init__.py", "from .runtime import *\n")
    _write(project / "runtime" / "runtime.py", "RUNTIME = 1\n")
    _write(project / "runtime" / "runtime_parity.py", "PARITY = 1\n")
    _write(project / "runtime" / "browser_profiles" / "profile.txt")
    _write(project / "runtime" / "browsers" / "chrome.exe")
    _write(project / "runtime" / "playwright" / "chromium.exe")
    _write(project / "storage" / "state.json", "{}\n")
    _write(project / "data" / "accounts" / "accounts.json", "[]\n")
    _write(project / "tools" / "build_artifacts" / "junk.bin")
    _write(project / ".env", "CLIENT_DISTRIBUTION=1\n")

    monkeypatch.setattr(build_helpers, "project_root", lambda: project)
    monkeypatch.setattr(build_helpers, "build_temp_root", lambda: temp_build_root)

    temp_root, workspace = build_helpers.prepare_workspace("workspace_test")
    try:
        assert (workspace / "runtime" / "__init__.py").exists()
        assert (workspace / "runtime" / "runtime.py").exists()
        assert (workspace / "runtime" / "runtime_parity.py").exists()
        assert not (workspace / "runtime" / "browser_profiles").exists()
        assert not (workspace / "runtime" / "browsers").exists()
        assert not (workspace / "runtime" / "playwright").exists()
        assert not (workspace / "storage").exists()
        assert not (workspace / "data").exists()
        assert not (workspace / "tools" / "build_artifacts").exists()
        assert not (workspace / ".env").exists()
    finally:
        if temp_root.exists():
            import shutil

            shutil.rmtree(temp_root, ignore_errors=True)


def test_sanitize_tree_prunes_runtime_transient_dirs(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"

    _write(workspace / "runtime" / "__init__.py", "from .runtime import *\n")
    _write(workspace / "runtime" / "runtime.py", "RUNTIME = 1\n")
    _write(workspace / "runtime" / "runtime_parity.py", "PARITY = 1\n")
    _write(workspace / "runtime" / "screenshots" / "capture.png")
    _write(workspace / "runtime" / "sessions" / "session.json", "{}\n")
    _write(workspace / "runtime" / "browser_profiles" / "profile.txt")
    _write(workspace / "runtime" / "playwright" / "chromium.exe")

    _sanitize_tree(workspace)

    assert (workspace / "runtime" / "__init__.py").exists()
    assert (workspace / "runtime" / "runtime.py").exists()
    assert (workspace / "runtime" / "runtime_parity.py").exists()
    assert not (workspace / "runtime" / "screenshots").exists()
    assert not (workspace / "runtime" / "sessions").exists()
    assert not (workspace / "runtime" / "browser_profiles").exists()
    assert not (workspace / "runtime" / "playwright").exists()


def test_assemble_client_layout_writes_runtime_dirs_and_license_key(tmp_path: Path) -> None:
    built_executable = tmp_path / "InstaCRM.exe"
    built_executable.write_text("binary", encoding="utf-8")

    target_dir = tmp_path / "dist" / "InstaCRM"
    build_helpers.assemble_client_layout(
        built_executable,
        target_dir=target_dir,
        version="1.2.3",
        license_key="ABCD-EFGH-IJKL-MNOP",
        bundle_playwright=False,
    )

    assert (target_dir / "InstaCRM.exe").exists()
    assert (target_dir / "license.key").read_text(encoding="utf-8") == "ABCD-EFGH-IJKL-MNOP\n"
    assert (target_dir / "app" / "license.key").read_text(encoding="utf-8") == "ABCD-EFGH-IJKL-MNOP\n"
    assert (target_dir / "app" / ".env").exists()
    assert (target_dir / "data").is_dir()
    assert (target_dir / "runtime").is_dir()
    assert (target_dir / "updates").is_dir()


def test_build_client_main_only_reports_folder_output(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    called: dict[str, object] = {}

    def fake_build_client_distribution(
        *,
        target_dir: Path,
        version: str = "",
        license_payload: Path | None = None,
        license_key: str = "",
        bundle_playwright: bool = True,
    ) -> Path:
        called["target_dir"] = target_dir
        called["version"] = version
        called["license_payload"] = license_payload
        called["license_key"] = license_key
        called["bundle_playwright"] = bundle_playwright
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir

    monkeypatch.setattr(build_client_module, "dist_root", lambda: tmp_path)
    monkeypatch.setattr(
        build_client_module,
        "build_client_distribution",
        fake_build_client_distribution,
    )
    monkeypatch.setattr(build_client_module, "current_version", lambda: "9.9.9")
    monkeypatch.setattr(sys, "argv", ["build_client.py"])

    assert build_client_module.main() == 0

    output = capsys.readouterr().out
    assert "Client folder:" in output
    assert "Client package:" not in output
    assert called["target_dir"] == tmp_path / "InstaCRM"
    assert called["bundle_playwright"] is True
    assert not (tmp_path / "InstaCRM_client_package.zip").exists()
