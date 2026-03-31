from __future__ import annotations

from pathlib import Path

import licensekit


def _write(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_export_client_distribution_copies_folder_and_license_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifact_dir = tmp_path / "artifact"
    _write(artifact_dir / "InstaCRM.exe", "binary")
    _write(artifact_dir / "app" / ".env", "CLIENT_DISTRIBUTION=1\n")
    _write(artifact_dir / "runtime" / "sessions" / "placeholder.txt")

    delivery_root = tmp_path / "Desktop"
    monkeypatch.setenv("DELIVERY_ROOT", str(delivery_root))

    record = {
        "client_name": "Cliente Demo",
        "license_key": "ABCD-EFGH-IJKL-MNOP",
        "expires_at": "2099-01-01T00:00:00+00:00",
        "status": "active",
    }

    delivery_dir = licensekit._export_client_distribution(record, artifact_dir)

    assert delivery_dir == delivery_root / "Clientes" / "Cliente Demo"
    assert (delivery_dir / "InstaCRM.exe").exists()
    assert (delivery_dir / "app" / ".env").exists()
    assert (delivery_dir / "license.key").read_text(encoding="utf-8") == "ABCD-EFGH-IJKL-MNOP\n"
    assert (delivery_dir / "INSTRUCCIONES.txt").exists()
    assert not list(delivery_dir.glob("*.zip"))


def test_create_managed_license_record_uses_current_admin_client(monkeypatch) -> None:
    created_payload: dict[str, object] = {}

    class FakeAdminClient:
        def create_license(self, **kwargs):
            created_payload.update(kwargs)
            return {
                "client_name": kwargs["client_name"],
                "license_key": "ABCD-EFGH-IJKL-MNOP",
                "expires_at": kwargs["expires_at"],
                "status": "active",
                "notes": kwargs["notes"],
            }

    monkeypatch.setattr(licensekit, "_license_admin_client", lambda: FakeAdminClient())
    monkeypatch.setattr(licensekit, "_upsert_local_license", lambda record: dict(record))

    record = licensekit._create_managed_license_record(
        "Cliente Demo",
        days=45,
        email="demo@example.com",
    )

    assert created_payload["client_name"] == "Cliente Demo"
    assert created_payload["plan_name"] == "standard"
    assert created_payload["max_devices"] == 2
    assert created_payload["notes"] == "Contact email: demo@example.com"
    assert record["license_key"] == "ABCD-EFGH-IJKL-MNOP"
