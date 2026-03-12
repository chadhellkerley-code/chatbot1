from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from src.licensing.device_id import collect_device_identity
from src.licensing.license_client import (
    LicenseStartupError,
    SupabaseLicenseClient,
    load_local_license_key,
)


class FakeRestClient:
    def __init__(self, *, license_row: dict, activation_rows: list[dict] | None = None) -> None:
        self.config = SimpleNamespace(url="https://example.supabase.co", key="test-key")
        self.license_row = dict(license_row)
        self.activation_rows = [dict(row) for row in activation_rows or []]
        self.insert_calls: list[tuple[str, dict]] = []
        self.update_calls: list[tuple[str, dict, dict]] = []

    def select(
        self,
        table: str,
        *,
        filters: dict | None = None,
        columns: str = "*",
        order: str = "",
        limit: int | None = None,
        single: bool = False,
    ):
        del columns, order, limit
        if table == "licenses":
            clean_key = str(filters.get("license_key", "")).replace("eq.", "") if filters else ""
            row = self.license_row if self.license_row.get("license_key") == clean_key else None
            if single:
                return dict(row) if row else None
            return [dict(row)] if row else []
        if table == "license_activations":
            rows = list(self.activation_rows)
            if filters:
                for key, value in filters.items():
                    clean = str(value).replace("eq.", "")
                    rows = [row for row in rows if str(row.get(key) or "") == clean]
            return [dict(row) for row in rows]
        raise AssertionError(f"Unexpected table {table}")

    def insert(self, table: str, payload, *, returning: str = "representation"):
        del returning
        assert table == "license_activations"
        row = dict(payload)
        row.setdefault("id", f"activation-{len(self.activation_rows) + 1}")
        self.activation_rows.append(row)
        self.insert_calls.append((table, row))
        return [dict(row)]

    def update(self, table: str, payload: dict, *, filters: dict, returning: str = "representation"):
        del returning
        if table == "licenses":
            self.license_row.update(payload)
            self.update_calls.append((table, dict(payload), dict(filters)))
            return [dict(self.license_row)]
        if table == "license_activations":
            activation_id = str(filters.get("id", "")).replace("eq.", "")
            for row in self.activation_rows:
                if str(row.get("id") or "") != activation_id:
                    continue
                row.update(payload)
                self.update_calls.append((table, dict(payload), dict(filters)))
                return [dict(row)]
            return []
        raise AssertionError(f"Unexpected table {table}")

    def delete(self, table: str, *, filters: dict, returning: str = "representation"):
        del returning
        assert table == "license_activations"
        key, value = next(iter(filters.items()))
        clean = str(value).replace("eq.", "")
        deleted = [row for row in self.activation_rows if str(row.get(key) or "") == clean]
        self.activation_rows = [row for row in self.activation_rows if str(row.get(key) or "") != clean]
        return deleted


def test_validate_and_activate_registers_new_device_when_capacity_is_available() -> None:
    rest = FakeRestClient(
        license_row={
            "id": "license-1",
            "license_key": "ABCD-EFGH-IJKL-MNOP",
            "status": "active",
            "max_devices": 2,
            "expires_at": "2099-01-01T00:00:00+00:00",
            "client_name": "Cliente Demo",
            "plan_name": "pro",
        }
    )
    client = SupabaseLicenseClient(rest_client=rest)
    device = collect_device_identity(
        hostname="pc-1",
        os_user="owner",
        mac_address="aa:bb:cc:dd:ee:ff",
    )

    context = client.validate_and_activate(
        "ABCD-EFGH-IJKL-MNOP",
        device=device,
        app_version="1.2.3",
    )

    assert context.license_key == "ABCD-EFGH-IJKL-MNOP"
    assert context.device_id == device.device_id
    assert len(rest.insert_calls) == 1
    inserted = rest.insert_calls[0][1]
    assert inserted["device_id"] == device.device_id
    assert inserted["license_key"] == "ABCD-EFGH-IJKL-MNOP"


def test_validate_and_activate_blocks_when_license_reaches_max_devices() -> None:
    rest = FakeRestClient(
        license_row={
            "id": "license-2",
            "license_key": "WXYZ-0000-1111-2222",
            "status": "active",
            "max_devices": 2,
            "expires_at": "2099-01-01T00:00:00+00:00",
        },
        activation_rows=[
            {"id": "a1", "license_key": "WXYZ-0000-1111-2222", "device_id": "device-1", "status": "active"},
            {"id": "a2", "license_key": "WXYZ-0000-1111-2222", "device_id": "device-2", "status": "active"},
        ],
    )
    client = SupabaseLicenseClient(rest_client=rest)
    device = collect_device_identity(
        hostname="pc-3",
        os_user="owner",
        mac_address="00:11:22:33:44:55",
    )

    with pytest.raises(LicenseStartupError) as exc_info:
        client.validate_and_activate(
            "WXYZ-0000-1111-2222",
            device=device,
            app_version="1.2.3",
        )

    assert exc_info.value.code == "device_limit_reached"
    assert not rest.insert_calls


def test_load_local_license_key_reads_plain_license_key_file(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    license_path = tmp_path / "license.key"
    license_path.write_text("ABCD-EFGH-IJKL-MNOP\n", encoding="utf-8")
    monkeypatch.setenv("INSTACRM_LICENSE_FILE", str(license_path))

    assert load_local_license_key() == "ABCD-EFGH-IJKL-MNOP"


def test_load_local_license_key_accepts_legacy_json_as_key_source(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    license_path = tmp_path / "license.json"
    license_path.write_text(
        json.dumps({"license_key": "WXYZ-0000-1111-2222"}),
        encoding="utf-8",
    )
    monkeypatch.setenv("INSTACRM_LICENSE_FILE", str(license_path))

    assert load_local_license_key() == "WXYZ-0000-1111-2222"
