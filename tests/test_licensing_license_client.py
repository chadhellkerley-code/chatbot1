from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import requests

from src.licensing.device_id import DeviceIdentity, collect_device_identity
from src.licensing.license_client import (
    LicenseStartupError,
    SupabaseConfig,
    SupabaseLicenseClient,
    SupabaseRestClient,
    _get_embedded_supabase_config,
    clear_runtime_context,
    launch_with_license,
    load_local_license_key,
    save_local_license_cache,
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


class FakeResponse:
    def __init__(self, *, status_code: int, payload=None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


<<<<<<< HEAD
=======
def _set_embedded_supabase(
    monkeypatch: pytest.MonkeyPatch,
    *,
    url: str = "https://embedded.example.supabase.co",
    key: str = "embedded-key",
) -> tuple[str, str]:
    monkeypatch.setattr("src.licensing._embedded_supabase.SUPABASE_URL", url)
    monkeypatch.setattr("src.licensing._embedded_supabase.SUPABASE_KEY", key)
    return url, key


>>>>>>> origin/main
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
            "created_at": "2026-01-01T00:00:00+00:00",
            "last_seen_at": "2026-01-01T00:00:00+00:00",
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
    assert inserted["license_id"] == "license-1"
    assert inserted["device_id"] == device.device_id
    assert inserted["last_seen_at"] == inserted["activated_at"]


def test_validate_and_activate_blocks_when_license_reaches_max_devices() -> None:
    rest = FakeRestClient(
        license_row={
            "id": "license-2",
            "license_key": "WXYZ-0000-1111-2222",
            "status": "active",
            "max_devices": 2,
            "expires_at": "2099-01-01T00:00:00+00:00",
            "client_name": "Cliente Demo",
            "plan_name": "pro",
            "created_at": "2026-01-01T00:00:00+00:00",
            "last_seen_at": "2026-01-01T00:00:00+00:00",
        },
        activation_rows=[
            {
                "id": "a1",
                "license_id": "license-2",
                "device_id": "device-1",
                "machine_name": "pc-1",
                "os_user": "owner",
                "mac_address": "00:00:00:00:00:01",
                "activated_at": "2026-03-17T00:00:00+00:00",
                "last_seen_at": "2026-03-17T01:00:00+00:00",
                "status": "active",
            },
            {
                "id": "a2",
                "license_id": "license-2",
                "device_id": "device-2",
                "machine_name": "pc-2",
                "os_user": "owner",
                "mac_address": "00:00:00:00:00:02",
                "activated_at": "2026-03-17T00:00:00+00:00",
                "last_seen_at": "2026-03-17T01:00:00+00:00",
                "status": "active",
            },
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


def test_supabase_license_client_uses_embedded_config_even_when_env_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_URL", "https://wrong-project.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "wrong-key")
<<<<<<< HEAD
=======
    expected_url, expected_key = _set_embedded_supabase(monkeypatch)
>>>>>>> origin/main

    client = SupabaseLicenseClient(admin=False)
    url, key = _get_embedded_supabase_config()

<<<<<<< HEAD
=======
    assert url == expected_url
    assert key == expected_key
>>>>>>> origin/main
    assert client.config.url == url
    assert client.config.key == key


<<<<<<< HEAD
def test_supabase_rest_client_sends_embedded_url_and_key_in_headers() -> None:
=======
def test_supabase_rest_client_sends_embedded_url_and_key_in_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_embedded_supabase(monkeypatch)
>>>>>>> origin/main
    url, key = _get_embedded_supabase_config()
    rest = SupabaseRestClient(SupabaseConfig(url=url, key=key))
    captured: dict[str, object] = {}

    class _Session:
        def request(self, **kwargs):
            captured.update(kwargs)
            return FakeResponse(status_code=200, payload=[{"id": "license-1"}], text='[{"id":"license-1"}]')

    rest._session = _Session()

    payload = rest.request("get", "licenses", params={"select": "*"})

    assert payload == [{"id": "license-1"}]
    assert captured["url"] == f"{url}/rest/v1/licenses"
    assert captured["headers"]["apikey"] == key
    assert captured["headers"]["Authorization"] == f"Bearer {key}"


<<<<<<< HEAD
def test_supabase_rest_client_maps_network_errors_to_connection_failure() -> None:
=======
def test_supabase_rest_client_maps_network_errors_to_connection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_embedded_supabase(monkeypatch)
>>>>>>> origin/main
    url, key = _get_embedded_supabase_config()
    rest = SupabaseRestClient(SupabaseConfig(url=url, key=key))

    class _Session:
        def request(self, **kwargs):
            raise requests.ConnectionError("network down")

    rest._session = _Session()

    with pytest.raises(LicenseStartupError) as exc_info:
        rest.request("get", "licenses")

    assert exc_info.value.code == "supabase_request_failed"


<<<<<<< HEAD
def test_supabase_rest_client_maps_auth_errors_to_auth_failure() -> None:
=======
def test_supabase_rest_client_maps_auth_errors_to_auth_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_embedded_supabase(monkeypatch)
>>>>>>> origin/main
    url, key = _get_embedded_supabase_config()
    rest = SupabaseRestClient(SupabaseConfig(url=url, key=key))

    class _Session:
        def request(self, **kwargs):
            return FakeResponse(
                status_code=401,
                payload={"message": "Invalid API key"},
                text='{"message":"Invalid API key"}',
            )

    rest._session = _Session()

    with pytest.raises(LicenseStartupError) as exc_info:
        rest.request("get", "licenses")

    assert exc_info.value.code == "supabase_auth_failed"


def test_launch_with_license_uses_fresh_local_cache_without_remote_call(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_runtime_context()
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))

    device = DeviceIdentity(
        hostname="pc-1",
        os_user="owner",
        mac_address="aa:bb:cc:dd:ee:ff",
        device_id="device-abc",
    )
    monkeypatch.setattr(
        "src.licensing.license_client.collect_device_identity",
        lambda: device,
    )

    now = datetime.now(timezone.utc).replace(microsecond=0)
    save_local_license_cache(
        {
            "license_key": "ABCD-EFGH-IJKL-MNOP",
            "device_id": device.device_id,
            "validated_at": (now - timedelta(hours=1)).isoformat(),
            "expires_at": (now + timedelta(days=30)).isoformat(),
        }
    )

    class _ShouldNotInstantiate:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError("SupabaseLicenseClient should not be instantiated when cache is fresh.")

    monkeypatch.setattr("src.licensing.license_client.SupabaseLicenseClient", _ShouldNotInstantiate)

    context = launch_with_license()
    assert context.license_key == "ABCD-EFGH-IJKL-MNOP"
    assert context.device_id == device.device_id


def test_launch_with_license_falls_back_to_cache_when_supabase_is_down(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_runtime_context()
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))

    device = DeviceIdentity(
        hostname="pc-1",
        os_user="owner",
        mac_address="aa:bb:cc:dd:ee:ff",
        device_id="device-abc",
    )
    monkeypatch.setattr(
        "src.licensing.license_client.collect_device_identity",
        lambda: device,
    )

    now = datetime.now(timezone.utc).replace(microsecond=0)
    save_local_license_cache(
        {
            "license_key": "ABCD-EFGH-IJKL-MNOP",
            "device_id": device.device_id,
            "validated_at": (now - timedelta(days=2)).isoformat(),
            "expires_at": (now + timedelta(days=30)).isoformat(),
        }
    )

    class _OfflineClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def validate_and_activate(self, *args, **kwargs):
            raise LicenseStartupError(
                code="supabase_request_failed",
                user_message="offline",
                detail="network down",
            )

    monkeypatch.setattr("src.licensing.license_client.SupabaseLicenseClient", _OfflineClient)

    context = launch_with_license()
    assert context.license_key == "ABCD-EFGH-IJKL-MNOP"
    assert context.device_id == device.device_id


def test_launch_with_license_blocks_when_offline_cache_is_too_old(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_runtime_context()
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))

    device = DeviceIdentity(
        hostname="pc-1",
        os_user="owner",
        mac_address="aa:bb:cc:dd:ee:ff",
        device_id="device-abc",
    )
    monkeypatch.setattr(
        "src.licensing.license_client.collect_device_identity",
        lambda: device,
    )

    now = datetime.now(timezone.utc).replace(microsecond=0)
    save_local_license_cache(
        {
            "license_key": "ABCD-EFGH-IJKL-MNOP",
            "device_id": device.device_id,
            "validated_at": (now - timedelta(days=5)).isoformat(),
            "expires_at": (now + timedelta(days=30)).isoformat(),
        }
    )

    class _OfflineClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def validate_and_activate(self, *args, **kwargs):
            raise LicenseStartupError(
                code="supabase_request_failed",
                user_message="offline",
                detail="network down",
            )

    monkeypatch.setattr("src.licensing.license_client.SupabaseLicenseClient", _OfflineClient)

    with pytest.raises(LicenseStartupError) as exc_info:
        launch_with_license()

    assert exc_info.value.code == "offline_cache_expired"
