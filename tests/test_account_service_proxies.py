from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

import application.services.account_service as account_service_module
from application.services.account_service import AccountService
from application.services.base import ServiceContext, ServiceError


def _build_service(tmp_path: Path) -> AccountService:
    return AccountService(ServiceContext.default(tmp_path))


def test_upsert_proxy_normalizes_server_and_encrypts_credentials(tmp_path: Path) -> None:
    service = _build_service(tmp_path)

    record = service.upsert_proxy(
        {
            "id": "proxy-a",
            "server": "127.0.0.1:9000",
            "user": "alice",
            "pass": "secret",
            "active": True,
        }
    )

    assert record["server"] == "http://127.0.0.1:9000"
    stored = json.loads((tmp_path / "storage" / "accounts" / "proxies.json").read_text(encoding="utf-8"))
    assert stored["schema_version"] == 2
    proxy_row = stored["proxies"][0]
    assert proxy_row["server"] == "http://127.0.0.1:9000"
    assert proxy_row["user_enc"].startswith("enc:v1:")
    assert proxy_row["pass_enc"].startswith("enc:v1:")
    assert "user" not in proxy_row
    assert "pass" not in proxy_row
    with sqlite3.connect(tmp_path / "data" / "proxy_registry.sqlite3") as connection:
        row = connection.execute(
            "select server, user_enc, pass_enc from proxies where id = ?",
            ("proxy-a",),
        ).fetchone()
    assert row is not None
    assert row[0] == "http://127.0.0.1:9000"
    assert str(row[1]).startswith("enc:v1:")
    assert str(row[2]).startswith("enc:v1:")


def test_load_proxies_migrates_legacy_json_into_sqlite(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    for module_name in ("core.proxy_registry", "src.proxy_pool"):
        sys.modules.pop(module_name, None)
    import core.proxy_registry as proxy_registry  # type: ignore

    proxy_registry = importlib.reload(proxy_registry)
    target = tmp_path / "storage" / "accounts" / "proxies.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            [
                {
                    "id": "proxy-a",
                    "server": "127.0.0.1:9000",
                    "user": "alice",
                    "pass": "secret",
                    "active": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    rows = proxy_registry.load_proxies(target)

    assert rows == [
        {
            "id": "proxy-a",
            "server": "http://127.0.0.1:9000",
            "user": "alice",
            "pass": "secret",
            "active": True,
            "disabled_reason": "",
            "last_test_at": "",
            "last_success_at": "",
            "last_failure_at": "",
            "last_public_ip": "",
            "last_latency_ms": None,
            "last_error": "",
            "failure_count": 0,
            "success_count": 0,
            "consecutive_failures": 0,
            "quarantine_until": 0.0,
            "quarantine_reason": "",
            "last_event_at": "",
        }
    ]
    with sqlite3.connect(tmp_path / "data" / "proxy_registry.sqlite3") as connection:
        row = connection.execute(
            "select server, user_enc, pass_enc from proxies where id = ?",
            ("proxy-a",),
        ).fetchone()
    assert row is not None
    assert row[0] == "http://127.0.0.1:9000"
    assert str(row[1]).startswith("enc:v1:")
    assert str(row[2]).startswith("enc:v1:")
    repaired = json.loads(target.read_text(encoding="utf-8"))
    assert repaired["schema_version"] == 2
    assert repaired["proxies"][0]["server"] == "http://127.0.0.1:9000"
    assert repaired["proxies"][0]["user_enc"].startswith("enc:v1:")


def test_load_proxy_audit_entries_migrates_legacy_jsonl_into_sqlite(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    for module_name in ("core.proxy_registry",):
        sys.modules.pop(module_name, None)
    import core.proxy_registry as proxy_registry  # type: ignore

    proxy_registry = importlib.reload(proxy_registry)
    audit_path = tmp_path / "runtime" / "logs" / "proxy_audit.jsonl"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps(
            {
                "ts": "2026-03-12T00:00:00Z",
                "proxy_id": "proxy-a",
                "event": "proxy_test",
                "status": "ok",
                "message": "legacy",
                "meta": {"public_ip": "1.1.1.1"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    rows = proxy_registry.load_proxy_audit_entries(path=audit_path, proxy_id="proxy-a", limit=10)

    assert rows == [
        {
            "ts": "2026-03-12T00:00:00Z",
            "proxy_id": "proxy-a",
            "event": "proxy_test",
            "status": "ok",
            "message": "legacy",
            "meta": {"public_ip": "1.1.1.1"},
        }
    ]
    with sqlite3.connect(tmp_path / "data" / "proxy_registry.sqlite3") as connection:
        row = connection.execute(
            "select count(*) from proxy_audit where proxy_id = ?",
            ("proxy-a",),
        ).fetchone()
    assert row is not None
    assert int(row[0]) == 1


def test_account_proxy_links_migrate_from_accounts_json(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    for module_name in ("core.proxy_registry",):
        sys.modules.pop(module_name, None)
    import core.proxy_registry as proxy_registry  # type: ignore

    proxy_registry = importlib.reload(proxy_registry)
    proxies_path = tmp_path / "storage" / "accounts" / "proxies.json"
    proxies_path.parent.mkdir(parents=True, exist_ok=True)
    proxies_path.write_text(
        json.dumps({"schema_version": 2, "proxies": [{"id": "proxy-a", "server": "http://127.0.0.1:9000"}]}),
        encoding="utf-8",
    )
    accounts_path = tmp_path / "storage" / "accounts" / "accounts.json"
    accounts_path.write_text(
        json.dumps(
            [
                {"username": "uno", "alias": "default", "assigned_proxy_id": "proxy-a"},
                {"username": "dos", "alias": "default", "proxy_url": "http://legacy:9000"},
            ]
        ),
        encoding="utf-8",
    )

    proxy_registry.load_proxies(proxies_path)

    assert proxy_registry.assigned_accounts_for_proxy("proxy-a", proxies_path) == ["uno"]
    with sqlite3.connect(tmp_path / "data" / "proxy_registry.sqlite3") as connection:
        row = connection.execute(
            "select has_legacy_proxy from account_proxy_links where username = ?",
            ("dos",),
        ).fetchone()
    assert row is not None
    assert int(row[0]) == 1


def test_update_account_rejects_missing_assigned_proxy(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    for module_name in ("core.proxy_registry", "core.accounts"):
        sys.modules.pop(module_name, None)
    import core.accounts as accounts_module  # type: ignore
    import core.proxy_registry as proxy_registry  # type: ignore

    accounts_module = importlib.reload(accounts_module)
    proxy_registry = importlib.reload(proxy_registry)

    assert accounts_module.add_account("uno", "default") is True

    with pytest.raises(ValueError, match="no existe"):
        accounts_module.update_account("uno", {"assigned_proxy_id": "missing-proxy"})


def test_delete_proxy_record_restricted_by_account_proxy_links(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    for module_name in ("core.proxy_registry", "core.accounts"):
        sys.modules.pop(module_name, None)
    import core.accounts as accounts_module  # type: ignore
    import core.proxy_registry as proxy_registry  # type: ignore

    accounts_module = importlib.reload(accounts_module)
    proxy_registry = importlib.reload(proxy_registry)
    proxies_path = tmp_path / "storage" / "accounts" / "proxies.json"

    proxy_registry.upsert_proxy_record(
        {"id": "proxy-a", "server": "http://127.0.0.1:9000"},
        proxies_path,
    )
    assert accounts_module.add_account("uno", "default") is True
    assert accounts_module.update_account("uno", {"assigned_proxy_id": "proxy-a"}) is True

    with pytest.raises(proxy_registry.ProxyValidationError, match="cuenta\\(s\\) asignadas"):
        proxy_registry.delete_proxy_record("proxy-a", proxies_path)


def test_add_account_with_legacy_proxy_materializes_managed_proxy(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    for module_name in ("core.proxy_registry", "core.accounts"):
        sys.modules.pop(module_name, None)
    import core.accounts as accounts_module  # type: ignore
    import core.proxy_registry as proxy_registry  # type: ignore

    accounts_module = importlib.reload(accounts_module)
    proxy_registry = importlib.reload(proxy_registry)
    assert accounts_module.add_account(
        "uno",
        "default",
        {
            "proxy_url": "http://127.0.0.1:9000",
            "proxy_user": "alice",
            "proxy_pass": "secret",
            "proxy_sticky_minutes": 10,
        },
    ) is True

    account = accounts_module.get_account("uno")

    assert account is not None
    assert account["assigned_proxy_id"] == "acct:uno"
    assert account["proxy_url"] == "http://127.0.0.1:9000"
    proxy_row = proxy_registry.get_proxy_by_id("acct:uno", path=tmp_path / "storage" / "accounts" / "proxies.json")
    assert proxy_row is not None
    assert proxy_row["server"] == "http://127.0.0.1:9000"
    assert proxy_row["user"] == "alice"
    assert proxy_row["pass"] == "secret"


def test_load_accounts_migrates_legacy_proxy_url_to_managed_assignment(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    for module_name in ("core.proxy_registry", "core.accounts"):
        sys.modules.pop(module_name, None)
    import core.accounts as accounts_module  # type: ignore
    import core.proxy_registry as proxy_registry  # type: ignore

    proxy_registry = importlib.reload(proxy_registry)
    accounts_path = tmp_path / "storage" / "accounts" / "accounts.json"
    accounts_path.parent.mkdir(parents=True, exist_ok=True)
    accounts_path.write_text(
        json.dumps(
            [
                {
                    "username": "uno",
                    "alias": "default",
                    "proxy_url": "http://127.0.0.1:9000",
                    "proxy_user": "alice",
                    "proxy_pass": "secret",
                    "proxy_sticky_minutes": 10,
                }
            ]
        ),
        encoding="utf-8",
    )

    accounts_module = importlib.reload(accounts_module)
    rows = accounts_module.list_all()

    assert rows[0]["assigned_proxy_id"] == "acct:uno"
    stored_accounts = json.loads(accounts_path.read_text(encoding="utf-8"))
    assert stored_accounts[0]["assigned_proxy_id"] == "acct:uno"
    proxy_row = proxy_registry.get_proxy_by_id("acct:uno", path=tmp_path / "storage" / "accounts" / "proxies.json")
    assert proxy_row is not None
    assert proxy_row["server"] == "http://127.0.0.1:9000"


def test_assign_proxy_requires_existing_active_proxy(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000", "active": False})

    updated_calls: list[tuple[str, dict]] = []

    def _fake_update_account(username: str, updates: dict) -> bool:
        updated_calls.append((username, dict(updates)))
        return True

    monkeypatch.setattr(account_service_module.accounts_module, "update_account", _fake_update_account)

    with pytest.raises(ServiceError, match="inactivo"):
        service.assign_proxy(["uno"], "proxy-a")

    assert updated_calls == []


def test_assign_proxy_clears_legacy_proxy_fields(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000", "active": True})

    updated_calls: list[tuple[str, dict]] = []

    def _fake_update_account(username: str, updates: dict) -> bool:
        updated_calls.append((username, dict(updates)))
        return True

    monkeypatch.setattr(account_service_module.accounts_module, "update_account", _fake_update_account)

    updated = service.assign_proxy(["uno"], "proxy-a")

    assert updated == 1
    assert updated_calls == [
        (
            "uno",
            {
                "assigned_proxy_id": "proxy-a",
                "proxy_url": "",
                "proxy_user": "",
                "proxy_pass": "",
                "proxy_sticky_minutes": None,
            },
        )
    ]


def test_delete_proxy_rejects_assigned_accounts(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000"})

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [{"username": "uno", "alias": "default", "assigned_proxy_id": "proxy-a"}],
    )

    with pytest.raises(ServiceError, match="cuenta\\(s\\) asignadas"):
        service.delete_proxy("proxy-a")


def test_test_proxy_persists_health_metadata(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000"})

    class _Binding:
        public_ip = "1.2.3.4"
        masked_ip = "1.2.3.x"
        latency = 0.321

    monkeypatch.setattr(account_service_module, "test_proxy_connection", lambda config: _Binding())

    result = service.test_proxy("proxy-a")

    assert result["proxy_id"] == "proxy-a"
    assert result["public_ip"] == "1.2.3.4"
    row = service.list_proxy_records()[0]
    assert row["last_public_ip"] == "1.2.3.4"
    assert row["last_test_at"]
    assert row["last_success_at"]
    assert row["last_latency_ms"] == pytest.approx(321.0)
    assert row["last_error"] == ""


def test_test_proxy_failure_opens_quarantine_and_writes_audit(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000"})

    monkeypatch.setattr(
        account_service_module,
        "test_proxy_connection",
        lambda config: (_ for _ in ()).throw(RuntimeError("timeout proxy")),
    )

    for _ in range(3):
        with pytest.raises(ServiceError, match="timeout proxy"):
            service.test_proxy("proxy-a")

    row = service.list_proxy_records()[0]
    assert row["consecutive_failures"] >= 3
    assert float(row["quarantine_until"] or 0.0) > 0.0
    assert row["quarantine_reason"] == "timeout proxy"
    assert service.proxy_health_label(row).startswith("Cuarentena")
    audit_rows = service.recent_proxy_audit(proxy_id="proxy-a", limit=10)
    assert any(entry["event"] == "proxy_quarantine_opened" for entry in audit_rows)


def test_successful_test_clears_existing_quarantine(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000"})

    monkeypatch.setattr(
        account_service_module,
        "test_proxy_connection",
        lambda config: (_ for _ in ()).throw(RuntimeError("timeout proxy")),
    )
    for _ in range(3):
        with pytest.raises(ServiceError):
            service.test_proxy("proxy-a")

    class _Binding:
        public_ip = "9.9.9.9"
        masked_ip = "9.9.9.x"
        latency = 0.123

    monkeypatch.setattr(account_service_module, "test_proxy_connection", lambda config: _Binding())
    result = service.test_proxy("proxy-a")

    assert result["public_ip"] == "9.9.9.9"
    row = service.list_proxy_records()[0]
    assert float(row["quarantine_until"] or 0.0) == 0.0
    assert row["quarantine_reason"] == ""
    assert row["consecutive_failures"] == 0


def test_login_marks_inactive_assigned_proxy_as_invalid(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000", "active": False})

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [
            {
                "username": "bad_proxy",
                "alias": "alias-a",
                "password": "secret-one",
                "assigned_proxy_id": "proxy-a",
            }
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
        lambda *args, **kwargs: pytest.fail("No deberia intentar construir payload con proxy inactivo"),
    )

    result = service.login("alias-a", ["bad_proxy"])

    assert result == [
        {
            "username": "bad_proxy",
            "status": "failed",
            "message": "inactive",
            "profile_path": "",
            "row_number": None,
        }
    ]


def test_login_marks_quarantined_assigned_proxy_as_invalid(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000", "active": True})

    monkeypatch.setattr(
        account_service_module,
        "test_proxy_connection",
        lambda config: (_ for _ in ()).throw(RuntimeError("timeout proxy")),
    )
    for _ in range(3):
        with pytest.raises(ServiceError):
            service.test_proxy("proxy-a")

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [
            {
                "username": "bad_proxy",
                "alias": "alias-a",
                "password": "secret-one",
                "assigned_proxy_id": "proxy-a",
            }
        ],
    )
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "_account_password",
        lambda record: record.get("password"),
    )

    result = service.login("alias-a", ["bad_proxy"])

    assert result == [
        {
            "username": "bad_proxy",
            "status": "failed",
            "message": "quarantined",
            "profile_path": "",
            "row_number": None,
        }
    ]


def test_load_proxies_repairs_corrupted_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    for module_name in ("core.proxy_registry", "src.proxy_pool"):
        sys.modules.pop(module_name, None)
    import core.proxy_registry as proxy_registry  # type: ignore

    proxy_registry = importlib.reload(proxy_registry)
    target = tmp_path / "storage" / "accounts" / "proxies.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{broken json", encoding="utf-8")

    rows = proxy_registry.load_proxies(target)

    assert rows == []
    repaired = json.loads(target.read_text(encoding="utf-8"))
    assert repaired == {"schema_version": 2, "proxies": []}


def test_proxy_from_account_raises_for_inactive_assigned_proxy(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    for module_name in ("core.proxy_registry", "src.proxy_pool", "src.proxy_payload"):
        sys.modules.pop(module_name, None)
    import core.proxy_registry as proxy_registry  # type: ignore
    import src.proxy_payload as proxy_payload  # type: ignore

    proxy_registry = importlib.reload(proxy_registry)
    proxy_payload = importlib.reload(proxy_payload)
    target = tmp_path / "storage" / "accounts" / "proxies.json"
    proxy_registry.save_proxy_records(
        [{"id": "proxy-a", "server": "http://127.0.0.1:9000", "active": False}],
        target,
    )

    with pytest.raises(proxy_registry.ProxyResolutionError, match="inactivo"):
        proxy_payload.proxy_from_account({"assigned_proxy_id": "proxy-a"})


def test_proxy_preflight_for_accounts_blocks_quarantined_assignments(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000", "active": True})

    monkeypatch.setattr(
        account_service_module,
        "test_proxy_connection",
        lambda config: (_ for _ in ()).throw(RuntimeError("timeout proxy")),
    )
    for _ in range(3):
        with pytest.raises(ServiceError):
            service.test_proxy("proxy-a")

    payload = service.proxy_preflight_for_accounts(
        [{"username": "uno", "alias": "default", "assigned_proxy_id": "proxy-a"}]
    )

    assert payload["ready_accounts"] == []
    assert payload["blocked"] == 1
    assert payload["blocked_accounts"][0]["status"] == "quarantined"
    assert payload["blocked_accounts"][0]["username"] == "uno"


def test_sweep_proxy_health_limits_to_assigned_active_proxies(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000", "active": True})
    service.upsert_proxy({"id": "proxy-b", "server": "http://127.0.0.1:9001", "active": True})
    service.upsert_proxy({"id": "proxy-c", "server": "http://127.0.0.1:9002", "active": False})

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [
            {"username": "uno", "alias": "default", "assigned_proxy_id": "proxy-a"},
            {"username": "dos", "alias": "default", "assigned_proxy_id": "proxy-c"},
        ],
    )

    called_urls: list[str] = []

    class _Binding:
        public_ip = "8.8.8.8"
        masked_ip = "8.8.8.x"
        latency = 0.222

    def _fake_test_proxy_connection(config):
        called_urls.append(str(config.url))
        return _Binding()

    monkeypatch.setattr(account_service_module, "test_proxy_connection", _fake_test_proxy_connection)

    result = service.sweep_proxy_health(
        only_assigned=True,
        active_only=True,
        source="pytest",
    )

    assert result["checked"] == 1
    assert result["succeeded"] == 1
    assert result["failed"] == 0
    assert [row["proxy_id"] for row in result["results"]] == ["proxy-a"]
    assert called_urls == ["http://127.0.0.1:9000"]


def test_sweep_proxy_health_skips_recently_tested_proxies(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_proxy({"id": "proxy-a", "server": "http://127.0.0.1:9000", "active": True})

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [{"username": "uno", "alias": "default", "assigned_proxy_id": "proxy-a"}],
    )

    class _Binding:
        public_ip = "1.1.1.1"
        masked_ip = "1.1.1.x"
        latency = 0.111

    monkeypatch.setattr(account_service_module, "test_proxy_connection", lambda config: _Binding())
    service.test_proxy("proxy-a")

    result = service.sweep_proxy_health(
        only_assigned=True,
        active_only=True,
        stale_after_seconds=3600.0,
        source="pytest",
    )

    assert result["checked"] == 0
    assert result["skipped_not_due"] == 1
