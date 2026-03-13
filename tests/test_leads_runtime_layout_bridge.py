from __future__ import annotations

import json
from pathlib import Path

from application.services.base import ServiceContext
from application.services.leads_service import LeadsService
from core import leads as leads_module


def test_service_migrates_legacy_lead_lists_into_active_data_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    legacy_root = tmp_path / "storage" / "leads"
    legacy_root.mkdir(parents=True, exist_ok=True)
    (legacy_root / "demo.txt").write_text("uno\ndos\n", encoding="utf-8")
    monkeypatch.setenv("INSTACRM_DATA_ROOT", str(data_root))

    service = LeadsService(ServiceContext(root_dir=tmp_path))

    assert service.list_lists() == ["demo"]
    assert service.load_list("demo") == ["uno", "dos"]
    assert (data_root / "leads" / "demo.txt").exists()
    assert leads_module.load_list("demo") == ["uno", "dos"]


def test_service_migrates_legacy_filter_storage_into_active_data_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    legacy_root = tmp_path / "storage" / "lead_filters"
    legacy_lists = legacy_root / "lists"
    legacy_lists.mkdir(parents=True, exist_ok=True)
    (legacy_root / "filters_config.json").write_text(
        json.dumps({"classic": {}, "text": {}, "image": {}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (legacy_root / "account_http_meta.json").write_text(
        json.dumps({"acct": {"cooldown_until": ""}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (legacy_lists / "legacy_run.json").write_text(
        json.dumps({"id": "legacy_run", "items": [], "export_alias": "demo"}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setenv("INSTACRM_DATA_ROOT", str(data_root))

    service = LeadsService(ServiceContext(root_dir=tmp_path))
    filter_rows = service.list_filter_lists()

    assert [str(row.get("id") or "") for row in filter_rows] == ["legacy_run"]
    assert (data_root / "lead_filters" / "filters_config.json").exists()
    assert (data_root / "lead_filters" / "account_http_meta.json").exists()


def test_service_migrates_templates_into_active_data_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    legacy_root = tmp_path / "storage"
    legacy_root.mkdir(parents=True, exist_ok=True)
    data_root.mkdir(parents=True, exist_ok=True)
    (legacy_root / "templates.json").write_text(
        json.dumps(
            [
                {
                    "id": "tpl_alpha",
                    "name": "Alpha",
                    "text": "legacy alpha",
                    "created_at": "2026-03-12T09:00:00+00:00",
                    "updated_at": "2026-03-12T09:00:00+00:00",
                    "schema_version": 1,
                },
                {
                    "id": "tpl_beta",
                    "name": "Beta",
                    "text": "legacy beta",
                    "created_at": "2026-03-12T10:00:00+00:00",
                    "updated_at": "2026-03-12T10:00:00+00:00",
                    "schema_version": 1,
                },
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (legacy_root / "templates_state.json").write_text(
        json.dumps({"acct:tpl_beta": 3}, ensure_ascii=False),
        encoding="utf-8",
    )
    (data_root / "templates.json").write_text(
        json.dumps(
            [
                {
                    "id": "tpl_alpha",
                    "name": "Alpha",
                    "text": "active alpha",
                    "created_at": "2026-03-12T11:00:00+00:00",
                    "updated_at": "2026-03-12T11:00:00+00:00",
                    "schema_version": 1,
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("INSTACRM_DATA_ROOT", str(data_root))

    service = LeadsService(ServiceContext(root_dir=tmp_path))
    rows = service.list_template_rows()

    assert [str(row.get("name") or "") for row in rows] == ["Alpha", "Beta"]
    assert rows[0]["text"] == "active alpha"
    assert rows[1]["text"] == "legacy beta"
    assert (data_root / "templates.json").exists()
    assert (data_root / "templates_state.json").exists()
    merged_state = json.loads((data_root / "templates_state.json").read_text(encoding="utf-8"))
    assert merged_state["acct:tpl_beta"] == 3


def test_service_merges_templates_from_data_root_when_storage_is_active(
    tmp_path: Path,
) -> None:
    storage_root = tmp_path / "storage"
    data_root = tmp_path / "data"
    storage_root.mkdir(parents=True, exist_ok=True)
    data_root.mkdir(parents=True, exist_ok=True)
    (storage_root / "templates.json").write_text(
        json.dumps(
            [
                {
                    "id": "tpl_alpha",
                    "name": "Alpha",
                    "text": "storage alpha",
                    "created_at": "2026-03-12T08:00:00+00:00",
                    "updated_at": "2026-03-12T08:00:00+00:00",
                    "schema_version": 1,
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (data_root / "templates.json").write_text(
        json.dumps(
            [
                {
                    "id": "tpl_alpha",
                    "name": "Alpha",
                    "text": "data alpha",
                    "created_at": "2026-03-12T12:00:00+00:00",
                    "updated_at": "2026-03-12T12:00:00+00:00",
                    "schema_version": 1,
                },
                {
                    "id": "tpl_gamma",
                    "name": "Gamma",
                    "text": "data gamma",
                    "created_at": "2026-03-12T13:00:00+00:00",
                    "updated_at": "2026-03-12T13:00:00+00:00",
                    "schema_version": 1,
                },
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (data_root / "templates_state.json").write_text(
        json.dumps({"acct:tpl_gamma": 4}, ensure_ascii=False),
        encoding="utf-8",
    )

    service = LeadsService(ServiceContext(root_dir=tmp_path))
    rows = service.list_template_rows()

    assert [str(row.get("name") or "") for row in rows] == ["Alpha", "Gamma"]
    assert rows[0]["text"] == "data alpha"
    assert rows[1]["text"] == "data gamma"
    merged_state = json.loads((storage_root / "templates_state.json").read_text(encoding="utf-8"))
    assert merged_state["acct:tpl_gamma"] == 4
