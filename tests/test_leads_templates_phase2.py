from __future__ import annotations

import json
from pathlib import Path

from application.services.base import ServiceContext
from application.services.campaign_service import CampaignService
from application.services.leads_service import LeadsService
from core.templates_store import TemplateStore, load_templates, next_round_robin


def test_template_store_uses_isolated_root_dirs(tmp_path: Path) -> None:
    root_a = tmp_path / "tenant_a"
    root_b = tmp_path / "tenant_b"

    TemplateStore(root_a).save_templates([{"name": "Alpha", "text": "hola"}])
    TemplateStore(root_b).save_templates([{"name": "Beta", "text": "chau"}])

    assert [item["name"] for item in load_templates(root_dir=root_a)] == ["Alpha"]
    assert [item["name"] for item in load_templates(root_dir=root_b)] == ["Beta"]
    assert (root_a / "storage" / "templates.json").exists()
    assert (root_b / "storage" / "templates.json").exists()


def test_template_store_repairs_corrupted_json_and_keeps_backup(tmp_path: Path) -> None:
    storage_path = tmp_path / "storage" / "templates.json"
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    storage_path.write_text("{bad json", encoding="utf-8")

    templates = load_templates(root_dir=tmp_path)

    assert templates == []
    assert json.loads(storage_path.read_text(encoding="utf-8")) == []
    assert list(storage_path.parent.glob("templates.json.json_parse_error*.bak"))


def test_template_store_repairs_invalid_json_structure_and_keeps_backup(tmp_path: Path) -> None:
    storage_path = tmp_path / "storage" / "templates.json"
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    storage_path.write_text(json.dumps({"unexpected": True}), encoding="utf-8")

    templates = load_templates(root_dir=tmp_path)

    assert templates == []
    assert json.loads(storage_path.read_text(encoding="utf-8")) == []
    assert list(storage_path.parent.glob("templates.json.json_schema_error*.bak"))


def test_template_round_robin_state_is_isolated_by_root_dir(tmp_path: Path) -> None:
    candidates = ["uno", "dos"]
    root_a = tmp_path / "tenant_a"
    root_b = tmp_path / "tenant_b"

    assert next_round_robin("acc", "tpl_demo", candidates, root_dir=root_a) == ("uno", 0)
    assert next_round_robin("acc", "tpl_demo", candidates, root_dir=root_a) == ("dos", 1)
    assert next_round_robin("acc", "tpl_demo", candidates, root_dir=root_b) == ("uno", 0)


def test_services_read_templates_from_their_own_context_root(tmp_path: Path) -> None:
    root_a = tmp_path / "tenant_a"
    root_b = tmp_path / "tenant_b"

    leads_a = LeadsService(ServiceContext(root_dir=root_a))
    leads_b = LeadsService(ServiceContext(root_dir=root_b))

    alpha = leads_a.upsert_template("Alpha", "hola")
    beta = leads_b.upsert_template("Beta", "chau")

    campaign_a = CampaignService(ServiceContext(root_dir=root_a))
    campaign_b = CampaignService(ServiceContext(root_dir=root_b))

    assert [item["name"] for item in leads_a.list_templates()] == ["Alpha"]
    assert [item["name"] for item in leads_b.list_templates()] == ["Beta"]
    assert [item["id"] for item in campaign_a.list_templates()] == [alpha["id"]]
    assert [item["id"] for item in campaign_b.list_templates()] == [beta["id"]]
