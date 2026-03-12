from __future__ import annotations

import json
from pathlib import Path

from application.services import AutomationService, ServiceContext
from core import responder


def _configure_responder_paths(monkeypatch, root: Path) -> tuple[Path, Path]:
    data_root = root / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    prompts_path = data_root / "autoresponder_prompts.json"
    packs_path = data_root / "conversational_packs.json"
    monkeypatch.setattr(responder, "_PROMPTS_FILE", prompts_path)
    monkeypatch.setattr(responder, "_PACKS_FILE", packs_path)
    monkeypatch.setattr(responder, "_FOLLOWUP_FILE", data_root / "followups.json")
    monkeypatch.setattr(responder, "_ACCOUNT_MEMORY_FILE", data_root / "autoresponder_account_memory.json")
    monkeypatch.setattr(responder, "_PROMPTS_STATE", None)
    monkeypatch.setattr(responder, "_PACKS_STATE", None)
    monkeypatch.setattr(responder, "_FOLLOWUP_STATE", None)
    monkeypatch.setattr(responder, "_ACCOUNT_MEMORY_STATE", None)
    return prompts_path, packs_path


def _legacy_flow_payload() -> dict[str, object]:
    return {
        "aliases": {
            "ventas": {
                "alias": "ventas",
                "flow_config": {
                    "version": 1,
                    "entry_stage_id": "stage_1",
                    "stages": [
                        {
                            "id": "stage_1",
                            "action_type": "PACK_BIENVENIDA",
                            "transitions": {
                                "positive": "stage_1",
                                "negative": "stage_1",
                                "doubt": "stage_1",
                                "neutral": "stage_1",
                            },
                            "followups": [],
                            "post_objection": {
                                "enabled": False,
                                "action_type": "",
                                "max_steps": 3,
                                "resolved_transition": "positive",
                                "unresolved_transition": "negative",
                            },
                        }
                    ],
                },
            }
        },
        "accounts": {},
    }


def _legacy_packs_payload(name: str = "Pack Legacy") -> dict[str, object]:
    return {
        "packs": [
            {
                "id": "pack_legacy",
                "name": name,
                "type": "PACK_BIENVENIDA",
                "delay_min": 3,
                "delay_max": 8,
                "active": True,
                "actions": [{"type": "text_fixed", "content": "hola"}],
            }
        ]
    }


def test_automation_service_syncs_legacy_prompts_and_packs_into_data_root(monkeypatch, tmp_path: Path) -> None:
    prompts_path, packs_path = _configure_responder_paths(monkeypatch, tmp_path)
    legacy_root = tmp_path / "storage"
    legacy_root.mkdir(parents=True, exist_ok=True)
    (legacy_root / "autoresponder_prompts.json").write_text(
        json.dumps(_legacy_flow_payload()),
        encoding="utf-8",
    )
    (legacy_root / "conversational_packs.json").write_text(
        json.dumps(_legacy_packs_payload()),
        encoding="utf-8",
    )

    service = AutomationService(ServiceContext.default(root_dir=tmp_path))

    assert prompts_path.exists()
    assert packs_path.exists()
    assert service.list_packs()[0]["name"] == "Pack Legacy"
    flow = service.get_flow_config("ventas")
    assert flow["entry_stage_id"] == "stage_1"
    assert flow["stages"][0]["action_type"] == "PACK_BIENVENIDA"


def test_automation_service_does_not_overwrite_existing_data_root_files(monkeypatch, tmp_path: Path) -> None:
    prompts_path, packs_path = _configure_responder_paths(monkeypatch, tmp_path)
    legacy_root = tmp_path / "storage"
    legacy_root.mkdir(parents=True, exist_ok=True)
    prompts_path.write_text(
        json.dumps(
            {
                "aliases": {
                    "ventas": {
                        "alias": "ventas",
                        "flow_config": {"version": 1, "entry_stage_id": "stage_primary", "stages": [{"id": "stage_primary"}]},
                    }
                },
                "accounts": {},
            }
        ),
        encoding="utf-8",
    )
    packs_path.write_text(
        json.dumps(_legacy_packs_payload(name="Pack Primary")),
        encoding="utf-8",
    )
    (legacy_root / "autoresponder_prompts.json").write_text(
        json.dumps(_legacy_flow_payload()),
        encoding="utf-8",
    )
    (legacy_root / "conversational_packs.json").write_text(
        json.dumps(_legacy_packs_payload(name="Pack Legacy")),
        encoding="utf-8",
    )

    service = AutomationService(ServiceContext.default(root_dir=tmp_path))

    assert service.list_packs()[0]["name"] == "Pack Primary"
    flow = service.get_flow_config("ventas")
    assert flow["entry_stage_id"] == "stage_primary"
