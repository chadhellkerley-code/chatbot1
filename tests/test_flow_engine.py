from __future__ import annotations

import time

import pytest
import core.responder as responder
def _stage(stage_id: str, action_type: str, *, positive: str, negative: str, doubt: str, neutral: str, followups=None, objection=None):
    return {
        "id": stage_id,
        "action_type": action_type,
        "transitions": {
            "positive": positive,
            "negative": negative,
            "doubt": doubt,
            "neutral": neutral,
        },
        "followups": list(followups or []),
        "post_objection": dict(objection or {"enabled": False, "action_type": "", "max_steps": 1}),
    }


def test_flow_simple_3_stages_progresses_without_jumps() -> None:
    config = responder._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "s1",
            "stages": [
                _stage("s1", "PACK_1", positive="s2", negative="s1", doubt="s1", neutral="s1"),
                _stage("s2", "PACK_2", positive="s3", negative="s2", doubt="s2", neutral="s2"),
                _stage("s3", "PACK_3", positive="s3", negative="s3", doubt="s3", neutral="s3"),
            ],
        }
    )
    engine = responder.FlowEngine(config)
    decision = engine.evaluate(
        {
            "flow_state": {"stage_id": "s1", "followup_level": 0, "objection_step": 0},
            "inbound_text": "si, me interesa",
            "latest_inbound_id": "in-1",
            "last_inbound_id_seen": "",
            "pending_reply": False,
            "pending_inbound_id": "",
            "last_outbound_ts": time.time() - 3600,
            "now_ts": time.time(),
            "objection_strategy_name": "OBJECION",
        }
    )
    assert decision["decision"] == "reply"
    assert decision["next_stage_id"] == "s2"
    assert decision["action_type"] == "PACK_2"
    updated = engine.apply_outbound(
        {"stage_id": "s1", "followup_level": 0, "objection_step": 0},
        decision,
        sent_at=time.time(),
    )
    assert updated["stage_id"] == "s2"


def test_flow_complex_10_plus_clamps_out_of_order_transition() -> None:
    stages = []
    for idx in range(1, 12):
        stage_id = f"s{idx}"
        positive_target = f"s{idx + 1}" if idx < 11 else "s11"
        stages.append(
            _stage(
                stage_id,
                f"PACK_{idx}",
                positive=positive_target,
                negative=stage_id,
                doubt=stage_id,
                neutral=stage_id,
            )
        )
    stages[0]["transitions"]["positive"] = "s11"
    config = responder._normalize_flow_config(
        {"version": 1, "entry_stage_id": "s1", "stages": stages}
    )
    engine = responder.FlowEngine(config)
    decision = engine.evaluate(
        {
            "flow_state": {"stage_id": "s1", "followup_level": 0, "objection_step": 0},
            "inbound_text": "si",
            "latest_inbound_id": "in-2",
            "last_inbound_id_seen": "",
            "pending_reply": False,
            "pending_inbound_id": "",
            "last_outbound_ts": time.time() - 3600,
            "now_ts": time.time(),
            "objection_strategy_name": "",
        }
    )
    assert decision["next_stage_id"] == "s2"
    assert decision["action_type"] == "PACK_2"


def test_followups_multiple_levels_no_repeat_no_double_send() -> None:
    now_ts = time.time()
    config = responder._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "s1",
            "stages": [
                _stage(
                    "s1",
                    "PACK_1",
                    positive="s1",
                    negative="s1",
                    doubt="s1",
                    neutral="s1",
                    followups=[
                        {"delay_hours": 0, "action_type": "FU_1"},
                        {"delay_hours": 1, "action_type": "FU_2"},
                    ],
                )
            ],
        }
    )
    engine = responder.FlowEngine(config)
    state = {
        "stage_id": "s1",
        "followup_level": 0,
        "followup_anchor_ts": now_ts - 5,
        "last_outbound_ts": now_ts - 5,
        "objection_step": 0,
    }
    first = engine.evaluate(
        {
            "flow_state": state,
            "inbound_text": "",
            "latest_inbound_id": "",
            "last_inbound_id_seen": "",
            "pending_reply": False,
            "pending_inbound_id": "",
            "last_outbound_ts": now_ts - 5,
            "now_ts": now_ts,
            "objection_strategy_name": "",
        }
    )
    assert first["decision"] == "followup"
    assert first["action_type"] == "FU_1"
    state = engine.apply_outbound(state, first, sent_at=now_ts)
    second = engine.evaluate(
        {
            "flow_state": state,
            "inbound_text": "",
            "latest_inbound_id": "",
            "last_inbound_id_seen": "",
            "pending_reply": False,
            "pending_inbound_id": "",
            "last_outbound_ts": now_ts,
            "now_ts": now_ts + 30,
            "objection_strategy_name": "",
        }
    )
    assert second["decision"] == "wait"
    third = engine.evaluate(
        {
            "flow_state": state,
            "inbound_text": "",
            "latest_inbound_id": "",
            "last_inbound_id_seen": "",
            "pending_reply": False,
            "pending_inbound_id": "",
            "last_outbound_ts": now_ts,
            "now_ts": now_ts + 3601,
            "objection_strategy_name": "",
        }
    )
    assert third["decision"] == "followup"
    assert third["action_type"] == "FU_2"


def test_chained_objections_resolve_and_reset_step() -> None:
    config = responder._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "s1",
            "stages": [
                _stage(
                    "s1",
                    "PACK_1",
                    positive="s2",
                    negative="s1",
                    doubt="s1",
                    neutral="s1",
                    objection={
                        "enabled": True,
                        "action_type": "OBJECION",
                        "max_steps": 2,
                        "resolved_transition": "positive",
                        "unresolved_transition": "negative",
                    },
                ),
                _stage("s2", "PACK_2", positive="s2", negative="s2", doubt="s2", neutral="s2"),
            ],
        }
    )
    engine = responder.FlowEngine(config)
    d1 = engine.evaluate(
        {
            "flow_state": {"stage_id": "s1", "followup_level": 0, "objection_step": 0},
            "inbound_text": "no me interesa",
            "latest_inbound_id": "in-1",
            "last_inbound_id_seen": "",
            "pending_reply": False,
            "pending_inbound_id": "",
            "last_outbound_ts": time.time() - 10,
            "now_ts": time.time(),
            "objection_strategy_name": "OBJECION",
        }
    )
    assert d1["action_type"] == "OBJECION"
    assert d1["objection_step_after"] == 1
    state = engine.apply_outbound({"stage_id": "s1", "followup_level": 0, "objection_step": 0}, d1, sent_at=time.time())
    d2 = engine.evaluate(
        {
            "flow_state": state,
            "inbound_text": "si, contame",
            "latest_inbound_id": "in-2",
            "last_inbound_id_seen": "in-1",
            "pending_reply": False,
            "pending_inbound_id": "",
            "last_outbound_ts": time.time() - 10,
            "now_ts": time.time(),
            "objection_strategy_name": "OBJECION",
        }
    )
    assert d2["next_stage_id"] == "s2"
    assert d2["action_type"] == "PACK_2"
    assert d2["objection_step_after"] == 0


def test_flow_config_can_be_deleted_and_recreated() -> None:
    with pytest.raises(responder.FlowConfigRequiredError):
        responder._resolve_flow_config_for_prompt_entry(
            {
                "flow_config": {
                    "version": 1,
                    "entry_stage_id": "",
                    "stages": [],
                    "allow_empty": True,
                }
            },
            followup_schedule_hours=[4, 8],
        )
    deleted = responder._resolve_flow_config_for_prompt_entry(
        {
            "flow_config": {
                "version": 1,
                "entry_stage_id": "",
                "stages": [],
                "allow_empty": True,
            }
        },
        followup_schedule_hours=[4, 8],
        flow_required=False,
    )
    assert deleted["stages"] == []
    recreated = responder._resolve_flow_config_for_prompt_entry(
        {
            "flow_config": {
                "version": 1,
                "entry_stage_id": "a",
                "stages": [
                    _stage("a", "A", positive="b", negative="a", doubt="a", neutral="a"),
                    _stage("b", "B", positive="c", negative="b", doubt="b", neutral="b"),
                    _stage("c", "C", positive="c", negative="c", doubt="c", neutral="c"),
                ],
            }
        },
        followup_schedule_hours=[4, 8],
    )
    assert len(recreated["stages"]) == 3
    assert recreated["entry_stage_id"] == "a"


def test_migration_reconstructs_flow_state_from_legacy_chat() -> None:
    flow_config = responder._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "s1",
            "stages": [
                _stage("s1", "PEACH_A", positive="s2", negative="s1", doubt="s1", neutral="s1"),
                _stage("s2", "AGENDA_A", positive="s3", negative="s2", doubt="s2", neutral="s2"),
                _stage("s3", "LINK_A", positive="s3", negative="s3", doubt="s3", neutral="s3"),
            ],
        }
    )
    conv_state = {
        "messages_sent": [
            {"text": "hola", "first_sent_at": time.time() - 7200, "is_followup": False},
            {"text": "seguimos?", "first_sent_at": time.time() - 1800, "is_followup": True},
        ],
        "followup_stage": 1,
        "last_message_sent_at": time.time() - 1800,
        "created_at": time.time() - 8000,
        "updated_at": time.time() - 1200,
    }
    reconstructed = responder._reconstruct_flow_state_for_thread(conv_state, flow_config)
    assert reconstructed["stage_id"] == "s1"
    assert reconstructed["reconstruction_status"] == "legacy_migrated"
    assert reconstructed["followup_level"] == 0
    assert reconstructed["last_outbound_ts"] is not None


def test_flow_normalizes_legacy_initial_alias_to_canonical_inicial() -> None:
    config = responder._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "initial",
            "stages": [
                _stage("initial", "PACK_1", positive="stage_2", negative="initial", doubt="initial", neutral="initial"),
                _stage("stage_2", "PACK_2", positive="stage_2", negative="stage_2", doubt="stage_2", neutral="stage_2"),
            ],
        }
    )

    assert config["entry_stage_id"] == "inicial"
    assert config["stages"][0]["id"] == "inicial"
    assert config["stages"][0]["transitions"]["negative"] == "inicial"

    engine = responder.FlowEngine(config)
    updated = engine.apply_outbound(
        {"stage_id": "initial", "followup_level": 0, "objection_step": 0},
        {
            "decision": "reply",
            "next_stage_id": "inicial",
            "objection_step_after": 0,
        },
        sent_at=time.time(),
    )

    assert engine.entry_stage_id == "inicial"
    assert engine.has_initial_stage is True
    assert updated["stage_id"] == "inicial"


def test_followup_preconversation_requires_real_initial_stage() -> None:
    config = responder._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "etapa_1",
            "stages": [
                _stage(
                    "etapa_1",
                    "PACK_1",
                    positive="etapa_2",
                    negative="etapa_1",
                    doubt="etapa_1",
                    neutral="etapa_1",
                    followups=[{"delay_hours": 0, "action_type": "FU_1"}],
                ),
                _stage("etapa_2", "PACK_2", positive="etapa_2", negative="etapa_2", doubt="etapa_2", neutral="etapa_2"),
            ],
        }
    )
    engine = responder.FlowEngine(config)

    followup = engine.compute_followup_due(
        {
            "flow_state": {
                "stage_id": "initial",
                "followup_level": 0,
                "followup_anchor_ts": time.time() - 30,
                "last_outbound_ts": time.time() - 30,
                "objection_step": 0,
            },
            "last_outbound_ts": time.time() - 30,
            "followup_level": 0,
            "has_inbound_history": False,
            "preconversation_initial_placeholder": True,
            "now_ts": time.time(),
        }
    )

    assert engine.has_initial_stage is False
    assert followup["due"] is False
    assert followup["reason"] == "preconversation_without_initial_stage"


def test_flow_config_normalization_resolves_pack_ids_to_pack_types(monkeypatch) -> None:
    monkeypatch.setattr(
        responder,
        "_list_packs",
        lambda: [
            {
                "id": "pack-uuid-1",
                "name": "Pack A",
                "type": "PACK_A",
                "active": True,
                "actions": [{"type": "text_fixed", "content": "hola"}],
            },
            {
                "id": "pack-uuid-2",
                "name": "Pack FU",
                "type": "PACK_FU",
                "active": True,
                "actions": [{"type": "text_fixed", "content": "seguimiento"}],
            },
        ],
    )
    config = responder._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "s1",
            "stages": [
                _stage(
                    "s1",
                    "pack-uuid-1",
                    positive="s1",
                    negative="s1",
                    doubt="s1",
                    neutral="s1",
                    followups=[{"delay_hours": 4, "action_type": "pack-uuid-2"}],
                    objection={
                        "enabled": True,
                        "action_type": "pack-uuid-1",
                        "max_steps": 2,
                        "resolved_transition": "positive",
                        "unresolved_transition": "negative",
                    },
                )
            ],
        }
    )

    stage = config["stages"][0]
    assert stage["action_type"] == "PACK_A"
    assert stage["followups"][0]["action_type"] == "PACK_FU"
    assert stage["post_objection"]["action_type"] == "PACK_A"

    ok, reason = responder._validate_flow_pack_bindings(config, account_id="acct-1")
    assert ok, reason


def test_select_pack_accepts_pack_id_strategy_name(monkeypatch) -> None:
    monkeypatch.setattr(
        responder,
        "_list_packs",
        lambda: [
            {
                "id": "pack-uuid-1",
                "name": "Pack A 1",
                "type": "PACK_A",
                "active": True,
                "actions": [{"type": "text_fixed", "content": "hola"}],
            },
            {
                "id": "pack-uuid-2",
                "name": "Pack A 2",
                "type": "PACK_A",
                "active": True,
                "actions": [{"type": "text_fixed", "content": "hola 2"}],
            },
        ],
    )
    memory_state = {"last_pack_used": {}}
    monkeypatch.setattr(responder, "_get_account_memory", lambda _account_id: dict(memory_state))
    monkeypatch.setattr(responder, "_set_account_memory", lambda _account_id, payload: memory_state.update(payload))
    monkeypatch.setattr(responder.random, "choice", lambda items: items[0])

    selected = responder.select_pack("pack-uuid-1", "acct-1")

    assert selected is not None
    assert selected["type"] == "PACK_A"
    assert memory_state["last_pack_used"]["PACK_A"] == "pack-uuid-1"
