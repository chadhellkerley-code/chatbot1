from __future__ import annotations

from pathlib import Path

<<<<<<< HEAD
import pytest

=======
>>>>>>> origin/main
from core import responder as responder_module
from core.inbox.conversation_sender import ConversationSender
from core.inbox.conversation_store import ConversationStore
from src.runtime.automation_engine_adapter import AutomationEngineAdapter
from src.runtime.inbox_automation_runtime import InboxAutomationRuntime


def _flow_config() -> dict[str, object]:
    return responder_module._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "initial",
            "stages": [
                {
                    "id": "initial",
                    "action_type": "PACK_1",
                    "transitions": {
                        "positive": "stage_2",
                        "negative": "initial",
                        "doubt": "initial",
                        "neutral": "initial",
                    },
                    "followups": [],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
                {
                    "id": "stage_2",
                    "action_type": "PACK_2",
                    "transitions": {
                        "positive": "stage_2",
                        "negative": "stage_2",
                        "doubt": "stage_2",
                        "neutral": "stage_2",
                    },
                    "followups": [],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
            ],
        }
    )


def _flow_config_three_stages() -> dict[str, object]:
    return responder_module._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "initial",
            "stages": [
                {
                    "id": "initial",
                    "action_type": "PACK_1",
                    "transitions": {
                        "positive": "stage_2",
                        "negative": "initial",
                        "doubt": "initial",
                        "neutral": "initial",
                    },
                    "followups": [],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
                {
                    "id": "stage_2",
                    "action_type": "PACK_2",
                    "transitions": {
                        "positive": "stage_3",
                        "negative": "stage_2",
                        "doubt": "stage_2",
                        "neutral": "stage_2",
                    },
                    "followups": [],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
                {
                    "id": "stage_3",
                    "action_type": "PACK_3",
                    "transitions": {
                        "positive": "stage_3",
                        "negative": "stage_3",
                        "doubt": "stage_3",
                        "neutral": "stage_3",
                    },
                    "followups": [],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
            ],
        }
    )


def _flow_config_with_followups() -> dict[str, object]:
    return responder_module._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "initial",
            "stages": [
                {
                    "id": "initial",
                    "action_type": "PACK_1",
                    "transitions": {
                        "positive": "stage_2",
                        "negative": "initial",
                        "doubt": "initial",
                        "neutral": "initial",
                    },
                    "followups": [{"delay_hours": 0, "action_type": "PACK_FU_1"}],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
                {
                    "id": "stage_2",
                    "action_type": "PACK_2",
                    "transitions": {
                        "positive": "stage_3",
                        "negative": "stage_2",
                        "doubt": "stage_2",
                        "neutral": "stage_2",
                    },
                    "followups": [{"delay_hours": 0, "action_type": "PACK_FU_2"}],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
                {
                    "id": "stage_3",
                    "action_type": "PACK_3",
                    "transitions": {
                        "positive": "stage_3",
                        "negative": "stage_3",
                        "doubt": "stage_3",
                        "neutral": "stage_3",
                    },
                    "followups": [],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
            ],
        }
    )


def _flow_config_without_initial() -> dict[str, object]:
    return responder_module._normalize_flow_config(
        {
            "version": 1,
            "entry_stage_id": "etapa_1",
            "stages": [
                {
                    "id": "etapa_1",
                    "action_type": "PACK_1",
                    "transitions": {
                        "positive": "etapa_2",
                        "negative": "etapa_1",
                        "doubt": "etapa_1",
                        "neutral": "etapa_1",
                    },
                    "followups": [{"delay_hours": 0, "action_type": "PACK_FU_1"}],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
                {
                    "id": "etapa_2",
                    "action_type": "PACK_2",
                    "transitions": {
                        "positive": "etapa_2",
                        "negative": "etapa_2",
                        "doubt": "etapa_2",
                        "neutral": "etapa_2",
                    },
                    "followups": [],
                    "post_objection": {"enabled": False, "action_type": "", "max_steps": 1},
                },
            ],
        }
    )


def _patch_runtime_flow(monkeypatch, flow_config: dict[str, object]) -> None:
    prompt_entry = {"flow_config": flow_config, "objection_strategy_name": ""}
<<<<<<< HEAD
    required_types: list[str] = []
    for stage in flow_config.get("stages") or []:
        if not isinstance(stage, dict):
            continue
        stage_action = str(stage.get("action_type") or "").strip()
        if stage_action and not responder_module._canonical_flow_special_action_type(stage_action):
            required_types.append(stage_action)
        for followup in stage.get("followups") or []:
            if not isinstance(followup, dict):
                continue
            followup_action = str(followup.get("action_type") or "").strip()
            if followup_action and not responder_module._canonical_flow_special_action_type(followup_action):
                required_types.append(followup_action)
        objection = stage.get("post_objection") or {}
        if isinstance(objection, dict) and bool(objection.get("enabled")):
            objection_action = str(objection.get("action_type") or "").strip()
            if objection_action and not responder_module._canonical_flow_special_action_type(objection_action):
                required_types.append(objection_action)
=======
>>>>>>> origin/main
    monkeypatch.setattr(
        responder_module,
        "_get_prompt_entry",
        lambda _alias_or_account: dict(prompt_entry),
    )
    monkeypatch.setattr(
        responder_module,
        "_resolve_prompt_entry_for_user",
        lambda _account_id, active_alias=None, fallback_entry=None: dict(fallback_entry or prompt_entry),
    )
    monkeypatch.setattr(
        responder_module,
        "_resolve_flow_config_for_prompt_entry",
        lambda _prompt_entry: dict(flow_config),
    )
    monkeypatch.setattr(
        responder_module,
        "select_pack",
        lambda action_type, _account_id: {"id": "pack-2", "name": "Pack Dos", "type": action_type},
    )
    monkeypatch.setattr(
        responder_module,
        "_list_packs",
<<<<<<< HEAD
        lambda: [
            {
                "id": f"pack-{index + 1}",
                "name": str(pack_type),
                "type": str(pack_type),
                "actions": [{"type": "text_fixed", "content": f"Mensaje {index + 1}"}],
            }
            for index, pack_type in enumerate(required_types)
        ],
=======
        lambda: [{"id": "pack-2", "name": "Pack Dos", "type": "PACK_2", "actions": [{"type": "text_fixed", "content": "Hola"}]}],
>>>>>>> origin/main
    )
    monkeypatch.setattr(
        responder_module,
        "_flow_config_for_account",
        lambda _account_id: dict(flow_config),
    )


def _thread_payload(**overrides) -> dict[str, object]:
    base = {
        "thread_key": "acc1:thread-a",
        "thread_id": "thread-a",
        "account_id": "acc1",
        "alias_id": "ventas",
        "account_alias": "ventas",
        "recipient_username": "cliente_a",
        "display_name": "Cliente A",
        "owner": "auto",
        "bucket": "all",
        "status": "open",
        "stage_id": "initial",
        "followup_level": 0,
        "last_message_text": "me interesa",
        "last_message_timestamp": 120.0,
        "last_message_direction": "inbound",
        "last_message_id": "in-1",
        "last_inbound_at": 120.0,
        "unread_count": 1,
        "messages": [],
        "flow_state": {
            "stage_id": "initial",
            "followup_level": 0,
            "followup_anchor_ts": 60.0,
            "last_outbound_ts": 60.0,
            "objection_step": 0,
        },
    }
    base.update(overrides)
    return base


class _FakeDeliveryBrowserPool:
    def __init__(self, *, send_pack_result: dict[str, object] | None = None) -> None:
        self._send_pack_result = dict(send_pack_result or {"ok": True, "item_id": "pack-msg-1", "reason": "thread_read_confirmed"})
        self.send_pack_calls = 0

    def shutdown(self) -> None:
        return None

    def send_text(self, _thread, _text):
        raise AssertionError("send_text should not be used in these pack tests")

<<<<<<< HEAD
    def send_pack(self, _thread, _pack, *, conversation_text="", flow_config=None, job_type="auto_reply"):
        del conversation_text, flow_config, job_type
=======
    def send_pack(self, _thread, _pack, *, conversation_text="", flow_config=None):
        del conversation_text, flow_config
>>>>>>> origin/main
        self.send_pack_calls += 1
        return dict(self._send_pack_result)


def _queued_job_payload(job: dict[str, object]) -> dict[str, object]:
    payload = dict(job.get("payload") or {})
    return {
        "job_id": int(job.get("id") or 0),
        "thread_key": str(job.get("thread_key") or payload.get("thread_key") or "").strip(),
        "job_type": str(job.get("job_type") or job.get("task_type") or "").strip(),
        **payload,
    }


def test_pack_delay_uses_exact_configured_window_without_safezone_multiplier(monkeypatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr(responder_module, "_random_delay_seconds", lambda _min_value, _max_value: 12.0)
    monkeypatch.setattr(responder_module, "_safezone_delay_multiplier", lambda _account_id: 4.0)
    monkeypatch.setattr(responder_module, "sleep_with_stop", lambda delay: slept.append(float(delay)))

    responder_module._sleep_between_replies_for_account(
        "acc1",
        10,
        20,
        label="pack_action_delay",
        apply_safezone_multiplier=False,
    )

    assert slept == [12.0]


def test_pending_pack_run_normalizes_pack_delay_keys_without_runtime_ambiguity() -> None:
    pending = responder_module._normalize_pending_pack_run(
        {
            "pack_id": "pack-1",
            "pack_name": "Pack Uno",
            "strategy_name": "PACK_1",
            "delay_min": 3,
            "delay_max": 8,
            "actions": [{"type": "text_fixed", "content": "Hola"}],
        }
    )

    assert pending is not None
    assert pending["pack_delay_min"] == 3
    assert pending["pack_delay_max"] == 8
    assert "delay_min" not in pending
    assert "delay_max" not in pending


def test_adapter_marks_reply_pending_and_defers_stage_transition_for_pack(monkeypatch) -> None:
    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(),
        mode="auto",
    )

<<<<<<< HEAD
    assert "pending_reply" not in evaluation["state_updates"]
    assert "pending_inbound_id" not in evaluation["state_updates"]
=======
    assert evaluation["state_updates"]["pending_reply"] is True
    assert evaluation["state_updates"]["pending_inbound_id"] == "in-1"
>>>>>>> origin/main
    assert "stage_id" not in evaluation["thread_updates"]
    assert not any(action["type"] == "move_stage" for action in evaluation["actions"])

    pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")
<<<<<<< HEAD
    assert evaluation["decision"]["stage_id"] == "inicial"
    assert evaluation["decision"]["next_stage_id"] == "stage_2"
    assert evaluation["decision"]["action_type"] == "PACK_1"
    assert pack_action["latest_inbound_id"] == "in-1"
    assert pack_action["pack_id"] == "pack-2"
    assert pack_action["enqueue_state_updates"]["pending_reply"] is True
    assert pack_action["enqueue_state_updates"]["pending_inbound_id"] == "in-1"
    assert pack_action["enqueue_state_updates"]["pack_quota_deferral"] is None
=======
    assert pack_action["latest_inbound_id"] == "in-1"
>>>>>>> origin/main
    assert pack_action["post_send_thread_updates"]["stage_id"] == "stage_2"
    assert pack_action["post_send_thread_updates"]["followup_level"] == 0
    assert pack_action["post_send_state_updates"]["pending_reply"] is False
    assert pack_action["post_send_state_updates"]["pending_inbound_id"] is None
    assert pack_action["post_send_state_updates"]["last_inbound_id_seen"] == "in-1"
<<<<<<< HEAD
    assert pack_action["post_send_state_updates"]["pack_quota_deferral"] is None


def test_adapter_raises_explicitly_when_pack_binding_is_missing(monkeypatch) -> None:
    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    monkeypatch.setattr(responder_module, "select_pack", lambda _action_type, _account_id: None)
    adapter = AutomationEngineAdapter()

    with pytest.raises(ValueError, match="Sin pack valido para action_type: PACK_1"):
        adapter.evaluate_thread(
            account={"username": "acc1", "alias": "ventas"},
            thread=_thread_payload(),
            mode="auto",
        )
=======
>>>>>>> origin/main


def test_adapter_reuses_pending_inbound_marker_when_message_id_is_missing(monkeypatch) -> None:
    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            last_message_id="",
            pending_reply=True,
            pending_inbound_id="pending-in-42",
        ),
        mode="auto",
    )

    pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")
    assert pack_action["latest_inbound_id"] == "pending-in-42"
<<<<<<< HEAD
    assert "pending_inbound_id" not in evaluation["state_updates"]
    assert pack_action["enqueue_state_updates"]["pending_inbound_id"] == "pending-in-42"


def test_adapter_suppresses_pack_when_matching_quota_deferral_is_active(monkeypatch) -> None:
    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()
    monkeypatch.setattr("src.runtime.automation_engine_adapter.time.time", lambda: 1000.0)

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            pack_quota_deferral={
                "pack_id": "pack-2",
                "job_type": "auto_reply",
                "inbound_id": "in-1",
                "retry_after_ts": 1600.0,
            }
        ),
        mode="auto",
    )

    assert evaluation["actions"] == []
    assert evaluation["decision"]["decision"] == "wait"
    assert evaluation["decision"]["reason"] == "pack_quota_deferred"
    assert evaluation["decision"]["pack_quota_deferral"]["pack_id"] == "pack-2"


def test_adapter_retries_pack_after_quota_deferral_expires(monkeypatch) -> None:
    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()
    monkeypatch.setattr("src.runtime.automation_engine_adapter.time.time", lambda: 2000.0)

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            pack_quota_deferral={
                "pack_id": "pack-2",
                "job_type": "auto_reply",
                "inbound_id": "in-1",
                "retry_after_ts": 1500.0,
            }
        ),
        mode="auto",
    )

    pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")
    assert pack_action["job_type"] == "auto_reply"
    assert evaluation["state_updates"]["pack_quota_deferral"] is None
=======
    assert evaluation["state_updates"]["pending_inbound_id"] == "pending-in-42"
>>>>>>> origin/main


def test_adapter_blocks_followup_without_confirmed_stage_activation(monkeypatch) -> None:
    flow_config = _flow_config_with_followups()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            last_message_text="Pack Uno",
            last_message_timestamp=1.0,
            last_message_direction="outbound",
            last_message_id="local-out-1",
            last_outbound_at=1.0,
            last_inbound_at=None,
            unread_count=0,
            needs_reply=False,
            messages=[
                {
                    "message_id": "local-out-1",
                    "text": "Pack Uno",
                    "timestamp": 1.0,
                    "direction": "outbound",
                    "stage_id": "initial",
                    "delivery_status": "pending",
                    "sent_status": "queued",
                }
            ],
            flow_state={
                "stage_id": "initial",
                "followup_level": 0,
                "objection_step": 0,
            },
        ),
        mode="followup",
    )

    assert evaluation["actions"] == []
    assert evaluation["decision"]["confirmed_stage_activation"]["confirmed"] is False
    assert evaluation["decision"]["confirmed_stage_activation"]["reason"] == "missing_confirmed_outbound"


def test_adapter_emits_due_followup_for_unanswered_initial_thread(monkeypatch) -> None:
    flow_config = _flow_config_with_followups()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            last_message_text="Pack Uno",
            last_message_timestamp=1.0,
            last_message_direction="outbound",
            last_message_id="out-1",
            last_outbound_at=1.0,
            last_inbound_at=None,
            unread_count=0,
            needs_reply=False,
            messages=[
                {
                    "message_id": "out-1",
                    "text": "Pack Uno",
                    "timestamp": 1.0,
                    "direction": "outbound",
                }
            ],
            flow_state={
                "stage_id": "initial",
                "followup_level": 0,
                "followup_anchor_ts": 1.0,
                "last_outbound_ts": 1.0,
                "objection_step": 0,
            },
        ),
        mode="followup",
    )

    assert evaluation["decision"]["decision"] == "followup"
    pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")
    assert pack_action["job_type"] == "followup"
    assert pack_action["post_send_thread_updates"]["stage_id"] == "inicial"
    assert pack_action["post_send_thread_updates"]["followup_level"] == 1


def test_adapter_allows_first_real_inbound_when_thread_is_legacy_initial_but_flow_starts_at_stage_1(monkeypatch) -> None:
    flow_config = _flow_config_without_initial()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            stage_id="initial",
            last_message_text="si, me interesa",
            last_message_timestamp=240.0,
            last_message_direction="inbound",
            last_message_id="in-1",
            last_inbound_at=240.0,
            last_outbound_at=180.0,
            unread_count=1,
            needs_reply=True,
            messages=[
                {
                    "message_id": "out-legacy-1",
                    "text": "Hola inicial",
                    "timestamp": 180.0,
                    "direction": "outbound",
                    "stage_id": "initial",
                    "delivery_status": "sent",
                    "sent_status": "confirmed",
                    "confirmed_at": 180.0,
                },
                {
                    "message_id": "in-1",
                    "text": "si, me interesa",
                    "timestamp": 240.0,
                    "direction": "inbound",
                },
            ],
            flow_state={
                "stage_id": "initial",
                "followup_level": 0,
                "followup_anchor_ts": 180.0,
                "last_outbound_ts": 180.0,
                "objection_step": 0,
            },
        ),
        mode="auto",
    )

    pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")

    assert evaluation["decision"]["decision"] == "reply"
    assert evaluation["decision"]["reason"] == "inbound_relevant"
    assert evaluation["decision"]["confirmed_stage_activation"]["confirmed"] is False
    assert evaluation["decision"]["stage_id"] == "etapa_1"
<<<<<<< HEAD
    assert evaluation["decision"]["next_stage_id"] == "etapa_2"
    assert evaluation["decision"]["action_type"] == "PACK_1"
=======
>>>>>>> origin/main
    assert pack_action["post_send_thread_updates"]["stage_id"] == "etapa_2"


def test_adapter_blocks_preview_inbound_without_confirmed_stage_activation(monkeypatch) -> None:
    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            last_message_text="si, me interesa",
            last_message_timestamp=240.0,
            last_message_direction="inbound",
            last_message_id="preview-in-1",
            last_inbound_at=240.0,
            unread_count=1,
            needs_reply=True,
            messages=[],
            flow_state={
                "stage_id": "initial",
                "followup_level": 0,
                "objection_step": 0,
            },
        ),
        mode="auto",
    )

    assert evaluation["actions"] == []
    assert evaluation["decision"]["decision"] == "skip"
    assert evaluation["decision"]["reason"] == "stage_without_followups"
    assert evaluation["decision"]["inbound_snapshot_evidence"] == "preview_hint"
    assert evaluation["decision"]["confirmed_stage_activation"]["confirmed"] is False
    assert evaluation["state_updates"]["flow_state"]["last_outbound_ts"] is None


def test_adapter_emits_due_followup_for_nonqualified_later_stage(monkeypatch) -> None:
    flow_config = _flow_config_with_followups()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            stage_id="stage_2",
            status="replied",
            last_message_text="Pack Dos",
            last_message_timestamp=1.0,
            last_message_direction="outbound",
            last_message_id="out-2",
            last_outbound_at=1.0,
            last_inbound_at=0.5,
            last_inbound_id_seen="in-1",
            unread_count=0,
            needs_reply=False,
            messages=[
                {
                    "message_id": "in-1",
                    "text": "me interesa",
                    "timestamp": 0.5,
                    "direction": "inbound",
                },
                {
                    "message_id": "out-2",
                    "text": "Pack Dos",
                    "timestamp": 1.0,
                    "direction": "outbound",
                },
            ],
            flow_state={
                "stage_id": "stage_2",
                "followup_level": 0,
                "followup_anchor_ts": 1.0,
                "last_outbound_ts": 1.0,
                "objection_step": 0,
            },
        ),
        mode="followup",
    )

    assert evaluation["decision"]["decision"] == "followup"
    pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")
    assert pack_action["job_type"] == "followup"
    assert pack_action["post_send_thread_updates"]["stage_id"] == "stage_2"
    assert pack_action["post_send_thread_updates"]["followup_level"] == 1


def test_adapter_allows_real_inbound_transition_when_stage_activation_is_confirmed(monkeypatch) -> None:
    flow_config = _flow_config_three_stages()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            stage_id="stage_2",
            status="replied",
            last_message_text="si, me interesa",
            last_message_timestamp=240.0,
            last_message_direction="inbound",
            last_message_id="in-2",
            last_inbound_at=240.0,
            last_outbound_at=180.0,
            unread_count=1,
            needs_reply=True,
            messages=[
                {
                    "message_id": "out-2",
                    "text": "Pack Dos",
                    "timestamp": 180.0,
                    "direction": "outbound",
                    "stage_id": "stage_2",
                    "delivery_status": "sent",
                    "sent_status": "confirmed",
                    "confirmed_at": 180.0,
                },
                {
                    "message_id": "in-2",
                    "text": "si, me interesa",
                    "timestamp": 240.0,
                    "direction": "inbound",
                },
            ],
            flow_state={
                "stage_id": "stage_2",
                "followup_level": 0,
                "followup_anchor_ts": 180.0,
                "last_outbound_ts": 180.0,
                "objection_step": 0,
            },
            last_inbound_id_seen="in-1",
        ),
        mode="auto",
    )

    pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")
    assert evaluation["decision"]["decision"] == "reply"
    assert evaluation["decision"]["confirmed_stage_activation"]["confirmed"] is True
    assert evaluation["decision"]["confirmed_stage_activation"]["stage_id"] == "stage_2"
<<<<<<< HEAD
    assert evaluation["decision"]["next_stage_id"] == "stage_3"
    assert evaluation["decision"]["action_type"] == "PACK_2"
=======
>>>>>>> origin/main
    assert pack_action["latest_inbound_id"] == "in-2"
    assert pack_action["post_send_thread_updates"]["stage_id"] == "stage_3"


<<<<<<< HEAD
def test_first_inbound_keeps_current_stage_pack_until_send_confirms_transition(monkeypatch) -> None:
    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(),
        mode="auto",
    )

    pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")

    assert evaluation["decision"]["stage_id"] == "inicial"
    assert evaluation["decision"]["next_stage_id"] == "stage_2"
    assert evaluation["decision"]["action_type"] == "PACK_1"
    assert pack_action["post_send_thread_updates"]["stage_id"] == "stage_2"


=======
>>>>>>> origin/main
def test_adapter_blocks_due_followup_for_qualified_thread_even_when_auto_owned(monkeypatch) -> None:
    flow_config = _flow_config_with_followups()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            bucket="qualified",
            status="replied",
            last_message_text="Pack Uno",
            last_message_timestamp=1.0,
            last_message_direction="outbound",
            last_message_id="out-1",
            last_outbound_at=1.0,
            last_inbound_at=None,
            unread_count=0,
            needs_reply=False,
            messages=[
                {
                    "message_id": "out-1",
                    "text": "Pack Uno",
                    "timestamp": 1.0,
                    "direction": "outbound",
                }
            ],
            flow_state={
                "stage_id": "initial",
                "followup_level": 0,
                "followup_anchor_ts": 1.0,
                "last_outbound_ts": 1.0,
                "objection_step": 0,
            },
        ),
        mode="followup",
    )

    assert evaluation["decision"]["decision"] == "followup"
    assert evaluation["actions"] == []


def test_adapter_keeps_healthy_advanced_followup_when_confirmed_outbound_lives_on_thread(monkeypatch) -> None:
    flow_config = _flow_config_with_followups()
    _patch_runtime_flow(monkeypatch, flow_config)
    adapter = AutomationEngineAdapter()

    evaluation = adapter.evaluate_thread(
        account={"username": "acc1", "alias": "ventas"},
        thread=_thread_payload(
            stage_id="stage_2",
            status="replied",
            last_message_text="Pack Dos",
            last_message_timestamp=180.0,
            last_message_direction="outbound",
            last_message_id="thread-read-confirmed-pack-2",
            last_outbound_at=180.0,
            last_inbound_at=120.0,
            unread_count=0,
            needs_reply=False,
            messages=[],
            flow_state=None,
        ),
        mode="followup",
    )

    pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")
    assert evaluation["decision"]["decision"] == "followup"
    assert evaluation["decision"]["confirmed_stage_activation"]["confirmed"] is True
    assert evaluation["decision"]["confirmed_stage_activation"]["source"] == "thread_last_outbound"
    assert pack_action["job_type"] == "followup"
    assert pack_action["post_send_thread_updates"]["stage_id"] == "stage_2"
    assert pack_action["post_send_thread_updates"]["followup_level"] == 1


def test_runtime_advances_stage_only_after_confirmed_pack_send(tmp_path: Path, monkeypatch) -> None:
    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    store = ConversationStore(tmp_path)
    try:
        store.upsert_threads([_thread_payload()])
        store.update_thread_state(
            "acc1:thread-a",
            {
                "flow_state": dict(_thread_payload().get("flow_state") or {}),
                "pending_reply": False,
                "pending_inbound_id": None,
            },
        )
        store.upsert_runtime_alias_state("ventas", {"is_running": True})
        browser_pool = _FakeDeliveryBrowserPool(send_pack_result={"ok": True, "item_id": "pack-msg-1", "reason": "thread_read_confirmed"})
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)

        thread_key = "acc1:thread-a"
        evaluation = runtime._engine.evaluate_thread(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread(thread_key) or {},
            mode="auto",
        )
        if evaluation.get("thread_updates"):
            store.update_thread_record(thread_key, dict(evaluation.get("thread_updates") or {}))
        if evaluation.get("state_updates"):
            store.update_thread_state(thread_key, dict(evaluation.get("state_updates") or {}))
        runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread(thread_key) or {},
            actions=list(evaluation.get("actions") or []),
        )

        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]
        queued_thread = store.get_thread(thread_key) or {}
        assert queued_thread["stage_id"] == "initial"
        assert queued_thread["pending_reply"] is True
        assert queued_thread["pending_inbound_id"] == "in-1"

        sender._handle_send_pack(_queued_job_payload(queued_job))
        sent_thread = store.get_thread(thread_key) or {}

        assert browser_pool.send_pack_calls == 1
        assert sent_thread["stage_id"] == "stage_2"
        assert sent_thread["pending_reply"] is False
        assert sent_thread.get("pending_inbound_id") in {None, ""}
        assert sent_thread["last_inbound_id_seen"] == "in-1"
        assert sent_thread["needs_reply"] is False
        assert sent_thread["unread_count"] == 0
        assert sent_thread["last_message_direction"] == "outbound"
        assert str(sent_thread.get("last_message_id") or "").strip()
    finally:
        store.shutdown()


def test_adapter_prefers_newer_preview_inbound_when_message_cache_is_stale(tmp_path: Path, monkeypatch) -> None:
    flow_config = _flow_config_three_stages()
    _patch_runtime_flow(monkeypatch, flow_config)
    store = ConversationStore(tmp_path)
    try:
        thread_key = "acc1:thread-a"
        store.upsert_threads([_thread_payload()])
        store.seed_messages(
            thread_key,
            [
                {
                    "message_id": "in-1",
                    "text": "me interesa",
                    "timestamp": 120.0,
                    "direction": "inbound",
                }
            ],
        )
        store.update_thread_record(
            thread_key,
            {
                "stage_id": "stage_2",
                "followup_level": 0,
                "last_message_text": "Pack Dos",
                "last_message_timestamp": 180.0,
                "last_message_direction": "outbound",
                "last_message_id": "thread-read-confirmed-pack-2",
                "last_outbound_at": 180.0,
                "unread_count": 0,
                "needs_reply": False,
            },
        )
        store.update_thread_state(
            thread_key,
            {
                "flow_state": {"stage_id": "stage_2", "followup_level": 0, "objection_step": 0, "last_outbound_ts": 180.0},
                "pending_reply": False,
                "pending_inbound_id": None,
                "last_inbound_id_seen": "in-1",
            },
        )
        store.upsert_threads(
            [
                    _thread_payload(
                        stage_id="stage_2",
                        last_message_text="si, me interesa",
                        last_message_timestamp=240.0,
                        last_message_direction="inbound",
                        last_message_id="in-2",
                        last_inbound_at=240.0,
                    unread_count=1,
                    needs_reply=True,
                )
            ]
        )

        adapter = AutomationEngineAdapter()
        evaluation = adapter.evaluate_thread(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread(thread_key) or {},
            mode="auto",
        )

        pack_action = next(action for action in evaluation["actions"] if action["type"] == "send_pack")
<<<<<<< HEAD
        assert "pending_reply" not in evaluation["state_updates"]
        assert "pending_inbound_id" not in evaluation["state_updates"]
        assert pack_action["latest_inbound_id"] == "in-2"
        assert pack_action["enqueue_state_updates"]["pending_reply"] is True
        assert pack_action["enqueue_state_updates"]["pending_inbound_id"] == "in-2"
=======
        assert evaluation["state_updates"]["pending_reply"] is True
        assert evaluation["state_updates"]["pending_inbound_id"] == "in-2"
        assert pack_action["latest_inbound_id"] == "in-2"
>>>>>>> origin/main
        assert pack_action["post_send_thread_updates"]["stage_id"] == "stage_3"
    finally:
        store.shutdown()


def test_runtime_clears_pending_reply_when_pack_send_fails_but_keeps_retry_eligibility(tmp_path: Path, monkeypatch) -> None:
    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    store = ConversationStore(tmp_path)
    try:
        store.upsert_threads([_thread_payload()])
        store.update_thread_state(
            "acc1:thread-a",
            {
                "flow_state": dict(_thread_payload().get("flow_state") or {}),
                "pending_reply": False,
                "pending_inbound_id": None,
            },
        )
        store.upsert_runtime_alias_state("ventas", {"is_running": True})
        browser_pool = _FakeDeliveryBrowserPool(send_pack_result={"ok": False, "reason": "not_confirmed"})
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)

        thread_key = "acc1:thread-a"
        evaluation = runtime._engine.evaluate_thread(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread(thread_key) or {},
            mode="auto",
        )
        if evaluation.get("thread_updates"):
            store.update_thread_record(thread_key, dict(evaluation.get("thread_updates") or {}))
        if evaluation.get("state_updates"):
            store.update_thread_state(thread_key, dict(evaluation.get("state_updates") or {}))
        runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread(thread_key) or {},
            actions=list(evaluation.get("actions") or []),
        )

        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]
        sender._handle_send_pack(_queued_job_payload(queued_job))
        failed_thread = store.get_thread(thread_key) or {}
        retry_evaluation = runtime._engine.evaluate_thread(
            account={"username": "acc1", "alias": "ventas"},
            thread=failed_thread,
            mode="auto",
        )

        assert browser_pool.send_pack_calls == 1
        assert failed_thread["stage_id"] == "initial"
        assert failed_thread["pending_reply"] is False
        assert failed_thread.get("pending_inbound_id") in {None, ""}
        assert failed_thread.get("last_inbound_id_seen") in {None, ""}
        retry_action = next(action for action in retry_evaluation["actions"] if action["type"] == "send_pack")
        assert retry_action["job_type"] == "auto_reply"
        assert retry_action["latest_inbound_id"] == "in-1"
    finally:
        store.shutdown()


def test_runtime_clears_pending_reply_when_pack_send_raises(tmp_path: Path, monkeypatch) -> None:
    class _RaisingDeliveryBrowserPool:
        def __init__(self) -> None:
            self.send_pack_calls = 0

        def shutdown(self) -> None:
            return None

        def send_text(self, _thread, _text):
            raise AssertionError("send_text should not be used in these pack tests")

<<<<<<< HEAD
        def send_pack(self, _thread, _pack, *, conversation_text="", flow_config=None, job_type="auto_reply"):
            del conversation_text, flow_config, job_type
=======
        def send_pack(self, _thread, _pack, *, conversation_text="", flow_config=None):
            del conversation_text, flow_config
>>>>>>> origin/main
            self.send_pack_calls += 1
            raise RuntimeError("transport_crashed")

    flow_config = _flow_config()
    _patch_runtime_flow(monkeypatch, flow_config)
    store = ConversationStore(tmp_path)
    try:
        store.upsert_threads([_thread_payload()])
        store.update_thread_state(
            "acc1:thread-a",
            {
                "flow_state": dict(_thread_payload().get("flow_state") or {}),
                "pending_reply": False,
                "pending_inbound_id": None,
            },
        )
        store.upsert_runtime_alias_state("ventas", {"is_running": True})
        browser_pool = _RaisingDeliveryBrowserPool()
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)

        thread_key = "acc1:thread-a"
        evaluation = runtime._engine.evaluate_thread(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread(thread_key) or {},
            mode="auto",
        )
        if evaluation.get("thread_updates"):
            store.update_thread_record(thread_key, dict(evaluation.get("thread_updates") or {}))
        if evaluation.get("state_updates"):
            store.update_thread_state(thread_key, dict(evaluation.get("state_updates") or {}))
        runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread(thread_key) or {},
            actions=list(evaluation.get("actions") or []),
        )

        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]
        sender._handle_send_pack(_queued_job_payload(queued_job))
        failed_thread = store.get_thread(thread_key) or {}
        failed_job = store.get_send_queue_job(int(queued_job.get("id") or 0)) or {}

        assert browser_pool.send_pack_calls == 1
        assert failed_thread["pending_reply"] is False
        assert failed_thread.get("pending_inbound_id") in {None, ""}
        assert failed_job["state"] == "failed"
        assert failed_job["failure_reason"] == "transport_crashed"
    finally:
        store.shutdown()
