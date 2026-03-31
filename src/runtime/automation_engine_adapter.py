from __future__ import annotations

import hashlib
import time
from typing import Any

from core import responder as responder_module
from src.inbox.message_sender import build_conversation_text
from src.runtime.ownership_router import OwnershipRouter


class AutomationEngineAdapter:
    _INBOUND_PREVIEW_EPSILON_SECONDS = 0.000001
<<<<<<< HEAD
    PACK_QUOTA_DEFERRAL_STATE_KEY = "pack_quota_deferral"
=======
>>>>>>> origin/main

    def __init__(self) -> None:
        self._router = OwnershipRouter()

    def evaluate_thread(
        self,
        *,
        account: dict[str, Any],
        thread: dict[str, Any],
        mode: str,
    ) -> dict[str, Any]:
        account_id = str(account.get("username") or "").strip().lstrip("@")
        alias_id = str(account.get("alias") or thread.get("alias_id") or thread.get("account_alias") or "").strip()
        messages = [dict(row) for row in thread.get("messages") or [] if isinstance(row, dict)]
        prompt_entry = responder_module._resolve_prompt_entry_for_user(
            account_id,
            active_alias=alias_id or None,
            fallback_entry=responder_module._get_prompt_entry(alias_id or account_id),
        )
        flow_config = responder_module._resolve_flow_config_for_prompt_entry(prompt_entry)
        flow_engine = responder_module.FlowEngine(flow_config)
        raw_flow_state = dict(thread.get("flow_state") or {}) if isinstance(thread.get("flow_state"), dict) else {}
        entry_stage_id = str(flow_engine.entry_stage_id or responder_module._STAGE_INITIAL).strip() or responder_module._STAGE_INITIAL
        raw_stage_hint = str(raw_flow_state.get("stage_id") or thread.get("stage_id") or entry_stage_id).strip() or entry_stage_id
        stage_hint = responder_module._canonical_flow_stage_id(raw_stage_hint) or entry_stage_id
        stage_activation_hint = self._resolve_confirmed_stage_activation(
            thread=thread,
            messages=messages,
            raw_flow_state=raw_flow_state,
            stage_id=stage_hint,
        )
        fallback_stage_id = responder_module._canonical_flow_stage_id(raw_flow_state.get("stage_id"))
        if not fallback_stage_id and bool(stage_activation_hint.get("confirmed")):
            fallback_stage_id = stage_hint
        if not fallback_stage_id:
            fallback_stage_id = entry_stage_id
        flow_state = responder_module._normalize_flow_state(
            raw_flow_state,
            fallback_stage_id=fallback_stage_id,
            last_outbound_ts=self._ts(stage_activation_hint.get("anchor_ts")),
            followup_level_hint=self._int(thread.get("followup_level")) if bool(stage_activation_hint.get("confirmed")) else 0,
        )
        current_stage_id = str(flow_state.get("stage_id") or entry_stage_id).strip() or entry_stage_id
        if current_stage_id not in flow_engine.stage_map and entry_stage_id:
            current_stage_id = entry_stage_id
            flow_state["stage_id"] = current_stage_id
        confirmed_stage_activation = self._resolve_confirmed_stage_activation(
            thread=thread,
            messages=messages,
            raw_flow_state=raw_flow_state,
            stage_id=current_stage_id,
        )
        confirmed_last_outbound_ts = self._ts(confirmed_stage_activation.get("anchor_ts"))
        if confirmed_last_outbound_ts is not None:
            if self._ts(flow_state.get("last_outbound_ts")) is None:
                flow_state["last_outbound_ts"] = confirmed_last_outbound_ts
            if self._ts(flow_state.get("followup_anchor_ts")) is None:
                flow_state["followup_anchor_ts"] = confirmed_last_outbound_ts
        else:
            flow_state["last_outbound_ts"] = None
            flow_state["followup_anchor_ts"] = None
            flow_state["followup_level"] = 0
        latest_inbound = self._latest_message(messages, direction="inbound")
        inbound_snapshot = self._resolve_inbound_snapshot(
            thread=thread,
            latest_inbound=latest_inbound,
            confirmed_stage_activation=confirmed_stage_activation,
        )
        inbound_text = str(inbound_snapshot.get("text") or "").strip()
        latest_inbound_id = str(inbound_snapshot.get("message_id") or "").strip()
        last_inbound_seen = str(thread.get("last_inbound_id_seen") or "").strip()
        pending_reply = bool(thread.get("pending_reply"))
        pending_inbound_id = str(thread.get("pending_inbound_id") or "").strip()
        current_followup_level = self._int(flow_state.get("followup_level") or thread.get("followup_level"))
        preconversation_initial_placeholder = bool(
            responder_module._is_initial_flow_stage_id(raw_stage_hint)
            and not bool(flow_engine.has_initial_stage)
        )

<<<<<<< HEAD
        now_ts = time.time()

=======
>>>>>>> origin/main
        decision = flow_engine.evaluate(
            {
                "flow_state": flow_state,
                "inbound_text": inbound_text,
                "latest_inbound_id": latest_inbound_id,
                "last_inbound_id_seen": last_inbound_seen,
                "pending_reply": pending_reply,
                "pending_inbound_id": pending_inbound_id,
                "last_outbound_ts": confirmed_last_outbound_ts,
                "followup_level": self._int(thread.get("followup_level")),
                "has_inbound_history": bool(latest_inbound or self._ts(thread.get("last_inbound_at"))),
                "preconversation_initial_placeholder": preconversation_initial_placeholder,
<<<<<<< HEAD
                "now_ts": now_ts,
=======
                "now_ts": time.time(),
>>>>>>> origin/main
                "objection_strategy_name": str(prompt_entry.get("objection_strategy_name") or "").strip(),
            }
        )
        decision = {
            **decision,
            "confirmed_stage_activation": dict(confirmed_stage_activation),
            "inbound_snapshot_evidence": str(inbound_snapshot.get("evidence") or "").strip(),
        }

        actions: list[dict[str, Any]] = []
        thread_updates: dict[str, Any] = {}
        inbound_ts = self._ts(inbound_snapshot.get("timestamp")) or self._ts(thread.get("last_inbound_at"))
        if inbound_ts is not None:
            thread_updates["last_inbound_at"] = inbound_ts
        state_updates: dict[str, Any] = {"flow_state": flow_state}
<<<<<<< HEAD
        quota_deferral = self._pack_quota_deferral(thread)
        quota_deferral_retry_after_ts = self._ts((quota_deferral or {}).get("retry_after_ts"))
        if quota_deferral is not None and quota_deferral_retry_after_ts is not None and now_ts >= quota_deferral_retry_after_ts:
            state_updates[self.PACK_QUOTA_DEFERRAL_STATE_KEY] = None
=======
>>>>>>> origin/main

        inbound_type = str(decision.get("inbound_type") or "").strip().lower()
        decision_type = str(decision.get("decision") or "").strip().lower()
        next_stage_id = str(decision.get("next_stage_id") or decision.get("stage_id") or current_stage_id or "").strip()
        followup_allowed = self._router.can_followup_touch(thread)
        stage_activation_confirmed = bool(confirmed_stage_activation.get("confirmed"))

        if decision_type == "wait":
            if mode in {"followup", "both"} and followup_allowed and stage_activation_confirmed:
                followup_due = flow_engine.compute_followup_due(
                    {
                        "flow_state": flow_state,
                        "inbound_relevant": bool(latest_inbound_id and latest_inbound_id != last_inbound_seen),
                        "has_inbound_history": bool(latest_inbound_id or inbound_ts),
                        "preconversation_initial_placeholder": preconversation_initial_placeholder,
                        "last_outbound_ts": confirmed_last_outbound_ts,
                        "followup_level": self._int(thread.get("followup_level")),
<<<<<<< HEAD
                        "now_ts": now_ts,
=======
                        "now_ts": time.time(),
>>>>>>> origin/main
                    }
                )
                actions.append({"type": "schedule_followup", **followup_due})
            return {
                "actions": actions,
                "thread_updates": thread_updates,
                "state_updates": state_updates,
                "decision": decision,
            }

        if decision_type not in {"reply", "followup"}:
            return {
                "actions": actions,
                "thread_updates": thread_updates,
                "state_updates": state_updates,
                "decision": decision,
            }

        inbound_relevance = dict(decision.get("inbound_relevance") or {})
        allow_unconfirmed_entry_reply = bool(
            decision_type == "reply"
            and bool(inbound_relevance.get("relevant"))
            and latest_inbound_id
            and str(inbound_snapshot.get("evidence") or "").strip() != "preview_hint"
            and responder_module._is_initial_flow_stage_id(raw_stage_hint)
            and not bool(flow_engine.has_initial_stage)
        )
        if not stage_activation_confirmed and not allow_unconfirmed_entry_reply:
            blocked_decision = {
                **decision,
                "decision": "skip",
                "reason": "stage_activation_unconfirmed",
            }
            return {
                "actions": actions,
                "thread_updates": thread_updates,
                "state_updates": state_updates,
                "decision": blocked_decision,
            }

        if decision_type == "reply" and mode not in {"auto", "both"}:
            return {"actions": actions, "thread_updates": thread_updates, "state_updates": state_updates, "decision": decision}
        if decision_type == "followup" and mode not in {"followup", "both"}:
            return {"actions": actions, "thread_updates": thread_updates, "state_updates": state_updates, "decision": decision}
        if decision_type == "followup" and not followup_allowed:
            return {"actions": actions, "thread_updates": thread_updates, "state_updates": state_updates, "decision": decision}

<<<<<<< HEAD
        action_type = responder_module._canonical_flow_action_type(
            decision.get("action_type"),
            allow_empty=True,
            strict=bool(str(decision.get("action_type") or "").strip()),
        )
=======
        action_type = str(decision.get("action_type") or "").strip()
>>>>>>> origin/main
        if not action_type or responder_module._is_no_send_strategy(action_type):
            if inbound_type == "positive":
                actions.append({"type": "mark_qualified"})
            elif inbound_type == "negative":
                actions.append({"type": "mark_disqualified"})
            if next_stage_id and next_stage_id != current_stage_id:
                thread_updates["stage_id"] = next_stage_id
                thread_updates["followup_level"] = current_followup_level
                actions.append({"type": "move_stage", "stage_id": next_stage_id})
            return {"actions": actions, "thread_updates": thread_updates, "state_updates": state_updates, "decision": decision}

<<<<<<< HEAD
        enqueue_state_updates: dict[str, Any] = {}
        if decision_type == "reply":
            reply_marker = latest_inbound_id or pending_inbound_id
            enqueue_state_updates = {
                "pending_reply": True,
                "pending_inbound_id": reply_marker or None,
            }
        enqueue_state_updates[self.PACK_QUOTA_DEFERRAL_STATE_KEY] = None
=======
        if decision_type == "reply":
            reply_marker = latest_inbound_id or pending_inbound_id
            state_updates["pending_reply"] = True
            state_updates["pending_inbound_id"] = reply_marker or None
>>>>>>> origin/main

        post_send_route_updates: dict[str, Any] = {}
        if inbound_type == "positive":
            post_send_route_updates = self._router.mark_qualified(thread)
        elif inbound_type == "negative":
            post_send_route_updates = self._router.mark_disqualified(thread)

        post_send_flow_state = flow_engine.apply_outbound(flow_state, decision, sent_at=time.time())
        post_send_thread_updates = {
            **post_send_route_updates,
            "stage_id": str(
                post_send_flow_state.get("stage_id")
                or next_stage_id
                or current_stage_id
                or responder_module._STAGE_INITIAL
            ).strip()
            or responder_module._STAGE_INITIAL,
            "followup_level": self._int(post_send_flow_state.get("followup_level")),
        }
        post_send_state_updates = {
            "flow_state": post_send_flow_state,
            "pending_reply": False,
            "pending_inbound_id": None,
<<<<<<< HEAD
            self.PACK_QUOTA_DEFERRAL_STATE_KEY: None,
=======
>>>>>>> origin/main
        }
        if latest_inbound_id:
            post_send_state_updates["last_inbound_id_seen"] = latest_inbound_id

        if self._should_generate_text(action_type):
            text = self._generate_text(
                action_type=action_type,
                inbound_text=inbound_text,
                prompt_entry=prompt_entry,
                account_id=account_id,
                conversation_text=build_conversation_text(messages, limit=20),
            )
            if text:
                actions.append(
                    {
                        "type": "send_text",
                        "job_type": "followup" if decision_type == "followup" else "auto_reply",
                        "text": text,
                        "latest_inbound_id": latest_inbound_id,
<<<<<<< HEAD
                        "enqueue_state_updates": dict(enqueue_state_updates),
=======
>>>>>>> origin/main
                        "post_send_thread_updates": post_send_thread_updates,
                        "post_send_state_updates": post_send_state_updates,
                    }
                )
        else:
            selected_pack = responder_module.select_pack(action_type, account_id)
<<<<<<< HEAD
            if not isinstance(selected_pack, dict):
                raise ValueError(f"Sin pack valido para action_type: {action_type}")
            pack_id = str(selected_pack.get("id") or "").strip()
            active_quota_deferral = self._active_pack_quota_deferral(
                thread=thread,
                latest_inbound_id=latest_inbound_id,
                pending_inbound_id=pending_inbound_id,
                pack_id=pack_id,
                job_type="followup" if decision_type == "followup" else "auto_reply",
                now_ts=now_ts,
            )
            if active_quota_deferral is not None:
                blocked_decision = {
                    **decision,
                    "decision": "wait",
                    "reason": "pack_quota_deferred",
                    "pack_quota_deferral": dict(active_quota_deferral),
                }
                return {
                    "actions": actions,
                    "thread_updates": thread_updates,
                    "state_updates": state_updates,
                    "decision": blocked_decision,
                }
            actions.append(
                {
                    "type": "send_pack",
                    "job_type": "followup" if decision_type == "followup" else "auto_reply",
                    "pack_id": pack_id,
                    "pack_sendable_actions": responder_module._pack_sendable_action_count(selected_pack.get("actions")),
                    "latest_inbound_id": latest_inbound_id,
                    "enqueue_state_updates": dict(enqueue_state_updates),
                    "post_send_thread_updates": {
                        **post_send_thread_updates,
                        "last_pack_sent": pack_id,
                    },
                    "post_send_state_updates": post_send_state_updates,
                }
            )
=======
            if isinstance(selected_pack, dict):
                actions.append(
                    {
                        "type": "send_pack",
                        "job_type": "followup" if decision_type == "followup" else "auto_reply",
                        "pack_id": str(selected_pack.get("id") or "").strip(),
                        "latest_inbound_id": latest_inbound_id,
                        "post_send_thread_updates": {
                            **post_send_thread_updates,
                            "last_pack_sent": str(selected_pack.get("id") or "").strip(),
                        },
                        "post_send_state_updates": post_send_state_updates,
                    }
                )
>>>>>>> origin/main

        return {
            "actions": actions,
            "thread_updates": thread_updates,
            "state_updates": state_updates,
            "decision": decision,
        }

    @staticmethod
<<<<<<< HEAD
    def describe_evaluation(evaluation: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(evaluation or {}) if isinstance(evaluation, dict) else {}
        decision = dict(payload.get("decision") or {}) if isinstance(payload.get("decision"), dict) else {}
        actions = [dict(row) for row in payload.get("actions") or [] if isinstance(row, dict)]
        return {
            "decision": str(decision.get("decision") or "").strip().lower(),
            "reason": str(decision.get("reason") or "").strip(),
            "actions_count": len(actions),
            "action_types": [str(row.get("type") or "").strip().lower() for row in actions if str(row.get("type") or "").strip()],
        }

    @staticmethod
=======
>>>>>>> origin/main
    def _generate_text(
        *,
        action_type: str,
        inbound_text: str,
        prompt_entry: dict[str, Any],
        account_id: str,
        conversation_text: str,
    ) -> str:
        api_key = responder_module._resolve_ai_api_key()
<<<<<<< HEAD
        canonical_action = responder_module._canonical_flow_action_type(action_type, allow_empty=True)
        prompt_strategy = str(prompt_entry.get("objection_strategy_name") or "").strip()
        if str(prompt_entry.get("objection_prompt") or "").strip() and (
            canonical_action == "objection_engine"
            or str(action_type or "").strip() == prompt_strategy
        ):
=======
        if str(prompt_entry.get("objection_prompt") or "").strip() and str(action_type or "").strip() == str(
            prompt_entry.get("objection_strategy_name") or ""
        ).strip():
>>>>>>> origin/main
            return responder_module.generate_objection_response(
                inbound_text,
                str(prompt_entry.get("objection_prompt") or "").strip(),
                responder_module._get_account_memory(account_id),
                api_key=api_key,
                conversation_text=conversation_text,
            )
        return responder_module._generate_autoreply_response(
            inbound_text,
            getattr(responder_module, "_DEFAULT_RESPONDER_STRATEGY_PROMPT", ""),
            api_key=api_key,
            conversation_text=conversation_text,
            account_memory=responder_module._get_account_memory(account_id),
        )

    @staticmethod
    def _should_generate_text(action_type: str) -> bool:
<<<<<<< HEAD
        return responder_module._canonical_flow_action_type(action_type, allow_empty=True) in {
            "auto_reply",
            "followup_text",
            "objection_engine",
=======
        return responder_module._flow_action_token(action_type) in {
            "auto_reply",
            "autorespuesta",
            "reply_prompt",
            "followup_prompt",
            "followup_text",
>>>>>>> origin/main
        }

    @staticmethod
    def _latest_message(messages: list[dict[str, Any]], *, direction: str) -> dict[str, Any] | None:
        target = str(direction or "").strip().lower()
        candidates = [row for row in messages if str(row.get("direction") or "").strip().lower() == target]
        if not candidates:
            return None
        candidates.sort(key=lambda row: (AutomationEngineAdapter._ts(row.get("timestamp")) or 0.0, str(row.get("message_id") or "")))
        return dict(candidates[-1])

    @staticmethod
    def _resolve_inbound_snapshot(
        *,
        thread: dict[str, Any],
        latest_inbound: dict[str, Any] | None,
        confirmed_stage_activation: dict[str, Any] | None,
    ) -> dict[str, Any]:
        pending_reply = bool(thread.get("pending_reply"))
        pending_inbound_id = str(thread.get("pending_inbound_id") or "").strip()
        cached_candidate = {
            "text": str((latest_inbound or {}).get("text") or "").strip(),
            "timestamp": AutomationEngineAdapter._ts((latest_inbound or {}).get("timestamp")),
            "message_id": str((latest_inbound or {}).get("message_id") or "").strip(),
        }
        preview_candidate = AutomationEngineAdapter._preview_inbound_candidate(thread)
        cached_available = AutomationEngineAdapter._candidate_has_signal(cached_candidate)
        preview_available = AutomationEngineAdapter._candidate_has_signal(preview_candidate)
        preview_preferred = AutomationEngineAdapter._should_prefer_preview_inbound(
            cached_candidate=cached_candidate,
            preview_candidate=preview_candidate,
        )
        stage_confirmed = bool((confirmed_stage_activation or {}).get("confirmed"))
        selected_candidate: dict[str, Any] = {}
        evidence = "insufficient"
        actionable = False

        if cached_available:
            selected_candidate = dict(cached_candidate)
            evidence = "real"
            actionable = True
        elif preview_available:
            selected_candidate = dict(preview_candidate)
            evidence = "preview" if stage_confirmed else "preview_hint"
            actionable = stage_confirmed

        if preview_preferred and preview_available:
            if stage_confirmed:
                selected_candidate = dict(preview_candidate)
                evidence = "preview"
                actionable = True
            elif not cached_available:
                selected_candidate = dict(preview_candidate)
                evidence = "preview_hint"
                actionable = False

        text = str(selected_candidate.get("text") or "").strip() if actionable else ""
        timestamp = AutomationEngineAdapter._ts(selected_candidate.get("timestamp")) if actionable else None
        message_id = str(selected_candidate.get("message_id") or "").strip() if actionable else ""

        if not message_id and pending_reply and pending_inbound_id:
            message_id = pending_inbound_id

        if actionable and not message_id and (text or timestamp is not None):
            message_id = AutomationEngineAdapter._synthetic_inbound_id(
                thread_key=str(thread.get("thread_key") or thread.get("thread_id") or "").strip(),
                timestamp=timestamp,
                text=text,
            )

        return {
            "text": text,
            "timestamp": timestamp,
            "message_id": message_id,
            "evidence": evidence,
            "actionable": actionable,
            "preview_text": str(preview_candidate.get("text") or "").strip(),
            "preview_timestamp": AutomationEngineAdapter._ts(preview_candidate.get("timestamp")),
            "preview_message_id": str(preview_candidate.get("message_id") or "").strip(),
        }

    @staticmethod
    def _preview_inbound_candidate(thread: dict[str, Any]) -> dict[str, Any]:
        last_direction = str(thread.get("last_message_direction") or "").strip().lower()
        unread_count = AutomationEngineAdapter._int(thread.get("unread_count"))
        needs_reply = bool(thread.get("needs_reply"))
        preview_is_inbound = last_direction == "inbound" or (
            needs_reply
            and (
                unread_count > 0
                or bool(str(thread.get("last_message_id") or "").strip())
                or AutomationEngineAdapter._ts(thread.get("last_message_timestamp")) is not None
            )
        )
        if not preview_is_inbound:
            return {}
        return {
            "text": str(thread.get("last_message_text") or "").strip(),
            "timestamp": AutomationEngineAdapter._ts(thread.get("last_message_timestamp"))
            or AutomationEngineAdapter._ts(thread.get("last_inbound_at")),
            "message_id": str(thread.get("last_message_id") or "").strip(),
        }

    @classmethod
    def _should_prefer_preview_inbound(
        cls,
        *,
        cached_candidate: dict[str, Any],
        preview_candidate: dict[str, Any],
    ) -> bool:
        if not isinstance(preview_candidate, dict) or not preview_candidate:
            return False
        preview_id = str(preview_candidate.get("message_id") or "").strip()
        preview_text = str(preview_candidate.get("text") or "").strip()
        preview_ts = cls._ts(preview_candidate.get("timestamp"))
        if not preview_id and not preview_text and preview_ts is None:
            return False

        cached_id = str(cached_candidate.get("message_id") or "").strip()
        cached_text = str(cached_candidate.get("text") or "").strip()
        cached_ts = cls._ts(cached_candidate.get("timestamp"))
        if not cached_id and not cached_text and cached_ts is None:
            return True
        if preview_ts is not None and cached_ts is None:
            return True
        if preview_ts is not None and cached_ts is not None and preview_ts > (cached_ts + cls._INBOUND_PREVIEW_EPSILON_SECONDS):
            return True
        if preview_id and preview_id != cached_id:
            if preview_ts is None or cached_ts is None:
                return True
            if preview_ts >= (cached_ts - cls._INBOUND_PREVIEW_EPSILON_SECONDS):
                return True
        if (
            preview_text
            and preview_text != cached_text
            and preview_ts is not None
            and cached_ts is not None
            and preview_ts >= (cached_ts - cls._INBOUND_PREVIEW_EPSILON_SECONDS)
        ):
            return True
        return False

    @staticmethod
    def _candidate_has_signal(candidate: dict[str, Any] | None) -> bool:
        if not isinstance(candidate, dict):
            return False
        return bool(
            str(candidate.get("message_id") or "").strip()
            or str(candidate.get("text") or "").strip()
            or AutomationEngineAdapter._ts(candidate.get("timestamp")) is not None
        )

    @staticmethod
    def _resolve_confirmed_stage_activation(
        *,
        thread: dict[str, Any],
        messages: list[dict[str, Any]],
        raw_flow_state: dict[str, Any],
        stage_id: str,
    ) -> dict[str, Any]:
        clean_stage_id = responder_module._canonical_flow_stage_id(stage_id)
        if not clean_stage_id:
            return {
                "confirmed": False,
                "stage_id": "",
                "source": "",
                "anchor_ts": None,
                "message_id": "",
                "reason": "missing_stage_id",
            }

        confirmed_message = AutomationEngineAdapter._latest_confirmed_outbound_for_stage(messages, stage_id=clean_stage_id)
        if confirmed_message is not None:
            return {
                "confirmed": True,
                "stage_id": clean_stage_id,
                "source": "message",
                "anchor_ts": AutomationEngineAdapter._message_confirmation_ts(confirmed_message),
                "message_id": str(confirmed_message.get("message_id") or "").strip(),
                "reason": "confirmed_outbound_message",
            }

        flow_state_stage_id = responder_module._canonical_flow_stage_id(raw_flow_state.get("stage_id"))
        flow_anchor_ts = AutomationEngineAdapter._ts(raw_flow_state.get("followup_anchor_ts")) or AutomationEngineAdapter._ts(
            raw_flow_state.get("last_outbound_ts")
        )
        if responder_module._flow_stage_ids_match(flow_state_stage_id, clean_stage_id) and flow_anchor_ts is not None:
            return {
                "confirmed": True,
                "stage_id": clean_stage_id,
                "source": "flow_state",
                "anchor_ts": flow_anchor_ts,
                "message_id": "",
                "reason": "confirmed_flow_state_anchor",
            }

        thread_stage_id = responder_module._canonical_flow_stage_id(thread.get("stage_id"))
        last_direction = str(thread.get("last_message_direction") or "").strip().lower()
        last_message_id = str(thread.get("last_message_id") or "").strip()
        thread_anchor_ts = AutomationEngineAdapter._ts(thread.get("last_outbound_at")) or AutomationEngineAdapter._ts(
            thread.get("last_message_timestamp")
        )
        if (
            responder_module._flow_stage_ids_match(thread_stage_id, clean_stage_id)
            and last_direction == "outbound"
            and last_message_id
            and thread_anchor_ts is not None
            and not AutomationEngineAdapter._looks_like_local_outbound_id(last_message_id)
        ):
            matching_outbound = next(
                (
                    dict(row)
                    for row in messages
                    if str(row.get("message_id") or "").strip() == last_message_id
                    and str(row.get("direction") or "").strip().lower() == "outbound"
                ),
                None,
            )
            if matching_outbound is None or AutomationEngineAdapter._is_confirmed_outbound_message(matching_outbound):
                return {
                    "confirmed": True,
                    "stage_id": clean_stage_id,
                    "source": "thread_last_outbound",
                    "anchor_ts": AutomationEngineAdapter._message_confirmation_ts(matching_outbound) or thread_anchor_ts,
                    "message_id": last_message_id,
                    "reason": "confirmed_thread_last_outbound",
                }

        return {
            "confirmed": False,
            "stage_id": clean_stage_id,
            "source": "",
            "anchor_ts": None,
            "message_id": "",
            "reason": "missing_confirmed_outbound",
        }

    @staticmethod
    def _latest_confirmed_outbound_for_stage(
        messages: list[dict[str, Any]],
        *,
        stage_id: str,
    ) -> dict[str, Any] | None:
        clean_stage_id = responder_module._canonical_flow_stage_id(stage_id)
        candidates = [
            dict(row)
            for row in messages
            if str(row.get("direction") or "").strip().lower() == "outbound"
            and responder_module._flow_stage_ids_match(row.get("stage_id"), clean_stage_id)
            and AutomationEngineAdapter._is_confirmed_outbound_message(row)
        ]
        if not candidates:
            return None
        candidates.sort(
            key=lambda row: (
                AutomationEngineAdapter._message_confirmation_ts(row) or 0.0,
                str(row.get("message_id") or ""),
            )
        )
        return dict(candidates[-1])

    @staticmethod
    def _message_confirmation_ts(message: dict[str, Any] | None) -> float | None:
        if not isinstance(message, dict):
            return None
        return AutomationEngineAdapter._ts(message.get("confirmed_at")) or AutomationEngineAdapter._ts(message.get("timestamp"))

    @staticmethod
    def _is_confirmed_outbound_message(message: dict[str, Any]) -> bool:
        delivery_status = str(message.get("delivery_status") or "").strip().lower()
        sent_status = str(message.get("sent_status") or "").strip().lower()
        return bool(
            AutomationEngineAdapter._message_confirmation_ts(message) is not None
            and (
                delivery_status == "sent"
                or sent_status in {"confirmed", "sent"}
            )
        )

    @staticmethod
    def _looks_like_local_outbound_id(message_id: str) -> bool:
        clean_message_id = str(message_id or "").strip().lower()
        return clean_message_id.startswith("local-") or clean_message_id.startswith("synthetic-")

    @staticmethod
    def _synthetic_inbound_id(*, thread_key: str, timestamp: Any, text: str) -> str:
        normalized_ts = AutomationEngineAdapter._ts(timestamp) or 0.0
        normalized_text = str(text or "").strip().lower()
        seed = f"{thread_key}|{normalized_ts:.6f}|{normalized_text}"
        digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
        return f"synthetic-inbound:{digest}"

    @staticmethod
    def _ts(value: Any) -> float | None:
        try:
            if value in {None, ""}:
                return None
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _int(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except Exception:
            return 0
<<<<<<< HEAD

    @classmethod
    def _pack_quota_deferral(cls, thread: dict[str, Any]) -> dict[str, Any] | None:
        raw = thread.get(cls.PACK_QUOTA_DEFERRAL_STATE_KEY)
        return dict(raw) if isinstance(raw, dict) else None

    @classmethod
    def _active_pack_quota_deferral(
        cls,
        *,
        thread: dict[str, Any],
        latest_inbound_id: str,
        pending_inbound_id: str,
        pack_id: str,
        job_type: str,
        now_ts: float,
    ) -> dict[str, Any] | None:
        marker = cls._pack_quota_deferral(thread)
        if marker is None:
            return None
        retry_after_ts = cls._ts(marker.get("retry_after_ts"))
        if retry_after_ts is None or now_ts >= retry_after_ts:
            return None
        marker_job_type = str(marker.get("job_type") or "").strip().lower()
        clean_job_type = str(job_type or "").strip().lower()
        if marker_job_type and clean_job_type and marker_job_type != clean_job_type:
            return None
        marker_pack_id = str(marker.get("pack_id") or "").strip()
        clean_pack_id = str(pack_id or "").strip()
        if marker_pack_id and clean_pack_id and marker_pack_id != clean_pack_id:
            return None
        current_inbound_id = str(latest_inbound_id or pending_inbound_id or "").strip()
        marker_inbound_id = str(marker.get("inbound_id") or "").strip()
        if marker_inbound_id and current_inbound_id and marker_inbound_id != current_inbound_id:
            return None
        return marker
=======
>>>>>>> origin/main
