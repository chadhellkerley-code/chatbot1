from __future__ import annotations

from typing import Any


INBOUND_RECEIVED = "inbound_received"
THREAD_UPDATED = "thread_updated"
QUEUED_AUTO_REPLY = "queued_auto_reply"
QUEUED_FOLLOWUP = "queued_followup"
QUEUED_MANUAL_REPLY = "queued_manual_reply"
QUEUED_PACK = "queued_pack"
SENT_AUTO_REPLY = "sent_auto_reply"
SENT_FOLLOWUP = "sent_followup"
SENT_MANUAL_REPLY = "sent_manual_reply"
SENT_PACK = "sent_pack"
FAILED_AUTO_REPLY = "failed_auto_reply"
FAILED_FOLLOWUP = "failed_followup"
FAILED_MANUAL_REPLY = "failed_manual_reply"
FAILED_PACK = "failed_pack"

# Legacy aliases kept for compatibility with older imports only.
MESSAGE_SENT = "message_sent"
PACK_SENT = "pack_sent"
FOLLOWUP_SENT = "followup_sent"
STAGE_CHANGED = "stage_changed"
QUALIFIED = "qualified"
DISQUALIFIED = "disqualified"
MANUAL_TAKEN = "manual_taken"
SEND_FAILED = "send_failed"


def build_runtime_event(event_type: str, **payload: Any) -> dict[str, Any]:
    return {
        "event_type": str(event_type or "").strip().lower(),
        "payload": dict(payload or {}),
    }


def queued_thread_event(job_type: str, *, is_pack: bool = False) -> str:
    return _thread_delivery_event("queued", job_type, is_pack=is_pack)


def sent_thread_event(job_type: str, *, is_pack: bool = False) -> str:
    return _thread_delivery_event("sent", job_type, is_pack=is_pack)


def failed_thread_event(job_type: str, *, is_pack: bool = False) -> str:
    return _thread_delivery_event("failed", job_type, is_pack=is_pack)


def _thread_delivery_event(phase: str, job_type: str, *, is_pack: bool) -> str:
    clean_phase = str(phase or "").strip().lower()
    clean_job_type = str(job_type or "").strip().lower()
    if is_pack or clean_job_type == "manual_pack":
        return {
            "queued": QUEUED_PACK,
            "sent": SENT_PACK,
            "failed": FAILED_PACK,
        }.get(clean_phase, "")
    mapping = {
        "auto_reply": {
            "queued": QUEUED_AUTO_REPLY,
            "sent": SENT_AUTO_REPLY,
            "failed": FAILED_AUTO_REPLY,
        },
        "followup": {
            "queued": QUEUED_FOLLOWUP,
            "sent": SENT_FOLLOWUP,
            "failed": FAILED_FOLLOWUP,
        },
        "manual_reply": {
            "queued": QUEUED_MANUAL_REPLY,
            "sent": SENT_MANUAL_REPLY,
            "failed": FAILED_MANUAL_REPLY,
        },
    }
    return mapping.get(clean_job_type, {}).get(clean_phase, "")
