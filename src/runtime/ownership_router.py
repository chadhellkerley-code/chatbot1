from __future__ import annotations

from typing import Any


class OwnershipRouter:
    def initialize_thread(self, thread: dict[str, Any]) -> dict[str, Any]:
        current = dict(thread or {})
        owner = self._owner(current.get("owner") or "auto")
        bucket = self._bucket(current.get("bucket") or "all")
        status = self._status(current.get("status") or "open")
        if owner == "none" and bucket != "disqualified":
            bucket = "all"
        if bucket == "disqualified":
            owner = "none"
            status = "closed"
        return {
            "owner": owner,
            "bucket": bucket,
            "status": status,
            "stage_id": str(current.get("stage_id") or "initial").strip() or "initial",
            "manual_lock": bool(current.get("manual_lock", False) or owner == "manual"),
            "manual_assignee": str(current.get("manual_assignee") or "").strip(),
        }

    def manual_takeover(self, thread: dict[str, Any], operator_id: str) -> dict[str, Any]:
        current = self.initialize_thread(thread)
        updated = {
            "owner": "manual",
            "bucket": "qualified",
            "status": "open",
            "manual_lock": True,
            "manual_assignee": str(operator_id or "").strip(),
            "stage_id": str(current.get("stage_id") or "initial").strip() or "initial",
        }
        updated.update(self._takeover_context(thread, current=current))
        return updated

    def manual_release(self, thread: dict[str, Any]) -> dict[str, Any]:
        current = self.initialize_thread(thread)
        if self._owner(current.get("owner")) != "manual":
            return {}
        # Business rule: release hands ownership back to automation, restores the
        # last known pre-manual bucket/status when available, and otherwise keeps
        # the current bucket/status so "qualified" is never dropped implicitly.
        restored_bucket = self._optional_bucket((thread or {}).get("previous_bucket")) or self._bucket(current.get("bucket"))
        restored_status = self._optional_status((thread or {}).get("previous_status")) or self._status(current.get("status"))
        restored_owner = "none" if restored_bucket == "disqualified" else "auto"
        if restored_bucket == "disqualified":
            restored_status = "closed"
        return {
            "owner": restored_owner,
            "bucket": restored_bucket,
            "manual_lock": False,
            "manual_assignee": "",
            "status": restored_status,
            "previous_bucket": None,
            "previous_status": None,
            "previous_owner": None,
        }

    def mark_qualified(self, thread: dict[str, Any], operator_id: str = "runtime") -> dict[str, Any]:
        return self.manual_takeover(thread, operator_id)

    def mark_disqualified(self, thread: dict[str, Any]) -> dict[str, Any]:
        current = self.initialize_thread(thread)
        return {
            "owner": "none",
            "bucket": "disqualified",
            "status": "closed",
            "manual_lock": False,
            "manual_assignee": "",
            "stage_id": str(current.get("stage_id") or "initial").strip() or "initial",
        }

    def can_automation_touch(self, thread: dict[str, Any]) -> bool:
        current = self.initialize_thread(thread)
        if self._owner(current.get("owner")) == "manual":
            return False
        if self._bucket(current.get("bucket")) == "disqualified":
            return False
        return self._status(current.get("status")) not in {"closed", "failed", "paused"}

    def can_followup_touch(self, thread: dict[str, Any]) -> bool:
        current = self.initialize_thread(thread)
        if not self.can_automation_touch(current):
            return False
        # Conservative default: qualified conversations stay out of automatic
        # follow-up unless an explicit future policy allows them again.
        if self._bucket(current.get("bucket")) == "qualified":
            return False
        return self._owner(current.get("owner")) == "auto"

    def can_manual_send(self, thread: dict[str, Any], *, runtime_active: bool) -> bool:
        current = self.initialize_thread(thread)
        if self._bucket(current.get("bucket")) == "disqualified":
            return False
        if self._status(current.get("status")) == "closed":
            return not runtime_active
        if not runtime_active:
            return True
        return self._owner(current.get("owner")) == "manual" and self._bucket(current.get("bucket")) == "qualified"

    def can_manual_takeover(self, thread: dict[str, Any], *, runtime_active: bool) -> bool:
        current = self.initialize_thread(thread)
        if self._bucket(current.get("bucket")) == "disqualified":
            return False
        if not runtime_active:
            return True
        return self._owner(current.get("owner")) in {"auto", "manual"}

    def can_manual_release(self, thread: dict[str, Any]) -> bool:
        return self._owner((thread or {}).get("owner")) == "manual"

    def _takeover_context(self, thread: dict[str, Any], *, current: dict[str, Any]) -> dict[str, Any]:
        raw = dict(thread or {})
        if self._owner(current.get("owner")) == "manual":
            preserved: dict[str, Any] = {}
            previous_bucket = self._optional_bucket(raw.get("previous_bucket"))
            previous_status = self._optional_status(raw.get("previous_status"))
            previous_owner = self._optional_owner(raw.get("previous_owner"))
            if previous_bucket is not None:
                preserved["previous_bucket"] = previous_bucket
            if previous_status is not None:
                preserved["previous_status"] = previous_status
            if previous_owner is not None:
                preserved["previous_owner"] = previous_owner
            return preserved
        return {
            "previous_bucket": self._bucket(current.get("bucket")),
            "previous_status": self._status(current.get("status")),
            "previous_owner": self._owner(current.get("owner")),
        }

    @staticmethod
    def _owner(value: Any) -> str:
        owner = str(value or "").strip().lower() or "none"
        return owner if owner in {"auto", "manual", "none"} else "none"

    @staticmethod
    def _bucket(value: Any) -> str:
        bucket = str(value or "").strip().lower() or "all"
        return bucket if bucket in {"all", "qualified", "disqualified"} else "all"

    @staticmethod
    def _status(value: Any) -> str:
        status = str(value or "").strip().lower() or "open"
        allowed = {"open", "pending", "replied", "followup_sent", "paused", "closed", "failed", "pack_sent"}
        return status if status in allowed else "open"

    @classmethod
    def _optional_owner(cls, value: Any) -> str | None:
        text = str(value or "").strip().lower()
        if not text:
            return None
        return text if text in {"auto", "manual", "none"} else None

    @classmethod
    def _optional_bucket(cls, value: Any) -> str | None:
        text = str(value or "").strip().lower()
        if not text:
            return None
        if text in {"schedule", "scheduled"}:
            text = "qualified"
        return text if text in {"all", "qualified", "disqualified"} else None

    @classmethod
    def _optional_status(cls, value: Any) -> str | None:
        text = str(value or "").strip().lower()
        if not text:
            return None
        allowed = {"open", "pending", "replied", "followup_sent", "paused", "closed", "failed", "pack_sent"}
        return text if text in allowed else None
