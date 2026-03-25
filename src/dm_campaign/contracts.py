from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Mapping


class CampaignSendStatus(str, Enum):
    SENT = "sent"
    AMBIGUOUS = "ambiguous"
    SKIPPED = "skipped"
    FAILED = "failed"


class CampaignRunStatus(str, Enum):
    IDLE = "Idle"
    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"
    INTERRUPTED = "Interrupted"
    COMPLETED = "Completed"
    STOPPED = "Stopped"
    BLOCKED = "Blocked"
    FAILED = "Failed"

    @classmethod
    def parse(cls, value: Any, *, default: "CampaignRunStatus" | None = None) -> "CampaignRunStatus":
        fallback = default or cls.IDLE
        clean_value = str(value or "").strip() or fallback.value
        try:
            return cls(clean_value)
        except ValueError:
            return fallback

    @property
    def is_terminal(self) -> bool:
        return self in {self.INTERRUPTED, self.COMPLETED, self.STOPPED, self.BLOCKED, self.FAILED}


class WorkerExecutionState(str, Enum):
    IDLE = "idle"
    WAITING = "waiting"
    PROCESSING = "processing"
    STOPPING = "stopping"


class WorkerExecutionStage(str, Enum):
    IDLE = "idle"
    WAITING_QUEUE = "waiting_queue"
    BLOCKED_PROXY = "blocked_proxy"
    WAITING_ACCOUNT = "waiting_account"
    COOLDOWN = "cooldown"
    OPENING_SESSION = "opening_session"
    OPENING_DM = "opening_dm"
    SENDING = "sending"
    STOPPING = "stopping"


def _as_int(value: Any, *, default: int = 0, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    return max(int(minimum), parsed)


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _as_worker_rows(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(dict(row) for row in value if isinstance(row, dict))


@dataclass(frozen=True)
class CampaignCapacity:
    alias: str
    workers_capacity: int
    proxies: tuple[str, ...] = ()
    has_none_accounts: bool = False

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> "CampaignCapacity":
        data = dict(payload or {})
        proxies = tuple(
            str(item or "").strip()
            for item in (data.get("proxies") or [])
            if str(item or "").strip()
        )
        return cls(
            alias=_as_text(data.get("alias")),
            workers_capacity=_as_int(data.get("workers_capacity"), default=0, minimum=0),
            proxies=proxies,
            has_none_accounts=bool(data.get("has_none_accounts")),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "workers_capacity": self.workers_capacity,
            "proxies": list(self.proxies),
            "has_none_accounts": self.has_none_accounts,
        }


@dataclass(frozen=True)
class CampaignLaunchRequest:
    alias: str
    leads_alias: str
    templates: tuple[dict[str, Any], ...]
    run_id: str = ""
    delay_min: int = 0
    delay_max: int = 0
    workers_requested: int = 1
    workers_capacity: int = 0
    headless: bool | None = None
    total_leads: int = 0
    started_at: str = ""

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> "CampaignLaunchRequest":
        data = dict(payload or {})
        templates = tuple(
            dict(item)
            for item in (data.get("templates") or [])
            if isinstance(item, dict)
        )
        return cls(
            alias=_as_text(data.get("alias")),
            leads_alias=_as_text(data.get("leads_alias")),
            templates=templates,
            run_id=_as_text(data.get("run_id")),
            delay_min=_as_int(data.get("delay_min"), default=0, minimum=0),
            delay_max=_as_int(data.get("delay_max"), default=0, minimum=0),
            workers_requested=_as_int(data.get("workers_requested"), default=1, minimum=1),
            workers_capacity=_as_int(data.get("workers_capacity"), default=0, minimum=0),
            headless=data.get("headless"),
            total_leads=_as_int(data.get("total_leads"), default=0, minimum=0),
            started_at=_as_text(data.get("started_at")),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "leads_alias": self.leads_alias,
            "run_id": self.run_id,
            "templates": [dict(item) for item in self.templates],
            "delay_min": self.delay_min,
            "delay_max": self.delay_max,
            "workers_requested": self.workers_requested,
            "workers_capacity": self.workers_capacity,
            "headless": self.headless,
            "total_leads": self.total_leads,
            "started_at": self.started_at,
        }

    @property
    def workers_effective(self) -> int:
        if self.workers_capacity <= 0 or self.workers_requested <= 0:
            return 0
        return min(self.workers_requested, self.workers_capacity)

    def with_capacity(self, workers_capacity: int) -> "CampaignLaunchRequest":
        return type(self)(
            alias=self.alias,
            leads_alias=self.leads_alias,
            templates=self.templates,
            run_id=self.run_id,
            delay_min=self.delay_min,
            delay_max=self.delay_max,
            workers_requested=self.workers_requested,
            workers_capacity=_as_int(workers_capacity, default=0, minimum=0),
            headless=self.headless,
            total_leads=self.total_leads,
            started_at=self.started_at,
        )

    def to_runner_payload(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "leads_alias": self.leads_alias,
            "run_id": self.run_id,
            "templates": [dict(item) for item in self.templates],
            "delay_min": self.delay_min,
            "delay_max": self.delay_max,
            "workers_requested": self.workers_requested,
            "workers_capacity": self.workers_capacity,
            "headless": self.headless,
            "total_leads": self.total_leads,
        }


@dataclass(frozen=True)
class CampaignRunSnapshot:
    run_id: str = ""
    alias: str = ""
    leads_alias: str = ""
    started_at: str = ""
    finished_at: str = ""
    status: CampaignRunStatus = CampaignRunStatus.IDLE
    message: str = ""
    sent: int = 0
    failed: int = 0
    skipped: int = 0
    skipped_preblocked: int = 0
    retried: int = 0
    total_leads: int = 0
    remaining: int = 0
    workers_active: int = 0
    workers_requested: int = 0
    workers_capacity: int = 0
    workers_effective: int = 0
    worker_rows: tuple[dict[str, Any], ...] = ()
    task_active: bool = False

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> "CampaignRunSnapshot":
        data = dict(payload or {})
        return cls(
            run_id=_as_text(data.get("run_id")),
            alias=_as_text(data.get("alias")),
            leads_alias=_as_text(data.get("leads_alias")),
            started_at=_as_text(data.get("started_at")),
            finished_at=_as_text(data.get("finished_at")),
            status=CampaignRunStatus.parse(data.get("status")),
            message=_as_text(data.get("message")),
            sent=_as_int(data.get("sent"), default=0, minimum=0),
            failed=_as_int(data.get("failed"), default=0, minimum=0),
            skipped=_as_int(data.get("skipped"), default=0, minimum=0),
            skipped_preblocked=_as_int(data.get("skipped_preblocked"), default=0, minimum=0),
            retried=_as_int(data.get("retried"), default=0, minimum=0),
            total_leads=_as_int(data.get("total_leads"), default=0, minimum=0),
            remaining=_as_int(data.get("remaining"), default=0, minimum=0),
            workers_active=_as_int(data.get("workers_active"), default=0, minimum=0),
            workers_requested=_as_int(data.get("workers_requested"), default=0, minimum=0),
            workers_capacity=_as_int(data.get("workers_capacity"), default=0, minimum=0),
            workers_effective=_as_int(data.get("workers_effective"), default=0, minimum=0),
            worker_rows=_as_worker_rows(data.get("worker_rows")),
            task_active=bool(data.get("task_active")),
        )

    @classmethod
    def starting(cls, request: CampaignLaunchRequest) -> "CampaignRunSnapshot":
        return cls(
            run_id=request.run_id,
            alias=request.alias,
            leads_alias=request.leads_alias,
            started_at=request.started_at,
            status=CampaignRunStatus.STARTING,
            message="Preparando campaña y workers...",
            total_leads=request.total_leads,
            remaining=request.total_leads,
            workers_requested=request.workers_requested,
            workers_capacity=request.workers_capacity,
            workers_effective=request.workers_effective,
            task_active=True,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "alias": self.alias,
            "leads_alias": self.leads_alias,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status.value,
            "message": self.message,
            "sent": self.sent,
            "failed": self.failed,
            "skipped": self.skipped,
            "skipped_preblocked": self.skipped_preblocked,
            "retried": self.retried,
            "total_leads": self.total_leads,
            "remaining": self.remaining,
            "workers_active": self.workers_active,
            "workers_requested": self.workers_requested,
            "workers_capacity": self.workers_capacity,
            "workers_effective": self.workers_effective,
            "worker_rows": [dict(row) for row in self.worker_rows],
            "task_active": self.task_active,
        }


@dataclass(frozen=True)
class CampaignSendResult:
    ok: bool
    detail: str
    payload: Dict[str, Any] = field(default_factory=dict)
    status: CampaignSendStatus = CampaignSendStatus.FAILED
    reason_code: str = ""
    verified: bool = False

    @property
    def should_retry(self) -> bool:
        return self.status == CampaignSendStatus.FAILED

    @classmethod
    def from_sender_result(cls, send_result: Any) -> "CampaignSendResult":
        detail = ""
        payload: Dict[str, Any] = {}
        success = False

        if isinstance(send_result, tuple):
            if len(send_result) >= 1:
                success = bool(send_result[0])
            if len(send_result) >= 2 and send_result[1] is not None:
                detail = str(send_result[1])
            if len(send_result) >= 3 and isinstance(send_result[2], dict):
                payload = dict(send_result[2])
        else:
            success = bool(send_result)

        reason_code = str(payload.get("reason_code") or "").strip().upper()
        verified = bool(payload.get("verified"))
        detail_upper = detail.strip().upper()
        explicit_unverified = bool(payload.get("sent_unverified"))

        if explicit_unverified:
            success = True
            if not detail:
                detail = "sent_unverified"
            status = CampaignSendStatus.AMBIGUOUS
        elif success:
            status = CampaignSendStatus.SENT
        elif detail_upper.startswith("SKIPPED_") or reason_code.startswith("SKIPPED_"):
            status = CampaignSendStatus.SKIPPED
        elif reason_code == "SENT_UNVERIFIED" or detail_upper == "SEND_UNVERIFIED_BLOCKED":
            status = CampaignSendStatus.AMBIGUOUS
        else:
            status = CampaignSendStatus.FAILED

        return cls(
            ok=success,
            detail=detail,
            payload=payload,
            status=status,
            reason_code=reason_code,
            verified=verified,
        )
