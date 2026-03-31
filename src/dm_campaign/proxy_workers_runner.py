from __future__ import annotations

<<<<<<< HEAD
import base64
import json
import logging
import os
import random
import subprocess
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from multiprocessing.managers import AcquirerProxy, BaseManager, DictProxy
from pathlib import Path
import sys
import tempfile
=======
import json
import logging
import random
import threading
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
>>>>>>> origin/main
from typing import Any, Callable, Dict, MutableMapping, Optional

import health_store
from core.accounts import (
<<<<<<< HEAD
    account_usage_state,
    connected_status,
    get_account,
    has_playwright_storage_state,
    is_account_enabled_for_operation,
=======
    connected_status,
    get_account,
    has_playwright_storage_state,
>>>>>>> origin/main
    list_all,
    mark_connected,
    FILE as ACCOUNTS_FILE,
    normalize_alias,
    playwright_storage_state_path,
)
from core.leads import load_list
from core.proxy_preflight import DIRECT_NETWORK_KEY, account_proxy_preflight
from runtime.runtime import (
    EngineCancellationToken,
    STOP_EVENT,
    bind_stop_token,
<<<<<<< HEAD
=======
    bind_stop_token_callable,
>>>>>>> origin/main
    restore_stop_token,
)
from src.dm_campaign.adaptive_scheduler import AdaptiveScheduler, LeadTask
from src.dm_campaign.contracts import (
    CampaignRunSnapshot,
    CampaignRunStatus,
    CampaignSendResult,
    CampaignSendStatus,
    WorkerExecutionStage,
    WorkerExecutionState,
)
from src.dm_campaign.health_monitor import HealthMonitor
from src.dm_campaign.lead_status_store import (
    GLOBAL_CONTACT_TTL_SECONDS,
    apply_terminal_status_updates,
    get_prefilter_snapshot,
<<<<<<< HEAD
    mark_leads_pending,
=======
>>>>>>> origin/main
    mark_lead_failed,
    mark_lead_sent,
    mark_lead_skipped,
)
from src.dm_campaign.worker_state_machine import CampaignWorkerStateMachine, WorkerStateSnapshot
from src.runtime.playwright_runtime import (
<<<<<<< HEAD
    PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
=======
    PLAYWRIGHT_BROWSER_MODE_MANAGED,
>>>>>>> origin/main
    PlaywrightRuntimeCancelledError,
    PlaywrightRuntimeTimeoutError,
)
from src.transport.human_instagram_sender import HumanInstagramSender
from core.storage import (
    campaign_start_snapshot,
    log_sent,
    normalize_contact_username,
)
<<<<<<< HEAD
from core.account_limits import can_send_message_for_account
=======
>>>>>>> origin/main
from core.templates_store import render_template


logger = logging.getLogger(__name__)
LOCAL_WORKER_PROXY_ID = "__no_proxy__"
DEFAULT_LAUNCH_BATCH_SIZE = 8
<<<<<<< HEAD
DEFAULT_LAUNCH_STAGGER_MIN_SECONDS = 0.05
DEFAULT_LAUNCH_STAGGER_MAX_SECONDS = 0.25
DEFAULT_LAUNCH_BATCH_PAUSE_MIN_SECONDS = 0.0
DEFAULT_LAUNCH_BATCH_PAUSE_MAX_SECONDS = 0.0
CAMPAIGN_DESKTOP_WIDTH = 1366
CAMPAIGN_DESKTOP_HEIGHT = 900
REPO_ROOT = Path(__file__).resolve().parents[2]
WORKER_RUNTIME_DIR = REPO_ROOT / "runtime"
WORKER_CONFIG_DIR = WORKER_RUNTIME_DIR / "worker_configs"
WORKER_MANIFEST_DIR = WORKER_RUNTIME_DIR / "worker_manifests"
_WORKER_IPC_POLL_INTERVAL_SECONDS = 0.25


def _campaign_desktop_layout_payload() -> dict[str, int]:
    return {
        "width": int(CAMPAIGN_DESKTOP_WIDTH),
        "height": int(CAMPAIGN_DESKTOP_HEIGHT),
    }


def _normalize_runtime_root(root_dir: Any) -> Path | None:
    clean_root = str(root_dir or "").strip()
    if not clean_root:
        return None
    try:
        return Path(clean_root).resolve()
    except Exception:
        return None


def refresh_campaign_runtime_paths(root_dir: Any = None) -> dict[str, Path]:
    resolved_root = _normalize_runtime_root(root_dir)
    from core import leads as leads_module
    from core import storage as storage_module
    from src.dm_campaign import lead_status_store as lead_status_store_module

    lead_paths = leads_module.refresh_runtime_paths(resolved_root)
    storage_paths = storage_module.refresh_runtime_paths(resolved_root)
    lead_status_paths = lead_status_store_module.refresh_runtime_paths(resolved_root)
    return {
        "base": Path(lead_paths.get("base") or resolved_root or REPO_ROOT),
        "leads_root": Path(lead_paths.get("leads_root") or REPO_ROOT / "leads"),
        "storage_root": Path(storage_paths.get("storage_root") or REPO_ROOT / "storage"),
        "sent_log": Path(storage_paths.get("sent_log") or REPO_ROOT / "storage" / "sent_log.jsonl"),
        "lead_status": Path(lead_status_paths.get("lead_status") or REPO_ROOT / "storage" / "lead_status.json"),
    }


class _RuntimeEventSink:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: list[dict[str, Any]] = []

    def record_event(self, event: dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return
        with self._lock:
            self._events.append(dict(event))

    def drain_events(self) -> list[dict[str, Any]]:
        with self._lock:
            drained = [dict(item) for item in self._events if isinstance(item, dict)]
            self._events.clear()
            return drained


class _WorkerControlRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop_requests: dict[str, str] = {}

    def request_stop(self, worker_id: str, reason: str = "") -> None:
        clean_worker_id = str(worker_id or "").strip()
        if not clean_worker_id:
            return
        with self._lock:
            self._stop_requests[clean_worker_id] = str(reason or "").strip() or "stop_requested"

    def consume_stop_request(self, worker_id: str) -> str:
        clean_worker_id = str(worker_id or "").strip()
        if not clean_worker_id:
            return ""
        with self._lock:
            return str(self._stop_requests.pop(clean_worker_id, "") or "").strip()

    def clear_worker(self, worker_id: str) -> None:
        clean_worker_id = str(worker_id or "").strip()
        if not clean_worker_id:
            return
        with self._lock:
            self._stop_requests.pop(clean_worker_id, None)


_SCHEDULER_PROXY_EXPOSED = (
    "build_retry_task",
    "is_empty",
    "pop_task_for_proxy",
    "push_task",
    "queue_size",
    "update_worker_activity",
)
_HEALTH_MONITOR_PROXY_EXPOSED = (
    "account_cooldown_remaining",
    "is_account_available",
    "is_proxy_available",
    "proxy_status",
    "record_account_error",
    "record_account_success",
    "record_login_error",
    "record_send_error",
    "record_send_success",
    "set_account_cooldown",
)
_EVENT_SINK_PROXY_EXPOSED = ("record_event", "drain_events")
_CONTROL_PROXY_EXPOSED = ("clear_worker", "consume_stop_request", "request_stop")


class _WorkerIPCServerManager(BaseManager):
    pass


class _WorkerIPCClientManager(BaseManager):
    pass
=======
DEFAULT_LAUNCH_STAGGER_MIN_SECONDS = 0.6
DEFAULT_LAUNCH_STAGGER_MAX_SECONDS = 1.4
DEFAULT_LAUNCH_BATCH_PAUSE_MIN_SECONDS = 3.5
DEFAULT_LAUNCH_BATCH_PAUSE_MAX_SECONDS = 6.0
>>>>>>> origin/main


def _is_local_proxy_id(proxy_id: str) -> bool:
    normalized = str(proxy_id or "").strip().lower()
    return normalized in {"", LOCAL_WORKER_PROXY_ID, DIRECT_NETWORK_KEY}


def _normalize_effective_network_key(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized or normalized in {LOCAL_WORKER_PROXY_ID, DIRECT_NETWORK_KEY}:
        return DIRECT_NETWORK_KEY
    if normalized.startswith("proxy:"):
        proxy_id = normalized.split(":", 1)[1].strip()
        return f"proxy:{proxy_id}" if proxy_id else DIRECT_NETWORK_KEY
    return f"proxy:{normalized}"


def _runtime_proxy_id_from_network_key(network_key: str) -> str:
    normalized = _normalize_effective_network_key(network_key)
    if normalized == DIRECT_NETWORK_KEY:
        return LOCAL_WORKER_PROXY_ID
    return normalized.split(":", 1)[1].strip() or LOCAL_WORKER_PROXY_ID


def _effective_network_key_for_account(account: Dict[str, Any]) -> str:
    if not isinstance(account, dict):
        return ""
    explicit = str(account.get("effective_network_key") or "").strip()
    if explicit:
        return _normalize_effective_network_key(explicit)
    assigned_proxy_id = str(account.get("assigned_proxy_id") or "").strip()
    if assigned_proxy_id:
        return _normalize_effective_network_key(f"proxy:{assigned_proxy_id}")
    if str(account.get("proxy_url") or "").strip():
        return ""
    return DIRECT_NETWORK_KEY


def _network_mode_for_account(account: Dict[str, Any]) -> str:
    if not isinstance(account, dict):
        return ""
    mode = str(account.get("network_mode") or "").strip().lower()
    if mode:
        return mode
    worker_key = _effective_network_key_for_account(account)
    if not worker_key:
        return "legacy" if str(account.get("proxy_url") or "").strip() else ""
    return "direct" if worker_key == DIRECT_NETWORK_KEY else "proxy"


def _worker_label(network_key: str) -> str:
    normalized = _normalize_effective_network_key(network_key)
    if normalized == DIRECT_NETWORK_KEY:
        return DIRECT_NETWORK_KEY
    return normalized.split(":", 1)[1].strip() or DIRECT_NETWORK_KEY


def _runtime_proxy_id_for_account(account: Dict[str, Any]) -> str:
    worker_key = _effective_network_key_for_account(account)
    if not worker_key:
        return ""
    return _runtime_proxy_id_from_network_key(worker_key)


def _account_storage_state_path(username: str) -> Path:
    return playwright_storage_state_path(username)


def _account_has_storage_state(account: Dict[str, Any]) -> bool:
    if not isinstance(account, dict):
        return False
    username = str(account.get("username") or "").strip()
    if not username:
        return False
    return has_playwright_storage_state(username)


def _order_accounts_for_worker_start(accounts: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    ready_accounts: list[Dict[str, Any]] = []
    pending_accounts: list[Dict[str, Any]] = []
    for account in accounts:
        if _account_has_storage_state(account):
            ready_accounts.append(account)
            continue
        pending_accounts.append(account)
    return ready_accounts + pending_accounts


@dataclass
class AccountRuntimeState:
    account: Dict[str, Any]
    max_messages: int
    next_send_time: float = 0.0
    sent_count: int = 0
    fail_count: int = 0
    cooldown_until: float = 0.0
    disabled_for_campaign: bool = False
    session_ready: bool = False
    preflight_failure_reason: str = ""
    preflight_failure_message: str = ""
<<<<<<< HEAD
    active_in_worker: bool = False
    retired_reason: str = ""
=======
>>>>>>> origin/main


@dataclass(frozen=True)
class AccountWaitDecision:
    seconds: float
    stage: WorkerExecutionStage
    reason: str


class TemplateRotator:
    """Thread-safe round-robin selector for template variants."""

    def __init__(self, variants: list[str]) -> None:
        cleaned = [str(item or "").strip() for item in variants if str(item or "").strip()]
        self._variants: list[str] = cleaned or ["hola!"]
        self._cursor = 0
        self._lock = threading.Lock()

    @property
    def total_variants(self) -> int:
        return len(self._variants)

<<<<<<< HEAD
    @property
    def variants(self) -> list[str]:
        return list(self._variants)

=======
>>>>>>> origin/main
    def next_variant(self) -> tuple[str, int]:
        with self._lock:
            index = self._cursor % len(self._variants)
            self._cursor += 1
            return self._variants[index], index


<<<<<<< HEAD
def _register_worker_ipc_server(
    *,
    scheduler: AdaptiveScheduler,
    health_monitor: HealthMonitor,
    stats: Dict[str, int],
    stats_lock: threading.Lock,
    event_sink: _RuntimeEventSink,
    control_registry: _WorkerControlRegistry,
) -> None:
    _WorkerIPCServerManager.register(
        "get_scheduler",
        callable=lambda: scheduler,
        exposed=_SCHEDULER_PROXY_EXPOSED,
    )
    _WorkerIPCServerManager.register(
        "get_health_monitor",
        callable=lambda: health_monitor,
        exposed=_HEALTH_MONITOR_PROXY_EXPOSED,
    )
    _WorkerIPCServerManager.register(
        "get_stats",
        callable=lambda: stats,
        proxytype=DictProxy,
    )
    _WorkerIPCServerManager.register(
        "get_stats_lock",
        callable=lambda: stats_lock,
        proxytype=AcquirerProxy,
    )
    _WorkerIPCServerManager.register(
        "get_event_sink",
        callable=lambda: event_sink,
        exposed=_EVENT_SINK_PROXY_EXPOSED,
    )
    _WorkerIPCServerManager.register(
        "get_control_registry",
        callable=lambda: control_registry,
        exposed=_CONTROL_PROXY_EXPOSED,
    )


def _register_worker_ipc_client() -> None:
    _WorkerIPCClientManager.register("get_scheduler")
    _WorkerIPCClientManager.register("get_health_monitor")
    _WorkerIPCClientManager.register("get_stats", proxytype=DictProxy)
    _WorkerIPCClientManager.register("get_stats_lock", proxytype=AcquirerProxy)
    _WorkerIPCClientManager.register("get_event_sink")
    _WorkerIPCClientManager.register("get_control_registry")


def _build_worker_ipc_config(
    *,
    scheduler: AdaptiveScheduler,
    health_monitor: HealthMonitor,
    stats: Dict[str, int],
    stats_lock: threading.Lock,
    event_sink: _RuntimeEventSink,
    control_registry: _WorkerControlRegistry,
) -> dict[str, Any]:
    _register_worker_ipc_server(
        scheduler=scheduler,
        health_monitor=health_monitor,
        stats=stats,
        stats_lock=stats_lock,
        event_sink=event_sink,
        control_registry=control_registry,
    )
    authkey = os.urandom(24)
    manager = _WorkerIPCServerManager(address=("127.0.0.1", 0), authkey=authkey)
    server = manager.get_server()
    threading.Thread(target=server.serve_forever, name="dm-worker-ipc", daemon=True).start()
    host, port = server.address
    return {
        "host": str(host or "127.0.0.1"),
        "port": int(port or 0),
        "authkey_b64": base64.b64encode(authkey).decode("ascii"),
    }


def connect_worker_ipc(worker_cfg: MutableMapping[str, Any]) -> dict[str, Any]:
    ipc_cfg = dict(worker_cfg.get("ipc") or {})
    host = str(ipc_cfg.get("host") or "127.0.0.1").strip() or "127.0.0.1"
    port = int(ipc_cfg.get("port") or 0)
    authkey_b64 = str(ipc_cfg.get("authkey_b64") or "").strip()
    if port <= 0 or not authkey_b64:
        raise ValueError("worker config missing ipc connection details")
    _register_worker_ipc_client()
    manager = _WorkerIPCClientManager(
        address=(host, port),
        authkey=base64.b64decode(authkey_b64.encode("ascii")),
    )
    manager.connect()
    return {
        "manager": manager,
        "scheduler": manager.get_scheduler(),
        "health_monitor": manager.get_health_monitor(),
        "stats": manager.get_stats(),
        "stats_lock": manager.get_stats_lock(),
        "event_sink": manager.get_event_sink(),
        "control_registry": manager.get_control_registry(),
    }


def _worker_accounts_from_cfg(worker_cfg: MutableMapping[str, Any]) -> list[Dict[str, Any]]:
    return [dict(item) for item in (worker_cfg.get("accounts") or []) if isinstance(item, dict)]


def _worker_primary_account(worker_cfg: MutableMapping[str, Any]) -> str:
    for account in _worker_accounts_from_cfg(worker_cfg):
        username = str(account.get("username") or "").strip()
        if username:
            return username
    return ""


def _worker_profile_dir_for_manifest(worker_cfg: MutableMapping[str, Any]) -> str:
    username = _worker_primary_account(worker_cfg)
    if not username:
        return ""
    return str(_account_storage_state_path(username).parent)


def write_worker_manifest(worker_cfg: MutableMapping[str, Any], *, pid: int | None = None) -> Path:
    worker_id = str(worker_cfg.get("worker_id") or "").strip() or "worker"
    WORKER_MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = WORKER_MANIFEST_DIR / f"{worker_id}.json"
    payload = {
        "worker_id": worker_id,
        "account": _worker_primary_account(worker_cfg),
        "pid": int(pid or os.getpid()),
        "profile_dir": _worker_profile_dir_for_manifest(worker_cfg),
        "proxy_id": str(worker_cfg.get("proxy_id") or "").strip(),
        "started_at": datetime.utcnow().isoformat(),
    }
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def build_proxy_worker_from_config(
    worker_cfg: MutableMapping[str, Any],
    *,
    scheduler: AdaptiveScheduler,
    health_monitor: HealthMonitor,
    stats: Dict[str, int],
    stats_lock: threading.Lock,
    runtime_event_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ProxyWorker:
    template_rotator = TemplateRotator(list(worker_cfg.get("template_variants") or []))
    return ProxyWorker(
        worker_id=str(worker_cfg.get("worker_id") or "").strip(),
        network_key=str(worker_cfg.get("network_key") or "").strip(),
        proxy_id=str(worker_cfg.get("proxy_id") or "").strip(),
        accounts=_worker_accounts_from_cfg(worker_cfg),
        all_proxy_ids=[str(item or "").strip() for item in (worker_cfg.get("all_proxy_ids") or []) if str(item or "").strip()],
        scheduler=scheduler,
        health_monitor=health_monitor,
        stats=stats,
        stats_lock=stats_lock,
        delay_min=_as_int(worker_cfg.get("delay_min", 0), default=0, minimum=0),
        delay_max=_as_int(worker_cfg.get("delay_max", 0), default=0, minimum=0),
        template_rotator=template_rotator,
        cooldown_fail_threshold=_as_int(worker_cfg.get("cooldown_fail_threshold", 1), default=1, minimum=1),
        campaign_alias=str(worker_cfg.get("campaign_alias") or "").strip(),
        leads_alias=str(worker_cfg.get("leads_alias") or "").strip(),
        campaign_run_id=str(worker_cfg.get("campaign_run_id") or "").strip(),
        runtime_event_callback=runtime_event_callback,
        headless=bool(worker_cfg.get("headless", True)),
        send_flow_timeout_seconds=_as_float(
            worker_cfg.get("send_flow_timeout_seconds", 10.0),
            default=10.0,
            minimum=10.0,
        ),
        visible_browser_layout=dict(worker_cfg.get("visible_browser_layout") or {}) or None,
        active_account_limit=_as_int(worker_cfg.get("active_account_limit", 1), default=1, minimum=1),
        session_close_timeout_seconds=_as_float(
            worker_cfg.get("session_close_timeout_seconds", 10.0),
            default=10.0,
            minimum=1.0,
        ),
    )


def run_proxy_worker_from_config(
    worker_cfg: MutableMapping[str, Any],
    *,
    scheduler: AdaptiveScheduler,
    health_monitor: HealthMonitor,
    stats: Dict[str, int],
    stats_lock: threading.Lock,
    runtime_event_callback: Callable[[dict[str, Any]], None] | None = None,
    control_registry: Any = None,
) -> None:
    worker = build_proxy_worker_from_config(
        worker_cfg,
        scheduler=scheduler,
        health_monitor=health_monitor,
        stats=stats,
        stats_lock=stats_lock,
        runtime_event_callback=runtime_event_callback,
    )
    worker_id = worker.worker_id
    monitor_stop = threading.Event()

    def _watch_control() -> None:
        while not monitor_stop.is_set():
            try:
                reason = (
                    str(control_registry.consume_stop_request(worker_id) or "").strip()
                    if control_registry is not None
                    else ""
                )
            except Exception:
                reason = "control_registry_unavailable"
            if reason:
                worker.request_stop(reason)
                return
            monitor_stop.wait(_WORKER_IPC_POLL_INTERVAL_SECONDS)

    control_thread = threading.Thread(
        target=_watch_control,
        name=f"{worker_id}-control",
        daemon=True,
    )
    control_thread.start()
    try:
        worker.run()
    finally:
        monitor_stop.set()
        if control_registry is not None:
            try:
                control_registry.clear_worker(worker_id)
            except Exception:
                pass


def spawn_worker_process(worker_cfg: MutableMapping[str, Any]) -> tuple[subprocess.Popen[Any], Path]:
    WORKER_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        prefix=f"{str(worker_cfg.get('worker_id') or 'worker')}-",
        suffix=".json",
        dir=str(WORKER_CONFIG_DIR),
        text=True,
    )
    cfg_path = Path(temp_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(dict(worker_cfg), handle, ensure_ascii=False, indent=2)
        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        process = subprocess.Popen(
            [sys.executable, "-m", "src.dm_campaign.worker_process", str(cfg_path)],
            creationflags=creationflags,
            cwd=str(REPO_ROOT),
        )
        return process, cfg_path
    except Exception:
        try:
            cfg_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def cleanup_worker_process_slot(slot: MutableMapping[str, Any]) -> None:
    cfg_path = slot.get("config_path")
    if cfg_path:
        try:
            Path(str(cfg_path)).unlink(missing_ok=True)
        except Exception:
            pass


=======
>>>>>>> origin/main
class ProxyWorker:
    def __init__(
        self,
        *,
        worker_id: str,
        network_key: str = "",
        proxy_id: str,
        accounts: list[Dict[str, Any]],
        all_proxy_ids: list[str],
        scheduler: AdaptiveScheduler,
        health_monitor: HealthMonitor,
        stats: Dict[str, int],
        stats_lock: threading.Lock,
        delay_min: int,
        delay_max: int,
        template_rotator: TemplateRotator,
        cooldown_fail_threshold: int,
        campaign_alias: str,
        leads_alias: str,
        campaign_run_id: str,
        runtime_event_callback: Callable[[dict[str, Any]], None] | None = None,
        headless: bool,
        send_flow_timeout_seconds: float,
        visible_browser_layout: Optional[Dict[str, Any]] = None,
<<<<<<< HEAD
        active_account_limit: int = 6,
        session_close_timeout_seconds: float = 10.0,
=======
>>>>>>> origin/main
    ) -> None:
        self.worker_id = worker_id
        self.network_key = _normalize_effective_network_key(network_key or proxy_id)
        runtime_proxy_id = str(proxy_id or _runtime_proxy_id_from_network_key(self.network_key)).strip()
        self.proxy_id = runtime_proxy_id or LOCAL_WORKER_PROXY_ID
        self._is_local_worker = _is_local_proxy_id(self.proxy_id)
        self.accounts = accounts
        self.browser = None
        self.context = None
        self.lead_queue = scheduler
        self.delay_min = max(0, int(delay_min))
        self.delay_max = max(self.delay_min, int(delay_max))
        self._template_rotator = template_rotator
        self._scheduler = scheduler
        self._health = health_monitor
        self._stats = stats
        self._stats_lock = stats_lock
        self._campaign_alias = str(campaign_alias or "").strip().lower()
        self._leads_alias = str(leads_alias or "").strip().lower()
        self._campaign_run_id = str(campaign_run_id or "").strip()
        self._all_network_keys = [
            _normalize_effective_network_key(item)
            for item in all_proxy_ids
            if str(item or "").strip()
        ]
        self._cooldown_fail_threshold = max(1, int(cooldown_fail_threshold))
        self._send_flow_timeout_seconds = max(10.0, float(send_flow_timeout_seconds or 10.0))
<<<<<<< HEAD
        self._active_account_limit = max(1, int(active_account_limit or 1))
        self._session_close_timeout_seconds = max(1.0, float(session_close_timeout_seconds or 1.0))
=======
>>>>>>> origin/main
        self._runtime_event_callback = runtime_event_callback
        self._visible_browser_layout = (
            dict(visible_browser_layout or {})
            if (not headless and isinstance(visible_browser_layout, dict))
            else {}
        )
<<<<<<< HEAD
        self._campaign_desktop_layout = _campaign_desktop_layout_payload()
        self._sender = HumanInstagramSender(
            headless=headless,
            keep_browser_open_per_account=True,
            allow_header_thread_confirmation=True,
            enforce_account_quota=False,
=======
        self._sender = HumanInstagramSender(
            headless=headless,
            keep_browser_open_per_account=True,
>>>>>>> origin/main
        )
        self._sender_close_lock = threading.Lock()
        self._sender_closed = False
        self._worker_state = CampaignWorkerStateMachine(
            max_busy_seconds=max(20.0, self._send_flow_timeout_seconds + 10.0)
        )
        self._states: list[AccountRuntimeState] = []
        for account in _order_accounts_for_worker_start(accounts):
            if not isinstance(account, dict):
                continue
            username = str(account.get("username") or "").strip()
            if not username:
                continue
            limit = _resolve_account_message_limit(account)
<<<<<<< HEAD
            account_payload = dict(account)
            sent_today = _resolve_account_sent_today(account_payload)
            account_payload["effective_network_key"] = _effective_network_key_for_account(account_payload) or self.network_key
            account_payload["runtime_proxy_id"] = _runtime_proxy_id_for_account(account_payload) or self.proxy_id
            account_payload["campaign_desktop_layout"] = dict(self._campaign_desktop_layout)
            if self._visible_browser_layout:
                account_payload["visible_browser_layout"] = {
                    **self._visible_browser_layout,
                    **self._campaign_desktop_layout,
=======
            sent_today = _resolve_account_sent_today(account)
            account_payload = dict(account)
            account_payload["effective_network_key"] = _effective_network_key_for_account(account_payload) or self.network_key
            account_payload["runtime_proxy_id"] = _runtime_proxy_id_for_account(account_payload) or self.proxy_id
            if self._visible_browser_layout:
                account_payload["visible_browser_layout"] = {
                    **self._visible_browser_layout,
>>>>>>> origin/main
                    "worker_id": self.worker_id,
                    "proxy_id": self.proxy_id,
                    "network_key": self.network_key,
                }
                account_payload["manual_visible_browser"] = True
<<<<<<< HEAD
                account_payload["playwright_browser_mode"] = PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY
=======
                account_payload["playwright_browser_mode"] = PLAYWRIGHT_BROWSER_MODE_MANAGED
>>>>>>> origin/main
            state = AccountRuntimeState(
                account=account_payload,
                max_messages=limit,
                sent_count=sent_today,
                disabled_for_campaign=sent_today >= limit,
                session_ready=_account_has_storage_state(account),
            )
            state.account["sent_today"] = sent_today
            self._states.append(state)
        self._rotation_cursor = 0
<<<<<<< HEAD
        self._next_activation_index = 0
        self._stop_event = threading.Event()
        self._proxy_status_cache = "healthy" if self._is_local_worker else self._health.proxy_status(self.network_key)
        self._last_selected_account = ""
        self._fill_active_account_window(reason="worker_start")
=======
        self._stop_event = threading.Event()
        self._proxy_status_cache = "healthy" if self._is_local_worker else self._health.proxy_status(self.proxy_id)
        self._last_selected_account = ""
>>>>>>> origin/main

    def _log(self, level: str, message: str, *args: Any, exc_info: bool = False) -> None:
        log_method = getattr(logger, level)
        if level == "exception" and not exc_info:
            exc_info = True
        log_method(
            f"[run_id=%s worker=%s proxy=%s] {message}",
            self._campaign_run_id or "-",
            self.worker_id,
            self.proxy_id,
            *args,
            exc_info=exc_info,
        )

    def _emit_runtime_event(
        self,
        event_type: str,
        *,
        severity: str = "info",
        failure_kind: str = "",
        message: str,
        **payload: Any,
    ) -> None:
        if not callable(self._runtime_event_callback):
            return
        self._runtime_event_callback(
            {
                "run_id": self._campaign_run_id,
                "event_type": str(event_type or "").strip(),
                "severity": str(severity or "info").strip().lower() or "info",
                "failure_kind": str(failure_kind or "").strip().lower(),
                "message": str(message or "").strip(),
                "worker_id": self.worker_id,
                "proxy_id": self.proxy_id,
                "network_key": self.network_key,
                **payload,
            }
        )

    def _report_storage_failure(
        self,
        *,
        event_type: str,
        message: str,
        exc: Exception,
        failure_kind: str = "system",
        **payload: Any,
    ) -> None:
        self._log("exception", "%s", message, exc_info=True)
        self._emit_runtime_event(
            event_type,
            severity="error",
            failure_kind=failure_kind,
            message=message,
            error=str(exc) or exc.__class__.__name__,
            **payload,
        )

    def request_stop(self, reason: str = "") -> None:
        if reason:
            self._log("info", "stop solicitado (%s).", reason)
        self._stop_event.set()
        self._transition_state(self._worker_state.set_stopping(reason=reason or "stop_requested"))
<<<<<<< HEAD
        self._close_sender_sessions(timeout=self._session_close_timeout_seconds)

    @staticmethod
    def _account_username(state: AccountRuntimeState) -> str:
        return str(state.account.get("username") or "").strip()

    def _close_account_session(self, username: str, *, reason: str) -> None:
        clean_username = str(username or "").strip()
        if not clean_username:
            return
        try:
            self._sender.close_account_session_sync(
                clean_username,
                timeout=self._session_close_timeout_seconds,
            )
        except Exception as exc:
            self._report_storage_failure(
                event_type="account_session_close_failed",
                message=f"No se pudo cerrar la sesion cacheada de @{clean_username}.",
                exc=exc,
                account=clean_username,
                reason=reason,
            )

    def _fill_active_account_window(self, *, reason: str) -> None:
        active_count = sum(1 for state in self._states if state.active_in_worker and not state.disabled_for_campaign)
        while active_count < self._active_account_limit and self._next_activation_index < len(self._states):
            state = self._states[self._next_activation_index]
            self._next_activation_index += 1
            if state.disabled_for_campaign or state.active_in_worker:
                continue
            state.active_in_worker = True
            state.retired_reason = ""
            active_count += 1
            username = self._account_username(state)
            self._log(
                "info",
                "Cuenta @%s activada en worker window (%d/%d).",
                username or "-",
                active_count,
                self._active_account_limit,
            )
            self._emit_runtime_event(
                "worker_account_activated",
                message=f"Cuenta @{username or '-'} activada en worker {self.worker_id}.",
                account=username,
                reason=reason,
                window_active=active_count,
                window_limit=self._active_account_limit,
            )

    def _retire_account(self, state: AccountRuntimeState, *, reason: str, close_session: bool = True) -> None:
        username = self._account_username(state)
        state.active_in_worker = False
        state.disabled_for_campaign = True
        state.retired_reason = str(reason or "account_retired").strip() or "account_retired"
        state.next_send_time = 0.0
        state.cooldown_until = 0.0
        if close_session and username:
            self._close_account_session(username, reason=state.retired_reason)
        self._log(
            "warning",
            "Cuenta @%s retirada del run (%s).",
            username or "-",
            state.retired_reason,
        )
        self._emit_runtime_event(
            "worker_account_retired",
            severity="warning",
            failure_kind="retryable",
            message=f"Cuenta @{username or '-'} retirada del run actual.",
            account=username,
            reason=state.retired_reason,
        )
        self._fill_active_account_window(reason=f"replace:{state.retired_reason}")
=======
        self._close_sender_sessions()
>>>>>>> origin/main

    def _stop_requested(self) -> bool:
        return STOP_EVENT.is_set() or self._stop_event.is_set()

    def _wait_briefly(self, seconds: float) -> None:
        remaining = max(0.0, float(seconds))
        while remaining > 0:
            if self._stop_requested():
                return
            step = min(0.10, remaining)
            time.sleep(step)
            remaining = max(0.0, remaining - step)

    def _requeue_task_for_stop(self, task: LeadTask, *, reason: str) -> None:
        self._scheduler.push_task(task)
        self._transition_state(self._worker_state.set_stopping(reason=reason or "stop_requested"))

    def _sender_stage_callback(self, stage: str, payload: Dict[str, Any]) -> None:
        stage_name = str(stage or "").strip().lower()
        lead = str(payload.get("lead") or "").strip()
        account = str(payload.get("account") or "").strip()
        reason = str(payload.get("reason") or stage_name or "").strip()
<<<<<<< HEAD
        if stage_name == "flow_stage":
            self._log_lead_stage(
                lead=lead,
                stage_name=str(payload.get("stage_name") or "").strip() or "flow_stage",
                elapsed_ms=payload.get("elapsed_ms"),
                url=str(payload.get("url") or ""),
                title=str(payload.get("title") or ""),
                selector=str(payload.get("selector") or ""),
                outcome=str(payload.get("outcome") or ""),
            )
            self._heartbeat(sent=False)
            return
=======
>>>>>>> origin/main
        if stage_name == "opening_dm":
            self._transition_state(
                self._worker_state.set_opening_dm(
                    lead=lead,
                    account=account,
                    reason=reason or "open_outbound_dm",
                )
            )
            return
        if stage_name == "sending":
            self._transition_state(
                self._worker_state.set_sending(
                    lead=lead,
                    account=account,
                    reason=reason or "send_message",
                )
            )
            return
        self._heartbeat(sent=False)

<<<<<<< HEAD
    def _log_lead_stage(
        self,
        *,
        lead: str,
        stage_name: str,
        elapsed_ms: Any = 0,
        url: str = "",
        title: str = "",
        selector: str = "",
        outcome: str = "",
    ) -> None:
        clean_lead = str(lead or "").strip().lstrip("@") or "-"
        clean_stage = str(stage_name or "").strip() or "-"
        clean_url = str(url or "").strip() or "-"
        clean_title = str(title or "").strip() or "-"
        clean_selector = str(selector or "").strip() or "-"
        clean_outcome = str(outcome or "").strip() or "-"
        try:
            elapsed_value = max(0, int(float(elapsed_ms or 0)))
        except Exception:
            elapsed_value = 0
        logger.info(
            "[run_id=%s worker=%s lead=@%s] stage=%s elapsed_ms=%d url=%s title=%s selector=%s outcome=%s",
            self._campaign_run_id or "-",
            self.worker_id,
            clean_lead,
            clean_stage,
            elapsed_value,
            clean_url,
            clean_title,
            clean_selector,
            clean_outcome,
        )

=======
>>>>>>> origin/main
    def _transition_state(self, snapshot: WorkerStateSnapshot) -> WorkerStateSnapshot:
        self._scheduler.update_worker_activity(
            self.worker_id,
            sent=False,
            proxy_id=self.network_key,
            execution_state=snapshot.state,
            execution_stage=snapshot.stage,
            lead=snapshot.lead,
            account=snapshot.account,
            reason=snapshot.reason,
        )
        return snapshot

    def _heartbeat(self, *, sent: bool = False) -> None:
        snapshot = self._worker_state.snapshot()
        self._scheduler.update_worker_activity(
            self.worker_id,
            sent=sent,
            proxy_id=self.network_key,
            execution_state=snapshot.state,
            execution_stage=snapshot.stage,
            lead=snapshot.lead,
            account=snapshot.account,
            reason=snapshot.reason,
        )

    def _proxy_status(self, *, now: Optional[float] = None) -> str:
        if self._is_local_worker:
            return "healthy"
<<<<<<< HEAD
        return self._health.proxy_status(self.network_key, now=now)
=======
        return self._health.proxy_status(self.proxy_id, now=now)
>>>>>>> origin/main

    def _record_health_success(self, username: str, response_time: float) -> None:
        if self._is_local_worker:
            self._health.record_account_success(username, response_time)
            return
<<<<<<< HEAD
        self._health.record_send_success(self.network_key, username, response_time)
=======
        self._health.record_send_success(self.proxy_id, username, response_time)
>>>>>>> origin/main

    def _record_health_failure(
        self,
        username: str,
        reason: str,
        *,
        is_login_error: bool,
        response_time: float,
    ) -> None:
        if self._is_local_worker:
            self._health.record_account_error(
                username,
                reason,
                is_login_error=is_login_error,
                response_time=response_time,
            )
            return
        if is_login_error:
            self._health.record_login_error(
<<<<<<< HEAD
                self.network_key,
=======
                self.proxy_id,
>>>>>>> origin/main
                username,
                reason,
                response_time=response_time,
            )
            return
        self._health.record_send_error(
<<<<<<< HEAD
            self.network_key,
=======
            self.proxy_id,
>>>>>>> origin/main
            username,
            reason,
            response_time=response_time,
        )

    def busy_age(self, now: Optional[float] = None) -> float:
        return self._worker_state.busy_age(now=now)

    def is_busy(self, now: Optional[float] = None) -> bool:
        return self._worker_state.is_busy(now=now)

    def execution_state(self) -> WorkerExecutionState:
        return self._worker_state.execution_state()

    def execution_stage(self) -> WorkerExecutionStage:
        return self._worker_state.execution_stage()

    def has_schedulable_accounts(self, now: Optional[float] = None) -> bool:
        # "Schedulable" means at least one account can still run in this worker.
        # It intentionally ignores next_send_time to avoid false idle/restart loops
        # while accounts are waiting their configured delay window.
<<<<<<< HEAD
        self._fill_active_account_window(reason="schedulable_check")
        ts = time.time() if now is None else float(now)
        for state in self._states:
            if not state.active_in_worker:
                continue
            if state.disabled_for_campaign:
                continue
            if self._account_reached_limit(state):
                self._retire_account(state, reason="account_quota_reached")
=======
        ts = time.time() if now is None else float(now)
        for state in self._states:
            if state.disabled_for_campaign:
                continue
            if self._account_reached_limit(state):
                state.disabled_for_campaign = True
>>>>>>> origin/main
                continue
            username = str(state.account.get("username") or "").strip()
            if state.cooldown_until > ts:
                continue
            if not self._health.is_account_available(username, now=ts):
                remaining = self._health.account_cooldown_remaining(username, now=ts)
                state.cooldown_until = ts + remaining
                continue
            return True
        return False

    def run(self) -> None:
        self._log("info", "iniciado")
        self._log("info", "usando %d cuentas", len(self._states))
        self._transition_state(self._worker_state.set_idle(reason="worker_start"))
        try:
            while not self._stop_requested():
                proxy_status = self._proxy_status()
                self._log_proxy_status_change(proxy_status)
                if proxy_status == "blocked":
                    task = self._scheduler.pop_task_for_proxy(self.network_key)
                    if task is not None:
                        self._transition_state(self._worker_state.set_blocked_proxy(reason="proxy_blocked"))
                        self._handle_blocked_proxy_task(task)
                        self._heartbeat(sent=False)
                        continue
                    if self._scheduler.is_empty():
                        break
                    self._transition_state(self._worker_state.set_blocked_proxy(reason="proxy_blocked"))
                    self._heartbeat(sent=False)
                    self._wait_briefly(0.35)
                    continue

                task = self._scheduler.pop_task_for_proxy(self.network_key)
                if task is None:
                    if self._scheduler.is_empty():
                        break
                    self._transition_state(self._worker_state.set_waiting_queue(reason="queue_poll"))
                    self._heartbeat(sent=False)
                    self._wait_briefly(0.20)
                    continue

                sent = self._process_task(task)
                self._heartbeat(sent=sent)
        except Exception:
            self._log("exception", "worker crash", exc_info=True)
            self._emit_runtime_event(
                "worker_crashed",
                severity="error",
                failure_kind="system",
                message="Worker crasheo durante la ejecucion.",
            )
            raise
        finally:
            self._close_sender_sessions()

    def _process_task(self, task: LeadTask) -> bool:
        self._transition_state(self._worker_state.set_waiting_account(lead=task.lead, reason="select_account"))
        try:
            if self._stop_requested():
                self._requeue_task_for_stop(task, reason="stop_before_account_selection")
                return False

            account_state = self._next_ready_account(task)
            if account_state is None:
                wait_decision = self._next_account_wait_decision(task)
                if wait_decision is not None:
                    self._scheduler.push_task(task)
                    sleep_for = min(30.0, max(0.15, float(wait_decision.seconds)))
                    self._sleep_with_heartbeat(
                        sleep_for,
                        stage=wait_decision.stage,
                        reason=wait_decision.reason,
                    )
                    return False
                self._handle_no_account_available(task)
                return False

            account = account_state.account
            username = str(account.get("username") or "").strip()
            if not username:
                account_state.disabled_for_campaign = True
                self._mark_task_failed(task, reason="account_missing_username")
                return False

            self._log("info", "LeadQueue: %d", self._scheduler.queue_size())
            self._log("info", "[QUEUE] worker picked lead: %s", task.lead)
            self._log("info", "Lead tomado @%s con @%s", task.lead, username)
<<<<<<< HEAD
            self._log_lead_stage(
                lead=task.lead,
                stage_name="account_selected",
                outcome=f"account:@{username}",
            )
=======
>>>>>>> origin/main
            if self._last_selected_account and self._last_selected_account != username:
                _print_info_block(
                    "RotaciÃ³n de cuenta",
                    [f"Siguiente cuenta seleccionada: {username}"],
                )
            self._last_selected_account = username

            self._transition_state(
                self._worker_state.set_opening_session(
                    lead=task.lead,
                    account=username,
                    reason="ensure_session",
                )
            )
<<<<<<< HEAD
            session_started = time.perf_counter()
            if not self._ensure_session(account_state):
                self._log_lead_stage(
                    lead=task.lead,
                    stage_name="session_preflight",
                    elapsed_ms=(time.perf_counter() - session_started) * 1000.0,
                    outcome=account_state.preflight_failure_reason or "failed",
                )
=======
            if not self._ensure_session(account_state):
>>>>>>> origin/main
                if self._stop_requested():
                    self._requeue_task_for_stop(task, reason="stop_during_session_open")
                    return False
                session_failure_reason = account_state.preflight_failure_reason or "login_failed"
<<<<<<< HEAD
                self._handle_account_unavailable(
                    task=task,
                    account_state=account_state,
                    reason=session_failure_reason,
                )
                return False
            self._log_lead_stage(
                lead=task.lead,
                stage_name="session_preflight",
                elapsed_ms=(time.perf_counter() - session_started) * 1000.0,
                outcome="ok",
            )
=======
                self._handle_failure(
                    task=task,
                    account_state=account_state,
                    reason=session_failure_reason,
                    is_login_error=self._session_failure_is_login_error(session_failure_reason),
                    response_time=0.0,
                )
                return False
>>>>>>> origin/main

            message = self._render_message_for_lead(account_state.account, task.lead)
            if not message:
                self._mark_task_failed(task, reason="template_empty")
                return False

            if self._stop_requested():
                self._requeue_task_for_stop(task, reason="stop_before_send")
                return False

            self._log("info", "Abriendo DM a @%s con @%s", task.lead, username)
            self._transition_state(
                self._worker_state.set_opening_dm(
                    lead=task.lead,
                    account=username,
                    reason="open_outbound_dm",
                )
            )
            self._log("info", "Enviando primer mensaje a @%s con @%s", task.lead, username)
            started = time.time()
            try:
                send_result = self._sender.send_message_like_human_sync(
                    account=account_state.account,
                    target_username=task.lead,
                    text=message,
                    base_delay_seconds=0.0,
                    jitter_seconds=0.0,
                    return_detail=True,
                    return_payload=True,
                    flow_timeout_seconds=self._send_flow_timeout_seconds,
                    stage_callback=self._sender_stage_callback,
                )
                parsed_result = CampaignSendResult.from_sender_result(send_result)
            except PlaywrightRuntimeCancelledError:
                if self._stop_requested():
                    self._requeue_task_for_stop(task, reason="stop_during_send")
                    return False
                parsed_result = CampaignSendResult(
                    ok=False,
                    detail="send_cancelled",
                    payload={"reason_code": "SEND_CANCELLED"},
                )
            except PlaywrightRuntimeTimeoutError:
                parsed_result = CampaignSendResult(
                    ok=False,
                    detail="send_deadline_exceeded",
                    payload={"reason_code": "FLOW_TIMEOUT"},
                )
            except Exception as exc:
                if self._stop_requested():
                    self._requeue_task_for_stop(task, reason="stop_during_send")
                    return False
                parsed_result = CampaignSendResult(
                    ok=False,
                    detail=str(exc),
                    payload={},
                )
            elapsed = max(0.0, time.time() - started)
<<<<<<< HEAD
            self._log_lead_stage(
                lead=task.lead,
                stage_name="send_flow",
                elapsed_ms=elapsed * 1000.0,
                outcome=parsed_result.detail or ("ok" if parsed_result.ok else "failed"),
            )
=======
>>>>>>> origin/main

            if parsed_result.ok:
                self._handle_success(
                    task,
                    account_state,
                    detail=parsed_result.detail or "ok",
                    response_time=elapsed,
                    result=parsed_result,
                )
                return True

            if self._stop_requested():
                self._requeue_task_for_stop(task, reason="stop_after_send_attempt")
                return False

            reason = _campaign_failure_reason(parsed_result)
            self._handle_failure(
                task=task,
                account_state=account_state,
                reason=reason,
                is_login_error=False,
                response_time=elapsed,
            )
            return False
        finally:
            if self._stop_requested():
                self._transition_state(self._worker_state.set_stopping(reason="stop_requested"))
            else:
                self._transition_state(self._worker_state.set_idle(reason="task_complete"))

    def _handle_success(
        self,
        task: LeadTask,
        account_state: AccountRuntimeState,
        *,
        detail: str,
        response_time: float,
        result: CampaignSendResult,
    ) -> None:
        username = str(account_state.account.get("username") or "").strip()
        is_confirmed_send = (
            result.status == CampaignSendStatus.SENT
            and not bool(result.payload.get("sent_unverified"))
        )
        is_unverified_send = (
            result.status == CampaignSendStatus.AMBIGUOUS
            or bool(result.payload.get("sent_unverified"))
        )
        account_state.sent_count += 1
        account_state.account["sent_today"] = account_state.sent_count
        account_state.fail_count = 0
        account_state.cooldown_until = 0.0
        account_state.next_send_time = time.time() + random.uniform(self.delay_min, self.delay_max)
        if account_state.sent_count >= account_state.max_messages:
<<<<<<< HEAD
            self._retire_account(account_state, reason="account_quota_reached")
=======
            account_state.disabled_for_campaign = True
>>>>>>> origin/main

        self._record_health_success(username, response_time)
        self._log_proxy_status_change(self._proxy_status())
        mark_connected(username, True, invalidate_health=False)

        with self._stats_lock:
            self._stats["sent"] = int(self._stats.get("sent", 0)) + 1

        try:
<<<<<<< HEAD
            if is_confirmed_send or is_unverified_send:
=======
            if is_confirmed_send:
>>>>>>> origin/main
                mark_lead_sent(task.lead, sent_by=username, alias=self._campaign_alias)
        except Exception as exc:
            self._report_storage_failure(
                event_type="lead_status_write_failed",
<<<<<<< HEAD
                message="No se pudo persistir lead_status global para un envio campaign.",
=======
                message="No se pudo persistir lead_status global para un envio confirmado.",
>>>>>>> origin/main
                exc=exc,
                account=username,
                lead=task.lead,
            )
        try:
            log_sent(
                username,
                task.lead,
                True,
                detail,
                verified=bool(result.verified or is_confirmed_send),
                sent_unverified=is_unverified_send,
                duration_ms=int(max(0.0, response_time) * 1000),
                source_engine="campaign",
                campaign_alias=self._campaign_alias,
                leads_alias=self._leads_alias,
                run_id=self._campaign_run_id,
            )
        except Exception as exc:
            self._report_storage_failure(
                event_type="sent_log_write_failed",
                message="No se pudo persistir sent_log para un envio campaign.",
                exc=exc,
                account=username,
                lead=task.lead,
            )

        delay_left = max(0.0, account_state.next_send_time - time.time())
        delay_applied_seconds = max(0, int(round(delay_left)))
<<<<<<< HEAD
        self._log_lead_stage(
            lead=task.lead,
            stage_name="delay_applied",
            elapsed_ms=delay_left * 1000.0,
            outcome="scheduled",
        )
        self._log_lead_stage(
            lead=task.lead,
            stage_name="lead_finalization",
            outcome=detail,
        )
=======
>>>>>>> origin/main
        self._log("info", "Enviado @%s -> @%s (%s)", username, task.lead, detail)
        _print_send_block(
            account=username,
            lead=task.lead,
            delay_seconds=delay_applied_seconds,
            proxy_id=self.proxy_id,
        )

<<<<<<< HEAD
    def _handle_account_unavailable(
        self,
        *,
        task: LeadTask,
        account_state: AccountRuntimeState,
        reason: str,
    ) -> None:
        username = str(account_state.account.get("username") or "").strip()
        reason_text = str(reason or "account_unavailable").strip() or "account_unavailable"
        is_login_error = self._session_failure_is_login_error(reason_text)
        if is_login_error and username:
            mark_connected(username, False, invalidate_health=False)
        self._retire_account(account_state, reason=reason_text)
        if self._has_candidate_account_for_task(task):
            self._scheduler.push_task(task)
            self._log(
                "info",
                "Lead @%s reencolado por cuenta no usable @%s (%s).",
                task.lead,
                username or "-",
                reason_text,
            )
            _print_info_block(
                "Cuenta descartada del run",
                [
                    f"Cuenta: {username or '-'}",
                    f"Motivo: {self._humanize_reason(reason_text)}",
                    f"Lead reencolado: {task.lead}",
                ],
            )
            return
        self._handle_no_account_available(task)

=======
>>>>>>> origin/main
    def _handle_failure(
        self,
        *,
        task: LeadTask,
        account_state: AccountRuntimeState,
        reason: str,
        is_login_error: bool,
        response_time: float,
    ) -> None:
        username = str(account_state.account.get("username") or "").strip()
        reason_text = str(reason or "send_failed").strip() or "send_failed"
        reason_upper = self._normalize_failure_reason(reason_text)
        if reason_upper == "ACCOUNT_QUOTA_REACHED":
            account_state.sent_count = max(account_state.sent_count, account_state.max_messages)
            account_state.account["sent_today"] = account_state.sent_count
<<<<<<< HEAD
            self._retire_account(account_state, reason="account_quota_reached")
            self._handle_no_account_available(task)
            return
        if self._is_terminal_account_failure(reason_upper):
            self._handle_account_unavailable(
                task=task,
                account_state=account_state,
                reason=reason_text,
            )
            return
=======
            account_state.disabled_for_campaign = True
            self._handle_no_account_available(task)
            return
>>>>>>> origin/main
        if self._try_transient_same_proxy_retry(
            task=task,
            account_state=account_state,
            reason_upper=reason_upper,
            reason_text=reason_text,
        ):
            return

        if self._is_non_retryable_lead_failure(reason_upper):
            account_state.fail_count = 0
            account_state.cooldown_until = 0.0
            account_state.next_send_time = time.time() + 0.25
            self._log("info", "Lead @%s descartado sin retry (%s) usando @%s.", task.lead, reason_text, username)
            self._mark_task_failed(task, reason=reason_text, account_username=username)
            return

        account_state.fail_count += 1

        if is_login_error:
            mark_connected(username, False, invalidate_health=False)
        elif username:
            # Un error de envio no implica sesion rota; evitar desconectar cuentas sanas.
            mark_connected(username, True, invalidate_health=False)

        if is_login_error:
            self._record_health_failure(
                username,
                reason_text,
                is_login_error=True,
                response_time=response_time,
            )
        else:
            self._record_health_failure(
                username,
                reason_text,
                is_login_error=False,
                response_time=response_time,
            )

        if account_state.fail_count >= self._cooldown_fail_threshold:
            cooldown_until = self._health.set_account_cooldown(username, reason=reason_text)
            account_state.cooldown_until = cooldown_until
            account_state.fail_count = 0
            cooldown_seconds = max(0, int(cooldown_until - time.time()))
            self._log("warning", "Account cooldown: @%s en cooldown por %ss.", username, cooldown_seconds)
            _print_info_block(
                "Cuenta en cooldown",
                [
                    f"Cuenta: {username}",
                    f"Cooldown restante: {cooldown_seconds}s",
                ],
            )

        proxy_status = self._proxy_status()
        self._log_proxy_status_change(proxy_status)

        same_proxy_accounts = [
            str(state.account.get("username") or "")
            for state in self._states
            if isinstance(state.account, dict)
        ]
        retry_task = self._scheduler.build_retry_task(
            task,
            failed_proxy_id=self.network_key,
            failed_account_id=username,
            same_proxy_account_ids=same_proxy_accounts,
            all_proxy_ids=self._all_network_keys,
        )
        if retry_task is not None:
            self._scheduler.push_task(retry_task)
            with self._stats_lock:
                self._stats["retried"] = int(self._stats.get("retried", 0)) + 1
<<<<<<< HEAD
            self._log_lead_stage(
                lead=retry_task.lead,
                stage_name="lead_requeue",
                outcome=reason_text,
            )
=======
>>>>>>> origin/main
            self._log(
                "info",
                "Retry attempt: lead=@%s intento=%d proxy=%s",
                retry_task.lead,
                retry_task.attempt,
                retry_task.preferred_proxy_id or "any",
            )
            _print_info_block(
                "Lead reencolado",
                [
                    f"Lead: {retry_task.lead}",
                    f"Motivo: {self._humanize_reason(reason_text)}",
                    f"Intento: {retry_task.attempt}",
                ],
            )
            return

        self._mark_task_failed(task, reason=reason_text)

    def _try_transient_same_proxy_retry(
        self,
        *,
        task: LeadTask,
        account_state: AccountRuntimeState,
        reason_upper: str,
        reason_text: str,
    ) -> bool:
        if not self._is_transient_same_proxy_retry_reason(reason_upper):
            return False
        # Retry once on the same proxy/account to absorb startup race conditions.
        if task.attempt >= 2:
            return False

        retry_task = LeadTask(
            lead=task.lead,
            attempt=task.attempt + 1,
            preferred_proxy_id=self.network_key,
            excluded_accounts=tuple(),
            history=task.history + (f"{self.network_key}:{reason_upper}",),
        )
        account_state.fail_count = 0
        account_state.cooldown_until = 0.0
        account_state.next_send_time = time.time() + 2.0
        self._scheduler.push_task(retry_task)
        with self._stats_lock:
            self._stats["retried"] = int(self._stats.get("retried", 0)) + 1
        self._log(
            "info",
            "Retry transient: lead=@%s intento=%d reason=%s",
            retry_task.lead,
            retry_task.attempt,
            reason_text,
        )
        _print_info_block(
            "Lead reencolado",
            [
                f"Lead: {retry_task.lead}",
                f"Motivo: {self._humanize_reason(reason_text)}",
                f"Intento: {retry_task.attempt}",
            ],
        )
        return True

    def _sleep_with_heartbeat(
        self,
        seconds: float,
        *,
        stage: WorkerExecutionStage,
        reason: str,
    ) -> None:
        remaining = max(0.0, float(seconds))
        if self.execution_state() != WorkerExecutionState.STOPPING:
            if stage == WorkerExecutionStage.COOLDOWN:
                self._transition_state(self._worker_state.set_cooldown(reason=reason))
            else:
                self._transition_state(self._worker_state.set_waiting_queue(reason=reason))
        while remaining > 0:
            if self._stop_requested():
                return
            step = min(0.5, remaining)
            time.sleep(step)
            remaining = max(0.0, remaining - step)
            self._heartbeat(sent=False)

<<<<<<< HEAD
    def _close_sender_sessions(self, *, timeout: float | None = None) -> None:
=======
    def _close_sender_sessions(self) -> None:
>>>>>>> origin/main
        with self._sender_close_lock:
            if self._sender_closed:
                return
            self._sender_closed = True
        try:
<<<<<<< HEAD
            self._sender.close_all_sessions_sync(timeout=max(0.5, float(timeout or self._session_close_timeout_seconds)))
=======
            self._sender.close_all_sessions_sync(timeout=2.0)
>>>>>>> origin/main
        except Exception as exc:
            self._report_storage_failure(
                event_type="sender_close_failed",
                message="No se pudieron cerrar las sesiones del sender.",
                exc=exc,
            )

    @staticmethod
    def _normalize_failure_reason(reason: str) -> str:
        return str(reason or "").strip().upper()

    @staticmethod
    def _session_failure_is_login_error(reason: str) -> bool:
        normalized = str(reason or "").strip().upper()
        non_login_failures = {
            "INACTIVE",
            "ACCOUNT_COOLDOWN",
            "ACCOUNT_QUARANTINE",
            "HEALTH_BLOCKED",
            "NETWORK_IDENTITY_MISMATCH",
            "PROXY_INACTIVE",
            "PROXY_LEGACY",
            "PROXY_MISSING",
            "PROXY_QUARANTINED",
        }
        return normalized not in non_login_failures

    @staticmethod
    def _is_non_retryable_lead_failure(reason: str) -> bool:
        reason_upper = str(reason or "").strip().upper()
        if not reason_upper:
            return False
        if reason_upper.startswith("SKIPPED_") and reason_upper not in {"SKIPPED_UI_NOT_FOUND"}:
            return True
        terminal = {
            "SKIPPED_USERNAME_NOT_FOUND",
            "SKIPPED_NO_DM_OR_THREAD_BLOCKED",
            "SEND_UNVERIFIED_BLOCKED",
            "SENT_UNVERIFIED",
            "THREAD_OPEN_FAILED",
            "USERNAME_NOT_FOUND",
        }
        if reason_upper in terminal:
            return True
        return any(
            token in reason_upper
            for token in (
                "USERNAME_NOT_FOUND",
                "NO_RESULTS_FOUND",
            )
        )

    @staticmethod
<<<<<<< HEAD
    def _is_terminal_account_failure(reason: str) -> bool:
        reason_upper = str(reason or "").strip().upper()
        if not reason_upper:
            return False
        terminal = {
            "ACCOUNT_COOLDOWN",
            "ACCOUNT_QUARANTINE",
            "CHALLENGE_REQUIRED",
            "CHECKPOINT",
            "CONFIRM_EMAIL",
            "DISABLED",
            "DISCONNECTED",
            "HEALTH_BLOCKED",
            "LOGIN_FAILED",
            "NETWORK_IDENTITY_MISMATCH",
            "PROFILE_MODE_CONFLICT",
            "SESSION_OPEN_FAILED",
            "SESSION_OPEN_TIMEOUT",
            "STORAGE_STATE_INVALID",
            "STORAGE_STATE_MISSING",
            "SUSPENDED",
            "TWO_FACTOR",
        }
        if reason_upper in terminal:
            return True
        return reason_upper.startswith("PROXY_")

    @staticmethod
=======
>>>>>>> origin/main
    def _is_transient_same_proxy_retry_reason(reason: str) -> bool:
        reason_upper = str(reason or "").strip().upper()
        if not reason_upper:
            return False
        return "INBOX_NOT_READY" in reason_upper or "UI_NOT_FOUND" in reason_upper

    @staticmethod
    def _humanize_reason(reason: str) -> str:
        key = str(reason or "").strip()
        normalized = key.upper()
        mapping = {
<<<<<<< HEAD
            "ACCOUNT_COOLDOWN": "cuenta en cooldown",
            "ACCOUNT_QUARANTINE": "cuenta en cuarentena",
            "CHALLENGE_REQUIRED": "Instagram pidio challenge",
            "CHECKPOINT": "Instagram marco checkpoint",
            "CONFIRM_EMAIL": "Instagram pidio confirmar email",
            "DISABLED": "cuenta deshabilitada",
            "DISCONNECTED": "cuenta sin sesion conectada",
            "NETWORK_IDENTITY_MISMATCH": "la cuenta ya no pertenece a este worker",
            "PROFILE_MODE_CONFLICT": "el perfil esta en uso por otro navegador",
            "SESSION_OPEN_FAILED": "no se pudo abrir la sesion de la cuenta",
            "SESSION_OPEN_TIMEOUT": "timeout al abrir la sesion",
            "STORAGE_STATE_INVALID": "storage_state invalido",
            "STORAGE_STATE_MISSING": "la cuenta no tiene storage_state usable",
            "SUSPENDED": "cuenta suspendida",
            "TWO_FACTOR": "Instagram pidio 2FA",
=======
>>>>>>> origin/main
            "INBOX_NOT_READY": "inbox no disponible todavÃ­a",
            "LOGIN_FAILED": "fallÃ³ la sesiÃ³n de la cuenta",
            "NO_ACCOUNT_AVAILABLE": "no habÃ­a cuentas disponibles",
            "PROXY_BLOCKED": "proxy bloqueado",
            "THREAD_OPEN_FAILED": "no se pudo abrir la conversaciÃ³n",
            "USERNAME_NOT_FOUND": "usuario no encontrado",
            "UI_NOT_FOUND": "la interfaz no devolviÃ³ resultados",
            "SKIPPED_NO_DM_OR_THREAD_BLOCKED": "conversaciÃ³n existente o no admite DM",
            "SKIPPED_USERNAME_NOT_FOUND": "usuario no encontrado",
            "SKIPPED_UI_NOT_FOUND": "no se encontraron resultados en bÃºsqueda",
            "SKIPPED_CAMPAIGN_QUOTA_REACHED": "se alcanzÃ³ el cupo de mensajes de la cuenta",
            "SEND_UNVERIFIED_BLOCKED": "mensaje no confirmado por la plataforma",
            "SENT_UNVERIFIED": "mensaje no confirmado por la plataforma",
        }
        return mapping.get(normalized, key.replace("_", " ").strip().lower() or "error de envÃ­o")

    def _mark_task_failed(
        self,
        task: LeadTask,
        *,
        reason: str,
        account_username: str = "",
        force_skip: bool = False,
    ) -> None:
        reason_text = str(reason or "send_failed").strip() or "send_failed"
        reason_upper = self._normalize_failure_reason(reason_text)
        skip_lead = bool(force_skip) or self._is_non_retryable_lead_failure(reason_upper)

        with self._stats_lock:
            if skip_lead:
                self._stats["skipped"] = int(self._stats.get("skipped", 0)) + 1
            else:
                self._stats["failed"] = int(self._stats.get("failed", 0)) + 1

        self._log("warning", "Lead @%s marcado como fallido (%s).", task.lead, reason_text)
<<<<<<< HEAD
        self._log_lead_stage(
            lead=task.lead,
            stage_name="lead_finalization",
            outcome=("skipped" if skip_lead else "failed") + f":{reason_text}",
        )
=======
>>>>>>> origin/main

        try:
            if skip_lead:
                log_sent(
                    account_username or "-",
                    task.lead,
                    False,
                    reason_text,
                    skip=True,
                    skip_reason=reason_text,
                    source_engine="campaign",
                    campaign_alias=self._campaign_alias,
                    leads_alias=self._leads_alias,
                    run_id=self._campaign_run_id,
                )
                mark_lead_skipped(task.lead, reason=reason_text, alias=self._campaign_alias)
            else:
                log_sent(
                    account_username or "-",
                    task.lead,
                    False,
                    reason_text,
                    source_engine="campaign",
                    campaign_alias=self._campaign_alias,
                    leads_alias=self._leads_alias,
                    run_id=self._campaign_run_id,
                )
                mark_lead_failed(
                    task.lead,
                    reason=reason_text,
                    attempts=task.attempt,
                    alias=self._campaign_alias,
                )
        except Exception as exc:
            self._report_storage_failure(
                event_type="lead_failure_persist_failed",
                message="No se pudo persistir el resultado fallido del lead.",
                exc=exc,
                account=account_username or "-",
                lead=task.lead,
                failure_kind="terminal" if skip_lead else "retryable",
                reason=reason_text,
            )

        account_display = account_username or "-"
        if skip_lead:
            _print_skip_block(
                account=account_display,
                lead=task.lead,
                reason=self._humanize_reason(reason_text),
                proxy_id=self.proxy_id,
            )
            return
        _print_error_block(
            account=account_display,
            lead=task.lead,
            reason=self._humanize_reason(reason_text),
            proxy_id=self.proxy_id,
        )

    def _handle_no_account_available(self, task: LeadTask) -> None:
        if self._all_accounts_reached_limit():
            self._mark_task_failed(
                task,
                reason="SKIPPED_CAMPAIGN_QUOTA_REACHED",
                force_skip=True,
            )
            return
        same_proxy_accounts = [
            str(state.account.get("username") or "")
            for state in self._states
            if isinstance(state.account, dict)
        ]
        retry_task = self._scheduler.build_retry_task(
            task,
            failed_proxy_id=self.network_key,
            failed_account_id="",
            same_proxy_account_ids=same_proxy_accounts,
            all_proxy_ids=self._all_network_keys,
        )
        if retry_task is not None:
            self._scheduler.push_task(retry_task)
            with self._stats_lock:
                self._stats["retried"] = int(self._stats.get("retried", 0)) + 1
            self._log(
                "info",
                "Retry attempt: lead=@%s intento=%d proxy=%s (sin cuenta disponible)",
                retry_task.lead,
                retry_task.attempt,
                retry_task.preferred_proxy_id or "any",
            )
            _print_info_block(
                "Lead reencolado",
                [
                    f"Lead: {retry_task.lead}",
                    "Motivo: no habÃ­a cuentas disponibles",
                    f"Intento: {retry_task.attempt}",
                ],
            )
            return
        self._mark_task_failed(task, reason="no_account_available")

    def _all_accounts_reached_limit(self) -> bool:
        if not self._states:
            return False
        for state in self._states:
            if not self._account_reached_limit(state):
                return False
            state.disabled_for_campaign = True
        return True

    def _handle_blocked_proxy_task(self, task: LeadTask) -> None:
        retry_task = self._scheduler.build_retry_task(
            task,
            failed_proxy_id=self.network_key,
            failed_account_id="",
            same_proxy_account_ids=[],
            all_proxy_ids=self._all_network_keys,
        )
        if retry_task is not None:
            self._scheduler.push_task(retry_task)
            with self._stats_lock:
                self._stats["retried"] = int(self._stats.get("retried", 0)) + 1
            self._log(
                "info",
                "Retry attempt: lead=@%s intento=%d proxy=%s (proxy bloqueado)",
                retry_task.lead,
                retry_task.attempt,
                retry_task.preferred_proxy_id or "any",
            )
            _print_info_block(
                "Lead reencolado",
                [
                    f"Lead: {retry_task.lead}",
                    "Motivo: proxy bloqueado",
                    f"Intento: {retry_task.attempt}",
                ],
            )
            return
        self._mark_task_failed(task, reason="proxy_blocked")

    def _ensure_session(self, state: AccountRuntimeState) -> bool:
        account = state.account
        username = str(account.get("username") or "").strip()
        if not username:
            state.preflight_failure_reason = "missing_username"
            state.preflight_failure_message = "Cuenta sin username valido."
            return False

        refreshed = dict(get_account(username) or account)
        readiness = _campaign_account_readiness(
            refreshed,
            expected_network_key=self.network_key,
        )
        if not bool(readiness.get("eligible")):
            state.preflight_failure_reason = str(readiness.get("reason_code") or "login_failed")
            state.preflight_failure_message = str(readiness.get("message") or "").strip()
            state.session_ready = False
            if state.preflight_failure_reason == "network_identity_mismatch":
                state.disabled_for_campaign = True
            self._log(
                "warning",
                "Cuenta @%s excluida durante ensure_session (%s).",
                username,
                state.preflight_failure_message or state.preflight_failure_reason,
            )
            return False

        state.preflight_failure_reason = ""
        state.preflight_failure_message = ""
        state.account = dict(readiness.get("account") or refreshed)
        refreshed_sent_today = _resolve_account_sent_today(state.account)
        state.sent_count = max(state.sent_count, refreshed_sent_today)
        state.account["sent_today"] = state.sent_count
<<<<<<< HEAD
=======
        state.max_messages = _resolve_account_message_limit(state.account)
>>>>>>> origin/main
        if state.sent_count >= state.max_messages:
            state.disabled_for_campaign = True

        state.session_ready = _account_has_storage_state(state.account)
        if state.session_ready:
            mark_connected(username, True, invalidate_health=False)
            return True

        state.preflight_failure_reason = "storage_state_missing"
        state.preflight_failure_message = f"La cuenta @{username} perdio el storage_state usable."
        return False

    def _account_schedulable(
        self,
        state: AccountRuntimeState,
        *,
        ts: float,
        excluded: set[str],
        require_session_ready: bool,
        mutate: bool,
    ) -> bool:
        username = str(state.account.get("username") or "").strip()
        username_norm = _norm_account(username)
<<<<<<< HEAD
        if not state.active_in_worker:
            return False
=======
>>>>>>> origin/main
        if state.disabled_for_campaign:
            return False
        if self._account_reached_limit(state):
            if mutate:
<<<<<<< HEAD
                self._retire_account(state, reason="account_quota_reached")
=======
                state.disabled_for_campaign = True
>>>>>>> origin/main
            return False
        if username_norm in excluded:
            return False
        if require_session_ready and not state.session_ready:
            return False
        if state.cooldown_until > ts:
            return False
        if not self._health.is_account_available(username, now=ts):
            remaining = self._health.account_cooldown_remaining(username, now=ts)
            if mutate:
                state.cooldown_until = ts + remaining
            return False
        if state.next_send_time > ts:
            return False
        return True

    def _next_ready_account(
        self,
        task: Optional[LeadTask],
        *,
        now: Optional[float] = None,
    ) -> Optional[AccountRuntimeState]:
<<<<<<< HEAD
        self._fill_active_account_window(reason="account_selection")
=======
>>>>>>> origin/main
        total = len(self._states)
        if total <= 0:
            return None

        ts = time.time() if now is None else float(now)
        excluded = set(task.excluded_accounts if task else ())
        for _ in range(total):
            index = self._rotation_cursor % total
            self._rotation_cursor += 1
            state = self._states[index]
            if self._account_schedulable(
                state,
                ts=ts,
                excluded=excluded,
                require_session_ready=False,
                mutate=True,
            ):
                return state
        return None

    def _next_account_wait_decision(self, task: LeadTask) -> Optional[AccountWaitDecision]:
<<<<<<< HEAD
        self._fill_active_account_window(reason="wait_decision")
=======
>>>>>>> origin/main
        ts = time.time()
        excluded = set(task.excluded_accounts if task else ())
        decision: Optional[AccountWaitDecision] = None

        for state in self._states:
<<<<<<< HEAD
            if not state.active_in_worker:
                continue
=======
>>>>>>> origin/main
            if state.disabled_for_campaign:
                continue
            if self._account_reached_limit(state):
                continue

            username = str(state.account.get("username") or "").strip()
            username_norm = _norm_account(username)
            if username_norm in excluded:
                continue

            if state.cooldown_until > ts:
                wait_seconds = max(0.0, state.cooldown_until - ts)
                decision = self._pick_wait_decision(
                    decision,
                    wait_seconds=wait_seconds,
                    stage=WorkerExecutionStage.COOLDOWN,
                    reason="account_cooldown",
                )
                continue

            if not self._health.is_account_available(username, now=ts):
                remaining = self._health.account_cooldown_remaining(username, now=ts)
                if remaining > 0:
                    decision = self._pick_wait_decision(
                        decision,
                        wait_seconds=float(remaining),
                        stage=WorkerExecutionStage.COOLDOWN,
                        reason="account_cooldown",
                    )
                continue

            if state.next_send_time > ts:
                wait_seconds = max(0.0, state.next_send_time - ts)
                decision = self._pick_wait_decision(
                    decision,
                    wait_seconds=wait_seconds,
                    stage=WorkerExecutionStage.COOLDOWN,
                    reason="account_rate_window",
                )
                continue

            return AccountWaitDecision(
                seconds=0.0,
                stage=WorkerExecutionStage.WAITING_QUEUE,
                reason="account_rotation",
            )

        return decision

    @staticmethod
    def _pick_wait_decision(
        current: Optional[AccountWaitDecision],
        *,
        wait_seconds: float,
        stage: WorkerExecutionStage,
        reason: str,
    ) -> AccountWaitDecision:
        candidate = AccountWaitDecision(
            seconds=max(0.0, float(wait_seconds)),
            stage=stage,
            reason=str(reason or "").strip() or stage.value,
        )
        if current is None or candidate.seconds < current.seconds:
            return candidate
        return current

    def _has_candidate_account_for_task(self, task: LeadTask) -> bool:
<<<<<<< HEAD
        self._fill_active_account_window(reason="candidate_scan")
        ts = time.time()
        excluded = set(task.excluded_accounts if task else ())
        for state in self._states:
            if not state.active_in_worker:
                continue
=======
        ts = time.time()
        excluded = set(task.excluded_accounts if task else ())
        for state in self._states:
>>>>>>> origin/main
            if state.disabled_for_campaign:
                continue
            if self._account_reached_limit(state):
                continue
            username = str(state.account.get("username") or "").strip()
            username_norm = _norm_account(username)
            if username_norm in excluded:
                continue
            if state.cooldown_until > ts:
                continue
            if not self._health.is_account_available(username, now=ts):
                continue
            return True
        return False

    def _render_message_for_lead(self, account: Dict[str, Any], lead: str) -> str:
        selected, _ = self._template_rotator.next_variant()
        variables = {
            "nombre": lead,
            "username": lead,
            "usuario": lead,
            "lead": lead,
            "cuenta": str(account.get("username") or ""),
            "account": str(account.get("username") or ""),
        }
        rendered = render_template(selected, variables)
        # Campaign DM templates are 1 message per line.
        # Enforce single-line payload even if source text contains newlines.
        for line in str(rendered or "").splitlines():
            candidate = line.strip()
            if candidate:
                return candidate
        return ""

    def _account_reached_limit(self, state: AccountRuntimeState) -> bool:
        sent_today = _resolve_account_sent_today(state.account)
        if sent_today > state.sent_count:
            state.sent_count = sent_today
        state.account["sent_today"] = state.sent_count
        return state.sent_count >= state.max_messages

    def _log_proxy_status_change(self, new_status: str) -> None:
        current = str(new_status or "healthy").strip().lower()
        if not current:
            current = "healthy"
        if current == self._proxy_status_cache:
            return
        self._proxy_status_cache = current
        if current == "degraded":
            self._log("warning", "Proxy degraded")
            self._emit_runtime_event(
                "proxy_degraded",
                severity="warning",
                failure_kind="retryable",
                message="Proxy degradado detectado por health monitor.",
            )
            return
        if current == "blocked":
            self._log("error", "Proxy blocked")
            self._emit_runtime_event(
                "proxy_blocked",
                severity="error",
                failure_kind="terminal",
                message="Proxy bloqueado detectado por health monitor.",
            )
            return
        self._log("info", "Proxy healthy")


def _account_usernames(accounts: list[Dict[str, Any]]) -> set[str]:
    return {
        str(account.get("username") or "").strip().lstrip("@").lower()
        for account in accounts
        if isinstance(account, dict) and str(account.get("username") or "").strip()
    }


def _timestamp_from_account(account: Dict[str, Any], *keys: str) -> float:
    if not isinstance(account, dict):
        return 0.0
    for key in keys:
        try:
            value = float(account.get(key) or 0.0)
        except Exception:
            value = 0.0
        if value > 0.0:
            return value
    return 0.0


def _campaign_block_payload(
    *,
    account: Dict[str, Any],
    reason_code: str,
    message: str,
    effective_network_key: str = "",
    network_mode: str = "",
    proxy_preflight_status: str = "",
    runtime_proxy_id: str = "",
) -> dict[str, Any]:
    username = str(account.get("username") or "").strip().lstrip("@")
    payload = {
        "username": username,
        "alias": str(account.get("alias") or "default").strip() or "default",
        "reason_code": str(reason_code or "").strip() or "blocked",
        "message": str(message or "").strip() or "Cuenta excluida por preflight.",
        "effective_network_key": _normalize_effective_network_key(effective_network_key)
        if str(effective_network_key or "").strip()
        else "",
        "network_mode": str(network_mode or "").strip().lower(),
        "proxy_preflight_status": str(proxy_preflight_status or "").strip().lower(),
        "runtime_proxy_id": str(runtime_proxy_id or "").strip(),
    }
    return payload


def _campaign_account_readiness(
    account: Dict[str, Any] | None,
    *,
    expected_network_key: str = "",
) -> dict[str, Any]:
    current = dict(account or {})
    username = str(current.get("username") or "").strip().lstrip("@")
    alias = str(current.get("alias") or "default").strip() or "default"
    if not username:
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account={"username": "", "alias": alias},
                reason_code="missing_username",
                message="Cuenta sin username valido.",
            ),
        }

    if not bool(current.get("active")):
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account=current,
                reason_code="inactive",
                message=f"La cuenta @{username} no esta activa.",
            ),
        }

<<<<<<< HEAD
    if account_usage_state(current) != "active":
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account=current,
                reason_code="usage_deactivated",
                message=f"La cuenta @{username} esta desactivada para uso operativo.",
            ),
        }

=======
>>>>>>> origin/main
    session_ready = _account_has_storage_state(current)
    if not session_ready:
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account=current,
                reason_code="storage_state_missing",
                message=f"La cuenta @{username} no tiene storage_state usable.",
            ),
        }

    connected = False
    try:
        connected = bool(
            connected_status(
                current,
                fast=False,
                persist=False,
                reason="campaign-preflight",
            )
        )
    except Exception:
        connected = bool(current.get("connected"))
    if not connected:
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account=current,
                reason_code="disconnected",
                message=f"La cuenta @{username} no esta connected.",
            ),
        }

    record, expired = health_store.get_record(username)
    if record is not None and not expired and health_store.is_dead_state(record.state):
        reason = str(record.reason or "").strip() or record.state
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account=current,
                reason_code="health_blocked",
                message=f"La cuenta @{username} quedo bloqueada por health state ({reason}).",
            ),
        }

    cooldown_until = _timestamp_from_account(current, "cooldown_until", "account_cooldown_until")
    if cooldown_until > time.time():
        seconds_left = max(0, int(cooldown_until - time.time()))
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account=current,
                reason_code="account_cooldown",
                message=f"La cuenta @{username} sigue en cooldown por {seconds_left}s.",
            ),
        }

    quarantine_until = _timestamp_from_account(current, "quarantine_until", "account_quarantine_until")
    if quarantine_until > time.time():
        seconds_left = max(0, int(quarantine_until - time.time()))
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account=current,
                reason_code="account_quarantine",
                message=f"La cuenta @{username} sigue en quarantine por {seconds_left}s.",
            ),
        }

    proxy_status = account_proxy_preflight(
        current,
        allow_proxyless=True,
        allow_legacy=False,
    )
    worker_key = _normalize_effective_network_key(proxy_status.get("effective_network_key"))
    network_mode = str(proxy_status.get("network_mode") or "").strip().lower()
    runtime_proxy_id = _runtime_proxy_id_from_network_key(worker_key) if worker_key else ""
    proxy_preflight_status = str(proxy_status.get("status") or "").strip().lower()
    if bool(proxy_status.get("blocking")):
        message = str(proxy_status.get("message") or proxy_preflight_status or "proxy_blocked").strip()
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account=current,
                reason_code=f"proxy_{proxy_preflight_status or 'blocked'}",
                message=message,
                effective_network_key=str(proxy_status.get("effective_network_key") or ""),
                network_mode=network_mode,
                proxy_preflight_status=proxy_preflight_status,
                runtime_proxy_id=runtime_proxy_id,
            ),
        }

    expected_key = _normalize_effective_network_key(expected_network_key) if expected_network_key else ""
    if expected_key and worker_key != expected_key:
        return {
            "eligible": False,
            "account": current,
            **_campaign_block_payload(
                account=current,
                reason_code="network_identity_mismatch",
                message=(
                    f"La cuenta @{username} ya no pertenece al worker {expected_key}; "
                    f"ahora resuelve a {worker_key or 'sin_worker'}."
                ),
                effective_network_key=worker_key,
                network_mode=network_mode,
                proxy_preflight_status=proxy_preflight_status,
                runtime_proxy_id=runtime_proxy_id,
            ),
        }

    current["effective_network_key"] = worker_key
    current["network_mode"] = network_mode
    current["proxy_preflight_status"] = proxy_preflight_status
    current["proxy_preflight_message"] = str(proxy_status.get("message") or "").strip()
    current["runtime_proxy_id"] = runtime_proxy_id
    current["connected"] = connected
    return {
        "eligible": True,
        "account": current,
        "username": username,
        "alias": alias,
        "reason_code": "",
        "message": "",
        "effective_network_key": worker_key,
        "network_mode": network_mode,
        "proxy_preflight_status": proxy_preflight_status,
        "runtime_proxy_id": runtime_proxy_id,
    }


def _campaign_accounts_preflight(accounts: list[Dict[str, Any]]) -> dict[str, Any]:
    ready_accounts: list[dict[str, Any]] = []
    blocked_accounts: list[dict[str, Any]] = []
    blocked_reason_counts: Counter[str] = Counter()
    network_mode_counts: Counter[str] = Counter()

    for account in accounts:
        if not isinstance(account, dict):
            continue
        readiness = _campaign_account_readiness(account)
        if not bool(readiness.get("eligible")):
            blocked_accounts.append(
                {
                    key: value
                    for key, value in readiness.items()
                    if key != "account"
                }
            )
            blocked_reason_counts[str(readiness.get("reason_code") or "blocked")] += 1
            continue
        current = dict(readiness.get("account") or {})
        ready_accounts.append(current)
        network_mode_counts[str(readiness.get("network_mode") or "unknown")] += 1

    return {
        "ready_accounts": ready_accounts,
        "blocked_accounts": blocked_accounts,
        "ready": len(ready_accounts),
        "blocked": len(blocked_accounts),
        "blocked_reason_counts": dict(blocked_reason_counts),
        "network_mode_counts": dict(network_mode_counts),
    }


def _load_selected_accounts(alias: str) -> list[Dict[str, Any]]:
    requested_alias = alias or "default"
    alias_norm = normalize_alias(requested_alias)
    selected: list[Dict[str, Any]] = []
    matched_any_alias = False
    available_aliases: set[str] = set()
    for account in list_all():
        if not isinstance(account, dict):
            continue
        username = str(account.get("username") or "").strip()
        if not username:
            continue
<<<<<<< HEAD
        if not is_account_enabled_for_operation(account):
            continue
=======
>>>>>>> origin/main
        account_alias = account.get("alias") or "default"
        available_aliases.add(str(account_alias or "default"))
        if normalize_alias(account_alias) != alias_norm:
            continue
        matched_any_alias = True
        selected.append(dict(account))

    if alias_norm and not matched_any_alias:
        ui_alias_exists = False
        try:
            registry_path = Path(ACCOUNTS_FILE).with_name("aliases.json")
            payload = json.loads(registry_path.read_text(encoding="utf-8"))
            raw_aliases: list[Any] = []
            if isinstance(payload, dict):
                raw_aliases = payload.get("aliases") if isinstance(payload.get("aliases"), list) else []
            elif isinstance(payload, list):
                raw_aliases = payload
            for entry in raw_aliases:
                if isinstance(entry, dict):
                    display_name = entry.get("display_name") or entry.get("alias") or entry.get("name")
                else:
                    display_name = entry
                if normalize_alias(str(display_name or "")) == alias_norm:
                    ui_alias_exists = True
                    break
        except Exception:
            ui_alias_exists = False

        if ui_alias_exists:
            aliases_sorted = sorted(available_aliases)
            logger.debug("Alias mismatch: requested=%s available=%s", requested_alias, aliases_sorted)
    return selected


def _apply_sent_today_counts(
    accounts: list[Dict[str, Any]],
    *,
    sent_today_counts: Dict[str, int] | None,
) -> list[Dict[str, Any]]:
    counts_today = {
        str(username or "").strip().lstrip("@").lower(): max(0, int(value or 0))
        for username, value in dict(sent_today_counts or {}).items()
        if str(username or "").strip()
    }
    for account in accounts:
        username = str(account.get("username") or "").strip().lower()
        if not username:
            continue
<<<<<<< HEAD
        snapshot_count = int(counts_today.get(username, 0))
        try:
            current_count = max(0, int(account.get("sent_today") or 0))
        except Exception:
            current_count = 0
        account["sent_today"] = max(current_count, snapshot_count)
=======
        account["sent_today"] = int(counts_today.get(username, 0))
>>>>>>> origin/main
    return accounts


def load_accounts(
    alias: str,
    *,
    run_id: str = "",
    sent_today_counts: Dict[str, int] | None = None,
) -> list[Dict[str, Any]]:
    _ = run_id
    alias_norm = str(alias or "default").strip().lower()
    preflight = _campaign_accounts_preflight(_load_selected_accounts(alias_norm))
    selected = _order_accounts_for_worker_start(
        [dict(account) for account in (preflight.get("ready_accounts") or []) if isinstance(account, dict)]
    )
    counts_today = sent_today_counts
    if counts_today is None:
        counts_today = dict(
            campaign_start_snapshot(
                _account_usernames(selected),
                campaign_alias=alias_norm,
            ).get("daily_counts")
            or {}
        )
<<<<<<< HEAD
    selected = _apply_sent_today_counts(selected, sent_today_counts=counts_today)
    return _refresh_accounts_sent_today_from_log(selected)
=======
    return _apply_sent_today_counts(selected, sent_today_counts=counts_today)
>>>>>>> origin/main


def load_leads(leads_alias: str) -> list[str]:
    raw = load_list(str(leads_alias or "").strip())
<<<<<<< HEAD
    return _normalize_lead_batch(raw)


def _normalize_lead_batch(values: list[Any] | tuple[Any, ...]) -> list[str]:
    leads: list[str] = []
    seen: set[str] = set()
    for item in values:
=======
    leads: list[str] = []
    seen: set[str] = set()
    for item in raw:
>>>>>>> origin/main
        lead = normalize_contact_username(item)
        if not lead:
            continue
        if lead in seen:
            continue
        seen.add(lead)
        leads.append(lead)
    return leads


def _normalize_campaign_alias(value: Any) -> str:
    return normalize_alias(value)


def _campaign_account_usernames(alias: str) -> set[str]:
    alias_norm = _normalize_campaign_alias(alias)
    usernames: set[str] = set()
    if not alias_norm:
        return usernames
    for account in list_all():
        if not isinstance(account, dict):
            continue
<<<<<<< HEAD
        if not is_account_enabled_for_operation(account):
            continue
=======
>>>>>>> origin/main
        account_alias = _normalize_campaign_alias(account.get("alias"))
        if account_alias != alias_norm:
            continue
        username = _norm_account(str(account.get("username") or ""))
        if username:
            usernames.add(username)
    return usernames


def _legacy_terminal_status_matches_alias(
    *,
    alias: str,
    alias_accounts: set[str],
    entry: Dict[str, Any] | None,
) -> bool:
    if not isinstance(entry, dict):
        return False
    status = str(entry.get("status") or "").strip().lower()
    if status not in {"sent", "skipped"}:
        return False
    reason = str(entry.get("last_error") or "").strip().lower()
    if status == "skipped" and reason == "already_contacted":
        return False
    entry_alias = _normalize_campaign_alias(entry.get("last_alias"))
    if entry_alias:
        return entry_alias == _normalize_campaign_alias(alias)
    if status != "sent":
        return False
    sent_by = _norm_account(str(entry.get("sent_by") or ""))
    return bool(sent_by and sent_by in alias_accounts)


def _collect_legacy_terminal_status_update(
    lead: str,
    *,
    entry: Dict[str, Any],
    sent_updates: list[tuple[str, str]],
    skipped_updates: list[tuple[str, str]],
) -> None:
    status = str(entry.get("status") or "").strip().lower()
    if status == "sent":
        sent_updates.append((lead, str(entry.get("sent_by") or "").strip()))
        return
    if status != "skipped":
        return
    reason = str(entry.get("last_error") or "").strip()
    if reason.lower() == "already_contacted":
        return
    skipped_updates.append((lead, reason))


def _global_contact_is_active(entry: Dict[str, Any] | None, *, now_ts: int) -> bool:
    if not isinstance(entry, dict):
        return False
    status = str(entry.get("last_status") or entry.get("status") or "sent").strip().lower()
    if status and status != "sent":
        return False
    timestamp = 0
    for key in ("last_contacted_at", "sent_timestamp", "updated_at", "ts"):
        try:
            timestamp = int(entry.get(key) or 0)
        except Exception:
            timestamp = 0
        if timestamp > 0:
            break
    if timestamp <= 0:
        return False
    return max(0, now_ts - timestamp) < GLOBAL_CONTACT_TTL_SECONDS


<<<<<<< HEAD
def _alias_sent_contact_is_active(entry: Dict[str, Any] | None, *, now_ts: int) -> bool:
    if not isinstance(entry, dict):
        return False
    status = str(entry.get("status") or "").strip().lower()
    if status != "sent":
        return False
    timestamp = 0
    for key in ("sent_timestamp", "last_contacted_at", "updated_at", "ts"):
        try:
            timestamp = int(entry.get(key) or 0)
        except Exception:
            timestamp = 0
        if timestamp > 0:
            break
    if timestamp <= 0:
        return False
    return max(0, now_ts - timestamp) < GLOBAL_CONTACT_TTL_SECONDS


=======
>>>>>>> origin/main
def _filter_pending_leads_for_campaign(
    leads: list[str],
    *,
    alias: str,
<<<<<<< HEAD
    run_id: str = "",
=======
>>>>>>> origin/main
    alias_accounts: set[str] | None = None,
    campaign_registry: set[str] | None = None,
    shared_registry: set[str] | None = None,
    alias_status_map: Dict[str, Dict[str, Any]] | None = None,
    global_contact_map: Dict[str, Dict[str, Any]] | None = None,
) -> tuple[list[str], Dict[str, int]]:
    alias_norm = _normalize_campaign_alias(alias)
    resolved_alias_accounts = set(alias_accounts or _campaign_account_usernames(alias_norm))
    storage_snapshot = None
    if campaign_registry is None or shared_registry is None:
        storage_snapshot = campaign_start_snapshot(
            resolved_alias_accounts,
            campaign_alias=alias_norm,
        )
    blocked_by_campaign_registry_set = set(
        campaign_registry
        if campaign_registry is not None
        else (storage_snapshot.get("campaign_registry") if storage_snapshot is not None else set())
    )
    shared_registry_set = set(
        shared_registry
        if shared_registry is not None
        else (storage_snapshot.get("shared_registry") if storage_snapshot is not None else set())
    )
    if alias_status_map is None or global_contact_map is None:
        snapshot_alias_status_map, snapshot_global_contact_map = get_prefilter_snapshot(alias_norm)
        if alias_status_map is None:
            alias_status_map = snapshot_alias_status_map
        if global_contact_map is None:
            global_contact_map = snapshot_global_contact_map
    resolved_alias_status_map = {
        str(lead_key): dict(entry)
        for lead_key, entry in (alias_status_map or {}).items()
        if isinstance(entry, dict)
    }
    resolved_global_contact_map = {
        str(lead_key): dict(entry)
        for lead_key, entry in (global_contact_map or {}).items()
        if isinstance(entry, dict)
    }
<<<<<<< HEAD
    pending_selected: list[str] = []
    pending_fresh: list[str] = []
    skipped_duplicates = 0
    skipped_already_sent = 0
    blocked_by_global_contact = 0
    blocked_by_alias_sent_status = 0
    blocked_by_alias_skipped_status = 0
    preserved_pending = 0
    advisory_alias_sent_ignored = 0
    advisory_campaign_registry_hits = 0
    advisory_shared_registry_hits = 0
    stale_pending_ignored = 0
    seen: set[str] = set()
    now_ts = int(time.time())
    run_id_norm = str(run_id or "").strip()
=======
    pending: list[str] = []
    skipped_duplicates = 0
    skipped_already_sent = 0
    blocked_by_global_contact = 0
    blocked_by_alias_skipped_status = 0
    advisory_alias_sent_ignored = 0
    advisory_campaign_registry_hits = 0
    advisory_shared_registry_hits = 0
    seen: set[str] = set()
    now_ts = int(time.time())
>>>>>>> origin/main
    del resolved_alias_accounts

    for lead in leads:
        normalized = normalize_contact_username(lead)
        if not normalized:
            continue
        if normalized in seen:
            skipped_duplicates += 1
            continue
        seen.add(normalized)

        alias_entry = resolved_alias_status_map.get(normalized)
        alias_status = str(alias_entry.get("status") or "").strip().lower() if isinstance(alias_entry, dict) else ""
<<<<<<< HEAD
        pending_run_id = str(alias_entry.get("pending_run_id") or "").strip() if isinstance(alias_entry, dict) else ""
        if alias_status == "pending" and run_id_norm and pending_run_id == run_id_norm:
            pending_selected.append(normalized)
            preserved_pending += 1
            continue
        if alias_status == "pending":
            stale_pending_ignored += 1
=======
>>>>>>> origin/main
        blocked_by_global = _global_contact_is_active(
            resolved_global_contact_map.get(normalized),
            now_ts=now_ts,
        )
<<<<<<< HEAD
        blocked_by_alias_sent = _alias_sent_contact_is_active(alias_entry, now_ts=now_ts)
        blocked_by_alias_skip = alias_status == "skipped"

        if blocked_by_global or blocked_by_alias_sent or blocked_by_alias_skip:
            skipped_already_sent += 1
            if blocked_by_global:
                blocked_by_global_contact += 1
            if blocked_by_alias_sent:
                blocked_by_alias_sent_status += 1
=======
        blocked_by_alias_skip = alias_status == "skipped"

        if blocked_by_global or blocked_by_alias_skip:
            skipped_already_sent += 1
            if blocked_by_global:
                blocked_by_global_contact += 1
>>>>>>> origin/main
            if blocked_by_alias_skip:
                blocked_by_alias_skipped_status += 1
            continue

        if alias_status == "sent":
            advisory_alias_sent_ignored += 1
        if normalized in blocked_by_campaign_registry_set:
            advisory_campaign_registry_hits += 1
        if normalized in shared_registry_set:
            advisory_shared_registry_hits += 1

<<<<<<< HEAD
        pending_fresh.append(normalized)

    pending = pending_selected + pending_fresh
=======
        pending.append(normalized)

>>>>>>> origin/main
    return pending, {
        "skipped_duplicates": skipped_duplicates,
        "skipped_already_sent": skipped_already_sent,
        "pending": len(pending),
        "blocked_total": skipped_already_sent,
        "valid_total": len(pending),
        "blocked_by_global_contact": blocked_by_global_contact,
<<<<<<< HEAD
        "blocked_by_alias_sent_status": blocked_by_alias_sent_status,
        "blocked_by_alias_skipped_status": blocked_by_alias_skipped_status,
        "preserved_pending": preserved_pending,
        "advisory_alias_sent_ignored": advisory_alias_sent_ignored,
        "advisory_campaign_registry_hits": advisory_campaign_registry_hits,
        "advisory_shared_registry_hits": advisory_shared_registry_hits,
        "stale_pending_ignored": stale_pending_ignored,
=======
        "blocked_by_alias_skipped_status": blocked_by_alias_skipped_status,
        "advisory_alias_sent_ignored": advisory_alias_sent_ignored,
        "advisory_campaign_registry_hits": advisory_campaign_registry_hits,
        "advisory_shared_registry_hits": advisory_shared_registry_hits,
>>>>>>> origin/main
        "blocked_by_campaign_history": 0,
    }


def _log_campaign_diagnostics(
    *,
    alias: str,
    leads_alias: str,
    total_leads_loaded: int,
    lead_filter_stats: Dict[str, int],
    log_callback: Callable[..., None] | None = None,
) -> None:
    blocked_total = max(0, int(lead_filter_stats.get("blocked_total", lead_filter_stats.get("skipped_already_sent", 0))))
    valid_total = max(0, int(lead_filter_stats.get("valid_total", lead_filter_stats.get("pending", 0))))
    duplicates = max(0, int(lead_filter_stats.get("skipped_duplicates", 0)))
    blocked_global_contact = max(0, int(lead_filter_stats.get("blocked_by_global_contact", 0)))
<<<<<<< HEAD
    blocked_alias_sent = max(0, int(lead_filter_stats.get("blocked_by_alias_sent_status", 0)))
    blocked_alias_skipped = max(0, int(lead_filter_stats.get("blocked_by_alias_skipped_status", 0)))
    preserved_pending = max(0, int(lead_filter_stats.get("preserved_pending", 0)))
=======
    blocked_alias_skipped = max(0, int(lead_filter_stats.get("blocked_by_alias_skipped_status", 0)))
>>>>>>> origin/main
    advisory_alias_sent_ignored = max(0, int(lead_filter_stats.get("advisory_alias_sent_ignored", 0)))
    advisory_campaign_registry_hits = max(0, int(lead_filter_stats.get("advisory_campaign_registry_hits", 0)))
    advisory_shared_registry_hits = max(0, int(lead_filter_stats.get("advisory_shared_registry_hits", 0)))
    blocked_history = max(0, int(lead_filter_stats.get("blocked_by_campaign_history", 0)))

    if callable(log_callback):
        log_callback(
            "info",
            "Campaign diagnostics | alias=%s leads_alias=%s total=%d blocked=%d valid=%d duplicates=%d "
<<<<<<< HEAD
            "global_contact=%d alias_sent=%d alias_skipped=%d preserved_pending=%d alias_sent_ignored=%d campaign_registry_ignored=%d "
=======
            "global_contact=%d alias_skipped=%d alias_sent_ignored=%d campaign_registry_ignored=%d "
>>>>>>> origin/main
            "shared_registry_ignored=%d campaign_history=%d",
            alias,
            leads_alias,
            total_leads_loaded,
            blocked_total,
            valid_total,
            duplicates,
            blocked_global_contact,
<<<<<<< HEAD
            blocked_alias_sent,
            blocked_alias_skipped,
            preserved_pending,
=======
            blocked_alias_skipped,
>>>>>>> origin/main
            advisory_alias_sent_ignored,
            advisory_campaign_registry_hits,
            advisory_shared_registry_hits,
            blocked_history,
        )
    else:
        logger.info(
            "Campaign diagnostics | alias=%s leads_alias=%s total=%d blocked=%d valid=%d duplicates=%d "
<<<<<<< HEAD
            "global_contact=%d alias_sent=%d alias_skipped=%d preserved_pending=%d alias_sent_ignored=%d campaign_registry_ignored=%d "
=======
            "global_contact=%d alias_skipped=%d alias_sent_ignored=%d campaign_registry_ignored=%d "
>>>>>>> origin/main
            "shared_registry_ignored=%d campaign_history=%d",
            alias,
            leads_alias,
            total_leads_loaded,
            blocked_total,
            valid_total,
            duplicates,
            blocked_global_contact,
<<<<<<< HEAD
            blocked_alias_sent,
            blocked_alias_skipped,
            preserved_pending,
=======
            blocked_alias_skipped,
>>>>>>> origin/main
            advisory_alias_sent_ignored,
            advisory_campaign_registry_hits,
            advisory_shared_registry_hits,
            blocked_history,
        )
    _print_info_block(
        "Campaign diagnostics",
        [
            f"alias: {alias}",
            f"leads alias: {leads_alias}",
            f"total leads loaded: {max(0, int(total_leads_loaded))}",
            f"duplicates ignored: {duplicates}",
            f"blocked leads: {blocked_total}",
            f"valid leads: {valid_total}",
            "source of block:",
            "note: source counts may overlap",
            f"global contact blocked (<7d): {blocked_global_contact}",
<<<<<<< HEAD
            f"alias sent blocked (<7d): {blocked_alias_sent}",
            f"alias skipped blocked: {blocked_alias_skipped}",
            f"already selected/pending preserved: {preserved_pending}",
=======
            f"alias skipped blocked: {blocked_alias_skipped}",
>>>>>>> origin/main
            f"alias sent ignored without active global block: {advisory_alias_sent_ignored}",
            f"campaign sent_log bootstrap/advisory hits: {advisory_campaign_registry_hits}",
            f"campaign history: {blocked_history}",
            f"shared sent_log advisory only: {advisory_shared_registry_hits}",
        ],
    )


def _worker_network_key(account: Dict[str, Any]) -> str:
    return _effective_network_key_for_account(account)


def _account_remaining_capacity(account: Dict[str, Any]) -> int:
    limit = _resolve_account_message_limit(account)
    sent_today = _resolve_account_sent_today(account)
    return max(0, int(limit) - int(sent_today))


<<<<<<< HEAD
def _refresh_account_sent_today_from_log(account: Dict[str, Any]) -> int:
    username = str(account.get("username") or "").strip()
    current = _resolve_account_sent_today(account)
    if not username:
        account["sent_today"] = current
        return current
    try:
        _can_send, live_sent_today, _limit = can_send_message_for_account(
            account=account,
            username=username,
            default=None,
        )
        current = max(current, max(0, int(live_sent_today or 0)))
    except Exception:
        current = max(0, current)
    account["sent_today"] = current
    return current


def _refresh_accounts_sent_today_from_log(accounts: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    for account in accounts:
        if not isinstance(account, dict):
            continue
        _refresh_account_sent_today_from_log(account)
    return accounts


=======
>>>>>>> origin/main
def _group_remaining_capacity(accounts: list[Dict[str, Any]]) -> int:
    total = 0
    for account in accounts:
        if not isinstance(account, dict):
            continue
        total += _account_remaining_capacity(account)
    return max(0, total)


def _total_remaining_capacity_for_groups(
    group_capacities: Dict[str, int],
    worker_ids: list[str],
) -> int:
    total = 0
    for worker_id in worker_ids:
        total += max(0, int(group_capacities.get(worker_id, 0)))
    return max(0, total)


def _limit_leads_to_worker_capacity(
    leads: list[str],
    *,
    group_capacities: Dict[str, int],
    worker_ids: list[str],
) -> tuple[list[str], int]:
    total_capacity = _total_remaining_capacity_for_groups(group_capacities, worker_ids)
    if total_capacity <= 0:
        return [], len(leads)
    limited = list(leads[:total_capacity])
    return limited, max(0, len(leads) - len(limited))


def _build_initial_worker_tasks(
    leads: list[str],
    *,
    worker_ids: list[str],
    group_capacities: Dict[str, int],
) -> list[LeadTask]:
    remaining_slots = {
        worker_id: max(0, int(group_capacities.get(worker_id, 0)))
        for worker_id in worker_ids
    }
    active_workers = [
        worker_id
        for worker_id in worker_ids
        if remaining_slots.get(worker_id, 0) > 0
    ]
    tasks: list[LeadTask] = []
    cursor = 0

    for lead in leads:
        if not active_workers:
            break
        index = cursor % len(active_workers)
        worker_id = active_workers[index]
        tasks.append(
            LeadTask(
                lead=str(lead or "").strip().lstrip("@"),
                attempt=1,
                preferred_proxy_id=worker_id,
            )
        )
        remaining_slots[worker_id] = max(0, remaining_slots[worker_id] - 1)
        if remaining_slots[worker_id] <= 0:
            active_workers.pop(index)
            if active_workers:
                cursor = index % len(active_workers)
            continue
        cursor = index + 1

    return tasks


def _validate_initial_worker_tasks(
    tasks: list[LeadTask],
    *,
    log_callback: Callable[..., None] | None = None,
) -> list[LeadTask]:
    validated: list[LeadTask] = []
    invalid = 0
    for task in tasks:
        if not isinstance(task, LeadTask):
            invalid += 1
            continue
        normalized = normalize_contact_username(task.lead)
        if not normalized:
            invalid += 1
            continue
        validated.append(
            LeadTask(
                lead=normalized,
                attempt=max(1, int(task.attempt or 1)),
                preferred_proxy_id=task.preferred_proxy_id,
                excluded_accounts=tuple(str(item or "").strip() for item in task.excluded_accounts if str(item or "").strip()),
                history=tuple(str(item or "").strip() for item in task.history if str(item or "").strip()),
            )
        )
    if invalid > 0:
        if callable(log_callback):
            log_callback("warning", "[QUEUE] invalid leads dropped before worker start: %d", invalid)
        else:
            logger.warning("[QUEUE] invalid leads dropped before worker start: %d", invalid)
    if callable(log_callback):
        log_callback("info", "[QUEUE] total leads enqueued: %d", len(validated))
    else:
        logger.info("[QUEUE] total leads enqueued: %d", len(validated))
    return validated


def calculate_workers(accounts: list[Dict[str, Any]]) -> Dict[str, Any]:
    worker_groups = {
        worker_key: _order_accounts_for_worker_start(grouped_accounts)
        for worker_key, grouped_accounts in _group_accounts_by_proxy(accounts).items()
        if grouped_accounts
    }
    group_capacities = {
        worker_key: _group_remaining_capacity(grouped_accounts)
        for worker_key, grouped_accounts in worker_groups.items()
    }
    worker_proxy_map = {
        worker_key: _runtime_proxy_id_for_account(grouped_accounts[0]) if grouped_accounts else LOCAL_WORKER_PROXY_ID
        for worker_key, grouped_accounts in worker_groups.items()
    }
    ranked = sorted(
        [
            (worker_key, items)
            for worker_key, items in worker_groups.items()
            if items and int(group_capacities.get(worker_key, 0)) > 0
        ],
        key=lambda item: (
            -sum(1 for account in item[1] if _account_has_storage_state(account)),
            -int(group_capacities.get(item[0], 0)),
            -len(item[1]),
            str(item[0] or ""),
        ),
    )
    ordered_worker_ids = [worker_key for worker_key, _items in ranked]
    proxy_ids = [
        _runtime_proxy_id_from_network_key(worker_key)
        for worker_key in ordered_worker_ids
        if worker_key != DIRECT_NETWORK_KEY
    ]
    has_none_accounts = any(worker_key == DIRECT_NETWORK_KEY for worker_key in ordered_worker_ids)

    return {
        "worker_groups": worker_groups,
        "proxy_groups": worker_groups,
        "group_capacities": group_capacities,
        "proxies": proxy_ids,
        "has_none_accounts": has_none_accounts,
        "workers_capacity": len(ordered_worker_ids),
        "ordered_worker_ids": ordered_worker_ids,
        "worker_proxy_map": worker_proxy_map,
    }


<<<<<<< HEAD
def _account_remaining_payload(account: Dict[str, Any]) -> dict[str, Any]:
    limit = _resolve_account_message_limit(account)
    sent_today = _resolve_account_sent_today(account)
    return {
        "username": str(account.get("username") or "").strip().lstrip("@"),
        "sent_today": max(0, int(sent_today)),
        "limit": max(0, int(limit or 0)) if limit is not None else 0,
        "remaining": _account_remaining_capacity(account),
        "worker_key": _normalize_effective_network_key(_worker_network_key(account)),
        "proxy_id": _runtime_proxy_id_for_account(account),
    }


def build_campaign_plan(
    alias: str,
    leads_alias: str = "",
    *,
    workers_requested: int = 0,
    run_id: str = "",
    root_dir: str | Path | None = None,
    measure: Callable[[str, Callable[[], Any]], Any] | None = None,
) -> Dict[str, Any]:
    def _run_step(label: str, factory: Callable[[], Any]) -> Any:
        if callable(measure):
            return measure(label, factory)
        return factory()

    alias_norm = str(alias or "default").strip().lower() or "default"
    run_id_norm = str(run_id or "").strip()
    refresh_campaign_runtime_paths(root_dir)

    selected_accounts = _run_step("load_accounts", lambda: _load_selected_accounts(alias_norm))
    preflight = _run_step(
        "proxy_preflight",
        lambda: _campaign_accounts_preflight(selected_accounts),
    )
    accounts = [
        dict(account)
        for account in (preflight.get("ready_accounts") or [])
        if isinstance(account, dict)
    ]
    blocked_accounts = [
        dict(item)
        for item in (preflight.get("blocked_accounts") or [])
        if isinstance(item, dict)
    ]
    blocked_reason_counts = {
        str(key): int(value or 0)
        for key, value in (preflight.get("blocked_reason_counts") or {}).items()
    }
    network_mode_counts = {
        str(key): int(value or 0)
        for key, value in (preflight.get("network_mode_counts") or {}).items()
    }

    alias_accounts = _account_usernames(accounts)
    start_snapshot = _run_step(
        "start_snapshot",
        lambda: campaign_start_snapshot(alias_accounts, campaign_alias=alias_norm),
    )
    accounts = _apply_sent_today_counts(
        accounts,
        sent_today_counts=dict(start_snapshot.get("daily_counts") or {}),
    )
    accounts = _refresh_accounts_sent_today_from_log(accounts)

    capacity = _run_step("capacity", lambda: calculate_workers(accounts))
    worker_groups = capacity.get("worker_groups") or capacity.get("proxy_groups") or {}
    group_capacities = {
        _normalize_effective_network_key(worker_key): max(0, int(value or 0))
        for worker_key, value in (capacity.get("group_capacities") or {}).items()
    }
    ordered_worker_ids = [
        _normalize_effective_network_key(item)
        for item in (capacity.get("ordered_worker_ids") or [])
    ]
    worker_proxy_map = {
        _normalize_effective_network_key(worker_key): str(proxy_id or "").strip() or LOCAL_WORKER_PROXY_ID
        for worker_key, proxy_id in (capacity.get("worker_proxy_map") or {}).items()
    }
    workers_capacity = max(0, int(capacity.get("workers_capacity") or 0))
    workers_requested_clean = max(0, int(workers_requested or 0))
    workers_effective = (
        min(workers_requested_clean, workers_capacity)
        if workers_requested_clean > 0
        else workers_capacity
    )
    selected_worker_keys = (
        ordered_worker_ids[:workers_effective]
        if workers_effective > 0
        else []
    )
    remaining_slots_total = _total_remaining_capacity_for_groups(group_capacities, ordered_worker_ids)
    selected_remaining_slots = _total_remaining_capacity_for_groups(group_capacities, selected_worker_keys)
    account_remaining = [
        _account_remaining_payload(account)
        for account in accounts
        if isinstance(account, dict)
    ]
    account_remaining.sort(key=lambda row: str(row.get("username") or "").lower())

    raw_leads = _run_step("load_leads", lambda: load_leads(leads_alias)) if leads_alias else []
    selected_leads_total = len(raw_leads)
    alias_status_map: Dict[str, Dict[str, Any]] = {}
    global_contact_map: Dict[str, Dict[str, Any]] = {}
    if leads_alias:
        alias_status_map, global_contact_map = _run_step(
            "prefilter_snapshot",
            lambda: get_prefilter_snapshot(alias_norm),
        )
    eligible_leads: list[str] = []
    lead_filter_stats: Dict[str, int] = {
        "skipped_duplicates": 0,
        "skipped_already_sent": 0,
        "pending": 0,
        "blocked_total": 0,
        "valid_total": 0,
        "blocked_by_global_contact": 0,
        "blocked_by_alias_sent_status": 0,
        "blocked_by_alias_skipped_status": 0,
        "preserved_pending": 0,
        "advisory_alias_sent_ignored": 0,
        "advisory_campaign_registry_hits": 0,
        "advisory_shared_registry_hits": 0,
        "stale_pending_ignored": 0,
        "blocked_by_campaign_history": 0,
    }
    if raw_leads:
        eligible_leads, lead_filter_stats = _run_step(
            "filter_pending",
            lambda: _filter_pending_leads_for_campaign(
                raw_leads,
                alias=alias_norm,
                run_id=run_id_norm,
                alias_accounts=alias_accounts,
                campaign_registry=set(start_snapshot.get("campaign_registry") or set()),
                shared_registry=set(start_snapshot.get("shared_registry") or set()),
                alias_status_map=alias_status_map,
                global_contact_map=global_contact_map,
            ),
        )
    planned_eligible_leads = len(eligible_leads)
    planned_queue, skipped_for_quota = _limit_leads_to_worker_capacity(
        eligible_leads,
        group_capacities=group_capacities,
        worker_ids=selected_worker_keys,
    )

    result = {
        "alias": alias_norm,
        "leads_alias": str(leads_alias or "").strip(),
        "accounts": accounts,
        "account_remaining": account_remaining,
        "blocked_accounts": blocked_accounts,
        "blocked_reason_counts": blocked_reason_counts,
        "network_mode_counts": network_mode_counts,
        "alias_accounts": alias_accounts,
        "start_snapshot": start_snapshot,
        "worker_groups": worker_groups,
        "group_capacities": group_capacities,
        "ordered_worker_ids": ordered_worker_ids,
        "worker_proxy_map": worker_proxy_map,
        "workers_capacity": workers_capacity,
        "workers_requested": workers_requested_clean,
        "workers_effective": workers_effective,
        "selected_worker_keys": selected_worker_keys,
        "remaining_slots_total": remaining_slots_total,
        "selected_remaining_slots": selected_remaining_slots,
        "raw_leads": raw_leads,
        "selected_leads_total": selected_leads_total,
        "lead_filter_stats": lead_filter_stats,
        "eligible_leads": eligible_leads,
        "planned_eligible_leads": planned_eligible_leads,
        "planned_queue": planned_queue,
        "planned_runnable_leads": len(planned_queue),
        "skipped_for_quota": max(0, int(skipped_for_quota)),
        "proxies": list(capacity.get("proxies") or []),
        "has_none_accounts": bool(capacity.get("has_none_accounts")),
    }
    result["skipped_preblocked"] = max(
        0,
        int(lead_filter_stats.get("skipped_already_sent", 0)) + int(result["skipped_for_quota"]),
    )
    return result


def calculate_workers_for_alias(
    alias: str,
    *,
    leads_alias: str = "",
    workers_requested: int = 0,
    run_id: str = "",
    root_dir: str | Path | None = None,
) -> Dict[str, Any]:
    return build_campaign_plan(
        alias,
        leads_alias,
        workers_requested=workers_requested,
        run_id=run_id,
        root_dir=root_dir,
    )
=======
def calculate_workers_for_alias(alias: str) -> Dict[str, Any]:
    accounts = load_accounts(alias)
    capacity = calculate_workers(accounts)
    capacity["accounts"] = accounts
    return capacity
>>>>>>> origin/main


def run_dynamic_campaign(
    config: MutableMapping[str, Any],
    *,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    alias = str(config.get("alias") or "default").strip() or "default"
    leads_alias = str(config.get("leads_alias") or alias).strip() or alias
    run_id = str(config.get("run_id") or "").strip() or datetime.now().strftime("campaign-%Y%m%d%H%M%S%f")
<<<<<<< HEAD
    root_dir = str(config.get("root_dir") or "").strip()
    refresh_campaign_runtime_paths(root_dir or None)
    delay_min = _as_int(config.get("delay_min", 10), default=10, minimum=0)
    delay_max = _as_int(config.get("delay_max", max(delay_min, 20)), default=max(delay_min, 20), minimum=delay_min)
    workers_requested = _as_int(config.get("workers_requested", 1), default=1, minimum=1)
    headless = bool(config.get("headless", False))
=======
    delay_min = _as_int(config.get("delay_min", 10), default=10, minimum=0)
    delay_max = _as_int(config.get("delay_max", max(delay_min, 20)), default=max(delay_min, 20), minimum=delay_min)
    workers_requested = _as_int(config.get("workers_requested", 1), default=1, minimum=1)
    headless = bool(config.get("headless", True))
>>>>>>> origin/main
    max_attempts_per_lead = _as_int(config.get("max_attempts_per_lead", 3), default=3, minimum=1)
    worker_idle_seconds = _as_int(config.get("worker_idle_seconds", 30), default=30, minimum=1)
    worker_restart_limit = _as_int(config.get("worker_restart_limit", 20), default=20, minimum=1)
    monitor_interval = _as_float(config.get("worker_monitor_interval", 0.5), default=0.5, minimum=0.1)
    send_flow_timeout_seconds = _as_float(
        config.get("send_flow_timeout_seconds", 75.0),
        default=75.0,
        minimum=10.0,
    )
<<<<<<< HEAD
    worker_active_account_limit = _as_int(
        config.get("worker_active_account_limit", 6),
        default=6,
        minimum=1,
    )
    worker_session_close_timeout_seconds = _as_float(
        config.get("worker_session_close_timeout_seconds", 12.0),
        default=12.0,
        minimum=1.0,
    )
    worker_shutdown_timeout_seconds = _as_float(
        config.get("worker_shutdown_timeout_seconds", max(12.0, worker_session_close_timeout_seconds + 4.0)),
        default=max(12.0, worker_session_close_timeout_seconds + 4.0),
=======
    worker_shutdown_timeout_seconds = _as_float(
        config.get("worker_shutdown_timeout_seconds", 8.0),
        default=8.0,
>>>>>>> origin/main
        minimum=1.0,
    )
    cooldown_fail_threshold = _as_int(config.get("cooldown_fail_threshold", 3), default=3, minimum=1)
    cooldown_seconds = _as_int(config.get("account_cooldown_seconds", 600), default=600, minimum=1)
    proxy_degraded_threshold = _as_int(config.get("proxy_degraded_threshold", 5), default=5, minimum=1)
    proxy_blocked_threshold = _as_int(config.get("proxy_blocked_threshold", 10), default=10, minimum=2)
    proxy_block_seconds = _as_int(config.get("proxy_block_seconds", 600), default=600, minimum=1)
    template_variants = _normalize_templates(config.get("templates"))
    template_rotator = TemplateRotator(template_variants)
    total_leads_hint = max(0, int(config.get("total_leads") or 0))
<<<<<<< HEAD
    selected_leads_total_hint = max(0, int(config.get("selected_leads_total") or 0))
    planned_eligible_leads_hint = max(0, int(config.get("planned_eligible_leads") or 0))
    configured_planned_queue = _normalize_lead_batch(list(config.get("planned_queue") or []))
=======
>>>>>>> origin/main
    preflight_started_at = time.perf_counter()
    preflight_timings_ms: dict[str, float] = {}
    last_progress_message = ""

    def _run_log(level: str, message: str, *args: Any, exc_info: bool = False) -> None:
        log_method = getattr(logger, level)
        if level == "exception" and not exc_info:
            exc_info = True
        log_method(f"[run_id=%s] {message}", run_id, *args, exc_info=exc_info)

    def _measure_preflight(label: str, factory: Callable[[], Any]) -> Any:
        started_at = time.perf_counter()
        result = factory()
        preflight_timings_ms[label] = (time.perf_counter() - started_at) * 1000.0
        return result

    def _log_preflight_timings() -> None:
        _run_log(
            "info",
            "Campaign preflight timings | alias=%s leads_alias=%s total_ms=%.2f "
            "load_accounts_ms=%.2f proxy_preflight_ms=%.2f start_snapshot_ms=%.2f load_leads_ms=%.2f "
            "prefilter_snapshot_ms=%.2f filter_pending_ms=%.2f capacity_ms=%.2f",
            alias,
            leads_alias,
            (time.perf_counter() - preflight_started_at) * 1000.0,
            preflight_timings_ms.get("load_accounts", 0.0),
            preflight_timings_ms.get("proxy_preflight", 0.0),
            preflight_timings_ms.get("start_snapshot", 0.0),
            preflight_timings_ms.get("load_leads", 0.0),
            preflight_timings_ms.get("prefilter_snapshot", 0.0),
            preflight_timings_ms.get("filter_pending", 0.0),
            preflight_timings_ms.get("capacity", 0.0),
        )

    def _worker_rows_snapshot(
        scheduler: AdaptiveScheduler | None,
        worker_slots: Dict[str, Dict[str, Any]] | None,
        health_monitor: HealthMonitor | None,
    ) -> list[dict[str, Any]]:
        if scheduler is None or not worker_slots:
            return []
        rows: list[dict[str, Any]] = []
        now = time.time()
        for worker_id, slot in worker_slots.items():
            snapshot = scheduler.worker_snapshot(worker_id)
            network_key = _normalize_effective_network_key(slot.get("network_key"))
            proxy_id = str(slot.get("proxy_id") or "")
            rows.append(
                {
                    "worker_id": worker_id,
                    "network_key": network_key,
                    "proxy_id": proxy_id,
                    "proxy_label": _proxy_label(network_key or proxy_id),
                    "proxy_status": (
                        "healthy"
                        if _is_local_proxy_id(proxy_id)
<<<<<<< HEAD
                        else health_monitor.proxy_status(network_key, now=now) if health_monitor is not None else ""
=======
                        else health_monitor.proxy_status(proxy_id, now=now) if health_monitor is not None else ""
>>>>>>> origin/main
                    ),
                    "execution_state": (
                        snapshot.execution_state.value if snapshot is not None else WorkerExecutionState.IDLE.value
                    ),
                    "execution_stage": (
                        snapshot.execution_stage.value if snapshot is not None else WorkerExecutionStage.IDLE.value
                    ),
                    "current_lead": snapshot.current_lead if snapshot is not None else "",
                    "current_account": snapshot.current_account if snapshot is not None else "",
                    "state_reason": snapshot.state_reason if snapshot is not None else "",
                    "restarts": int(snapshot.restarts or 0) if snapshot is not None else 0,
                }
            )
        rows.sort(key=lambda item: str(item.get("worker_id") or ""))
        return rows

    def _emit_progress(
        status: str,
        *,
        message: str = "",
        stats_snapshot: Dict[str, int] | None = None,
        total_leads: int | None = None,
        remaining: int | None = None,
        workers_active: int = 0,
        workers_capacity: int = 0,
        workers_effective: int = 0,
        worker_slots: Dict[str, Dict[str, Any]] | None = None,
        scheduler: AdaptiveScheduler | None = None,
        health_monitor: HealthMonitor | None = None,
        runtime_events: list[dict[str, Any]] | None = None,
    ) -> None:
        nonlocal last_progress_message
        if not callable(progress_callback):
            return
        payload_message = str(message or "").strip()
        if payload_message:
            last_progress_message = payload_message
        else:
            payload_message = last_progress_message
        counters = dict(stats_snapshot or {})
        payload = CampaignRunSnapshot.from_payload(
            {
                "run_id": run_id,
                "alias": alias,
                "leads_alias": leads_alias,
                "status": str(status or "").strip() or CampaignRunStatus.IDLE.value,
                "message": payload_message,
                "sent": int(counters.get("sent", 0)),
                "failed": int(counters.get("failed", 0)),
                "skipped": int(counters.get("skipped", 0)),
                "skipped_preblocked": int(counters.get("skipped_preblocked", 0)),
                "retried": int(counters.get("retried", 0)),
                "remaining": max(0, int(remaining if remaining is not None else 0)),
                "total_leads": max(0, int(total_leads if total_leads is not None else total_leads_hint)),
<<<<<<< HEAD
                "selected_leads_total": max(0, int(selected_leads_total_hint or 0)),
                "planned_eligible_leads": max(0, int(planned_eligible_leads_hint or 0)),
=======
>>>>>>> origin/main
                "workers_active": max(0, int(workers_active or 0)),
                "workers_requested": workers_requested,
                "workers_capacity": max(0, int(workers_capacity or 0)),
                "workers_effective": max(0, int(workers_effective or 0)),
                "worker_rows": _worker_rows_snapshot(scheduler, worker_slots, health_monitor),
                "task_active": not CampaignRunStatus.parse(status).is_terminal,
            }
        ).to_payload()
        if runtime_events:
            payload["runtime_events"] = [dict(item) for item in runtime_events if isinstance(item, dict)]
        progress_callback(payload)

    def _build_result(
        *,
        sent: int = 0,
        failed: int = 0,
        skipped: int = 0,
        retried: int = 0,
        remaining: int = 0,
        workers_capacity: int = 0,
        workers_effective: int = 0,
        proxies: int = 0,
        worker_restarts: int = 0,
        skipped_preblocked: int = 0,
        health_state: dict[str, Any] | None = None,
        account_health: dict[str, Any] | None = None,
        preflight_blocked: list[dict[str, Any]] | None = None,
        worker_plan: list[dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        return {
            "sent": max(0, int(sent or 0)),
            "failed": max(0, int(failed or 0)),
            "skipped": max(0, int(skipped or 0)),
            "retried": max(0, int(retried or 0)),
            "remaining": max(0, int(remaining or 0)),
            "workers_requested": workers_requested,
            "workers_capacity": max(0, int(workers_capacity or 0)),
            "workers_effective": max(0, int(workers_effective or 0)),
            "proxies": max(0, int(proxies or 0)),
            "worker_restarts": max(0, int(worker_restarts or 0)),
            "skipped_preblocked": max(0, int(skipped_preblocked or 0)),
<<<<<<< HEAD
            "selected_leads_total": max(0, int(selected_leads_total_hint or 0)),
            "planned_eligible_leads": max(0, int(planned_eligible_leads_hint or 0)),
=======
>>>>>>> origin/main
            "health_state": dict(health_state or {}),
            "account_health": dict(account_health or {}),
            "preflight_blocked": [dict(item) for item in (preflight_blocked or []) if isinstance(item, dict)],
            "worker_plan": [dict(item) for item in (worker_plan or []) if isinstance(item, dict)],
        }

    accounts = _measure_preflight("load_accounts", lambda: _load_selected_accounts(alias))
    if not accounts:
        _run_log("warning", "No hay cuentas cargadas en alias '%s'.", alias)
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay cuentas cargadas para iniciar la campaña.",
            total_leads=total_leads_hint,
        )
        return _build_result()

    campaign_preflight = _measure_preflight(
        "proxy_preflight",
        lambda: _campaign_accounts_preflight(accounts),
    )
    for blocked in campaign_preflight.get("blocked_accounts") or []:
        if not isinstance(blocked, dict):
            continue
        username = str(blocked.get("username") or "").strip().lstrip("@") or "-"
        message = str(blocked.get("message") or blocked.get("reason_code") or "campaign_preflight_blocked").strip()
        _run_log("warning", "Cuenta excluida por campaign preflight @%s: %s", username, message)
    accounts = [
        dict(account)
        for account in (campaign_preflight.get("ready_accounts") or [])
        if isinstance(account, dict)
    ]
    blocked_accounts = [
        dict(item)
        for item in (campaign_preflight.get("blocked_accounts") or [])
        if isinstance(item, dict)
    ]
    network_mode_counts = {
        str(key): int(value or 0)
        for key, value in (campaign_preflight.get("network_mode_counts") or {}).items()
    }
    blocked_reason_counts = {
        str(key): int(value or 0)
        for key, value in (campaign_preflight.get("blocked_reason_counts") or {}).items()
    }
    _run_log(
        "info",
        "Campaign account preflight | alias=%s total=%d ready=%d blocked=%d proxied=%d direct=%d reasons=%s",
        alias,
        len(accounts) + len(blocked_accounts),
        len(accounts),
        len(blocked_accounts),
        int(network_mode_counts.get("proxy", 0)),
        int(network_mode_counts.get("direct", 0)),
        blocked_reason_counts,
    )
    if blocked_accounts:
        _print_info_block(
            "Cuentas excluidas por preflight",
            [
                f"@{str(item.get('username') or '-')} -> {str(item.get('message') or item.get('reason_code') or 'blocked')}"
                for item in blocked_accounts
            ],
        )
    if not accounts:
        _run_log("warning", "No hay cuentas operables tras campaign preflight en alias '%s'.", alias)
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay cuentas operables para iniciar la campaña.",
            total_leads=total_leads_hint,
        )
        return _build_result(preflight_blocked=blocked_accounts)

    alias_accounts = _account_usernames(accounts)
    start_snapshot = _measure_preflight(
        "start_snapshot",
        lambda: campaign_start_snapshot(alias_accounts, campaign_alias=alias),
    )
    accounts = _apply_sent_today_counts(
        accounts,
        sent_today_counts=dict(start_snapshot.get("daily_counts") or {}),
    )

<<<<<<< HEAD
    raw_leads = (
        []
        if configured_planned_queue
        else _measure_preflight("load_leads", lambda: load_leads(leads_alias))
    )
    if not raw_leads and not configured_planned_queue:
=======
    raw_leads = _measure_preflight("load_leads", lambda: load_leads(leads_alias))
    total_leads_hint = max(total_leads_hint, len(raw_leads))
    if not raw_leads:
>>>>>>> origin/main
        _run_log("warning", "No hay leads en alias '%s'.", leads_alias)
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay leads cargados para la campaña.",
            total_leads=total_leads_hint,
        )
        return _build_result()

<<<<<<< HEAD
    skipped_for_quota = 0
    if configured_planned_queue:
        leads = list(configured_planned_queue)
        blocked_total = max(0, int(selected_leads_total_hint) - int(planned_eligible_leads_hint))
        lead_filter_stats = {
            "skipped_duplicates": 0,
            "skipped_already_sent": blocked_total,
            "pending": len(leads),
            "blocked_total": blocked_total,
            "valid_total": max(0, int(planned_eligible_leads_hint or len(leads))),
            "blocked_by_global_contact": 0,
            "blocked_by_alias_sent_status": 0,
            "blocked_by_alias_skipped_status": 0,
            "preserved_pending": 0,
            "advisory_alias_sent_ignored": 0,
            "advisory_campaign_registry_hits": 0,
            "advisory_shared_registry_hits": 0,
            "stale_pending_ignored": 0,
            "blocked_by_campaign_history": 0,
        }
        _run_log(
            "info",
            "Campaign runtime reusing planned queue from launch: raw=%d eligible=%d queued=%d",
            max(0, int(selected_leads_total_hint or len(raw_leads))),
            max(0, int(planned_eligible_leads_hint or len(leads))),
            len(leads),
        )
    else:
        alias_status_map, global_contact_map = _measure_preflight(
            "prefilter_snapshot",
            lambda: get_prefilter_snapshot(alias),
        )
        leads, lead_filter_stats = _measure_preflight(
            "filter_pending",
            lambda: _filter_pending_leads_for_campaign(
                raw_leads,
                alias=alias,
                run_id=run_id,
                alias_accounts=alias_accounts,
                campaign_registry=set(start_snapshot.get("campaign_registry") or set()),
                shared_registry=set(start_snapshot.get("shared_registry") or set()),
                alias_status_map=alias_status_map,
                global_contact_map=global_contact_map,
            ),
        )
    _log_campaign_diagnostics(
        alias=alias,
        leads_alias=leads_alias,
        total_leads_loaded=max(0, int(selected_leads_total_hint or len(raw_leads))),
=======
    alias_status_map, global_contact_map = _measure_preflight(
        "prefilter_snapshot",
        lambda: get_prefilter_snapshot(alias),
    )
    skipped_for_quota = 0
    leads, lead_filter_stats = _measure_preflight(
        "filter_pending",
        lambda: _filter_pending_leads_for_campaign(
            raw_leads,
            alias=alias,
            alias_accounts=alias_accounts,
            campaign_registry=set(start_snapshot.get("campaign_registry") or set()),
            shared_registry=set(start_snapshot.get("shared_registry") or set()),
            alias_status_map=alias_status_map,
            global_contact_map=global_contact_map,
        ),
    )
    _log_campaign_diagnostics(
        alias=alias,
        leads_alias=leads_alias,
        total_leads_loaded=len(raw_leads),
>>>>>>> origin/main
        lead_filter_stats=lead_filter_stats,
        log_callback=_run_log,
    )
    if not leads:
<<<<<<< HEAD
        total_leads_hint = 0
=======
>>>>>>> origin/main
        _run_log(
            "info",
            "Proxy Worker Runner: no quedaron leads elegibles tras el prefilter de campaña (alias=%s leads=%s).",
            alias,
            leads_alias,
        )
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="Todos los leads quedaron excluidos antes de iniciar workers.",
            total_leads=total_leads_hint,
            remaining=0,
        )
        return _build_result(
            skipped_preblocked=int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota,
            preflight_blocked=blocked_accounts,
        )

    capacity = _measure_preflight("capacity", lambda: calculate_workers(accounts))
    worker_groups = capacity.get("worker_groups") or capacity.get("proxy_groups") or {}
    group_capacities = {
        _normalize_effective_network_key(worker_key): max(0, int(value or 0))
        for worker_key, value in (capacity.get("group_capacities") or {}).items()
    }
    ordered_worker_ids = [_normalize_effective_network_key(item) for item in (capacity.get("ordered_worker_ids") or [])]
    worker_proxy_map = {
        _normalize_effective_network_key(worker_key): str(proxy_id or "").strip() or LOCAL_WORKER_PROXY_ID
        for worker_key, proxy_id in (capacity.get("worker_proxy_map") or {}).items()
    }
    workers_capacity = int(capacity.get("workers_capacity") or 0)

    if workers_capacity <= 0:
        skipped_for_quota = len(leads)
        _run_log(
            "info",
            "Proxy Worker Runner: sin capacidad disponible en cuentas del alias '%s'.",
            alias,
        )
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay workers disponibles para ejecutar la campaña.",
            total_leads=total_leads_hint,
            remaining=0,
        )
        return _build_result(
            skipped_preblocked=int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota,
            preflight_blocked=blocked_accounts,
        )

    workers_effective = min(workers_requested, workers_capacity)
    selected_worker_keys = ordered_worker_ids[:workers_effective]
    leads, skipped_for_quota = _limit_leads_to_worker_capacity(
        leads,
        group_capacities=group_capacities,
        worker_ids=selected_worker_keys,
    )
<<<<<<< HEAD
    total_leads_hint = len(leads)
=======
>>>>>>> origin/main
    if skipped_for_quota > 0:
        _run_log(
            "info",
            "Campaign quota cap applied: alias=%s queued=%d deferred=%d capacity=%d",
            alias,
            len(leads),
            skipped_for_quota,
            _total_remaining_capacity_for_groups(group_capacities, selected_worker_keys),
        )
        _print_info_block(
            "Capacidad de campana",
            [
                f"Leads encolados para este run: {len(leads)}",
                f"Leads diferidos por limite de cuentas: {skipped_for_quota}",
            ],
        )
    if not leads:
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay capacidad disponible en las cuentas para nuevos envios.",
            total_leads=total_leads_hint,
            remaining=0,
        )
        return _build_result(
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            skipped_preblocked=int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota,
            preflight_blocked=blocked_accounts,
        )
<<<<<<< HEAD
    try:
        mark_leads_pending(leads, alias=alias, run_id=run_id)
    except Exception as exc:
        _run_log(
            "exception",
            "No se pudo persistir la preseleccion pending para alias '%s'.",
            alias,
            exc_info=True,
        )
        _record_runtime_event(
            {
                "event_type": "pending_selection_persist_failed",
                "severity": "error",
                "failure_kind": "system",
                "message": "No se pudo persistir la preseleccion pending antes de iniciar workers.",
                "error": str(exc) or exc.__class__.__name__,
            }
        )
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.FAILED.value,
            message="No se pudo persistir la preseleccion de usernames.",
            stats_snapshot={"skipped_preblocked": int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota},
            total_leads=total_leads_hint,
            remaining=0,
            workers_active=0,
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            worker_slots={},
        )
        return _build_result(
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            skipped_preblocked=int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota,
            preflight_blocked=blocked_accounts,
        )
=======
>>>>>>> origin/main
    proxy_worker_count = sum(1 for worker_key in selected_worker_keys if worker_key != DIRECT_NETWORK_KEY)
    _log_preflight_timings()
    _print_info_block("Inicializando workers")

    health_monitor = HealthMonitor(
        proxy_degraded_threshold=proxy_degraded_threshold,
        proxy_blocked_threshold=proxy_blocked_threshold,
        proxy_block_seconds=proxy_block_seconds,
        account_cooldown_threshold=cooldown_fail_threshold,
        account_cooldown_seconds=cooldown_seconds,
    )
    initial_tasks = _build_initial_worker_tasks(
        leads,
        worker_ids=selected_worker_keys,
        group_capacities=group_capacities,
    )
    initial_tasks = _validate_initial_worker_tasks(initial_tasks, log_callback=_run_log)
    lead_queue_lock = threading.Lock()
    scheduler = AdaptiveScheduler(
        lead_queue=initial_tasks,
        lead_queue_lock=lead_queue_lock,
        health_monitor=health_monitor,
        idle_seconds=worker_idle_seconds,
        max_attempts_per_lead=max_attempts_per_lead,
    )
    scheduler.register_proxy_queues(selected_worker_keys)

    stats: Dict[str, int] = {
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "retried": 0,
        "worker_restarts": 0,
        "skipped_preblocked": int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota,
    }
    stats_lock = threading.Lock()
    runtime_event_counter = 0
    campaign_started_at = time.time()
    last_progress_at = 0.0
    progress_interval_seconds = 20.0
<<<<<<< HEAD
    worker_slots: Dict[str, Dict[str, Any]] = {}
    runtime_event_sink = _RuntimeEventSink()
    worker_control_registry = _WorkerControlRegistry()
    ipc_config = _build_worker_ipc_config(
        scheduler=scheduler,
        health_monitor=health_monitor,
        stats=stats,
        stats_lock=stats_lock,
        event_sink=runtime_event_sink,
        control_registry=worker_control_registry,
    )
=======
>>>>>>> origin/main

    def _stats_snapshot() -> Dict[str, int]:
        with stats_lock:
            return {
                "sent": int(stats.get("sent", 0)),
                "failed": int(stats.get("failed", 0)),
                "skipped": int(stats.get("skipped", 0)),
                "retried": int(stats.get("retried", 0)),
                "worker_restarts": int(stats.get("worker_restarts", 0)),
                "skipped_preblocked": int(stats.get("skipped_preblocked", 0)),
            }

<<<<<<< HEAD
    def _slot_process_running(slot: MutableMapping[str, Any]) -> bool:
        process = slot.get("process")
        return bool(process is not None and process.poll() is None)

=======
>>>>>>> origin/main
    def _record_runtime_event(raw_event: dict[str, Any]) -> None:
        nonlocal runtime_event_counter
        if not isinstance(raw_event, dict):
            return
        event_type = str(raw_event.get("event_type") or "").strip()
        if not event_type:
            return
        runtime_event_counter += 1
        event = {
            **raw_event,
            "run_id": str(raw_event.get("run_id") or run_id).strip(),
            "event_type": event_type,
            "severity": str(raw_event.get("severity") or "info").strip().lower() or "info",
            "message": str(raw_event.get("message") or last_progress_message or "").strip(),
<<<<<<< HEAD
            "created_at": str(
                raw_event.get("created_at")
                or datetime.now(timezone.utc).isoformat()
            ).strip(),
=======
            "created_at": str(raw_event.get("created_at") or datetime.utcnow().isoformat()).strip(),
>>>>>>> origin/main
            "event_id": str(raw_event.get("event_id") or f"{run_id}:{runtime_event_counter:05d}:{event_type}").strip(),
        }
        _emit_progress(
            str(raw_event.get("status") or CampaignRunStatus.RUNNING.value).strip() or CampaignRunStatus.RUNNING.value,
            message=str(event.get("message") or last_progress_message or "").strip(),
            stats_snapshot=_stats_snapshot(),
            total_leads=total_leads_hint,
            remaining=scheduler.queue_size() if scheduler is not None else total_leads_hint,
<<<<<<< HEAD
            workers_active=sum(1 for slot in worker_slots.values() if _slot_process_running(slot)) if worker_slots else 0,
=======
            workers_active=sum(1 for slot in worker_slots.values() if not slot["future"].done()) if worker_slots else 0,
>>>>>>> origin/main
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            worker_slots=worker_slots,
            scheduler=scheduler,
            health_monitor=health_monitor,
            runtime_events=[event],
        )

    def _emit_live_progress(status: str, message: str) -> None:
        _emit_progress(
            status,
            message=message,
            stats_snapshot=_stats_snapshot(),
            total_leads=total_leads_hint,
            remaining=scheduler.queue_size(),
<<<<<<< HEAD
            workers_active=sum(1 for slot in worker_slots.values() if _slot_process_running(slot)),
=======
            workers_active=sum(1 for slot in worker_slots.values() if not slot["future"].done()),
>>>>>>> origin/main
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            worker_slots=worker_slots,
            scheduler=scheduler,
            health_monitor=health_monitor,
        )

<<<<<<< HEAD
    def _drain_worker_runtime_events() -> None:
        try:
            events = runtime_event_sink.drain_events()
        except Exception as exc:
            _run_log("warning", "No se pudieron drenar runtime_events de workers: %s", exc)
            return
        for event in events:
            _record_runtime_event(event)

=======
>>>>>>> origin/main
    launch_batch_size = _as_int(
        config.get("launch_batch_size", DEFAULT_LAUNCH_BATCH_SIZE),
        default=DEFAULT_LAUNCH_BATCH_SIZE,
        minimum=1,
    )
    launch_stagger_min_seconds = _as_float(
        config.get("launch_stagger_min_seconds", DEFAULT_LAUNCH_STAGGER_MIN_SECONDS),
        default=DEFAULT_LAUNCH_STAGGER_MIN_SECONDS,
        minimum=0.0,
    )
    launch_stagger_max_seconds = _as_float(
        config.get("launch_stagger_max_seconds", max(launch_stagger_min_seconds, DEFAULT_LAUNCH_STAGGER_MAX_SECONDS)),
        default=max(launch_stagger_min_seconds, DEFAULT_LAUNCH_STAGGER_MAX_SECONDS),
        minimum=launch_stagger_min_seconds,
    )
    launch_batch_pause_min_seconds = _as_float(
        config.get("launch_batch_pause_min_seconds", DEFAULT_LAUNCH_BATCH_PAUSE_MIN_SECONDS),
        default=DEFAULT_LAUNCH_BATCH_PAUSE_MIN_SECONDS,
        minimum=0.0,
    )
    launch_batch_pause_max_seconds = _as_float(
        config.get(
            "launch_batch_pause_max_seconds",
            max(launch_batch_pause_min_seconds, DEFAULT_LAUNCH_BATCH_PAUSE_MAX_SECONDS),
        ),
        default=max(launch_batch_pause_min_seconds, DEFAULT_LAUNCH_BATCH_PAUSE_MAX_SECONDS),
        minimum=launch_batch_pause_min_seconds,
    )
    worker_plan = [
        {
            "network_key": worker_key,
            "proxy_id": worker_proxy_map.get(worker_key, _runtime_proxy_id_from_network_key(worker_key)),
            "proxy_label": _proxy_label(worker_key),
            "account_count": len(worker_groups.get(worker_key, [])),
            "accounts": [
                str(account.get("username") or "").strip()
                for account in worker_groups.get(worker_key, [])
                if str(account.get("username") or "").strip()
            ],
        }
        for worker_key in selected_worker_keys
    ]

    _run_log(
        "info",
        "alias=%s leads=%d proxies=%d workers=%d",
        alias,
        len(leads),
        proxy_worker_count,
        workers_effective,
    )
    visible_browser_layout = None
    if not headless:
        visible_browser_layout = {
            "scope": f"campaign:{run_id}",
            "target_count": workers_effective,
            "layout_policy": "compact",
<<<<<<< HEAD
            **_campaign_desktop_layout_payload(),
=======
>>>>>>> origin/main
            "stagger_min_ms": 300,
            "stagger_max_ms": 800,
            "stagger_step_ms": 100,
        }

    campaign_token = EngineCancellationToken(f"proxy-campaign:{alias}")
    token_binding = bind_stop_token(campaign_token)
<<<<<<< HEAD
    try:
        def _launch_sleep_processes(seconds: float) -> None:
=======
    worker_slots: Dict[str, Dict[str, Any]] = {}

    with ThreadPoolExecutor(max_workers=workers_effective, thread_name_prefix="proxy-worker") as executor:

        def _launch_sleep(seconds: float) -> None:
>>>>>>> origin/main
            remaining = max(0.0, float(seconds))
            while remaining > 0:
                if STOP_EVENT.is_set():
                    return
                step = min(0.20, remaining)
                time.sleep(step)
                remaining = max(0.0, remaining - step)

<<<<<<< HEAD
        def _build_worker_process_cfg(worker_id: str, worker_key: str) -> dict[str, Any]:
=======
        def _spawn_worker(worker_id: str, worker_key: str) -> None:
>>>>>>> origin/main
            runtime_proxy_id = worker_proxy_map.get(worker_key, _runtime_proxy_id_from_network_key(worker_key))
            retry_proxy_ids = list(selected_worker_keys)
            if worker_key not in retry_proxy_ids:
                retry_proxy_ids.append(worker_key)
<<<<<<< HEAD
            return {
                "worker_id": worker_id,
                "network_key": worker_key,
                "proxy_id": runtime_proxy_id,
                "accounts": [dict(item) for item in worker_groups.get(worker_key, []) if isinstance(item, dict)],
                "all_proxy_ids": retry_proxy_ids,
                "delay_min": delay_min,
                "delay_max": delay_max,
                "template_variants": template_rotator.variants,
                "cooldown_fail_threshold": cooldown_fail_threshold,
                "campaign_alias": alias,
                "leads_alias": leads_alias,
                "campaign_run_id": run_id,
                "headless": headless,
                "send_flow_timeout_seconds": send_flow_timeout_seconds,
                "visible_browser_layout": dict(visible_browser_layout or {}),
                "active_account_limit": worker_active_account_limit,
                "session_close_timeout_seconds": worker_session_close_timeout_seconds,
                "ipc": dict(ipc_config),
            }

        def _request_worker_process_stop(worker_id: str, reason: str) -> None:
            slot = worker_slots.get(worker_id)
            if slot is None:
                return
            if not slot.get("stop_requested_at"):
                slot["stop_requested_at"] = time.time()
            worker_control_registry.request_stop(worker_id, reason)

        def _terminate_worker_process(worker_id: str, slot: MutableMapping[str, Any], *, reason: str) -> None:
            process = slot.get("process")
            if process is None or process.poll() is not None:
                return
            _run_log("warning", "Terminando worker %s por %s.", worker_id, reason)
            try:
                process.terminate()
            except Exception as exc:
                _run_log("warning", "No se pudo terminar worker %s: %s", worker_id, exc)

        def _spawn_worker_process_slot(worker_id: str, worker_key: str, *, register_runtime: bool) -> None:
            runtime_proxy_id = worker_proxy_map.get(worker_key, _runtime_proxy_id_from_network_key(worker_key))
            if register_runtime:
                scheduler.register_worker(worker_id, worker_key)
            worker_control_registry.clear_worker(worker_id)
            process, cfg_path = spawn_worker_process(_build_worker_process_cfg(worker_id, worker_key))
            previous_slot = worker_slots.get(worker_id)
            if previous_slot is not None:
                cleanup_worker_process_slot(previous_slot)
            worker_slots[worker_id] = {
                "process": process,
                "config_path": cfg_path,
                "network_key": worker_key,
                "proxy_id": runtime_proxy_id,
                "restart_requested": False,
                "next_network_key": "",
                "stop_requested_at": 0.0,
            }
=======
            worker = ProxyWorker(
                worker_id=worker_id,
                network_key=worker_key,
                proxy_id=runtime_proxy_id,
                accounts=worker_groups.get(worker_key, []),
                all_proxy_ids=retry_proxy_ids,
                scheduler=scheduler,
                health_monitor=health_monitor,
                stats=stats,
                stats_lock=stats_lock,
                delay_min=delay_min,
                delay_max=delay_max,
                template_rotator=template_rotator,
                cooldown_fail_threshold=cooldown_fail_threshold,
                campaign_alias=alias,
                leads_alias=leads_alias,
                campaign_run_id=run_id,
                runtime_event_callback=_record_runtime_event,
                headless=headless,
                send_flow_timeout_seconds=send_flow_timeout_seconds,
                visible_browser_layout=visible_browser_layout,
            )
            scheduler.register_worker(worker_id, worker_key)
            future = executor.submit(bind_stop_token_callable(campaign_token, worker.run))
>>>>>>> origin/main
            worker_suffix = str(worker_id).split("-")[-1] or worker_id
            print("")
            print(f"Worker #{worker_suffix} iniciado")
            print(f"Worker key: {worker_key}")
            print(f"Red efectiva: {_proxy_label(worker_key)}")
            print(f"Proxy runtime: {runtime_proxy_id}")
            print(f"Cuentas asignadas: {len(worker_groups.get(worker_key, []))}")
<<<<<<< HEAD

        def _spawn_workers_in_batches_processes() -> None:
=======
            worker_slots[worker_id] = {
                "worker": worker,
                "future": future,
                "network_key": worker_key,
                "proxy_id": runtime_proxy_id,
            }

        def _spawn_workers_in_batches() -> None:
>>>>>>> origin/main
            specs = [(f"worker-{index}", worker_key) for index, worker_key in enumerate(selected_worker_keys, start=1)]
            if not specs:
                return
            total_batches = max(1, (len(specs) + launch_batch_size - 1) // launch_batch_size)
            for batch_index in range(total_batches):
                if STOP_EVENT.is_set():
                    return
                start = batch_index * launch_batch_size
                batch = specs[start : start + launch_batch_size]
                _run_log(
                    "info",
                    "Worker launch batch %d/%d size=%d",
                    batch_index + 1,
                    total_batches,
                    len(batch),
                )
                _emit_live_progress(
                    "Starting",
                    f"Apertura escalonada: iniciando tanda {batch_index + 1} de {total_batches}.",
                )
                for item_index, (worker_id, worker_key) in enumerate(batch, start=1):
                    if STOP_EVENT.is_set():
                        return
<<<<<<< HEAD
                    _spawn_worker_process_slot(worker_id, worker_key, register_runtime=True)
=======
                    _spawn_worker(worker_id, worker_key)
>>>>>>> origin/main
                    if item_index >= len(batch):
                        continue
                    delay_seconds = random.uniform(launch_stagger_min_seconds, launch_stagger_max_seconds)
                    _run_log(
                        "info",
                        "Worker launch stagger: worker=%s network=%s sleep=%.2fs",
                        worker_id,
                        worker_key,
                        delay_seconds,
                    )
<<<<<<< HEAD
                    _launch_sleep_processes(delay_seconds)
=======
                    _launch_sleep(delay_seconds)
>>>>>>> origin/main
                if batch_index >= total_batches - 1:
                    continue
                pause_seconds = random.uniform(launch_batch_pause_min_seconds, launch_batch_pause_max_seconds)
                _run_log(
                    "info",
                    "Worker launch batch pause after batch %d: %.2fs",
                    batch_index + 1,
                    pause_seconds,
                )
                _emit_live_progress(
                    "Starting",
                    f"Apertura escalonada: pausa entre tandas ({pause_seconds:.1f}s).",
                )
<<<<<<< HEAD
                _launch_sleep_processes(pause_seconds)
=======
                _launch_sleep(pause_seconds)
>>>>>>> origin/main

        _print_info_block(
            "Workers construidos",
            [
                (
                    f"{row['network_key']} -> {', '.join(row['accounts'])}"
                    if row["accounts"]
                    else f"{row['network_key']} -> sin cuentas"
                )
                for row in worker_plan
            ],
        )
<<<<<<< HEAD
        _spawn_workers_in_batches_processes()
=======
        _spawn_workers_in_batches()
>>>>>>> origin/main

        _emit_live_progress(
            "Starting",
            "Workers inicializados con apertura escalonada. Preparando cuentas y cola de leads.",
        )

<<<<<<< HEAD
        _print_info_block("Cuentas listas para envÃƒÂ­o")
=======
        _print_info_block("Cuentas listas para envÃ­o")
>>>>>>> origin/main
        reported_accounts: set[str] = set()
        for worker_key in selected_worker_keys:
            for account in worker_groups.get(worker_key, []):
                username = str(account.get("username") or "").strip()
                if not username or username in reported_accounts:
                    continue
                reported_accounts.add(username)
                session_label = (
<<<<<<< HEAD
                    "session_ready Ã¢Å“â€œ"
=======
                    "session_ready âœ“"
>>>>>>> origin/main
                    if has_playwright_storage_state(username)
                    else "session_pending"
                )
                print("")
                print(f"Cuenta: {username}")
                print(f"Worker: {worker_key}")
                print(f"Estado: {session_label}")

        _emit_live_progress(
            "Running",
<<<<<<< HEAD
            "CampaÃ±a iniciada. Workers activos procesando la cola.",
        )

        while worker_slots and not STOP_EVENT.is_set():
            _drain_worker_runtime_events()
=======
            "Campaña iniciada. Workers activos procesando la cola.",
        )

        while worker_slots and not STOP_EVENT.is_set():
>>>>>>> origin/main
            queue_size = scheduler.queue_size()
            now = time.time()
            if now - last_progress_at >= progress_interval_seconds:
                _print_progress_block(
                    sent=int(stats.get("sent", 0)),
                    failed=int(stats.get("failed", 0)),
                    skipped=int(stats.get("skipped", 0)),
                    remaining=queue_size,
                    started_at=campaign_started_at,
                )
                last_progress_at = now

            _emit_live_progress(
                "Running",
<<<<<<< HEAD
                "Procesando cola activa de campaÃ±a.",
=======
                "Procesando cola activa de campaña.",
>>>>>>> origin/main
            )

            if queue_size > 0:
                for worker_id, slot in list(worker_slots.items()):
<<<<<<< HEAD
                    process = slot.get("process")
                    if process is None or process.poll() is not None:
                        continue
                    snapshot = scheduler.worker_snapshot(worker_id)
                    if snapshot is None:
                        continue
                    if not scheduler.worker_is_stalled(worker_id, now=now):
                        continue
                    current_network_key = _normalize_effective_network_key(slot.get("network_key"))
                    current_proxy = str(slot.get("proxy_id") or "")
                    proxy_status = (
                        "healthy"
                        if _is_local_proxy_id(current_proxy)
                        else health_monitor.proxy_status(current_network_key, now=now)
                    )
=======
                    worker: ProxyWorker = slot["worker"]
                    snapshot = scheduler.worker_snapshot(worker_id)
                    if snapshot is None:
                        continue
                    if worker.is_busy(now=now):
                        continue
                    if not scheduler.worker_is_stalled(worker_id, now=now):
                        continue
                    current_network_key = _normalize_effective_network_key(slot.get("network_key"))
                    current_proxy = str(slot["proxy_id"] or "")
                    proxy_status = "healthy" if _is_local_proxy_id(current_proxy) else health_monitor.proxy_status(current_proxy, now=now)
>>>>>>> origin/main
                    activity_age = max(0.0, now - snapshot.last_activity_at)
                    stage_age = max(0.0, now - snapshot.state_entered_at)
                    _run_log(
                        "warning",
                        "Worker stalled detectado: %s network=%s proxy=%s status=%s exec_state=%s exec_stage=%s lead=%s account=%s activity_age=%.1fs stage_age=%.1fs queue=%d",
                        worker_id,
                        current_network_key,
                        current_proxy,
                        proxy_status,
                        snapshot.execution_state.value,
                        snapshot.execution_stage.value,
                        snapshot.current_lead or "-",
                        snapshot.current_account or "-",
                        activity_age,
                        stage_age,
                        queue_size,
                    )
                    _record_runtime_event(
                        {
                            "event_type": "worker_stalled",
                            "severity": "warning",
                            "failure_kind": "retryable",
                            "message": f"Worker {worker_id} detectado como stalled.",
                            "worker_id": worker_id,
                            "network_key": current_network_key,
                            "proxy_id": current_proxy,
                            "lead": snapshot.current_lead or "",
                            "account": snapshot.current_account or "",
                            "queue_size": queue_size,
                            "activity_age_seconds": round(activity_age, 1),
                            "stage_age_seconds": round(stage_age, 1),
                            "proxy_status": proxy_status,
                        }
                    )
<<<<<<< HEAD
                    if not slot.get("restart_requested"):
                        restart_reason = "worker_stalled"
                        next_network_key = current_network_key
                        if proxy_status == "blocked":
                            next_network_key = scheduler.reassign_worker_proxy(
                                worker_id,
                                current_proxy=current_network_key,
                                all_proxy_ids=selected_worker_keys,
                            )
                            restart_reason = "idle_reassignment"
                        slot["next_network_key"] = next_network_key
                        slot["restart_requested"] = True
                        _request_worker_process_stop(worker_id, restart_reason)
                        continue
                    stop_requested_at = float(slot.get("stop_requested_at") or 0.0)
                    if stop_requested_at and (now - stop_requested_at) > worker_shutdown_timeout_seconds:
                        _terminate_worker_process(worker_id, slot, reason="stalled_shutdown_timeout")

            for worker_id in list(worker_slots.keys()):
                slot = worker_slots[worker_id]
                process = slot.get("process")
                if process is None:
                    continue
                exit_code = process.poll()
                if exit_code is None:
                    continue
                _drain_worker_runtime_events()
                cleanup_worker_process_slot(slot)
                queue_pending = scheduler.queue_size() > 0
                should_restart = queue_pending and not STOP_EVENT.is_set()
                reason = "completed"
                if exit_code != 0:
                    reason = f"exit_code:{exit_code}"
                    _run_log("error", "Worker %s termino con exit code %s.", worker_id, exit_code)
                    _record_runtime_event(
                        {
                            "event_type": "worker_process_crashed",
                            "severity": "error",
                            "failure_kind": "system",
                            "message": f"Worker {worker_id} termino de forma inesperada.",
                            "worker_id": worker_id,
                            "network_key": str(slot.get("network_key") or ""),
                            "proxy_id": str(slot.get("proxy_id") or ""),
                            "exit_code": exit_code,
=======
                    if proxy_status == "blocked":
                        new_network_key = scheduler.reassign_worker_proxy(
                            worker_id,
                            current_proxy=current_network_key,
                            all_proxy_ids=selected_worker_keys,
                        )
                        slot["next_network_key"] = new_network_key
                        worker.request_stop("idle_reassignment")

            for worker_id in list(worker_slots.keys()):
                slot = worker_slots[worker_id]
                future: Future = slot["future"]
                if not future.done():
                    continue

                exc = future.exception()
                queue_pending = scheduler.queue_size() > 0
                should_restart = queue_pending and not STOP_EVENT.is_set()
                reason = "completed"
                if exc is not None:
                    reason = f"exception:{exc}"
                    _run_log("error", "Worker %s termino con excepcion: %s", worker_id, exc)
                    _record_runtime_event(
                        {
                            "event_type": "worker_future_exception",
                            "severity": "error",
                            "failure_kind": "system",
                            "message": f"Worker {worker_id} termino con excepcion.",
                            "worker_id": worker_id,
                            "network_key": str(slot.get("network_key") or ""),
                            "proxy_id": str(slot.get("proxy_id") or ""),
                            "error": str(exc) or exc.__class__.__name__,
>>>>>>> origin/main
                        }
                    )
                elif should_restart:
                    reason = "queue_pending"

                if should_restart:
                    restart_count = scheduler.record_worker_restart(worker_id)
                    with stats_lock:
                        stats["worker_restarts"] = int(stats.get("worker_restarts", 0)) + 1
                    if restart_count > worker_restart_limit:
<<<<<<< HEAD
=======
                        logger.error(
                            "Worker %s alcanzÃ³ lÃ­mite de reinicios (%d).",
                            worker_id,
                            worker_restart_limit,
                        )
>>>>>>> origin/main
                        _run_log("error", "Worker %s alcanzo limite de reinicios (%d).", worker_id, worker_restart_limit)
                        _record_runtime_event(
                            {
                                "event_type": "worker_restart_limit_reached",
                                "severity": "error",
                                "failure_kind": "terminal",
                                "message": f"Worker {worker_id} alcanzo el limite de reinicios.",
                                "worker_id": worker_id,
                                "network_key": str(slot.get("network_key") or ""),
                                "proxy_id": str(slot.get("proxy_id") or ""),
                                "restart_count": restart_count,
                                "restart_limit": worker_restart_limit,
                            }
                        )
                        worker_slots.pop(worker_id, None)
                        continue

                    restart_network_key = _normalize_effective_network_key(
                        slot.get("next_network_key") or slot.get("network_key") or ""
                    )
                    restart_proxy = worker_proxy_map.get(
                        restart_network_key,
                        _runtime_proxy_id_from_network_key(restart_network_key),
                    )
<<<<<<< HEAD
                    if restart_network_key != DIRECT_NETWORK_KEY and not health_monitor.is_proxy_available(restart_network_key):
=======
                    if restart_network_key != DIRECT_NETWORK_KEY and not health_monitor.is_proxy_available(restart_proxy):
>>>>>>> origin/main
                        restart_network_key = scheduler.reassign_worker_proxy(
                            worker_id,
                            current_proxy=restart_network_key,
                            all_proxy_ids=selected_worker_keys,
                        )
                        restart_proxy = worker_proxy_map.get(
                            restart_network_key,
                            _runtime_proxy_id_from_network_key(restart_network_key),
                        )
                    if restart_network_key not in worker_groups:
                        restart_network_key = _normalize_effective_network_key(slot.get("network_key") or "")
                        restart_proxy = str(slot.get("proxy_id") or "")

                    _run_log(
                        "warning",
                        "Worker restarted: %s network=%s proxy=%s reason=%s restart=%d",
                        worker_id,
                        restart_network_key,
                        restart_proxy,
                        reason,
                        restart_count,
                    )
                    _record_runtime_event(
                        {
                            "event_type": "worker_restarted",
                            "severity": "warning",
                            "failure_kind": "retryable",
                            "message": f"Worker {worker_id} relanzado en {_proxy_label(restart_network_key)}.",
                            "worker_id": worker_id,
                            "network_key": restart_network_key,
                            "proxy_id": restart_proxy,
                            "reason": reason,
                            "restart_count": restart_count,
                        }
                    )
<<<<<<< HEAD
                    _spawn_worker_process_slot(worker_id, restart_network_key, register_runtime=False)
=======
                    _spawn_worker(worker_id, restart_network_key)
                    _run_log("info", "Worker %s relanzado en worker %s.", worker_id, restart_network_key)
>>>>>>> origin/main
                    _emit_live_progress(
                        "Running",
                        f"Worker {worker_id} relanzado en {_proxy_label(restart_network_key)}.",
                    )
                    continue

                worker_slots.pop(worker_id, None)

<<<<<<< HEAD
            if scheduler.is_empty() and all(not _slot_process_running(slot) for slot in worker_slots.values()):
                break
            time.sleep(monitor_interval)

        _drain_worker_runtime_events()
        for worker_id, slot in list(worker_slots.items()):
            _request_worker_process_stop(worker_id, "campaign_shutdown")
            process = slot.get("process")
            if process is None:
                continue
            try:
                process.wait(timeout=worker_shutdown_timeout_seconds)
            except subprocess.TimeoutExpired:
                _terminate_worker_process(worker_id, slot, reason="campaign_shutdown_timeout")
                try:
                    process.wait(timeout=worker_session_close_timeout_seconds)
                except subprocess.TimeoutExpired:
                    try:
                        process.kill()
                    except Exception:
                        pass
                _run_log(
                    "warning",
                    "Worker %s no se detuvo dentro de %.1fs durante shutdown.",
                    worker_id,
                    worker_shutdown_timeout_seconds,
                )
=======
            if scheduler.is_empty():
                if all(slot["future"].done() for slot in worker_slots.values()):
                    break
            time.sleep(monitor_interval)

        for worker_id, slot in list(worker_slots.items()):
            worker: ProxyWorker = slot["worker"]
            worker.request_stop("campaign_shutdown")
            future: Future = slot["future"]
            try:
                future.result(timeout=worker_shutdown_timeout_seconds)
            except FutureTimeoutError:
                _run_log("warning", "Worker %s no se detuvo dentro de %.1fs durante shutdown.", worker_id, worker_shutdown_timeout_seconds)
>>>>>>> origin/main
                _record_runtime_event(
                    {
                        "event_type": "worker_shutdown_timeout",
                        "severity": "warning",
                        "failure_kind": "system",
                        "message": f"Worker {worker_id} no se detuvo dentro del timeout de shutdown.",
                        "worker_id": worker_id,
                        "proxy_id": str(slot.get("proxy_id") or ""),
                        "timeout_seconds": worker_shutdown_timeout_seconds,
                    }
                )
            except Exception as exc:
                _run_log("exception", "Worker %s fallo durante shutdown.", worker_id, exc_info=True)
                _record_runtime_event(
                    {
                        "event_type": "worker_shutdown_failed",
                        "severity": "error",
                        "failure_kind": "system",
                        "message": f"Worker {worker_id} fallo durante shutdown.",
                        "worker_id": worker_id,
                        "proxy_id": str(slot.get("proxy_id") or ""),
                        "error": str(exc) or exc.__class__.__name__,
                    }
                )
<<<<<<< HEAD
            cleanup_worker_process_slot(slot)
=======
>>>>>>> origin/main
            worker_suffix = str(worker_id).split("-")[-1] or worker_id
            _print_info_block(
                "Worker detenido",
                [f"Worker #{worker_suffix} finalizado"],
            )
<<<<<<< HEAD
        worker_slots.clear()
        _drain_worker_runtime_events()

        stop_requested = STOP_EVENT.is_set()
        residual_tasks = scheduler.drain_all()
        residual_count = len(residual_tasks)
        if residual_tasks and not stop_requested:
            with stats_lock:
                stats["failed"] = int(stats.get("failed", 0)) + residual_count
            _run_log(
                "warning",
                "Proxy Worker Runner: %d leads marcados como fallidos por falta de workers activos.",
                residual_count,
            )
            for task in residual_tasks:
                try:
                    mark_lead_failed(
                        task.lead,
                        reason="worker_exhausted",
                        attempts=task.attempt,
                        alias=alias,
                    )
                except Exception as exc:
                    _run_log("exception", "No se pudo persistir worker_exhausted para @%s.", task.lead, exc_info=True)
                    _record_runtime_event(
                        {
                            "event_type": "worker_exhausted_persist_failed",
                            "severity": "error",
                            "failure_kind": "system",
                            "message": "No se pudo persistir worker_exhausted en lead_status.",
                            "lead": task.lead,
                            "error": str(exc) or exc.__class__.__name__,
                        }
                    )
        elif residual_tasks:
            _run_log(
                "info",
                "Proxy Worker Runner: stop solicitado; %d leads quedan pendientes para un proximo run.",
                residual_count,
            )

        result = _build_result(
            sent=int(stats.get("sent", 0)),
            failed=int(stats.get("failed", 0)),
            skipped=int(stats.get("skipped", 0)),
            retried=int(stats.get("retried", 0)),
            remaining=residual_count if stop_requested else scheduler.queue_size(),
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            proxies=proxy_worker_count,
            worker_restarts=int(stats.get("worker_restarts", 0)),
            skipped_preblocked=int(stats.get("skipped_preblocked", 0)),
            health_state=health_monitor.snapshot(),
            account_health=health_monitor.accounts_snapshot(),
            preflight_blocked=blocked_accounts,
            worker_plan=worker_plan,
        )
        _run_log(
            "info",
            "Proxy Worker Runner finalizado: sent=%d failed=%d retried=%d remaining=%d workers=%d proxies=%d restarts=%d",
            result["sent"],
            result["failed"],
            result["retried"],
            result["remaining"],
            result["workers_effective"],
            result["proxies"],
            result["worker_restarts"],
        )
        finished_at = time.time()
        if stop_requested:
            _print_campaign_end_block(
                completed=False,
                reason="detenida por usuario",
                sent=result["sent"],
                failed=result["failed"],
                skipped=result["skipped"],
                remaining=result["remaining"],
                started_at=campaign_started_at,
                finished_at=finished_at,
            )
            _emit_progress(
                CampaignRunStatus.STOPPED.value,
                message="CampaÃ±a detenida por usuario.",
                stats_snapshot=_stats_snapshot(),
                total_leads=total_leads_hint,
                remaining=result["remaining"],
                workers_active=0,
                workers_capacity=workers_capacity,
                workers_effective=workers_effective,
                worker_slots={},
                scheduler=scheduler,
                health_monitor=health_monitor,
            )
        else:
            _print_campaign_end_block(
                completed=True,
                reason="todos los leads procesados",
                sent=result["sent"],
                failed=result["failed"],
                skipped=result["skipped"],
                remaining=result["remaining"],
                started_at=campaign_started_at,
                finished_at=finished_at,
            )
            _emit_progress(
                CampaignRunStatus.COMPLETED.value,
                message="CampaÃ±a finalizada. Todos los leads del run actual fueron procesados.",
                stats_snapshot=_stats_snapshot(),
                total_leads=total_leads_hint,
                remaining=result["remaining"],
                workers_active=0,
                workers_capacity=workers_capacity,
                workers_effective=workers_effective,
                worker_slots={},
                scheduler=scheduler,
                health_monitor=health_monitor,
            )
        return result

        worker_slots: Dict[str, Dict[str, Any]] = {}
    
        with _LEGACY_THREADPOOL_EXECUTOR_REMOVED(max_workers=workers_effective, thread_name_prefix="proxy-worker") as executor:
    
            def _launch_sleep(seconds: float) -> None:
                remaining = max(0.0, float(seconds))
                while remaining > 0:
                    if STOP_EVENT.is_set():
                        return
                    step = min(0.20, remaining)
                    time.sleep(step)
                    remaining = max(0.0, remaining - step)
    
            def _spawn_worker(worker_id: str, worker_key: str) -> None:
                runtime_proxy_id = worker_proxy_map.get(worker_key, _runtime_proxy_id_from_network_key(worker_key))
                retry_proxy_ids = list(selected_worker_keys)
                if worker_key not in retry_proxy_ids:
                    retry_proxy_ids.append(worker_key)
                worker = ProxyWorker(
                    worker_id=worker_id,
                    network_key=worker_key,
                    proxy_id=runtime_proxy_id,
                    accounts=worker_groups.get(worker_key, []),
                    all_proxy_ids=retry_proxy_ids,
                    scheduler=scheduler,
                    health_monitor=health_monitor,
                    stats=stats,
                    stats_lock=stats_lock,
                    delay_min=delay_min,
                    delay_max=delay_max,
                    template_rotator=template_rotator,
                    cooldown_fail_threshold=cooldown_fail_threshold,
                    campaign_alias=alias,
                    leads_alias=leads_alias,
                    campaign_run_id=run_id,
                    runtime_event_callback=_record_runtime_event,
                    headless=headless,
                    send_flow_timeout_seconds=send_flow_timeout_seconds,
                    visible_browser_layout=visible_browser_layout,
                    active_account_limit=worker_active_account_limit,
                    session_close_timeout_seconds=worker_session_close_timeout_seconds,
                )
                scheduler.register_worker(worker_id, worker_key)
                future = executor.submit(_legacy_bind_stop_token_callable_removed(campaign_token, worker.run))
                worker_suffix = str(worker_id).split("-")[-1] or worker_id
                print("")
                print(f"Worker #{worker_suffix} iniciado")
                print(f"Worker key: {worker_key}")
                print(f"Red efectiva: {_proxy_label(worker_key)}")
                print(f"Proxy runtime: {runtime_proxy_id}")
                print(f"Cuentas asignadas: {len(worker_groups.get(worker_key, []))}")
                worker_slots[worker_id] = {
                    "worker": worker,
                    "future": future,
                    "network_key": worker_key,
                    "proxy_id": runtime_proxy_id,
                }
    
            def _spawn_workers_in_batches() -> None:
                specs = [(f"worker-{index}", worker_key) for index, worker_key in enumerate(selected_worker_keys, start=1)]
                if not specs:
                    return
                total_batches = max(1, (len(specs) + launch_batch_size - 1) // launch_batch_size)
                for batch_index in range(total_batches):
                    if STOP_EVENT.is_set():
                        return
                    start = batch_index * launch_batch_size
                    batch = specs[start : start + launch_batch_size]
                    _run_log(
                        "info",
                        "Worker launch batch %d/%d size=%d",
                        batch_index + 1,
                        total_batches,
                        len(batch),
                    )
                    _emit_live_progress(
                        "Starting",
                        f"Apertura escalonada: iniciando tanda {batch_index + 1} de {total_batches}.",
                    )
                    for item_index, (worker_id, worker_key) in enumerate(batch, start=1):
                        if STOP_EVENT.is_set():
                            return
                        _spawn_worker(worker_id, worker_key)
                        if item_index >= len(batch):
                            continue
                        delay_seconds = random.uniform(launch_stagger_min_seconds, launch_stagger_max_seconds)
                        _run_log(
                            "info",
                            "Worker launch stagger: worker=%s network=%s sleep=%.2fs",
                            worker_id,
                            worker_key,
                            delay_seconds,
                        )
                        _launch_sleep(delay_seconds)
                    if batch_index >= total_batches - 1:
                        continue
                    pause_seconds = random.uniform(launch_batch_pause_min_seconds, launch_batch_pause_max_seconds)
                    _run_log(
                        "info",
                        "Worker launch batch pause after batch %d: %.2fs",
                        batch_index + 1,
                        pause_seconds,
                    )
                    _emit_live_progress(
                        "Starting",
                        f"Apertura escalonada: pausa entre tandas ({pause_seconds:.1f}s).",
                    )
                    _launch_sleep(pause_seconds)
    
            _print_info_block(
                "Workers construidos",
                [
                    (
                        f"{row['network_key']} -> {', '.join(row['accounts'])}"
                        if row["accounts"]
                        else f"{row['network_key']} -> sin cuentas"
                    )
                    for row in worker_plan
                ],
            )
            _spawn_workers_in_batches()
    
            _emit_live_progress(
                "Starting",
                "Workers inicializados con apertura escalonada. Preparando cuentas y cola de leads.",
            )
    
            _print_info_block("Cuentas listas para envÃ­o")
            reported_accounts: set[str] = set()
            for worker_key in selected_worker_keys:
                for account in worker_groups.get(worker_key, []):
                    username = str(account.get("username") or "").strip()
                    if not username or username in reported_accounts:
                        continue
                    reported_accounts.add(username)
                    session_label = (
                        "session_ready âœ“"
                        if has_playwright_storage_state(username)
                        else "session_pending"
                    )
                    print("")
                    print(f"Cuenta: {username}")
                    print(f"Worker: {worker_key}")
                    print(f"Estado: {session_label}")
    
            _emit_live_progress(
                "Running",
                "Campaña iniciada. Workers activos procesando la cola.",
            )
    
            while worker_slots and not STOP_EVENT.is_set():
                queue_size = scheduler.queue_size()
                now = time.time()
                if now - last_progress_at >= progress_interval_seconds:
                    _print_progress_block(
                        sent=int(stats.get("sent", 0)),
                        failed=int(stats.get("failed", 0)),
                        skipped=int(stats.get("skipped", 0)),
                        remaining=queue_size,
                        started_at=campaign_started_at,
                    )
                    last_progress_at = now
    
                _emit_live_progress(
                    "Running",
                    "Procesando cola activa de campaña.",
                )
    
                if queue_size > 0:
                    for worker_id, slot in list(worker_slots.items()):
                        worker: ProxyWorker = slot["worker"]
                        snapshot = scheduler.worker_snapshot(worker_id)
                        if snapshot is None:
                            continue
                        if worker.is_busy(now=now):
                            continue
                        if not scheduler.worker_is_stalled(worker_id, now=now):
                            continue
                        current_network_key = _normalize_effective_network_key(slot.get("network_key"))
                        current_proxy = str(slot["proxy_id"] or "")
                        proxy_status = (
                            "healthy"
                            if _is_local_proxy_id(current_proxy)
                            else health_monitor.proxy_status(current_network_key, now=now)
                        )
                        activity_age = max(0.0, now - snapshot.last_activity_at)
                        stage_age = max(0.0, now - snapshot.state_entered_at)
                        _run_log(
                            "warning",
                            "Worker stalled detectado: %s network=%s proxy=%s status=%s exec_state=%s exec_stage=%s lead=%s account=%s activity_age=%.1fs stage_age=%.1fs queue=%d",
                            worker_id,
                            current_network_key,
                            current_proxy,
                            proxy_status,
                            snapshot.execution_state.value,
                            snapshot.execution_stage.value,
                            snapshot.current_lead or "-",
                            snapshot.current_account or "-",
                            activity_age,
                            stage_age,
                            queue_size,
                        )
                        _record_runtime_event(
                            {
                                "event_type": "worker_stalled",
                                "severity": "warning",
                                "failure_kind": "retryable",
                                "message": f"Worker {worker_id} detectado como stalled.",
                                "worker_id": worker_id,
                                "network_key": current_network_key,
                                "proxy_id": current_proxy,
                                "lead": snapshot.current_lead or "",
                                "account": snapshot.current_account or "",
                                "queue_size": queue_size,
                                "activity_age_seconds": round(activity_age, 1),
                                "stage_age_seconds": round(stage_age, 1),
                                "proxy_status": proxy_status,
                            }
                        )
                        if not slot.get("restart_requested"):
                            restart_reason = "worker_stalled"
                            next_network_key = current_network_key
                            if proxy_status == "blocked":
                                next_network_key = scheduler.reassign_worker_proxy(
                                    worker_id,
                                    current_proxy=current_network_key,
                                    all_proxy_ids=selected_worker_keys,
                                )
                                restart_reason = "idle_reassignment"
                            slot["next_network_key"] = next_network_key
                            slot["restart_requested"] = True
                            worker.request_stop(restart_reason)
    
                for worker_id in list(worker_slots.keys()):
                    slot = worker_slots[worker_id]
                    future: Future = slot["future"]
                    if not future.done():
                        continue
    
                    exc = future.exception()
                    queue_pending = scheduler.queue_size() > 0
                    should_restart = queue_pending and not STOP_EVENT.is_set()
                    reason = "completed"
                    if exc is not None:
                        reason = f"exception:{exc}"
                        _run_log("error", "Worker %s termino con excepcion: %s", worker_id, exc)
                        _record_runtime_event(
                            {
                                "event_type": "worker_future_exception",
                                "severity": "error",
                                "failure_kind": "system",
                                "message": f"Worker {worker_id} termino con excepcion.",
                                "worker_id": worker_id,
                                "network_key": str(slot.get("network_key") or ""),
                                "proxy_id": str(slot.get("proxy_id") or ""),
                                "error": str(exc) or exc.__class__.__name__,
                            }
                        )
                    elif should_restart:
                        reason = "queue_pending"
    
                    if should_restart:
                        restart_count = scheduler.record_worker_restart(worker_id)
                        with stats_lock:
                            stats["worker_restarts"] = int(stats.get("worker_restarts", 0)) + 1
                        if restart_count > worker_restart_limit:
                            logger.error(
                                "Worker %s alcanzÃ³ lÃ­mite de reinicios (%d).",
                                worker_id,
                                worker_restart_limit,
                            )
                            _run_log("error", "Worker %s alcanzo limite de reinicios (%d).", worker_id, worker_restart_limit)
                            _record_runtime_event(
                                {
                                    "event_type": "worker_restart_limit_reached",
                                    "severity": "error",
                                    "failure_kind": "terminal",
                                    "message": f"Worker {worker_id} alcanzo el limite de reinicios.",
                                    "worker_id": worker_id,
                                    "network_key": str(slot.get("network_key") or ""),
                                    "proxy_id": str(slot.get("proxy_id") or ""),
                                    "restart_count": restart_count,
                                    "restart_limit": worker_restart_limit,
                                }
                            )
                            worker_slots.pop(worker_id, None)
                            continue
    
                        restart_network_key = _normalize_effective_network_key(
                            slot.get("next_network_key") or slot.get("network_key") or ""
                        )
                        restart_proxy = worker_proxy_map.get(
                            restart_network_key,
                            _runtime_proxy_id_from_network_key(restart_network_key),
                        )
                        if restart_network_key != DIRECT_NETWORK_KEY and not health_monitor.is_proxy_available(restart_network_key):
                            restart_network_key = scheduler.reassign_worker_proxy(
                                worker_id,
                                current_proxy=restart_network_key,
                                all_proxy_ids=selected_worker_keys,
                            )
                            restart_proxy = worker_proxy_map.get(
                                restart_network_key,
                                _runtime_proxy_id_from_network_key(restart_network_key),
                            )
                        if restart_network_key not in worker_groups:
                            restart_network_key = _normalize_effective_network_key(slot.get("network_key") or "")
                            restart_proxy = str(slot.get("proxy_id") or "")
    
                        _run_log(
                            "warning",
                            "Worker restarted: %s network=%s proxy=%s reason=%s restart=%d",
                            worker_id,
                            restart_network_key,
                            restart_proxy,
                            reason,
                            restart_count,
                        )
                        _record_runtime_event(
                            {
                                "event_type": "worker_restarted",
                                "severity": "warning",
                                "failure_kind": "retryable",
                                "message": f"Worker {worker_id} relanzado en {_proxy_label(restart_network_key)}.",
                                "worker_id": worker_id,
                                "network_key": restart_network_key,
                                "proxy_id": restart_proxy,
                                "reason": reason,
                                "restart_count": restart_count,
                            }
                        )
                        _spawn_worker(worker_id, restart_network_key)
                        _run_log("info", "Worker %s relanzado en worker %s.", worker_id, restart_network_key)
                        _emit_live_progress(
                            "Running",
                            f"Worker {worker_id} relanzado en {_proxy_label(restart_network_key)}.",
                        )
                        continue
    
                    worker_slots.pop(worker_id, None)
    
                if scheduler.is_empty():
                    if all(slot["future"].done() for slot in worker_slots.values()):
                        break
                time.sleep(monitor_interval)
    
            for worker_id, slot in list(worker_slots.items()):
                worker: ProxyWorker = slot["worker"]
                worker.request_stop("campaign_shutdown")
                future: Future = slot["future"]
                try:
                    future.result(timeout=worker_shutdown_timeout_seconds)
                except _LegacyFutureTimeoutErrorRemoved:
                    worker._close_sender_sessions(timeout=worker_session_close_timeout_seconds)
                    _run_log("warning", "Worker %s no se detuvo dentro de %.1fs durante shutdown.", worker_id, worker_shutdown_timeout_seconds)
                    _record_runtime_event(
                        {
                            "event_type": "worker_shutdown_timeout",
                            "severity": "warning",
                            "failure_kind": "system",
                            "message": f"Worker {worker_id} no se detuvo dentro del timeout de shutdown.",
                            "worker_id": worker_id,
                            "proxy_id": str(slot.get("proxy_id") or ""),
                            "timeout_seconds": worker_shutdown_timeout_seconds,
                        }
                    )
                except Exception as exc:
                    _run_log("exception", "Worker %s fallo durante shutdown.", worker_id, exc_info=True)
                    _record_runtime_event(
                        {
                            "event_type": "worker_shutdown_failed",
                            "severity": "error",
                            "failure_kind": "system",
                            "message": f"Worker {worker_id} fallo durante shutdown.",
                            "worker_id": worker_id,
                            "proxy_id": str(slot.get("proxy_id") or ""),
                            "error": str(exc) or exc.__class__.__name__,
                        }
                    )
                worker_suffix = str(worker_id).split("-")[-1] or worker_id
                _print_info_block(
                    "Worker detenido",
                    [f"Worker #{worker_suffix} finalizado"],
                )
    
        stop_requested = STOP_EVENT.is_set()
        residual_tasks = scheduler.drain_all()
        residual_count = len(residual_tasks)
        if residual_tasks and not stop_requested:
            with stats_lock:
                stats["failed"] = int(stats.get("failed", 0)) + residual_count
            _run_log(
                "warning",
                "Proxy Worker Runner: %d leads marcados como fallidos por falta de workers activos.",
                residual_count,
            )
            for task in residual_tasks:
                try:
                    mark_lead_failed(
                        task.lead,
                        reason="worker_exhausted",
                        attempts=task.attempt,
                        alias=alias,
                    )
                except Exception as exc:
                    _run_log("exception", "No se pudo persistir worker_exhausted para @%s.", task.lead, exc_info=True)
                    _record_runtime_event(
                        {
                            "event_type": "worker_exhausted_persist_failed",
                            "severity": "error",
                            "failure_kind": "system",
                            "message": "No se pudo persistir worker_exhausted en lead_status.",
                            "lead": task.lead,
                            "error": str(exc) or exc.__class__.__name__,
                        }
                    )
        elif residual_tasks:
            _run_log(
                "info",
                "Proxy Worker Runner: stop solicitado; %d leads quedan pendientes para un proximo run.",
                residual_count,
            )
    
        result = _build_result(
            sent=int(stats.get("sent", 0)),
            failed=int(stats.get("failed", 0)),
            skipped=int(stats.get("skipped", 0)),
            retried=int(stats.get("retried", 0)),
            remaining=residual_count if stop_requested else scheduler.queue_size(),
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            proxies=proxy_worker_count,
            worker_restarts=int(stats.get("worker_restarts", 0)),
            skipped_preblocked=int(stats.get("skipped_preblocked", 0)),
            health_state=health_monitor.snapshot(),
            account_health=health_monitor.accounts_snapshot(),
            preflight_blocked=blocked_accounts,
            worker_plan=worker_plan,
        )
        _run_log(
            "info",
            "Proxy Worker Runner finalizado: sent=%d failed=%d retried=%d remaining=%d workers=%d proxies=%d restarts=%d",
            result["sent"],
            result["failed"],
            result["retried"],
            result["remaining"],
            result["workers_effective"],
            result["proxies"],
            result["worker_restarts"],
        )
        finished_at = time.time()
        if stop_requested:
            _print_campaign_end_block(
                completed=False,
                reason="detenida por usuario",
                sent=result["sent"],
                failed=result["failed"],
                skipped=result["skipped"],
                remaining=result["remaining"],
                started_at=campaign_started_at,
                finished_at=finished_at,
            )
            _emit_progress(
                CampaignRunStatus.STOPPED.value,
                message="Campaña detenida por usuario.",
                stats_snapshot=_stats_snapshot(),
                total_leads=total_leads_hint,
                remaining=result["remaining"],
                workers_active=0,
                workers_capacity=workers_capacity,
                workers_effective=workers_effective,
                worker_slots={},
                scheduler=scheduler,
                health_monitor=health_monitor,
            )
        else:
            _print_campaign_end_block(
                completed=True,
                reason="todos los leads procesados",
                sent=result["sent"],
                failed=result["failed"],
                skipped=result["skipped"],
                remaining=result["remaining"],
                started_at=campaign_started_at,
                finished_at=finished_at,
            )
            _emit_progress(
                CampaignRunStatus.COMPLETED.value,
                message="Campaña finalizada. Todos los leads del run actual fueron procesados.",
                stats_snapshot=_stats_snapshot(),
                total_leads=total_leads_hint,
                remaining=result["remaining"],
                workers_active=0,
                workers_capacity=workers_capacity,
                workers_effective=workers_effective,
                worker_slots={},
                scheduler=scheduler,
                health_monitor=health_monitor,
            )
        return result
    finally:
        restore_stop_token(token_binding)
=======

    stop_requested = STOP_EVENT.is_set()
    residual_tasks = scheduler.drain_all()
    residual_count = len(residual_tasks)
    if residual_tasks and not stop_requested:
        with stats_lock:
            stats["failed"] = int(stats.get("failed", 0)) + residual_count
        _run_log(
            "warning",
            "Proxy Worker Runner: %d leads marcados como fallidos por falta de workers activos.",
            residual_count,
        )
        for task in residual_tasks:
            try:
                mark_lead_failed(
                    task.lead,
                    reason="worker_exhausted",
                    attempts=task.attempt,
                    alias=alias,
                )
            except Exception as exc:
                _run_log("exception", "No se pudo persistir worker_exhausted para @%s.", task.lead, exc_info=True)
                _record_runtime_event(
                    {
                        "event_type": "worker_exhausted_persist_failed",
                        "severity": "error",
                        "failure_kind": "system",
                        "message": "No se pudo persistir worker_exhausted en lead_status.",
                        "lead": task.lead,
                        "error": str(exc) or exc.__class__.__name__,
                    }
                )
    elif residual_tasks:
        _run_log(
            "info",
            "Proxy Worker Runner: stop solicitado; %d leads quedan pendientes para un proximo run.",
            residual_count,
        )

    result = _build_result(
        sent=int(stats.get("sent", 0)),
        failed=int(stats.get("failed", 0)),
        skipped=int(stats.get("skipped", 0)),
        retried=int(stats.get("retried", 0)),
        remaining=residual_count if stop_requested else scheduler.queue_size(),
        workers_capacity=workers_capacity,
        workers_effective=workers_effective,
        proxies=proxy_worker_count,
        worker_restarts=int(stats.get("worker_restarts", 0)),
        skipped_preblocked=int(stats.get("skipped_preblocked", 0)),
        health_state=health_monitor.snapshot(),
        account_health=health_monitor.accounts_snapshot(),
        preflight_blocked=blocked_accounts,
        worker_plan=worker_plan,
    )
    _run_log(
        "info",
        "Proxy Worker Runner finalizado: sent=%d failed=%d retried=%d remaining=%d workers=%d proxies=%d restarts=%d",
        result["sent"],
        result["failed"],
        result["retried"],
        result["remaining"],
        result["workers_effective"],
        result["proxies"],
        result["worker_restarts"],
    )
    finished_at = time.time()
    if stop_requested:
        _print_campaign_end_block(
            completed=False,
            reason="detenida por usuario",
            sent=result["sent"],
            failed=result["failed"],
            skipped=result["skipped"],
            remaining=result["remaining"],
            started_at=campaign_started_at,
            finished_at=finished_at,
        )
        _emit_progress(
            CampaignRunStatus.STOPPED.value,
            message="Campaña detenida por usuario.",
            stats_snapshot=_stats_snapshot(),
            total_leads=total_leads_hint,
            remaining=result["remaining"],
            workers_active=0,
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            worker_slots={},
            scheduler=scheduler,
            health_monitor=health_monitor,
        )
    else:
        _print_campaign_end_block(
            completed=True,
            reason="todos los leads procesados",
            sent=result["sent"],
            failed=result["failed"],
            skipped=result["skipped"],
            remaining=result["remaining"],
            started_at=campaign_started_at,
            finished_at=finished_at,
        )
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="Campaña finalizada. Todos los leads del run actual fueron procesados.",
            stats_snapshot=_stats_snapshot(),
            total_leads=total_leads_hint,
            remaining=result["remaining"],
            workers_active=0,
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            worker_slots={},
            scheduler=scheduler,
            health_monitor=health_monitor,
        )
    restore_stop_token(token_binding)
    return result
>>>>>>> origin/main


def _resolve_account_message_limit(account: Dict[str, Any]) -> int:
    for key in ("messages_per_account", "max_messages"):
        current = account.get(key)
        try:
            parsed = int(current)
            if parsed > 0:
                return parsed
        except Exception:
            continue
    return 25


def _resolve_account_sent_today(account: Dict[str, Any]) -> int:
    current = account.get("sent_today")
    try:
        parsed = int(current)
        if parsed > 0:
            return parsed
    except Exception:
        pass
    return 0


def _normalize_templates(raw_templates: Any) -> list[str]:
    if raw_templates is None:
        return []
    items = raw_templates if isinstance(raw_templates, list) else [raw_templates]
    templates: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _extract_template_text(item)
        if not text:
            continue
        for variant in _expand_template_variants(text):
            key = variant.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            templates.append(variant)
    return templates


def _expand_template_variants(text: str) -> list[str]:
    normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    variants = [line.strip() for line in normalized.splitlines() if line.strip()]
    if variants:
        return variants
    single = normalized.strip()
    return [single] if single else []


def _extract_template_text(item: Any) -> str:
    if isinstance(item, dict):
        for key in ("text", "content", "message", "body", "template", "value"):
            value = str(item.get(key) or "").strip()
            if value:
                return value
        return ""
    return str(item or "").strip()


def _group_accounts_by_proxy(accounts: list[Dict[str, Any]]) -> Dict[str, list[Dict[str, Any]]]:
    grouped: Dict[str, list[Dict[str, Any]]] = {}
    for account in accounts:
        if not isinstance(account, dict):
            continue
        if _account_remaining_capacity(account) <= 0:
            continue
        worker_key = _worker_network_key(account)
        if not worker_key:
            continue
        grouped.setdefault(worker_key, []).append(account)
    return grouped


def _norm_account(value: str) -> str:
    return str(value or "").strip().lstrip("@").lower()


def _parse_send_result(send_result: Any) -> tuple[bool, str, Dict[str, Any]]:
    parsed = CampaignSendResult.from_sender_result(send_result)
    return parsed.ok, parsed.detail, dict(parsed.payload)


def _campaign_failure_reason(parsed: CampaignSendResult) -> str:
    detail = str(parsed.detail or "").strip()
    reason_code = str(parsed.reason_code or "").strip()
    detail_upper = detail.upper()
    if detail and (
        detail_upper.startswith("SKIPPED_")
        or parsed.status in {CampaignSendStatus.SKIPPED, CampaignSendStatus.AMBIGUOUS}
    ):
        return detail
    return reason_code or detail or "send_failed"


def _as_int(value: Any, *, default: int, minimum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, parsed)


def _as_float(value: Any, *, default: float, minimum: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    return max(float(minimum), parsed)


def _now_hms() -> str:
    return time.strftime("%H:%M:%S")


def _proxy_label(proxy_id: str) -> str:
    normalized = _normalize_effective_network_key(proxy_id)
    if normalized == DIRECT_NETWORK_KEY:
        return DIRECT_NETWORK_KEY
    return normalized.split(":", 1)[1].strip() or DIRECT_NETWORK_KEY


def _print_info_block(title: str, lines: list[str] | None = None) -> None:
    print(f"[INFO] {str(title or '').strip()}")
    if lines:
        print("")
        for line in lines:
            if str(line or "").strip():
                print(str(line))


def _print_send_block(*, account: str, lead: str, delay_seconds: int, proxy_id: str) -> None:
    print(f"{_now_hms()}  {account} â†’ {lead}")
    print("Estado: enviado âœ“")
    print(f"Delay aplicado: {max(0, int(delay_seconds))}s")
    print(f"Proxy: {_proxy_label(proxy_id)}")
    print("")


def _print_error_block(*, account: str, lead: str, reason: str, proxy_id: str) -> None:
    print(f"{_now_hms()}  {account} â†’ {lead}")
    print(f"ERROR: {str(reason or '').strip() or 'error de envÃ­o'}")
    print(f"Proxy: {_proxy_label(proxy_id)}")
    print("")


def _print_skip_block(*, account: str, lead: str, reason: str, proxy_id: str) -> None:
    print(f"{_now_hms()}  {account} â†’ {lead}")
    print("Estado: omitido")
    print(f"Motivo: {str(reason or '').strip() or 'omitido por reglas de campaÃ±a'}")
    print(f"Proxy: {_proxy_label(proxy_id)}")
    print("")


def _format_human_duration(seconds: float) -> str:
    total = max(0, int(round(float(seconds))))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m:02d}m"
    if m > 0:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _print_progress_block(
    *,
    sent: int,
    failed: int,
    skipped: int,
    remaining: int,
    started_at: float,
) -> None:
    elapsed = max(1.0, time.time() - float(started_at or time.time()))
    speed_h = max(0.0, (float(sent) / elapsed) * 3600.0)
    if speed_h <= 0.01:
        eta_text = "-"
    else:
        eta_seconds = max(0.0, (float(remaining) / speed_h) * 3600.0)
        eta_text = _format_human_duration(eta_seconds)
    print("[PROGRESS] Estado de campaÃ±a")
    print("")
    print(f"Leads enviados: {max(0, int(sent))}")
    print(f"Errores: {max(0, int(failed))}")
    print(f"Omitidos: {max(0, int(skipped))}")
    print(f"Leads restantes: {max(0, int(remaining))}")
    print(f"Velocidad actual: {int(round(speed_h))} mensajes/hora")
    print(f"Tiempo estimado restante: {eta_text}")


def _print_campaign_end_block(
    *,
    completed: bool,
    reason: str,
    sent: int,
    failed: int,
    skipped: int,
    remaining: int,
    started_at: float,
    finished_at: float,
) -> None:
    if completed:
        print("[INFO] CampaÃ±a completada")
    else:
        print("[INFO] CampaÃ±a finalizada")
    print("")
    print(f"Motivo: {str(reason or '').strip()}")
    print("")
    print(f"Leads enviados: {max(0, int(sent))}")
    print(f"Errores: {max(0, int(failed))}")
    print(f"Omitidos: {max(0, int(skipped))}")
    print(f"Leads restantes: {max(0, int(remaining))}")
    print("")
    print(f"Tiempo total ejecutado: {_format_human_duration(max(0.0, finished_at - started_at))}")
    print(f"Hora de finalizaciÃ³n: {time.strftime('%H:%M:%S', time.localtime(max(0.0, finished_at)))}")




