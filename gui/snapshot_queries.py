from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from core import storage as storage_module
from core.session_store import list_saved_sessions
from paths import browser_profiles_root, sessions_root
from src.dm_campaign.contracts import CampaignCapacity, CampaignRunSnapshot, CampaignRunStatus
from src.playwright_service import resolve_playwright_executable

from .page_base import message_limit, safe_float, safe_int


def _dashboard_today() -> datetime.date:
    tzinfo = getattr(storage_module, "TZ", None)
    if tzinfo is None:
        return datetime.now().astimezone().date()
    return datetime.now(tzinfo).date()


def _lead_filter_summary_rows(services: Any, *, status: str | None = None) -> list[dict[str, Any]]:
    summary_getter = getattr(services.leads, "list_filter_list_summaries", None)
    if callable(summary_getter):
        try:
            rows = summary_getter(status=status)
        except TypeError:
            rows = summary_getter()
        return list(rows) if isinstance(rows, list) else []
    try:
        rows = services.leads.list_filter_lists(status=status)
    except TypeError:
        rows = services.leads.list_filter_lists()
    return list(rows) if isinstance(rows, list) else []


def _leads_processed_today(services: Any) -> int:
    today = _dashboard_today()
    tzinfo = getattr(storage_module, "TZ", None)
    total = 0
    for row in services.leads.list_filter_lists():
        if not isinstance(row, dict):
            continue
        for item in row.get("items") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("status") or "").strip().upper() == "PENDING":
                continue
            updated_at = str(item.get("updated_at") or "").strip()
            if not updated_at:
                continue
            try:
                stamp = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                local_stamp = stamp.astimezone(tzinfo) if tzinfo is not None else stamp.astimezone()
            except Exception:
                continue
            if local_stamp.date() == today:
                total += 1
    return total


def build_dashboard_snapshot(services: Any, tasks: Any, *, active_alias: str) -> dict[str, Any]:
    snapshot = services.system.dashboard_snapshot()
    metrics = snapshot.get("metrics") if isinstance(snapshot, dict) else {}
    if not isinstance(metrics, dict):
        metrics = {}
    conversion = snapshot.get("conversion") if isinstance(snapshot, dict) else {}
    return {
        "values": {
            "total_accounts": safe_int(metrics.get("total_accounts")),
            "connected_accounts": safe_int(metrics.get("connected_accounts")),
            "messages_sent_today": safe_int(metrics.get("messages_sent_today")),
            "messages_error_today": safe_int(metrics.get("messages_error_today")),
            "replies_received_today": safe_int(metrics.get("messages_replied_today")),
            "active_campaigns": 1 if tasks.is_running("campaign") else 0,
            "leads_processed_today": _leads_processed_today(services),
        },
        "summary": (
            "Alias activo: "
            f"{active_alias}  |  "
            f"Conversion: {safe_float(getattr(conversion, 'get', lambda *_: 0)('rate'))}%  |  "
            f"Reset diario: {snapshot.get('last_reset_display') or '-'} "
            f"({snapshot.get('timezone_label') or 'America/Argentina/Cordoba'})"
        ),
    }


def build_campaign_home_snapshot(services: Any, tasks: Any) -> dict[str, Any]:
    templates = len(services.campaigns.list_templates())
    snapshot = services.system.dashboard_snapshot()
    metrics = snapshot.get("metrics") if isinstance(snapshot, dict) else {}
    if not isinstance(metrics, dict):
        metrics = {}
    return {
        "values": {
            "templates": templates,
            "active": "Si" if tasks.is_running("campaign") else "No",
            "sent": safe_int(metrics.get("messages_sent_today")),
            "errors": safe_int(metrics.get("messages_error_today")),
        },
        "summary": (
            "Estado del dia: "
            f"{safe_int(metrics.get('messages_sent_today'))} enviados, "
            f"{safe_int(metrics.get('messages_error_today'))} con error, "
            f"{safe_int(metrics.get('messages_replied_today'))} respuestas detectadas."
        ),
    }


def build_campaign_create_snapshot(services: Any, *, active_alias: str) -> dict[str, Any]:
    aliases = services.accounts.list_aliases()
    lead_counts: dict[str, int] = {}
    summary_getter = getattr(services.leads, "list_list_summaries", None)
    leads_lists: list[str] = []
    if callable(summary_getter):
        for row in summary_getter():
            if not isinstance(row, dict):
                continue
            leads_alias = str(row.get("name") or "").strip()
            if not leads_alias:
                continue
            leads_lists.append(leads_alias)
            lead_counts[leads_alias] = safe_int(row.get("count"))
    else:
        leads_lists = services.leads.list_lists()
        for leads_alias in leads_lists:
            try:
                lead_counts[leads_alias] = len(services.leads.load_list(leads_alias))
            except Exception:
                lead_counts[leads_alias] = 0
    templates: list[dict[str, str]] = []
    for item in services.campaigns.list_templates():
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        templates.append(
            {
                "name": name,
                "text": str(item.get("text") or ""),
                "id": str(item.get("id") or ""),
            }
        )
    selected_alias = str(active_alias or "").strip()
    if selected_alias not in aliases:
        selected_alias = str(aliases[0] if aliases else "").strip()
    capacity = services.campaigns.get_capacity(selected_alias) if selected_alias else {}
    return {
        "aliases": aliases,
        "active_alias": selected_alias,
        "lead_lists": leads_lists,
        "lead_counts": lead_counts,
        "templates": templates,
        "capacity": dict(capacity) if isinstance(capacity, dict) else {},
    }


def build_campaign_capacity_snapshot(services: Any, *, alias: str) -> dict[str, Any]:
    clean_alias = str(alias or "").strip()
    if not clean_alias:
        return CampaignCapacity(alias="", workers_capacity=0).to_payload()
    payload = services.campaigns.get_capacity(clean_alias)
    if isinstance(payload, dict):
        return CampaignCapacity.from_payload(payload).to_payload()
    return CampaignCapacity(alias=clean_alias, workers_capacity=0).to_payload()


def _build_account_row(services: Any, record: dict[str, Any]) -> dict[str, Any]:
    row = dict(record)
    connected = bool(services.accounts.connected_status(record))
    health = str(services.accounts.health_badge(record) or "-").strip() or "-"
    eligibility_resolver = getattr(services.accounts, "manual_action_eligibility", None)
    proxy_display_resolver = getattr(services.accounts, "proxy_display_for_account", None)
    login_progress_resolver = getattr(services.accounts, "login_progress_for_account", None)
    manual_action = (
        eligibility_resolver(row)
        if callable(eligibility_resolver)
        else {"allowed": True, "message": ""}
    )
    proxy_display = (
        proxy_display_resolver(row)
        if callable(proxy_display_resolver)
        else {"label": row.get("assigned_proxy_id") or row.get("proxy_url") or "-", "status": "unknown"}
    )
    login_progress = (
        login_progress_resolver(row)
        if callable(login_progress_resolver)
        else {"active": False, "state": "", "message": "", "label": "", "updated_at": ""}
    )
    row["connected"] = connected
    row["connected_label"] = "Si" if connected else "No"
    row["health_badge"] = health
    row["proxy_label"] = str(proxy_display.get("label") or "-")
    row["proxy_status"] = str(proxy_display.get("status") or "unknown")
    row["login_progress_active"] = bool(login_progress.get("active"))
    row["login_progress_state"] = str(login_progress.get("state") or "").strip()
    row["login_progress_label"] = str(login_progress.get("label") or "").strip()
    row["login_progress_message"] = str(login_progress.get("message") or "").strip()
    row["message_limit_label"] = message_limit(row)
    row["manual_action_allowed"] = bool(manual_action.get("allowed", True))
    row["manual_action_message"] = str(manual_action.get("message") or "").strip()
    return row


def build_accounts_home_snapshot(services: Any, *, active_alias: str) -> dict[str, Any]:
    aliases = services.accounts.list_aliases()
    snapshot = services.accounts.get_alias_snapshot(active_alias)
    return {
        "aliases_count": len(aliases),
        "snapshot": dict(snapshot),
        "summary": (
            f"Alias activo: {snapshot.get('alias')}  |  "
            f"Cuentas: {snapshot.get('accounts_total')}  |  "
            f"Conectadas: {snapshot.get('accounts_connected')}  |  "
            f"Proxies asignados: {snapshot.get('proxies_assigned')}"
        ),
    }


def build_alias_page_snapshot(services: Any, *, active_alias: str) -> dict[str, Any]:
    aliases = services.accounts.list_aliases()
    aliases.sort(key=lambda value: (value != active_alias, value.lower()))
    rows: list[dict[str, Any]] = []
    for alias in aliases:
        snapshot = dict(services.accounts.get_alias_snapshot(alias))
        status_parts = []
        if alias == active_alias:
            status_parts.append("Activo")
        total = int(snapshot.get("accounts_total") or 0)
        connected = int(snapshot.get("accounts_connected") or 0)
        blocked = int(snapshot.get("accounts_blocked") or 0)
        status_parts.append(f"{connected}/{total} conectadas" if total else "Sin cuentas")
        if blocked:
            status_parts.append(f"{blocked} bloqueadas")
        rows.append(
            {
                "alias": alias,
                "accounts_total": total,
                "proxies_assigned": int(snapshot.get("proxies_assigned") or 0),
                "status": "  |  ".join(status_parts),
            }
        )
    return {"rows": rows}


def build_accounts_table_snapshot(services: Any, *, active_alias: str) -> dict[str, Any]:
    aliases = services.accounts.list_aliases()
    selected_alias = str(active_alias or "").strip()
    if selected_alias:
        match = next((alias for alias in aliases if alias.lower() == selected_alias.lower()), "")
        selected_alias = match or selected_alias
    if not selected_alias and aliases:
        selected_alias = str(aliases[0] or "").strip()
    rows = sorted(
        [_build_account_row(services, record) for record in services.accounts.list_accounts(selected_alias)],
        key=lambda row: str(row.get("username") or "").strip().lower(),
    )
    return {
        "aliases": aliases,
        "active_alias": selected_alias,
        "rows": rows,
    }


def build_accounts_actions_snapshot(services: Any, *, active_alias: str) -> dict[str, Any]:
    snapshot = build_accounts_table_snapshot(services, active_alias=active_alias)
    rows = [dict(item) for item in snapshot.get("rows") or [] if isinstance(item, dict)]
    connected = sum(1 for row in rows if bool(row.get("connected")))
    return {
        **snapshot,
        "summary": (
            f"Alias activo: {snapshot.get('active_alias') or '-'}  |  "
            f"Cuentas activas visibles: {len(rows)}  |  "
            f"Conectadas: {connected}"
        ),
    }


def build_campaign_monitor_snapshot(services: Any, tasks: Any, *, monitor_state: dict[str, Any]) -> dict[str, Any]:
    state = dict(monitor_state or {})
    run_id = str(state.get("run_id") or "").strip()
    snapshot_getter = getattr(services.campaigns, "current_run_snapshot", None)
    current = (
        snapshot_getter(run_id=run_id)
        if callable(snapshot_getter)
        else {}
    )
    raw_payload = dict(current) if isinstance(current, dict) and current else state
    snapshot = CampaignRunSnapshot.from_payload(raw_payload)
    alias = snapshot.alias or str(state.get("alias") or "").strip()
    leads_alias = snapshot.leads_alias or str(state.get("leads_alias") or "").strip()
    sent = snapshot.sent
    failed = snapshot.failed
    skipped = snapshot.skipped
    skipped_preblocked = snapshot.skipped_preblocked
    retried = snapshot.retried
    total_leads = snapshot.total_leads or max(0, safe_int(state.get("total_leads")))
    has_explicit_remaining = "remaining" in raw_payload or "remaining" in state
    raw_remaining = snapshot.remaining if has_explicit_remaining else max(0, safe_int(state.get("remaining")))
    terminal_processed = min(
        total_leads,
        sent + failed + skipped + skipped_preblocked,
    ) if total_leads else max(0, sent + failed + skipped + skipped_preblocked)
    remaining = raw_remaining if has_explicit_remaining else (
        max(0, total_leads - terminal_processed) if total_leads else raw_remaining
    )
    worker_rows = [dict(row) for row in snapshot.worker_rows]
    status = str(raw_payload.get("status") or "").strip()
    if not status:
        status = CampaignRunStatus.STARTING.value if bool(snapshot.task_active) or bool(run_id and tasks.is_running("campaign")) else CampaignRunStatus.STOPPED.value
    parsed_status = CampaignRunStatus.parse(status)
    task_active = (bool(snapshot.task_active) or bool(run_id and tasks.is_running("campaign"))) and not parsed_status.is_terminal
    progress_processed = terminal_processed
    if total_leads and has_explicit_remaining and (not task_active or parsed_status.is_terminal):
        progress_processed = max(progress_processed, total_leads - remaining)
    progress = min(100, int((progress_processed / total_leads) * 100)) if total_leads else 0
    workers_active = snapshot.workers_active
    if workers_active <= 0 and worker_rows:
        workers_active = sum(
            1
            for row in worker_rows
            if str(row.get("execution_state") or "").strip().lower() not in {"idle", ""}
        )
    active_accounts = len(
        {
            str(row.get("current_account") or "").strip()
            for row in worker_rows
            if str(row.get("current_account") or "").strip()
        }
    )
    return {
        "run_id": str(snapshot.run_id or run_id or "-"),
        "alias": alias or "-",
        "leads_alias": leads_alias or "-",
        "sent": sent,
        "failed": failed,
        "errors": failed,
        "skipped": skipped,
        "skipped_preblocked": skipped_preblocked,
        "retried": retried,
        "remaining": remaining,
        "total_leads": total_leads,
        "active_accounts": safe_int(active_accounts),
        "workers_active": workers_active,
        "workers_requested": snapshot.workers_requested,
        "workers_capacity": snapshot.workers_capacity,
        "workers_effective": snapshot.workers_effective,
        "worker_rows": worker_rows,
        "started_at": snapshot.started_at or str(state.get("started_at") or ""),
        "finished_at": snapshot.finished_at,
        "message": snapshot.message,
        "task_active": task_active,
        "status": status,
        "progress": progress,
    }


def _read_text_file_since(path: Path, cursor: int | None) -> tuple[int, str, bool]:
    if not path.is_file():
        return 0, "", True
    try:
        size = int(path.stat().st_size)
        requested = 0 if cursor is None else max(0, int(cursor))
        if requested > size:
            requested = 0
        if requested == size:
            return size, "", False
        if requested == 0:
            return size, path.read_text(encoding="utf-8"), True
        with path.open("rb") as handle:
            handle.seek(requested)
            chunk = handle.read()
        return size, chunk.decode("utf-8", errors="replace"), False
    except Exception as exc:
        return 0, f"No se pudo leer el archivo de log.\n{exc}", True


def build_leads_home_snapshot(services: Any) -> dict[str, Any]:
    templates = len(services.leads.list_templates())
    lists_total = len(services.leads.list_lists())
    completed = len(_lead_filter_summary_rows(services, status="completed"))
    incomplete = len(_lead_filter_summary_rows(services, status="incomplete"))
    return {
        "values": {
            "templates": templates,
            "lists": lists_total,
            "completed": completed,
            "pending": incomplete,
        },
        "summary": (
            f"Plantillas: {templates}  |  Listas: {lists_total}  |  "
            f"Filtrados completos: {completed}  |  Pendientes: {incomplete}"
        ),
    }


def build_leads_templates_snapshot(services: Any) -> dict[str, Any]:
    rows = services.leads.list_template_rows()
    return {
        "rows": rows,
        "summary": (
            f"Plantillas guardadas: {len(rows)}  |  Variaciones totales: "
            f"{sum(int(row.get('variant_count') or 0) for row in rows if isinstance(row, dict))}"
        ),
    }


def build_leads_lists_snapshot(services: Any) -> dict[str, Any]:
    names = services.leads.list_lists()
    rows: list[dict[str, Any]] = []
    total_usernames = 0
    for name in names:
        usernames = services.leads.load_list(name)
        rows.append(
            {
                "name": name,
                "usernames": usernames,
                "count": len(usernames),
            }
        )
        total_usernames += len(usernames)
    return {
        "rows": rows,
        "summary": (
            f"Listas origen: {len(rows)}  |  Usernames persistidos: {total_usernames}"
        ),
    }


def build_leads_import_snapshot(services: Any) -> dict[str, Any]:
    status_snapshot = {}
    snapshot_getter = getattr(services.leads, "import_status_snapshot", None)
    if callable(snapshot_getter):
        payload = snapshot_getter()
        if isinstance(payload, dict):
            status_snapshot = dict(payload)
    return {
        "lists": services.leads.list_lists(),
        "latest_event": status_snapshot.get("latest_event"),
        "summary": (
            str(status_snapshot.get("summary") or "").strip()
            or "Importa archivos CSV o TXT en una lista existente o crea una nueva "
            "escribiendo su nombre en el destino."
        ),
    }


def build_leads_filter_config_snapshot(services: Any) -> dict[str, Any]:
    return {"payload": services.leads.effective_filter_config()}


def build_leads_filter_runner_snapshot(
    services: Any,
    *,
    active_alias: str,
    current_source: str,
    current_account_alias: str,
    current_export_alias: str,
) -> dict[str, Any]:
    source_lists = services.leads.list_lists()
    account_aliases = services.accounts.list_aliases()
    export_aliases = list(account_aliases)
    completed_rows = _lead_filter_summary_rows(services, status="completed")
    incomplete_rows = _lead_filter_summary_rows(services, status="incomplete")
    selected_alias = str(current_account_alias or active_alias).strip()
    account_rows: list[dict[str, Any]] = []
    for record in services.accounts.list_accounts(selected_alias):
        username = str(record.get("username") or "").strip().lstrip("@")
        if not username:
            continue
        proxy_label = str(
            record.get("assigned_proxy_id")
            or record.get("proxy_url")
            or record.get("proxy")
            or ""
        ).strip()
        account_rows.append(
            {
                "username": username,
                "connected": bool(services.accounts.connected_status(record)),
                "proxy": proxy_label,
            }
        )
    proxy_count = len({str(row.get("proxy") or "").strip() for row in account_rows if str(row.get("proxy") or "").strip()})
    if account_rows and proxy_count <= 0:
        computed_concurrency = 1
    else:
        computed_concurrency = min(len(account_rows), proxy_count) if account_rows and proxy_count else 0
    return {
        "source_lists": source_lists,
        "account_aliases": account_aliases,
        "export_aliases": export_aliases,
        "selected_source": current_source,
        "selected_account_alias": selected_alias,
        "selected_export_alias": current_export_alias,
        "account_rows": account_rows,
        "account_count": len(account_rows),
        "proxy_count": proxy_count,
        "computed_concurrency": computed_concurrency,
        "completed": completed_rows,
        "incomplete": incomplete_rows,
    }


def build_leads_filter_detail_snapshot(services: Any, *, list_id: str) -> dict[str, Any]:
    row = services.leads.find_filter_list(list_id)
    preview_rows = services.leads.filter_list_result_rows(list_id)[:20]
    return {
        "row": row,
        "preview_rows": preview_rows,
    }


def build_leads_filter_execution_snapshot(services: Any, *, list_id: str) -> dict[str, Any]:
    return {"row": services.leads.find_filter_list(list_id)}


def build_system_home_snapshot(services: Any, tasks: Any) -> dict[str, Any]:
    snapshot = services.system.dashboard_snapshot()
    metrics = snapshot.get("metrics") if isinstance(snapshot, dict) else {}
    if not isinstance(metrics, dict):
        metrics = {}
    licenses = services.system.list_licenses()
    tasks_running = tasks.running_tasks()
    saved_sessions = list_saved_sessions()
    return {
        "licenses": len(licenses),
        "connected_accounts": safe_int(metrics.get("connected_accounts")),
        "tasks": len(tasks_running),
        "sessions": len(saved_sessions),
        "summary": (
            f"Licencias: {len(licenses)}  |  "
            f"Cuentas conectadas: {safe_int(metrics.get('connected_accounts'))}  |  "
            f"Tareas activas: {len(tasks_running)}"
        ),
    }


def build_system_license_snapshot(services: Any, *, license_key: str = "") -> dict[str, Any]:
    clean_key = str(license_key or "").strip()
    activations = services.system.list_license_activations(clean_key) if clean_key else []
    return {
        "rows": services.system.list_licenses(),
        "activations": activations,
        "selected_license_key": clean_key,
    }


def build_system_logs_snapshot(
    logs: Any,
    *,
    log_path: Path,
    log_cursor: int | None = None,
    file_cursor: int | None = None,
) -> dict[str, Any]:
    next_log_cursor, log_text, log_reset = logs.read_since(log_cursor)
    next_file_cursor, file_text, file_reset = _read_text_file_since(log_path, file_cursor)
    return {
        "log_text": log_text,
        "log_cursor": next_log_cursor,
        "log_reset": log_reset,
        "file_text": file_text,
        "file_cursor": next_file_cursor,
        "file_reset": file_reset,
        "requested_log_cursor": 0 if log_cursor is None else max(0, int(log_cursor)),
        "requested_file_cursor": 0 if file_cursor is None else max(0, int(file_cursor)),
    }


def build_system_config_snapshot(services: Any) -> dict[str, Any]:
    return {"payload": services.system.update_config()}


def build_system_update_check_snapshot(services: Any) -> dict[str, Any]:
    return {"result": services.system.check_updates()}


def _profiles_with_storage(root_dir: Path) -> int:
    root = browser_profiles_root(root_dir)
    if not root.exists():
        return 0
    return sum(1 for path in root.glob("*/storage_state.json") if path.is_file())


def build_system_diagnostics_snapshot(services: Any, tasks: Any, *, root_dir: Path) -> dict[str, Any]:
    snapshot = services.system.dashboard_snapshot()
    metrics = snapshot.get("metrics") if isinstance(snapshot, dict) else {}
    if not isinstance(metrics, dict):
        metrics = {}
    inbox = services.inbox.diagnostics()
    session_files = list_saved_sessions()
    session_dir = sessions_root(root_dir)
    playwright_path = None
    playwright_ok = False
    playwright_error = ""
    try:
        playwright_path = resolve_playwright_executable(headless=True) or resolve_playwright_executable(headless=False)
        playwright_ok = playwright_path is not None
    except Exception as exc:
        playwright_error = str(exc)
    pending_leads = sum(
        safe_int(item.get("pending"))
        for item in _lead_filter_summary_rows(services)
        if isinstance(item, dict)
    )
    payload = {
        "playwright": {
            "ok": playwright_ok,
            "path": str(playwright_path or ""),
            "error": playwright_error,
        },
        "accounts_active": safe_int(metrics.get("connected_accounts")),
        "workers": {
            "gui_tasks": len(tasks.running_tasks()),
            "running_tasks": tasks.running_tasks(),
            "inbox_workers": safe_int(inbox.get("worker_count")),
        },
        "sessions": {
            "saved_sessions": len(session_files),
            "browser_profiles": _profiles_with_storage(root_dir),
            "session_dir": str(session_dir),
        },
        "queues": {
            "leads_pending": pending_leads,
            "inbox_tasks": safe_int(inbox.get("queued_tasks")),
            "inbox_dedupe": safe_int(inbox.get("dedupe_pending")),
        },
        "inbox": inbox,
    }
    return {"payload": payload}


def build_automation_home_snapshot(services: Any, *, active_alias: str) -> dict[str, Any]:
    packs = services.automation.list_packs()
    whatsapp = services.automation.whatsapp_snapshot()
    snapshot = services.automation.autoresponder_snapshot(active_alias)
    task_active = bool(snapshot.get("task_active"))
    return {
        "alias": active_alias,
        "packs": len(packs),
        "runs_active": safe_int(whatsapp.get("runs_active")),
        "pending_hydration": safe_int(snapshot.get("pending_hydration")),
        "summary": (
            f"Alias activo: {active_alias}  |  "
            f"Packs: {len(packs)}  |  "
            f"WhatsApp runs activos: {safe_int(whatsapp.get('runs_active'))}  |  "
            f"Hydration pendiente: {safe_int(snapshot.get('pending_hydration'))}  |  "
            f"Autoresponder activo: {'si' if task_active else 'no'}"
        ),
    }


def build_automation_config_snapshot(
    services: Any,
    *,
    active_alias: str,
    selected_alias: str,
) -> dict[str, Any]:
    aliases = services.accounts.list_aliases()
    alias = str(selected_alias or active_alias).strip() or active_alias
    account_rows: list[dict[str, Any]] = []
    account_groups: list[dict[str, Any]] = []
    for alias_name in aliases:
        rows = services.accounts.list_accounts(alias_name)
        usernames = [
            str(item.get("username") or "").strip().lstrip("@")
            for item in rows
            if isinstance(item, dict) and str(item.get("username") or "").strip()
        ]
        usernames = [item for item in usernames if item]
        account_groups.append({"alias": alias_name, "accounts": usernames})
        for username in usernames:
            account_rows.append({"alias": alias_name, "username": username})
    api_key = services.automation.load_openai_api_key()
    return {
        "aliases": aliases,
        "selected_alias": alias,
        "api_key": api_key,
        "api_key_present": bool(api_key),
        "objection_prompts": services.automation.list_objection_prompts(),
        "prompt_entry": services.automation.get_prompt_entry(alias),
        "followup_entry": services.automation.get_followup_entry(alias),
        "followup_selection": services.automation.get_followup_account_selection(alias),
        "followup_account_groups": account_groups,
        "followup_account_rows": account_rows,
    }


def build_automation_autoresponder_snapshot(
    services: Any,
    tasks: Any,
    *,
    active_alias: str,
    selected_alias: str,
) -> dict[str, Any]:
    aliases = services.accounts.list_aliases()
    alias = str(selected_alias or active_alias).strip() or active_alias
    snapshot = services.automation.autoresponder_snapshot(alias)
    alias_accounts = services.automation.alias_account_rows(alias)
    max_concurrency = services.automation.max_alias_concurrency(alias)
    task_active = bool(tasks.is_running("autoresponder")) or bool(snapshot.get("task_active"))
    if task_active:
        snapshot = dict(snapshot)
        snapshot["task_active"] = True
        status = str(snapshot.get("status") or "").strip()
        if status in {"", "Idle", "Stopped"}:
            snapshot["status"] = "Running"
    return {
        "aliases": aliases,
        "selected_alias": alias,
        "snapshot": snapshot,
        "task_active": task_active,
        "alias_accounts": alias_accounts,
        "max_concurrency": max_concurrency,
    }


def build_automation_packs_snapshot(services: Any) -> dict[str, Any]:
    return {
        "rows": services.automation.list_packs(),
        "prompt_options": services.automation.list_objection_prompts(),
    }


def build_automation_whatsapp_snapshot(services: Any) -> dict[str, Any]:
    return {"snapshot": services.automation.whatsapp_snapshot()}


def build_automation_flow_snapshot(
    services: Any,
    *,
    active_alias: str,
    selected_alias: str,
) -> dict[str, Any]:
    aliases = services.accounts.list_aliases()

    def _pick_known_alias(candidate: str) -> str:
        candidate_clean = str(candidate or "").strip()
        if not candidate_clean:
            return ""
        candidate_key = candidate_clean.lower()
        for option in aliases:
            option_clean = str(option or "").strip()
            if option_clean and option_clean.lower() == candidate_key:
                return option_clean
        return ""

    alias = (
        _pick_known_alias(selected_alias)
        or _pick_known_alias(active_alias)
        or (str(aliases[0]).strip() if aliases else "")
        or str(active_alias or "").strip()
        or str(selected_alias or "").strip()
    )
    packs = services.automation.list_packs()
    pack_rows: list[dict[str, str]] = []
    seen_pack_keys: set[str] = set()
    for item in packs:
        if not isinstance(item, dict):
            continue
        pack_key = str(item.get("type") or item.get("id") or item.get("name") or "").strip()
        if not pack_key or pack_key in seen_pack_keys:
            continue
        seen_pack_keys.add(pack_key)
        pack_rows.append(
            {
                "id": pack_key,
                "name": str(item.get("type") or item.get("name") or pack_key).strip() or pack_key,
            }
        )
    return {
        "aliases": aliases,
        "selected_alias": alias,
        "flow_config": services.automation.get_flow_config(alias),
        "pack_rows": pack_rows,
        "pack_options": [str(item.get("id") or "").strip() for item in pack_rows],
    }
