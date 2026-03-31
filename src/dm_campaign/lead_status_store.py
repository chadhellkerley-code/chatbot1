from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from core.storage_atomic import atomic_write_json, load_json_file, load_jsonl_entries
from paths import storage_root

_STORAGE = storage_root(Path(__file__).resolve().parent.parent.parent)
_FILE = _STORAGE / "lead_status.json"
_LOCK = threading.RLock()
_PREFILTER_SNAPSHOT_CACHE: OrderedDict[
    tuple[str, tuple[tuple[int, int] | None, tuple[int, int] | None]],
    tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]],
] = OrderedDict()
_PREFILTER_SNAPSHOT_LIMIT = 64
GLOBAL_CONTACT_TTL_SECONDS = 7 * 24 * 60 * 60
_GLOBAL_CONTACTS_KEY = "global_contacted_leads"
logger = logging.getLogger(__name__)


<<<<<<< HEAD
def refresh_runtime_paths(base: Path | None = None) -> dict[str, Path]:
    global _STORAGE, _FILE

    resolved_base = Path(base) if base is not None else Path(__file__).resolve().parent.parent.parent
    _STORAGE = storage_root(resolved_base)
    _FILE = _STORAGE / "lead_status.json"
    with _LOCK:
        _PREFILTER_SNAPSHOT_CACHE.clear()
    return {
        "storage_root": _STORAGE,
        "lead_status": _FILE,
        "sent_log": _STORAGE / "sent_log.jsonl",
    }


=======
>>>>>>> origin/main
def _normalize_lead(value: Any) -> str:
    return str(value or "").strip().lstrip("@").lower()


def _normalize_alias(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_account(value: Any) -> str:
    return str(value or "").strip().lstrip("@").lower()


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _default_payload() -> Dict[str, Any]:
    return {"version": 3, "aliases": {}, "legacy_global_leads": {}, _GLOBAL_CONTACTS_KEY: {}}


def _ensure_store() -> None:
    _STORAGE.mkdir(parents=True, exist_ok=True)
    if _FILE.exists():
        return
    atomic_write_json(_FILE, _default_payload())


def _file_token(path: Path) -> tuple[int, int] | None:
    if not path.exists():
        return None
    try:
        stat = path.stat()
    except OSError:
        return None
    return int(stat.st_mtime_ns), int(stat.st_size)


def _prefilter_file_token() -> tuple[tuple[int, int] | None, tuple[int, int] | None]:
    return _file_token(_FILE), _file_token(_STORAGE / "sent_log.jsonl")


def _contact_timestamp(entry: Any) -> int:
    if not isinstance(entry, dict):
        return 0
    for key in ("last_contacted_at", "sent_timestamp", "updated_at", "ts"):
        value = _as_int(entry.get(key))
        if value > 0:
            return value
    return 0


def _coerce_global_contact_entry(entry: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(entry, dict):
        return None
    cleaned: Dict[str, Any] = {
        "last_contacted_at": _contact_timestamp(entry),
        "last_status": "sent",
    }
    alias = _normalize_alias(entry.get("last_alias") or entry.get("campaign_alias"))
    if alias:
        cleaned["last_alias"] = alias
        cleaned["last_campaign"] = alias
    campaign = _normalize_alias(entry.get("last_campaign"))
    if campaign:
        cleaned["last_campaign"] = campaign
    account = _normalize_account(entry.get("last_account") or entry.get("sent_by") or entry.get("account"))
    if account:
        cleaned["last_account"] = account
    message_id = str(entry.get("last_message_id") or entry.get("message_id") or "").strip()
    if message_id:
        cleaned["last_message_id"] = message_id
    result = str(entry.get("last_result") or entry.get("detail") or "").strip()
    if result:
        cleaned["last_result"] = result
    return cleaned


def _upsert_global_contact(
    global_contacts: Dict[str, Dict[str, Any]],
    lead: str,
    entry: Dict[str, Any],
) -> bool:
    candidate = _coerce_global_contact_entry(entry)
    if not lead or not isinstance(global_contacts, dict) or candidate is None:
        return False
    current = global_contacts.get(lead)
    candidate_ts = _contact_timestamp(candidate)
    current_ts = _contact_timestamp(current)
    if isinstance(current, dict) and current_ts > candidate_ts:
        return False
    if isinstance(current, dict) and current == candidate:
        return False
    global_contacts[lead] = candidate
    return True


def _build_global_contact_entry(
    *,
    contacted_at: int,
    alias: Any = "",
    account: Any = "",
    result: Any = "",
    message_id: Any = "",
) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "last_contacted_at": max(0, int(contacted_at)),
        "last_status": "sent",
    }
    normalized_alias = _normalize_alias(alias)
    if normalized_alias:
        entry["last_alias"] = normalized_alias
        entry["last_campaign"] = normalized_alias
    normalized_account = _normalize_account(account)
    if normalized_account:
        entry["last_account"] = normalized_account
    result_text = str(result or "").strip()
    if result_text:
        entry["last_result"] = result_text
    message_id_text = str(message_id or "").strip()
    if message_id_text:
        entry["last_message_id"] = message_id_text
    return entry


<<<<<<< HEAD
def _sent_log_contact_entry(record: Any) -> Optional[tuple[str, Dict[str, Any]]]:
=======
def _sent_log_confirmed_contact_entry(record: Any) -> Optional[tuple[str, Dict[str, Any]]]:
>>>>>>> origin/main
    if not isinstance(record, dict):
        return None
    if bool(record.get("cancelled")) or bool(record.get("skipped")) or record.get("skip_reason"):
        return None
<<<<<<< HEAD
    if not bool(record.get("ok")):
        return None
    campaign_alias = _normalize_alias(record.get("campaign_alias"))
=======
    if not bool(record.get("ok")) or bool(record.get("sent_unverified")):
        return None
    source_engine = str(record.get("source_engine") or "").strip().lower()
    campaign_alias = _normalize_alias(record.get("campaign_alias"))
    if source_engine and source_engine != "campaign":
        return None
    if not source_engine and not campaign_alias:
        return None
>>>>>>> origin/main
    lead = _normalize_lead(record.get("to"))
    if not lead:
        return None
    return lead, _build_global_contact_entry(
        contacted_at=_as_int(record.get("ts")),
        alias=campaign_alias,
        account=record.get("account"),
        result=record.get("detail"),
        message_id=record.get("message_id"),
    )


def _bootstrap_global_contacts_unlocked(payload: Dict[str, Any]) -> bool:
    changed = False
    global_contacts = payload.setdefault(_GLOBAL_CONTACTS_KEY, {})
    if not isinstance(global_contacts, dict):
        global_contacts = {}
        payload[_GLOBAL_CONTACTS_KEY] = global_contacts
        changed = True

    aliases = payload.get("aliases") or {}
    if isinstance(aliases, dict):
        for alias_key, bucket in aliases.items():
            if not isinstance(bucket, dict):
                continue
            leads = bucket.get("leads") or {}
            if not isinstance(leads, dict):
                continue
            for lead_key, entry in leads.items():
                if not isinstance(entry, dict):
                    continue
                if str(entry.get("status") or "").strip().lower() != "sent":
                    continue
                lead = _normalize_lead(lead_key)
                if not lead:
                    continue
                changed = _upsert_global_contact(
                    global_contacts,
                    lead,
                    _build_global_contact_entry(
                        contacted_at=_contact_timestamp(entry),
                        alias=entry.get("last_alias") or alias_key,
                        account=entry.get("sent_by"),
                        result=entry.get("last_result"),
                        message_id=entry.get("last_message_id"),
                    ),
                ) or changed

    legacy = payload.get("legacy_global_leads") or {}
    if isinstance(legacy, dict):
        for lead_key, entry in legacy.items():
            if not isinstance(entry, dict):
                continue
            if str(entry.get("status") or "").strip().lower() != "sent":
                continue
            lead = _normalize_lead(lead_key)
            if not lead:
                continue
            changed = _upsert_global_contact(
                global_contacts,
                lead,
                _build_global_contact_entry(
                    contacted_at=_contact_timestamp(entry),
                    alias=entry.get("last_alias"),
                    account=entry.get("sent_by"),
                    result=entry.get("last_result") or entry.get("detail"),
                    message_id=entry.get("last_message_id"),
                ),
            ) or changed

    sent_log_path = _STORAGE / "sent_log.jsonl"
    if sent_log_path.exists():
        for record in load_jsonl_entries(sent_log_path, label="dm_campaign.lead_status.sent_log_bootstrap"):
<<<<<<< HEAD
            parsed = _sent_log_contact_entry(record)
=======
            parsed = _sent_log_confirmed_contact_entry(record)
>>>>>>> origin/main
            if parsed is None:
                continue
            lead, entry = parsed
            changed = _upsert_global_contact(global_contacts, lead, entry) or changed

    return changed


def _coerce_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}

    aliases_raw = payload.get("aliases")
    legacy_raw = payload.get("legacy_global_leads")
    global_contacts_raw = payload.get(_GLOBAL_CONTACTS_KEY)
    if not isinstance(aliases_raw, dict):
        aliases_raw = {}
    if not isinstance(legacy_raw, dict):
        legacy_raw = {}
    if not isinstance(global_contacts_raw, dict):
        global_contacts_raw = {}

    legacy_from_v1 = payload.get("leads")
    if isinstance(legacy_from_v1, dict):
        for lead_key, entry in legacy_from_v1.items():
            normalized_lead = _normalize_lead(lead_key)
            if not normalized_lead or not isinstance(entry, dict):
                continue
            legacy_raw.setdefault(normalized_lead, dict(entry))

    aliases: Dict[str, Dict[str, Any]] = {}
    for alias_key, bucket in aliases_raw.items():
        normalized_alias = _normalize_alias(alias_key)
        if not normalized_alias or not isinstance(bucket, dict):
            continue
        leads_raw = bucket.get("leads")
        if not isinstance(leads_raw, dict):
            leads_raw = {}
        normalized_leads: Dict[str, Dict[str, Any]] = {}
        for lead_key, entry in leads_raw.items():
            normalized_lead = _normalize_lead(lead_key)
            if not normalized_lead or not isinstance(entry, dict):
                continue
            cleaned_entry = dict(entry)
            cleaned_entry["last_alias"] = normalized_alias
            normalized_leads[normalized_lead] = cleaned_entry
        cleaned_bucket = {key: value for key, value in bucket.items() if key != "leads"}
        cleaned_bucket["leads"] = normalized_leads
        aliases[normalized_alias] = cleaned_bucket

    normalized_legacy: Dict[str, Dict[str, Any]] = {}
    for lead_key, entry in legacy_raw.items():
        normalized_lead = _normalize_lead(lead_key)
        if not normalized_lead or not isinstance(entry, dict):
            continue
        normalized_legacy[normalized_lead] = dict(entry)

    normalized_global_contacts: Dict[str, Dict[str, Any]] = {}
    for lead_key, entry in global_contacts_raw.items():
        normalized_lead = _normalize_lead(lead_key)
        normalized_entry = _coerce_global_contact_entry(entry)
        if not normalized_lead or normalized_entry is None:
            continue
        normalized_global_contacts[normalized_lead] = normalized_entry

    return {
        "version": 3,
        "aliases": aliases,
        "legacy_global_leads": normalized_legacy,
        _GLOBAL_CONTACTS_KEY: normalized_global_contacts,
    }


def _load_payload_unlocked() -> Dict[str, Any]:
    _ensure_store()
    try:
        payload = load_json_file(_FILE, _default_payload(), label="dm_campaign.lead_status")
    except Exception:
        logger.exception("No se pudo leer lead_status store: %s", _FILE)
        payload = _default_payload()
    return _coerce_payload(payload)


def _save_payload_unlocked(payload: Dict[str, Any]) -> None:
    atomic_write_json(_FILE, payload)
    _PREFILTER_SNAPSHOT_CACHE.clear()


def _alias_scope_unlocked(
    payload: Dict[str, Any],
    alias: Any,
    *,
    create: bool,
) -> Optional[tuple[str, Dict[str, Any], Dict[str, Any]]]:
    normalized_alias = _normalize_alias(alias)
    if not normalized_alias:
        return None
    aliases = payload.setdefault("aliases", {})
    bucket = aliases.get(normalized_alias)
    if not isinstance(bucket, dict):
        if not create:
            return None
        bucket = {}
    leads = bucket.get("leads")
    if not isinstance(leads, dict):
        if not create:
            return None
        leads = {}
    bucket["leads"] = leads
    aliases[normalized_alias] = bucket
    return normalized_alias, bucket, leads


def get_lead_status(lead: Any, *, alias: str = "") -> Optional[Dict[str, Any]]:
    normalized_lead = _normalize_lead(lead)
    normalized_alias = _normalize_alias(alias)
    if not normalized_lead or not normalized_alias:
        return None
    with _LOCK:
        payload = _load_payload_unlocked()
        scoped = _alias_scope_unlocked(payload, normalized_alias, create=False)
        if scoped is None:
            return None
        _alias_key, _bucket, leads = scoped
        entry = leads.get(normalized_lead)
        if not isinstance(entry, dict):
            return None
        return dict(entry)


def get_legacy_lead_status(lead: Any) -> Optional[Dict[str, Any]]:
    normalized_lead = _normalize_lead(lead)
    if not normalized_lead:
        return None
    with _LOCK:
        payload = _load_payload_unlocked()
        legacy = payload.get("legacy_global_leads") or {}
        entry = legacy.get(normalized_lead)
        if not isinstance(entry, dict):
            return None
        return dict(entry)


def get_terminal_leads(alias: str) -> set[str]:
    normalized_alias = _normalize_alias(alias)
    if not normalized_alias:
        return set()
    with _LOCK:
        payload = _load_payload_unlocked()
        scoped = _alias_scope_unlocked(payload, normalized_alias, create=False)
        if scoped is None:
            return set()
        _alias_key, _bucket, leads = scoped
        terminal: set[str] = set()
        for lead_key, entry in leads.items():
            if not isinstance(entry, dict):
                continue
            status = str(entry.get("status") or "").strip().lower()
            if status in {"sent", "skipped"}:
                terminal.add(str(lead_key))
        return terminal


def get_legacy_lead_status_map() -> Dict[str, Dict[str, Any]]:
    with _LOCK:
        payload = _load_payload_unlocked()
        legacy = payload.get("legacy_global_leads") or {}
        return {
            str(lead_key): dict(entry)
            for lead_key, entry in legacy.items()
            if isinstance(entry, dict)
        }


def get_global_contact_record(lead: Any) -> Optional[Dict[str, Any]]:
    normalized_lead = _normalize_lead(lead)
    if not normalized_lead:
        return None
    with _LOCK:
        payload = _load_payload_unlocked()
        if _bootstrap_global_contacts_unlocked(payload):
            _save_payload_unlocked(payload)
        global_contacts = payload.get(_GLOBAL_CONTACTS_KEY) or {}
        entry = global_contacts.get(normalized_lead)
        if not isinstance(entry, dict):
            return None
        return dict(entry)


def is_globally_contact_blocked(lead: Any, *, now: Optional[int] = None) -> bool:
    entry = get_global_contact_record(lead)
    if not isinstance(entry, dict):
        return False
    ts = _contact_timestamp(entry)
    if ts <= 0:
        return False
    reference_now = int(time.time()) if now is None else int(now)
    return max(0, reference_now - ts) < GLOBAL_CONTACT_TTL_SECONDS


def get_prefilter_snapshot(alias: str) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    normalized_alias = _normalize_alias(alias)
    if not normalized_alias:
        return {}, {}
    with _LOCK:
        cache_key = (normalized_alias, _prefilter_file_token())
        cached = _PREFILTER_SNAPSHOT_CACHE.get(cache_key)
        if cached is not None:
            _PREFILTER_SNAPSHOT_CACHE.move_to_end(cache_key)
            cached_alias_statuses, cached_global = cached
            return {
                str(lead_key): dict(entry)
                for lead_key, entry in cached_alias_statuses.items()
                if isinstance(entry, dict)
            }, {
                str(lead_key): dict(entry)
                for lead_key, entry in cached_global.items()
                if isinstance(entry, dict)
            }
        payload = _load_payload_unlocked()
        if _bootstrap_global_contacts_unlocked(payload):
            _save_payload_unlocked(payload)
            cache_key = (normalized_alias, _prefilter_file_token())
        scoped = _alias_scope_unlocked(payload, normalized_alias, create=False)
        alias_status_map: Dict[str, Dict[str, Any]] = {}
        if scoped is not None:
            _alias_key, _bucket, leads = scoped
            for lead_key, entry in leads.items():
                if not isinstance(entry, dict):
                    continue
                status = str(entry.get("status") or "").strip().lower()
<<<<<<< HEAD
                if status in {"pending", "sent", "skipped"}:
=======
                if status in {"sent", "skipped"}:
>>>>>>> origin/main
                    alias_status_map[str(lead_key)] = dict(entry)
        global_contacts = payload.get(_GLOBAL_CONTACTS_KEY) or {}
        global_contact_map = {
            str(lead_key): dict(entry)
            for lead_key, entry in global_contacts.items()
            if isinstance(entry, dict)
        }
        _PREFILTER_SNAPSHOT_CACHE[cache_key] = (
            {
                str(lead_key): dict(entry)
                for lead_key, entry in alias_status_map.items()
                if isinstance(entry, dict)
            },
            {
                str(lead_key): dict(entry)
                for lead_key, entry in global_contact_map.items()
                if isinstance(entry, dict)
            },
        )
        _PREFILTER_SNAPSHOT_CACHE.move_to_end(cache_key)
        while len(_PREFILTER_SNAPSHOT_CACHE) > _PREFILTER_SNAPSHOT_LIMIT:
            _PREFILTER_SNAPSHOT_CACHE.popitem(last=False)
        return alias_status_map, global_contact_map


def is_terminal_lead_status(lead: Any, *, alias: str = "") -> bool:
    entry = get_lead_status(lead, alias=alias)
    if not isinstance(entry, dict):
        return False
    status = str(entry.get("status") or "").strip().lower()
    return status in {"sent", "skipped"}


<<<<<<< HEAD
def mark_leads_pending(leads: Iterable[Any], *, alias: str = "", run_id: str = "") -> int:
    normalized_alias = _normalize_alias(alias)
    normalized_run_id = str(run_id or "").strip()
=======
def mark_leads_pending(leads: Iterable[Any], *, alias: str = "") -> int:
    normalized_alias = _normalize_alias(alias)
>>>>>>> origin/main
    if not normalized_alias:
        return 0
    normalized_leads: list[str] = []
    seen: set[str] = set()
    for lead in leads:
        normalized_lead = _normalize_lead(lead)
        if not normalized_lead or normalized_lead in seen:
            continue
        seen.add(normalized_lead)
        normalized_leads.append(normalized_lead)
    if not normalized_leads:
        return 0
    now = int(time.time())
    with _LOCK:
        payload = _load_payload_unlocked()
        scoped = _alias_scope_unlocked(payload, normalized_alias, create=True)
        if scoped is None:
            return 0
        _alias_key, _bucket, leads = scoped
        updated = False
        marked = 0
        for normalized_lead in normalized_leads:
            current = leads.get(normalized_lead)
            if not isinstance(current, dict):
                current = {}
            status = str(current.get("status") or "").strip().lower()
            if status in {"sent", "skipped"}:
                continue
            current["status"] = "pending"
            current["updated_at"] = now
            current["last_alias"] = normalized_alias
<<<<<<< HEAD
            current["pending_selected_at"] = now
            if normalized_run_id:
                current["pending_run_id"] = normalized_run_id
            else:
                current.pop("pending_run_id", None)
=======
>>>>>>> origin/main
            leads[normalized_lead] = current
            updated = True
            marked += 1
        if updated:
            _save_payload_unlocked(payload)
        return marked


def mark_lead_pending(lead: Any, *, alias: str = "") -> None:
    mark_leads_pending((lead,), alias=alias)


def apply_terminal_status_updates(
    *,
    alias: str = "",
    sent_updates: Iterable[tuple[Any, str]] = (),
    skipped_updates: Iterable[tuple[Any, str]] = (),
) -> int:
    normalized_alias = _normalize_alias(alias)
    if not normalized_alias:
        return 0
    normalized_sent: list[tuple[str, str]] = []
    normalized_skipped: list[tuple[str, str]] = []
    sent_seen: set[str] = set()
    skipped_seen: set[str] = set()
    for lead, sent_by in sent_updates:
        normalized_lead = _normalize_lead(lead)
        if not normalized_lead or normalized_lead in sent_seen:
            continue
        sent_seen.add(normalized_lead)
        normalized_sent.append((normalized_lead, str(sent_by or "").strip().lstrip("@")))
    for lead, reason in skipped_updates:
        normalized_lead = _normalize_lead(lead)
        if not normalized_lead or normalized_lead in sent_seen or normalized_lead in skipped_seen:
            continue
        skipped_seen.add(normalized_lead)
        normalized_skipped.append((normalized_lead, str(reason or "").strip()))
    if not normalized_sent and not normalized_skipped:
        return 0
    now = int(time.time())
    with _LOCK:
        payload = _load_payload_unlocked()
        scoped = _alias_scope_unlocked(payload, normalized_alias, create=True)
        if scoped is None:
            return 0
        _alias_key, _bucket, leads = scoped
        global_contacts = payload.setdefault(_GLOBAL_CONTACTS_KEY, {})
        if not isinstance(global_contacts, dict):
            global_contacts = {}
            payload[_GLOBAL_CONTACTS_KEY] = global_contacts
        updated = False
        marked = 0
        for normalized_lead, sent_by in normalized_sent:
            entry = leads.get(normalized_lead)
            if not isinstance(entry, dict):
                entry = {}
            entry["status"] = "sent"
            entry["updated_at"] = now
            entry["sent_timestamp"] = now
            entry["last_alias"] = normalized_alias
            if sent_by:
                entry["sent_by"] = sent_by
            leads[normalized_lead] = entry
            if _upsert_global_contact(
                global_contacts,
                normalized_lead,
                _build_global_contact_entry(
                    contacted_at=now,
                    alias=normalized_alias,
                    account=sent_by,
                ),
            ):
                updated = True
            updated = True
            marked += 1
        for normalized_lead, reason in normalized_skipped:
            entry = leads.get(normalized_lead)
            if not isinstance(entry, dict):
                entry = {}
            status = str(entry.get("status") or "").strip().lower()
            if status == "sent":
                continue
            entry["status"] = "skipped"
            entry["updated_at"] = now
            entry["skipped_timestamp"] = now
            entry["last_alias"] = normalized_alias
            if reason:
                entry["last_error"] = reason
            leads[normalized_lead] = entry
            updated = True
            marked += 1
        if updated:
            _save_payload_unlocked(payload)
        return marked


def mark_lead_sent(lead: Any, *, sent_by: str = "", alias: str = "") -> None:
    apply_terminal_status_updates(
        alias=alias,
        sent_updates=((lead, sent_by),),
    )


def mark_lead_failed(lead: Any, *, reason: str = "", attempts: int = 0, alias: str = "") -> None:
    normalized_lead = _normalize_lead(lead)
    normalized_alias = _normalize_alias(alias)
    if not normalized_lead or not normalized_alias:
        return
    now = int(time.time())
    with _LOCK:
        payload = _load_payload_unlocked()
        scoped = _alias_scope_unlocked(payload, normalized_alias, create=True)
        if scoped is None:
            return
        _alias_key, _bucket, leads = scoped
        entry = leads.get(normalized_lead)
        if not isinstance(entry, dict):
            entry = {}
        status = str(entry.get("status") or "").strip().lower()
        if status in {"sent", "skipped"}:
            return
        entry["status"] = "failed"
        entry["updated_at"] = now
        entry["failed_timestamp"] = now
        entry["last_alias"] = normalized_alias
        if attempts > 0:
            entry["attempts"] = int(attempts)
        if reason:
            entry["last_error"] = str(reason)
        leads[normalized_lead] = entry
        _save_payload_unlocked(payload)


def mark_lead_skipped(lead: Any, *, reason: str = "", alias: str = "") -> None:
    apply_terminal_status_updates(
        alias=alias,
        skipped_updates=((lead, reason),),
    )
