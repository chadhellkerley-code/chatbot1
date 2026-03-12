from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from core.storage_atomic import atomic_write_json, load_json_file
from paths import storage_root

_STORAGE = storage_root(Path(__file__).resolve().parent.parent.parent)
_FILE = _STORAGE / "lead_status.json"
_LOCK = threading.RLock()
_PREFILTER_SNAPSHOT_CACHE: OrderedDict[
    tuple[str, tuple[int, int] | None],
    tuple[set[str], Dict[str, Dict[str, Any]]],
] = OrderedDict()
_PREFILTER_SNAPSHOT_LIMIT = 64
logger = logging.getLogger(__name__)


def _normalize_lead(value: Any) -> str:
    return str(value or "").strip().lstrip("@").lower()


def _normalize_alias(value: Any) -> str:
    return str(value or "").strip().lower()


def _default_payload() -> Dict[str, Any]:
    return {"version": 2, "aliases": {}, "legacy_global_leads": {}}


def _ensure_store() -> None:
    _STORAGE.mkdir(parents=True, exist_ok=True)
    if _FILE.exists():
        return
    atomic_write_json(_FILE, _default_payload())


def _prefilter_file_token() -> tuple[int, int] | None:
    if not _FILE.exists():
        return None
    try:
        stat = _FILE.stat()
    except OSError:
        return None
    return int(stat.st_mtime_ns), int(stat.st_size)


def _coerce_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}

    aliases_raw = payload.get("aliases")
    legacy_raw = payload.get("legacy_global_leads")
    if not isinstance(aliases_raw, dict):
        aliases_raw = {}
    if not isinstance(legacy_raw, dict):
        legacy_raw = {}

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

    return {
        "version": 2,
        "aliases": aliases,
        "legacy_global_leads": normalized_legacy,
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


def get_prefilter_snapshot(alias: str) -> tuple[set[str], Dict[str, Dict[str, Any]]]:
    normalized_alias = _normalize_alias(alias)
    if not normalized_alias:
        return set(), {}
    cache_key = (normalized_alias, _prefilter_file_token())
    with _LOCK:
        cached = _PREFILTER_SNAPSHOT_CACHE.get(cache_key)
        if cached is not None:
            _PREFILTER_SNAPSHOT_CACHE.move_to_end(cache_key)
            cached_terminal, cached_legacy = cached
            return set(cached_terminal), {
                str(lead_key): dict(entry)
                for lead_key, entry in cached_legacy.items()
                if isinstance(entry, dict)
            }
        payload = _load_payload_unlocked()
        scoped = _alias_scope_unlocked(payload, normalized_alias, create=False)
        terminal: set[str] = set()
        if scoped is not None:
            _alias_key, _bucket, leads = scoped
            for lead_key, entry in leads.items():
                if not isinstance(entry, dict):
                    continue
                status = str(entry.get("status") or "").strip().lower()
                if status in {"sent", "skipped"}:
                    terminal.add(str(lead_key))
        legacy = payload.get("legacy_global_leads") or {}
        legacy_map = {
            str(lead_key): dict(entry)
            for lead_key, entry in legacy.items()
            if isinstance(entry, dict)
        }
        _PREFILTER_SNAPSHOT_CACHE[cache_key] = (
            set(terminal),
            {
                str(lead_key): dict(entry)
                for lead_key, entry in legacy_map.items()
                if isinstance(entry, dict)
            },
        )
        _PREFILTER_SNAPSHOT_CACHE.move_to_end(cache_key)
        while len(_PREFILTER_SNAPSHOT_CACHE) > _PREFILTER_SNAPSHOT_LIMIT:
            _PREFILTER_SNAPSHOT_CACHE.popitem(last=False)
        return terminal, legacy_map


def is_terminal_lead_status(lead: Any, *, alias: str = "") -> bool:
    entry = get_lead_status(lead, alias=alias)
    if not isinstance(entry, dict):
        return False
    status = str(entry.get("status") or "").strip().lower()
    return status in {"sent", "skipped"}


def mark_leads_pending(leads: Iterable[Any], *, alias: str = "") -> int:
    normalized_alias = _normalize_alias(alias)
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
