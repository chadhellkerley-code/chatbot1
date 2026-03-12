from __future__ import annotations

from typing import Any

from core.storage import sent_count_today_for_account


def account_message_limit(
    *,
    account: dict[str, Any] | None = None,
    username: str = "",
    default: int | None = None,
) -> int | None:
    record = dict(account or {})
    clean_username = str(username or record.get("username") or "").strip().lstrip("@")
    if not record and clean_username:
        from core import accounts as accounts_module

        stored = accounts_module.get_account(clean_username) or {}
        if isinstance(stored, dict):
            record = dict(stored)

    for key in ("messages_per_account", "max_messages"):
        raw_value = record.get(key)
        try:
            parsed = int(raw_value)
        except Exception:
            continue
        if parsed > 0:
            return parsed

    if default is None:
        return None
    try:
        parsed_default = int(default)
    except Exception:
        return None
    if parsed_default <= 0:
        return None
    return parsed_default


def can_send_message_for_account(
    *,
    account: dict[str, Any] | None = None,
    username: str = "",
    default: int | None = None,
    source_engine: str | None = None,
    campaign_alias: str | None = None,
    run_id: str | None = None,
    include_legacy: bool = True,
) -> tuple[bool, int, int | None]:
    record = dict(account or {})
    clean_username = str(username or record.get("username") or "").strip().lstrip("@")
    limit = account_message_limit(account=account, username=clean_username, default=default)
    if not clean_username:
        return True, 0, limit

    resolved_source_engine = str(
        source_engine or record.get("quota_source_engine") or ""
    ).strip().lower()
    resolved_campaign_alias = str(
        campaign_alias or record.get("quota_campaign_alias") or ""
    ).strip().lower()
    resolved_run_id = str(run_id or record.get("quota_run_id") or "").strip()
    resolved_include_legacy = include_legacy
    if source_engine is None and campaign_alias is None and "quota_include_legacy" in record:
        resolved_include_legacy = bool(record.get("quota_include_legacy"))

    sent_today = sent_count_today_for_account(
        clean_username,
        source_engine=resolved_source_engine or None,
        campaign_alias=resolved_campaign_alias or None,
        run_id=resolved_run_id or None,
        include_legacy=resolved_include_legacy,
    )
    if limit is None:
        return True, sent_today, None
    return sent_today < limit, sent_today, limit
