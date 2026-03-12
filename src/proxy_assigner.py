from __future__ import annotations

from typing import Any, Dict, List, MutableSequence

from src.proxy_pool import list_active_proxies


def assign_proxies_to_accounts(accounts: MutableSequence[Dict[str, Any]]) -> MutableSequence[Dict[str, Any]]:
    if not accounts:
        return accounts

    proxy_ids = _active_proxy_ids()
    if not proxy_ids:
        return accounts

    index = 0
    total = len(proxy_ids)
    for account in accounts:
        if not isinstance(account, dict):
            continue
        assigned = str(account.get("assigned_proxy_id") or "").strip()
        if assigned or _has_legacy_proxy(account):
            continue
        account["assigned_proxy_id"] = proxy_ids[index % total]
        index += 1
    return accounts


def _active_proxy_ids() -> List[str]:
    ids: List[str] = []
    for proxy in list_active_proxies():
        if not isinstance(proxy, dict):
            continue
        proxy_id = str(proxy.get("id") or proxy.get("proxy_id") or "").strip()
        if proxy_id:
            ids.append(proxy_id)
    return ids


def _has_legacy_proxy(account: Dict[str, Any]) -> bool:
    proxy_url = str(account.get("proxy_url") or "").strip()
    if proxy_url:
        return True
    return bool(account.get("proxy"))
