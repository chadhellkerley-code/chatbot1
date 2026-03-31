from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from core.proxy_registry import (
    get_proxy_by_id as _registry_get_proxy_by_id,
    is_active_proxy,
    list_active_proxies as _registry_list_active_proxies,
    load_proxies as _registry_load_proxies,
)


def load_proxies(path: Path | None = None) -> List[Dict[str, Any]]:
    return _registry_load_proxies(path)


def get_proxy_by_id(
    proxy_id: Any,
    *,
    active_only: bool = False,
    path: Path | None = None,
) -> Optional[Dict[str, Any]]:
    return _registry_get_proxy_by_id(proxy_id, active_only=active_only, path=path)


def list_active_proxies(path: Path | None = None) -> List[Dict[str, Any]]:
    return _registry_list_active_proxies(path)


def _is_active(proxy: Dict[str, Any]) -> bool:
    return is_active_proxy(proxy)

