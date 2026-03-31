from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - zoneinfo is stdlib on supported runtimes
    ZoneInfo = None  # type: ignore[assignment]

from core.proxy_preflight import account_proxy_preflight

try:  # pragma: no cover - optional dependency, but present in runtime
    import tzlocal  # type: ignore
except Exception:  # pragma: no cover - fallback path below
    tzlocal = None  # type: ignore[assignment]


def _clean(value: Any) -> str:
    return str(value or "").strip()


def business_timezone_id() -> str:
    # Business/day-counter timezone remains an app-level policy and is
    # intentionally separate from the live browser/session timezone policy.
    try:
        from core import storage as storage_module

        label = _clean(getattr(storage_module, "TZ_LABEL", ""))
        if label:
            return label
    except Exception:
        pass
    return "UTC"


def normalize_iana_timezone_id(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    if ZoneInfo is None:
        return text
    try:
        ZoneInfo(text)
    except Exception:
        return ""
    return text


def resolve_system_timezone_id() -> str:
    candidates: list[str] = []
    if tzlocal is not None:
        with_candidates = getattr(tzlocal, "get_localzone_name", None)
        if callable(with_candidates):
            try:
                resolved = _clean(with_candidates())
            except Exception:
                resolved = ""
            if resolved:
                candidates.append(resolved)
    try:
        from datetime import datetime

        tzinfo = datetime.now().astimezone().tzinfo
    except Exception:
        tzinfo = None
    for attr in ("key", "zone"):
        resolved = _clean(getattr(tzinfo, attr, ""))
        if resolved:
            candidates.append(resolved)
    for candidate in candidates:
        normalized = normalize_iana_timezone_id(candidate)
        if normalized:
            return normalized
    raise CampaignTimezoneResolutionError(
        reason_code="SYSTEM_TIMEZONE_UNRESOLVED",
        message="No se pudo resolver la zona horaria IANA del sistema para el navegador de campaign.",
        browser_timezone_source="system",
        has_proxy=False,
    )


@dataclass(frozen=True)
class CampaignBrowserTimezoneResolution:
    timezone_id: str
    browser_timezone_source: str
    business_timezone_id: str
    has_proxy: bool
    proxy_id: str = ""
    proxy_label: str = ""
    reason_code: str = ""


class CampaignTimezoneResolutionError(RuntimeError):
    def __init__(
        self,
        *,
        reason_code: str,
        message: str,
        browser_timezone_source: str,
        has_proxy: bool,
        proxy_id: str = "",
        proxy_label: str = "",
    ) -> None:
        self.reason_code = _clean(reason_code).upper() or "CAMPAIGN_TIMEZONE_RESOLUTION_FAILED"
        self.browser_timezone_source = _clean(browser_timezone_source).lower() or "fallback"
        self.has_proxy = bool(has_proxy)
        self.proxy_id = _clean(proxy_id)
        self.proxy_label = _clean(proxy_label)
        self.business_timezone_id = business_timezone_id()
        super().__init__(_clean(message) or self.reason_code.lower())


def _proxy_timezone_id(record: dict[str, Any] | None) -> str:
    if not isinstance(record, dict):
        return ""
    for key in ("timezone_id", "proxy_timezone_id", "timezone", "tz"):
        normalized = normalize_iana_timezone_id(record.get(key))
        if normalized:
            return normalized
    return ""


def resolve_campaign_browser_timezone(
    account: dict[str, Any] | None,
) -> CampaignBrowserTimezoneResolution:
    preflight = account_proxy_preflight(
        account,
        allow_proxyless=True,
        allow_legacy=True,
    )
    network_mode = _clean(preflight.get("network_mode")).lower()
    proxy_id = _clean(preflight.get("proxy_id"))
    proxy_label = _clean(preflight.get("proxy_label")) or proxy_id
    has_proxy = network_mode in {"proxy", "legacy"}
    business_tz = business_timezone_id()

    if network_mode == "direct":
        return CampaignBrowserTimezoneResolution(
            timezone_id=resolve_system_timezone_id(),
            browser_timezone_source="system",
            business_timezone_id=business_tz,
            has_proxy=False,
            proxy_id="",
            proxy_label=_clean(preflight.get("proxy_label")),
        )

    if network_mode == "proxy":
        record = preflight.get("record") if isinstance(preflight.get("record"), dict) else None
        proxy_tz = _proxy_timezone_id(record)
        if proxy_tz:
            return CampaignBrowserTimezoneResolution(
                timezone_id=proxy_tz,
                browser_timezone_source="proxy",
                business_timezone_id=business_tz,
                has_proxy=True,
                proxy_id=proxy_id,
                proxy_label=proxy_label,
            )
        raise CampaignTimezoneResolutionError(
            reason_code="PROXY_TIMEZONE_MISSING",
            message=f"El proxy {proxy_id or proxy_label or 'asignado'} no tiene timezone_id IANA explicito.",
            browser_timezone_source="proxy",
            has_proxy=True,
            proxy_id=proxy_id,
            proxy_label=proxy_label,
        )

    if network_mode == "legacy":
        raise CampaignTimezoneResolutionError(
            reason_code="LEGACY_PROXY_TIMEZONE_UNSUPPORTED",
            message="La cuenta usa proxy legacy sin entidad de proxy con timezone_id explicito.",
            browser_timezone_source="proxy",
            has_proxy=True,
            proxy_id=proxy_id,
            proxy_label=proxy_label,
        )

    raise CampaignTimezoneResolutionError(
        reason_code="CAMPAIGN_TIMEZONE_RESOLUTION_FAILED",
        message="No se pudo resolver la zona horaria del navegador de campaign.",
        browser_timezone_source="fallback",
        has_proxy=has_proxy,
        proxy_id=proxy_id,
        proxy_label=proxy_label,
    )
