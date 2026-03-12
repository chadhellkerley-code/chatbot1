from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, time, timedelta
from typing import Any

import health_store
import update_system
from config import read_supabase_config, refresh_settings, update_supabase_config
from core import accounts as accounts_module
from core import storage as storage_module
from src.analytics import stats_engine
from src.licensing import SupabaseLicenseClient

from .base import ServiceContext, ServiceError, normalize_alias


class SystemService:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context

    def _license_admin(self) -> SupabaseLicenseClient:
        try:
            return SupabaseLicenseClient(admin=True)
        except Exception as exc:
            raise ServiceError(
                "No se pudo conectar con Supabase. Usa Sistema > Configuracion > Configurar Supabase."
            ) from exc

    def dashboard_snapshot(self) -> dict[str, Any]:
        accounts = [item for item in accounts_module.list_all() if isinstance(item, dict)]
        sent_today, errors_today, last_reset, timezone_label = storage_module.sent_totals_today()
        today = self._today()

        account_rows = self._account_detail_rows(accounts, today=today)
        blocked_accounts = [
            item
            for item in account_rows
            if str(item.get("health_badge") or "").strip().upper() == "MUERTA"
        ]
        replied_today_rows = self._replied_today_rows(today=today)
        booked_today_rows = self._booked_today_rows(today=today)
        top_sent = sorted(
            account_rows,
            key=lambda item: int(item.get("messages_sent") or 0),
            reverse=True,
        )[:5]
        top_replied = sorted(
            account_rows,
            key=lambda item: int(item.get("replies") or 0),
            reverse=True,
        )[:5]
        top_messages, conversion = self._message_summary()
        return {
            "metrics": {
                "total_accounts": len(account_rows),
                "connected_accounts": sum(
                    1 for item in account_rows if bool(item.get("connected"))
                ),
                "blocked_accounts": len(blocked_accounts),
                "messages_sent_today": int(sent_today or 0),
                "messages_error_today": int(errors_today or 0),
                "messages_replied_today": len(replied_today_rows),
                "booked_today": len(booked_today_rows),
            },
            "account_rows": account_rows,
            "blocked_account_rows": blocked_accounts,
            "booked_today_rows": booked_today_rows,
            "replied_today_rows": replied_today_rows,
            "top_accounts_sent": top_sent,
            "top_accounts_replied": top_replied,
            "top_messages": top_messages,
            "conversion": conversion,
            "timezone_label": timezone_label,
            "last_reset_display": last_reset,
        }

    def list_licenses(self) -> list[dict[str, Any]]:
        return [item for item in self._license_admin().list_licenses() if isinstance(item, dict)]

    def fetch_license(self, license_key: str) -> dict[str, Any] | None:
        payload = self._license_admin().fetch_license(license_key)
        return dict(payload) if isinstance(payload, dict) else None

    def create_license(
        self,
        client_name: str,
        *,
        plan_name: str,
        max_devices: int = 2,
        expires_at: str,
        notes: str = "",
    ) -> dict[str, Any]:
        return dict(
            self._license_admin().create_license(
                client_name=client_name,
                plan_name=plan_name,
                max_devices=max_devices,
                expires_at=expires_at,
                notes=notes,
            )
        )

    def create_local_license(
        self,
        client_name: str,
        *,
        days: int = 30,
        email: str = "",
    ) -> dict[str, Any]:
        del email
        expires = datetime.now().astimezone() + timedelta(days=max(30, int(days or 30)))
        return self.create_license(
            client_name=client_name,
            plan_name="standard",
            max_devices=2,
            expires_at=expires.isoformat(),
            notes="",
        )

    def extend_license(self, license_key: str, *, days: int) -> dict[str, Any] | None:
        record = self._license_admin().extend_license(license_key, days=days)
        return dict(record) if isinstance(record, dict) else None

    def delete_license(self, license_key: str) -> bool:
        return bool(self.deactivate_license(license_key))

    def deactivate_license(self, license_key: str) -> dict[str, Any] | None:
        record = self._license_admin().deactivate_license(license_key)
        return dict(record) if isinstance(record, dict) else None

    def reset_device_activations(self, license_key: str) -> int:
        return int(self._license_admin().reset_device_activations(license_key))

    def list_license_activations(self, license_key: str) -> list[dict[str, Any]]:
        record = self.fetch_license(license_key)
        if not record:
            return []
        rows = self._license_admin().list_license_activations(
            str(record.get("license_key") or ""),
            license_id=str(record.get("id") or ""),
        )
        return [dict(row) for row in rows if isinstance(row, dict)]

    def check_updates(self) -> dict[str, Any]:
        return dict(update_system.check_for_updates())

    def update_config(self) -> dict[str, Any]:
        loader = getattr(update_system, "_load_update_config", None)
        if callable(loader):
            payload = loader()
            return dict(payload) if isinstance(payload, dict) else {}
        return {}

    def save_update_config(self, updates: dict[str, Any]) -> dict[str, Any]:
        loader = getattr(update_system, "_load_update_config", None)
        saver = getattr(update_system, "_save_update_config", None)
        current = loader() if callable(loader) else {}
        payload = dict(current or {})
        payload.update(dict(updates or {}))
        if callable(saver):
            saver(payload)
        return payload

    def supabase_config(self) -> dict[str, str]:
        payload = read_supabase_config()
        return {
            "supabase_url": str(payload.get("supabase_url") or "").strip(),
            "supabase_key": str(payload.get("supabase_key") or "").strip(),
        }

    def save_supabase_config(self, *, supabase_url: str, supabase_key: str) -> dict[str, str]:
        payload = update_supabase_config(
            supabase_url=supabase_url,
            supabase_key=supabase_key,
        )
        refresh_settings()
        return {
            "supabase_url": str(payload.get("supabase_url") or "").strip(),
            "supabase_key": str(payload.get("supabase_key") or "").strip(),
        }

    def _account_detail_rows(
        self,
        accounts: list[dict[str, Any]],
        *,
        today: datetime.date,
    ) -> list[dict[str, Any]]:
        sent_entries = self.context.read_jsonl(self.context.storage_path("sent_log.jsonl"))
        message_counts: dict[str, dict[str, int]] = defaultdict(
            lambda: {"sent": 0, "errors": 0}
        )
        for entry in sent_entries:
            account = str(entry.get("account") or "").strip().lstrip("@").lower()
            if not account:
                continue
            dt = self._entry_datetime(entry)
            if dt is None or dt.date() != today:
                continue
            if bool(entry.get("ok")):
                message_counts[account]["sent"] += 1
            else:
                message_counts[account]["errors"] += 1

        replied_counter: Counter[str] = Counter()
        for row in self._replied_today_rows(today=today):
            account = str(row.get("account") or "").strip().lstrip("@").lower()
            if account:
                replied_counter[account] += 1

        rows: list[dict[str, Any]] = []
        for record in accounts:
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            proxy_value = str(
                record.get("assigned_proxy_id") or record.get("proxy_url") or ""
            ).strip()
            badge, _cached = health_store.get_badge(username)
            connected = False
            try:
                connected = bool(
                    accounts_module.connected_status(
                        record,
                        strict=False,
                        reason="dashboard-details",
                        fast=True,
                        persist=False,
                    )
                )
            except Exception:
                connected = bool(record.get("connected", False))
            key = username.lower()
            rows.append(
                {
                    "alias": normalize_alias(record.get("alias")),
                    "username": username,
                    "proxy": proxy_value or "-",
                    "connected": connected,
                    "session_status": "conectada" if connected else "sin sesion",
                    "messages_sent": int(message_counts[key]["sent"]),
                    "errors": int(message_counts[key]["errors"]),
                    "replies": int(replied_counter[key]),
                    "health_badge": str(badge or ""),
                }
            )
        rows.sort(
            key=lambda item: (str(item.get("alias") or ""), str(item.get("username") or ""))
        )
        return rows

    def _today(self) -> datetime.date:
        tzinfo = getattr(storage_module, "TZ", None)
        if tzinfo is None:
            return datetime.now().astimezone().date()
        return datetime.now(tzinfo).date()

    def _day_bounds(self, today: datetime.date) -> tuple[datetime, datetime]:
        tzinfo = getattr(storage_module, "TZ", None)
        if tzinfo is None:
            start = datetime.combine(today, time.min).astimezone()
            end = datetime.combine(today, time.max).astimezone()
            return start, end
        start = datetime.combine(today, time.min, tzinfo=tzinfo)
        end = datetime.combine(today, time.max, tzinfo=tzinfo)
        return start, end

    def _replied_today_rows(self, *, today: datetime.date) -> list[dict[str, Any]]:
        start, end = self._day_bounds(today)
        rows = storage_module.conversation_rows(start=start, end=end)
        replied: list[dict[str, Any]] = []
        for row in rows:
            status = str(row.get("status") or "").strip().lower()
            if status in {"mensaje enviado", "sin respuesta"}:
                continue
            replied.append(dict(row))
        return replied

    def _booked_today_rows(self, *, today: datetime.date) -> list[dict[str, Any]]:
        keywords = ("appoint", "book", "calendar", "cita", "agenda", "turno", "meeting")
        booked: list[dict[str, Any]] = []
        for row in self._replied_today_rows(today=today):
            status = str(row.get("status") or "").strip().lower()
            if any(token in status for token in keywords):
                booked.append(dict(row))
        return booked

    def _entry_datetime(self, entry: dict[str, Any]) -> datetime | None:
        tzinfo = getattr(storage_module, "TZ", None)
        value = entry.get("started_at")
        if isinstance(value, str) and value.strip():
            text = value.strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(text)
                if parsed.tzinfo:
                    return parsed.astimezone(tzinfo) if tzinfo is not None else parsed.astimezone()
                return parsed.replace(tzinfo=tzinfo).astimezone(tzinfo) if tzinfo is not None else parsed.astimezone()
            except Exception:
                pass
        raw_ts = entry.get("ts")
        if raw_ts in (None, ""):
            return None
        try:
            if tzinfo is not None:
                return datetime.fromtimestamp(float(raw_ts), tz=tzinfo)
            return datetime.fromtimestamp(float(raw_ts)).astimezone()
        except Exception:
            return None

    def _message_summary(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        builder = getattr(stats_engine, "_build_report", None)
        if not callable(builder):
            return [], {"rate": 0.0, "sent": 0, "responded": 0}
        try:
            report = builder()
        except Exception:
            return [], {"rate": 0.0, "sent": 0, "responded": 0}
        top_messages: list[dict[str, Any]] = []
        total_sent = 0
        total_responded = 0
        roles = report.get("roles") if isinstance(report, dict) else {}
        if isinstance(roles, dict):
            for role_key, role_payload in roles.items():
                if not isinstance(role_payload, dict):
                    continue
                total_sent += int(role_payload.get("sent") or 0)
                total_responded += int(role_payload.get("responded") or 0)
                messages = role_payload.get("messages") or {}
                if not isinstance(messages, dict):
                    continue
                for item in messages.values():
                    if not isinstance(item, dict):
                        continue
                    sent = int(item.get("sent") or 0)
                    responded = int(item.get("responded") or 0)
                    rate = round((responded / sent) * 100, 1) if sent else 0.0
                    top_messages.append(
                        {
                            "role": str(role_key),
                            "text": str(item.get("text") or "").strip(),
                            "sent": sent,
                            "responded": responded,
                            "response_rate": rate,
                        }
                    )
        top_messages.sort(
            key=lambda item: (
                int(item.get("responded") or 0),
                float(item.get("response_rate") or 0.0),
            ),
            reverse=True,
        )
        conversion = {
            "sent": total_sent,
            "responded": total_responded,
            "rate": round((total_responded / total_sent) * 100, 1)
            if total_sent
            else 0.0,
        }
        return top_messages[:8], conversion
