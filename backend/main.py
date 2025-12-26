# -*- coding: utf-8 -*-
"""FastAPI backend for license creation and activation."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import secrets
import string
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

load_dotenv()

app = FastAPI(title="License Backend", version="1.0.0")


class CreateLicenseIn(BaseModel):
    name: str = Field(..., min_length=1)
    days: int = Field(..., ge=30)
    email: Optional[str] = None


class ActivateIn(BaseModel):
    license_key: str = Field(..., min_length=1)
    client_fingerprint: Optional[str] = None


def _get_supabase_credentials() -> tuple[str, str]:
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not key:
        key = os.environ.get("SUPABASE_KEY", "").strip()
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase credentials missing.")
    return url, key


def _hash_license_key(license_key: str, secret: str) -> str:
    payload = f"{secret}:{license_key}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _license_secret() -> str:
    secret = os.environ.get("LICENSE_HASH_SECRET", "").strip()
    if secret:
        return secret
    _, key = _get_supabase_credentials()
    return key


def _supabase_request(
    method: str,
    url: str,
    key: str,
    endpoint: str,
    *,
    params: Optional[Dict[str, str]] = None,
    payload: Optional[Dict[str, Any]] = None,
    prefer_return: bool = False,
) -> Any:
    base = url.rstrip("/") + "/rest/v1/"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if prefer_return:
        headers["Prefer"] = "return=representation"

    try:
        response = requests.request(
            method,
            base + endpoint.lstrip("/"),
            headers=headers,
            params=params,
            json=payload,
            timeout=30,
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise HTTPException(
            status_code=500, detail=f"Supabase error {response.status_code}: {detail}"
        )

    if not response.text:
        return None
    try:
        return response.json()
    except Exception:
        return response.text


def _select_one(
    table: str, filters: Dict[str, str], *, select: str = "*"
) -> Optional[Dict[str, Any]]:
    url, key = _get_supabase_credentials()
    params = {"select": select, **filters, "limit": "1"}
    data = _supabase_request("GET", url, key, table, params=params)
    if isinstance(data, list) and data:
        return data[0]
    return None


def _insert_row(table: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url, key = _get_supabase_credentials()
    data = _supabase_request(
        "POST", url, key, table, payload=payload, prefer_return=True
    )
    if isinstance(data, list) and data:
        return data[0]
    return None


def _update_rows(table: str, filters: Dict[str, str], payload: Dict[str, Any]) -> None:
    url, key = _get_supabase_credentials()
    _supabase_request("PATCH", url, key, table, params=filters, payload=payload)


def _generate_license_key(length: int = 20) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _parse_iso(value: str) -> Optional[dt.datetime]:
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _days_left(expires_at: Optional[str]) -> int:
    if not expires_at:
        return 0
    parsed = _parse_iso(expires_at)
    if not parsed:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    now = dt.datetime.now(dt.timezone.utc)
    delta = parsed - now
    return max(0, int(delta.total_seconds() // 86400))


def _is_expired(expires_at: Optional[str]) -> bool:
    if not expires_at:
        return False
    parsed = _parse_iso(expires_at)
    if not parsed:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    now = dt.datetime.now(dt.timezone.utc)
    return parsed < now


def _get_or_create_customer(name: str, email: Optional[str]) -> str:
    if email:
        existing = _select_one("customers", {"email": f"eq.{email}"}, select="id")
        if existing and existing.get("id"):
            return str(existing["id"])

    payload: Dict[str, Any] = {"name": name}
    if email:
        payload["email"] = email
    row = _insert_row("customers", payload)
    if not row or not row.get("id"):
        raise HTTPException(status_code=500, detail="Failed to create customer.")
    return str(row["id"])


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/admin/licenses")
def create_license(
    payload: CreateLicenseIn, x_admin_token: Optional[str] = Header(default=None)
) -> Dict[str, Any]:
    admin_token = os.environ.get("ADMIN_TOKEN", "").strip()
    if not admin_token or x_admin_token != admin_token:
        raise HTTPException(status_code=403, detail="Invalid admin token.")

    issued = dt.datetime.now(dt.timezone.utc)
    expires_at = issued + dt.timedelta(days=payload.days)

    license_key = _generate_license_key()
    license_hash = _hash_license_key(license_key, _license_secret())
    customer_id = _get_or_create_customer(payload.name, payload.email)

    _insert_row(
        "licenses",
        {
            "customer_id": customer_id,
            "license_key_hash": license_hash,
            "is_active": True,
            "expires_at": expires_at.isoformat(),
        },
    )

    return {
        "license_key": license_key,
        "expires_at": expires_at.isoformat(),
        "customer_id": customer_id,
    }


@app.post("/activate")
def activate_license(payload: ActivateIn, request: Request) -> Dict[str, Any]:
    license_key = payload.license_key.strip()
    if not license_key:
        raise HTTPException(status_code=400, detail="License key required.")

    license_hash = _hash_license_key(license_key, _license_secret())
    record = _select_one(
        "licenses",
        {"license_key_hash": f"eq.{license_hash}"},
        select="id,customer_id,is_active,expires_at",
    )
    if not record:
        raise HTTPException(status_code=403, detail="Invalid license.")
    if not record.get("is_active", True):
        raise HTTPException(status_code=403, detail="License inactive.")

    expires_at = record.get("expires_at")
    if _is_expired(expires_at):
        raise HTTPException(status_code=403, detail="License expired.")

    now = dt.datetime.now(dt.timezone.utc).isoformat()
    _update_rows(
        "licenses",
        {"license_key_hash": f"eq.{license_hash}"},
        {"last_seen_at": now},
    )

    _insert_row(
        "license_activations",
        {
            "license_id": record.get("id"),
            "client_fingerprint": payload.client_fingerprint,
            "ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
        },
    )

    return {
        "ok": True,
        "days_left": _days_left(expires_at),
        "customer_id": record.get("customer_id"),
        "expires_at": expires_at,
    }
