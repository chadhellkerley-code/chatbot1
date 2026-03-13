# -*- coding: utf-8 -*-
"""FastAPI backend for license creation and activation (Postgres)."""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
import secrets
import string
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field
from src.content_publisher.content_api import router as content_router

load_dotenv()

app = FastAPI(title="License Backend", version="1.0.0")
app.include_router(content_router)

logger = logging.getLogger(__name__)


class CreateLicenseIn(BaseModel):
    name: str = Field(..., min_length=1)
    days: int = Field(..., ge=30)
    email: Optional[str] = None


class ActivateIn(BaseModel):
    license_key: str = Field(..., min_length=1)
    client_fingerprint: Optional[str] = None


SCHEMA_SQL = [
    """
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS customers (
        id uuid primary key default gen_random_uuid(),
        name text not null,
        email text null,
        created_at timestamptz not null default now(),
        constraint customers_email_unique unique (email)
    );
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS licenses (
        id uuid primary key default gen_random_uuid(),
        customer_id uuid not null references customers (id) on delete cascade,
        license_key_hash text not null unique,
        is_active boolean not null default true,
        created_at timestamptz not null default now(),
        expires_at timestamptz not null,
        last_seen_at timestamptz null,
        notes text null
    );
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS license_activations (
        id uuid primary key default gen_random_uuid(),
        license_id uuid not null references licenses (id) on delete cascade,
        activated_at timestamptz not null default now(),
        client_fingerprint text null,
        ip text null,
        user_agent text null
    );
    """.strip(),
]


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise HTTPException(status_code=500, detail="Database not configured.")
    return url


def _get_conn():
    try:
        import psycopg2  # type: ignore
    except Exception as exc:
        raise HTTPException(status_code=500, detail="psycopg2 is not installed.") from exc
    try:
        return psycopg2.connect(_database_url())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _ensure_schema() -> None:
    conn = _get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for stmt in SCHEMA_SQL:
                cur.execute(stmt)
    finally:
        conn.close()


def _license_secret() -> str:
    secret = os.environ.get("LICENSE_HASH_SECRET", "").strip()
    if secret:
        return secret
    admin_token = os.environ.get("ADMIN_TOKEN", "").strip()
    if admin_token:
        return admin_token
    raise HTTPException(status_code=500, detail="LICENSE_HASH_SECRET missing.")


def _hash_license_key(license_key: str, secret: str) -> str:
    payload = f"{secret}:{license_key}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _generate_license_key(length: int = 20) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _parse_iso(value: str) -> Optional[dt.datetime]:
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


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


def _fetchone(cur) -> Optional[Dict[str, Any]]:
    row = cur.fetchone()
    if not row:
        return None
    columns = [desc[0] for desc in cur.description]
    return dict(zip(columns, row))


def _get_or_create_customer(conn, name: str, email: Optional[str]) -> str:
    with conn.cursor() as cur:
        if email:
            cur.execute("SELECT id FROM customers WHERE email = %s LIMIT 1", (email,))
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
        cur.execute("INSERT INTO customers (name, email) VALUES (%s, %s) RETURNING id", (name, email))
        new_id = cur.fetchone()[0]
        return str(new_id)


@app.on_event("startup")
def _startup() -> None:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        logger.warning("DATABASE_URL missing; skipping license schema startup.")
        return
    try:
        _ensure_schema()
    except HTTPException as exc:
        logger.warning("Skipping license schema startup: %s", exc.detail)


@app.get("/health")
def health() -> Dict[str, str]:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()
    return {"status": "ok", "backend": "postgres", "version": "no-supabase"}


@app.post("/admin/licenses")
def create_license(payload: CreateLicenseIn, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    admin_token = os.environ.get("ADMIN_TOKEN", "").strip()
    if not admin_token or x_admin_token != admin_token:
        raise HTTPException(status_code=403, detail="Invalid admin token.")

    issued = dt.datetime.now(dt.timezone.utc)
    expires_at = issued + dt.timedelta(days=payload.days)
    license_key = _generate_license_key()
    license_hash = _hash_license_key(license_key, _license_secret())

    conn = _get_conn()
    try:
        with conn:
            customer_id = _get_or_create_customer(conn, payload.name, payload.email)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO licenses (customer_id, license_key_hash, is_active, expires_at)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (customer_id, license_hash, True, expires_at.isoformat()),
                )
                cur.fetchone()
    finally:
        conn.close()

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

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, customer_id, is_active, expires_at
                    FROM licenses
                    WHERE license_key_hash = %s
                    LIMIT 1
                    """,
                    (license_hash,),
                )
                record = _fetchone(cur)
                if not record:
                    raise HTTPException(status_code=403, detail="Invalid license.")
                if not record.get("is_active", True):
                    raise HTTPException(status_code=403, detail="License inactive.")
                expires_at = record.get("expires_at")
                if _is_expired(expires_at):
                    raise HTTPException(status_code=403, detail="License expired.")

                cur.execute("UPDATE licenses SET last_seen_at = %s WHERE id = %s", (dt.datetime.now(dt.timezone.utc), record.get("id")))
                cur.execute(
                    """
                    INSERT INTO license_activations (license_id, client_fingerprint, ip, user_agent)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        record.get("id"),
                        payload.client_fingerprint,
                        request.client.host if request.client else None,
                        request.headers.get("user-agent"),
                    ),
                )

                return {
                    "ok": True,
                    "days_left": _days_left(expires_at),
                    "customer_id": record.get("customer_id"),
                    "expires_at": expires_at,
                }
    finally:
        conn.close()
