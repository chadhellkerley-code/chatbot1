import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel, EmailStr, Field
from supabase import Client, create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
HASH_SECRET = os.getenv("LICENSE_HASH_SECRET") or SUPABASE_SERVICE_ROLE_KEY

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")

if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN is required.")

if not HASH_SECRET:
    raise RuntimeError("A hash secret is required to protect license keys.")

ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
LICENSE_LENGTH = 20

app = FastAPI(title="License Service", version="0.1.0")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


class LicenseCreateRequest(BaseModel):
    name: str
    days: int = Field(..., ge=1)
    email: Optional[EmailStr] = None


class LicenseCreateResponse(BaseModel):
    license_key: str
    expires_at: str
    customer_id: str


class ActivateRequest(BaseModel):
    license_key: str = Field(..., min_length=8, max_length=64)
    client_fingerprint: Optional[str] = None


class ActivateResponse(BaseModel):
    ok: bool
    days_left: int
    customer_id: str


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def generate_license_key(length: int = LICENSE_LENGTH) -> str:
    return "".join(secrets.choice(ALPHABET) for _ in range(length))


def hash_license_key(license_key: str) -> str:
    payload = f"{HASH_SECRET}:{license_key}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_customer(name: str, email: Optional[str]) -> str:
    if email:
        existing = supabase.table("customers").select("id").eq("email", email).limit(1).execute()
        if getattr(existing, "error", None):
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(existing.error))
        if existing.data:
            return existing.data[0]["id"]

    created = supabase.table("customers").insert({"name": name, "email": email}).execute()
    if getattr(created, "error", None):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(created.error))
    return created.data[0]["id"]


def parse_expires_at(raw_expires_at) -> datetime:
    if isinstance(raw_expires_at, datetime):
        return raw_expires_at if raw_expires_at.tzinfo else raw_expires_at.replace(tzinfo=timezone.utc)
    text_value = str(raw_expires_at)
    if text_value.endswith("Z"):
        text_value = text_value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(text_value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def calculate_days_left(expires_at: datetime, reference: Optional[datetime] = None) -> int:
    reference_time = reference or now_utc()
    delta = expires_at - reference_time
    return max(0, int(delta.total_seconds() // 86400))


def get_admin_token(x_admin_token: str = Header(None, convert_underscores=False)) -> str:
    if not x_admin_token or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="forbidden")
    return x_admin_token


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/admin/licenses", response_model=LicenseCreateResponse)
def create_license(
    payload: LicenseCreateRequest,
    _: str = Depends(get_admin_token),
) -> LicenseCreateResponse:
    if payload.days < 30:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="days must be at least 30",
        )

    customer_id = ensure_customer(payload.name, payload.email)
    expires_at = now_utc() + timedelta(days=payload.days)

    license_key = generate_license_key()
    hashed_key = hash_license_key(license_key)

    result = supabase.table("licenses").insert(
        {
            "customer_id": customer_id,
            "license_key_hash": hashed_key,
            "expires_at": expires_at.isoformat(),
        }
    ).execute()

    if getattr(result, "error", None):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(result.error))

    return LicenseCreateResponse(
        license_key=license_key,
        expires_at=expires_at.isoformat(),
        customer_id=customer_id,
    )


@app.post("/activate", response_model=ActivateResponse)
def activate(payload: ActivateRequest, request: Request) -> ActivateResponse:
    hashed_key = hash_license_key(payload.license_key)

    lookup = (
        supabase.table("licenses")
        .select("id, customer_id, is_active, expires_at")
        .eq("license_key_hash", hashed_key)
        .limit(1)
        .execute()
    )
    if getattr(lookup, "error", None):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(lookup.error))

    if not lookup.data:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="invalid license")

    license_row = lookup.data[0]
    expires_at = parse_expires_at(license_row["expires_at"])
    current_time = now_utc()

    if not license_row.get("is_active", True) or expires_at <= current_time:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="license expired or inactive")

    updated = (
        supabase.table("licenses")
        .update({"last_seen_at": current_time.isoformat()})
        .eq("id", license_row["id"])
        .execute()
    )
    if getattr(updated, "error", None):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(updated.error))

    activation = supabase.table("license_activations").insert(
        {
            "license_id": license_row["id"],
            "client_fingerprint": payload.client_fingerprint,
            "ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
        }
    ).execute()
    if getattr(activation, "error", None):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(activation.error))

    return ActivateResponse(
        ok=True,
        days_left=calculate_days_left(expires_at, current_time),
        customer_id=license_row["customer_id"],
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
