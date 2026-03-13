from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException
from pydantic import BaseModel, Field

from .content_extract_service import ContentExtractService
from .content_library_service import ContentPublisherError
from .content_publish_service import ContentPublishService


router = APIRouter(prefix="/content", tags=["content"])
app = FastAPI(title="Content Publisher API", version="1.0.0")


class ContentExtractIn(BaseModel):
    alias: str = ""
    account_ids: list[str] = Field(default_factory=list)
    profile_urls: list[str] = Field(default_factory=list)
    posts_per_profile: int = Field(default=3, ge=1, le=50)
    root_dir: str | None = None


class ContentPublishIn(BaseModel):
    account_id: str = Field(..., min_length=1)
    media_path: str = Field(..., min_length=1)
    caption: str = ""
    root_dir: str | None = None


def _root_dir(value: str | None) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return Path(raw)


@router.post("/extract")
def extract_content(payload: ContentExtractIn) -> dict[str, Any]:
    if not payload.account_ids:
        raise HTTPException(status_code=400, detail="Selecciona al menos una cuenta para extraer contenido.")
    if not payload.profile_urls:
        raise HTTPException(status_code=400, detail="Pega al menos una URL de perfil.")
    service = ContentExtractService(root_dir=_root_dir(payload.root_dir))
    try:
        return service.extract(
            alias=payload.alias,
            account_ids=list(payload.account_ids),
            profile_urls=list(payload.profile_urls),
            posts_per_profile=int(payload.posts_per_profile),
        )
    except ContentPublisherError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/publish")
def publish_content(payload: ContentPublishIn) -> dict[str, Any]:
    service = ContentPublishService(root_dir=_root_dir(payload.root_dir))
    try:
        return service.publish(
            account_id=payload.account_id,
            media_path=payload.media_path,
            caption=payload.caption,
        )
    except ContentPublisherError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


app.include_router(router)
