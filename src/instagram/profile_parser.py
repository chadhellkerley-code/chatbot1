from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


class InstagramPublicParseError(RuntimeError):
    pass


@dataclass(slots=True)
class ProfileSnapshot:
    username: str
    biography: str
    full_name: str
    follower_count: int
    media_count: int
    is_private: bool
    profile_pic_url: str = ""
    user_id: str = ""
    external_url: str = ""
    is_verified: bool = False


def _parse_count(raw: Any, *, default: int = 0) -> int:
    if raw is None:
        return int(default or 0)
    try:
        parsed = int(raw)
    except Exception:
        return int(default or 0)
    if parsed < 0:
        return int(default or 0)
    return parsed


def parse_profile_snapshot(payload: Dict[str, Any], fallback_username: str):
    data = payload.get("data") or {}
    user_data = data.get("user") if isinstance(data, dict) else None
    if not isinstance(user_data, dict):
        return None

    biography = str(user_data.get("biography") or "").strip()
    full_name = str(user_data.get("full_name") or "").strip()

    edge_followed_by = user_data.get("edge_followed_by")
    if not isinstance(edge_followed_by, dict):
        edge_followed_by = {}
    edge_timeline = user_data.get("edge_owner_to_timeline_media")
    if not isinstance(edge_timeline, dict):
        edge_timeline = {}

    follower_count = _parse_count(edge_followed_by.get("count"), default=0)
    media_count = _parse_count(edge_timeline.get("count"), default=0)

    is_private = bool(user_data.get("is_private", False))
    is_verified = bool(user_data.get("is_verified", False))
    profile_pic_url = str(user_data.get("profile_pic_url_hd") or user_data.get("profile_pic_url") or "").strip()
    external_url = str(user_data.get("external_url") or "").strip()
    username = str(user_data.get("username") or fallback_username or "").strip().lstrip("@")
    user_id = str(user_data.get("id") or user_data.get("pk") or "").strip()

    return ProfileSnapshot(
        username=username,
        biography=biography,
        full_name=full_name,
        follower_count=follower_count,
        media_count=media_count,
        is_private=is_private,
        profile_pic_url=profile_pic_url,
        user_id=user_id,
        external_url=external_url,
        is_verified=is_verified,
    )


def profile_snapshot_to_dict(profile: Any) -> Dict[str, Any]:
    return {
        "username": str(getattr(profile, "username", "") or ""),
        "biography": str(getattr(profile, "biography", "") or ""),
        "full_name": str(getattr(profile, "full_name", "") or ""),
        "follower_count": int(getattr(profile, "follower_count", 0) or 0),
        "media_count": int(getattr(profile, "media_count", 0) or 0),
        "is_private": bool(getattr(profile, "is_private", False)),
        "profile_pic_url": str(getattr(profile, "profile_pic_url", "") or ""),
        "user_id": str(getattr(profile, "user_id", "") or ""),
        "external_url": str(getattr(profile, "external_url", "") or ""),
        "is_verified": bool(getattr(profile, "is_verified", False)),
    }


def profile_snapshot_from_dict(payload: Dict[str, Any]):
    return ProfileSnapshot(
        username=str(payload.get("username") or "").strip().lstrip("@"),
        biography=str(payload.get("biography") or "").strip(),
        full_name=str(payload.get("full_name") or "").strip(),
        follower_count=int(payload.get("follower_count") or 0),
        media_count=int(payload.get("media_count") or 0),
        is_private=bool(payload.get("is_private", False)),
        profile_pic_url=str(payload.get("profile_pic_url") or "").strip(),
        user_id=str(payload.get("user_id") or "").strip(),
        external_url=str(payload.get("external_url") or "").strip(),
        is_verified=bool(payload.get("is_verified", False)),
    )
