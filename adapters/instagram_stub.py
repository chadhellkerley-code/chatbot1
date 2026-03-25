"""Mock Instagram client that records operations instead of performing automation."""
from __future__ import annotations

import random
import time
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .base import BaseInstagramClient


class InstagramStubClient(BaseInstagramClient):
    """Stub client that behaves like a humanized automation backend."""

    def __init__(self, *, account: Optional[dict] = None) -> None:
        super().__init__(account=account)
        self.user_id: str = str(self.account.get("user_id") or random.randint(10**6, 10**7 - 1))
        self._threads: Dict[str, List[SimpleNamespace]] = {}
        self._thread_participants: Dict[str, List[str]] = {}
        self._followers: Dict[str, List[str]] = {}
        self._following: Dict[str, List[str]] = {}
        self._highlights: Dict[str, List[SimpleNamespace]] = {}
        self._medias: Dict[str, SimpleNamespace] = {}

    # ------------------------------------------------------------------ #
    # Required abstract implementations
    # ------------------------------------------------------------------ #
    def login(
        self,
        username: str,
        password: str,
        *,
        verification_code: Optional[str] = None,
    ) -> bool:
        # Password/code are ignored intentionally; we simply mark the session active.
        self._mark_logged_in(username)
        self._record_event(
            "login",
            {
                "username": username,
                "password_len": len(password),
                "verification_code": "***" if verification_code else None,
            },
        )
        return True

    def send_direct_message(self, target_username: str, message: str) -> bool:
        thread_id = self._ensure_thread(target_username)
        message_obj = SimpleNamespace(
            id=self._build_id("msg"),
            user_id=self.user_id,
            text=message,
            timestamp=time.time(),
        )
        self._threads.setdefault(thread_id, []).append(message_obj)
        self._record_event("send_dm", {"thread": thread_id, "target": target_username, "text": message})
        return True

    def reply_to_unread(self, *, limit: int = 10, strategy: Optional[dict] = None) -> List[Dict[str, Any]]:
        replies: List[Dict[str, Any]] = []
        for thread_id, messages in list(self._threads.items())[:limit]:
            payload = {
                "thread_id": thread_id,
                "reply": "Gracias por tu mensaje. Te responderemos pronto.",
                "strategy": strategy or {},
            }
            replies.append(payload)
            self._record_event("reply_unread", payload)
        return replies

    def follow_user(self, username: str) -> bool:
        normalized = username.strip().lstrip("@") or self._random_username()
        self._following.setdefault(self._username or "me", [])
        if normalized not in self._following[self._username or "me"]:
            self._following[self._username or "me"].append(normalized)
        self._record_event("follow", {"target": normalized})
        return True

    def like_post(self, url_or_code: str) -> bool:
        self._record_event("like", {"target": url_or_code})
        return True

    def comment_post(self, url_or_code: str, text: str) -> bool:
        self._record_event("comment", {"target": url_or_code, "text": text})
        return True

    def watch_reel(self, identifier: str) -> bool:
        self._record_event("watch_reel", {"target": identifier})
        return True

    # ------------------------------------------------------------------ #
    # Helpers used throughout the codebase
    # ------------------------------------------------------------------ #
    def ensure_logged_in(self) -> bool:
        # Override to automatically mark the session active if credentials exist.
        if not self._logged_in and self._username:
            self._logged_in = True
        return super().ensure_logged_in()

    def request_two_factor_code(self, channel: str) -> Dict[str, Any]:
        payload = super().request_two_factor_code(channel)
        payload["info"] = "stub"
        return payload

    def resend_two_factor_code(self, channel: str) -> Dict[str, Any]:
        payload = super().resend_two_factor_code(channel)
        payload["info"] = "stub"
        return payload

    def submit_two_factor_code(self, code: str) -> Dict[str, Any]:
        payload = super().submit_two_factor_code(code)
        payload["accepted"] = True
        return payload

    # ------------------------------------------------------------------ #
    # Compatibility methods that mimic instagrapi entry points
    # ------------------------------------------------------------------ #
    def direct_threads(self, amount: int = 10, selected_filter: Optional[str] = None) -> List[SimpleNamespace]:
        self._record_event("direct_threads", {"amount": amount, "filter": selected_filter})
        threads: List[SimpleNamespace] = []
        items = list(self._threads.items())[:amount] or [("thread_stub", [])]
        for thread_id, messages in items:
            participants = self._participants_for_thread(thread_id)
            thread = SimpleNamespace(
                id=thread_id,
                users=[SimpleNamespace(username=user, pk=self._build_id("user")) for user in participants],
                items=list(reversed(messages)),
                last_activity_at=messages[-1].timestamp if messages else time.time(),
            )
            threads.append(thread)
        return threads

    def direct_pending_inbox(self, amount: int = 10) -> List[SimpleNamespace]:
        return self.direct_threads(amount=amount, selected_filter="pending")

    def direct_messages(self, thread_id: str, amount: int = 10) -> List[SimpleNamespace]:
        return list(self._threads.get(thread_id, []))[-amount:]

    def direct_send(self, text: str, recipients: Sequence[str]) -> bool:
        targets = list(recipients)
        if not targets:
            return False
        for username in targets:
            self.send_direct_message(username, text)
        self._record_event("direct_send", {"targets": targets, "text": text})
        return True

    def account_info(self) -> SimpleNamespace:
        return SimpleNamespace(username=self._username or "stub_user", pk=self.user_id)

    def user_info(self, user_id: str) -> SimpleNamespace:
        return SimpleNamespace(
            pk=user_id,
            username=f"user_{user_id}",
            full_name=f"User {user_id}",
            follower_count=len(self._followers.get(user_id, [])),
            following_count=len(self._following.get(user_id, [])),
        )

    def user_info_by_username(self, username: str) -> SimpleNamespace:
        pk = self.user_id_from_username(username)
        return self.user_info(pk)

    def user_info_by_username_v1(self, username: str) -> SimpleNamespace:
        return self.user_info_by_username(username)

    def user_info_gql(self, username: str) -> SimpleNamespace:
        return self.user_info_by_username(username)

    def user_info_by_username_gql(self, username: str) -> SimpleNamespace:
        return self.user_info_by_username(username)

    def user_id_from_username(self, username: str) -> str:
        normalized = username.strip().lower()
        if not normalized:
            return self.user_id
        return str(abs(hash(normalized)) % 10**9)

    def user_followers(self, user_id: str, amount: int = 0) -> List[SimpleNamespace]:
        followers = self._followers.get(user_id, [])
        data = followers[: amount or len(followers)] or [self._random_username()]
        return [SimpleNamespace(pk=self.user_id_from_username(name), username=name) for name in data]

    def user_following(self, user_id: str, amount: int = 0) -> List[SimpleNamespace]:
        following = self._following.get(user_id, [])
        data = following[: amount or len(following)] or [self._random_username()]
        return [SimpleNamespace(pk=self.user_id_from_username(name), username=name) for name in data]

    def hashtag_medias_recent(self, hashtag: str, amount: int = 10) -> List[SimpleNamespace]:
        medias = []
        for _ in range(amount):
            media = SimpleNamespace(pk=self._build_id("media"), code=self._build_id("code"), caption_text=f"#{hashtag}")
            medias.append(media)
        return medias

    def media_like(self, media_pk: str) -> bool:
        self._record_event("media_like", {"media_pk": media_pk})
        return True

    def user_follow(self, user_id: str) -> bool:
        username = f"user_{user_id}"
        return self.follow_user(username)

    def media_pk_from_url(self, url: str) -> str:
        return self._build_id("media")

    def media_pk_from_code(self, code: str) -> str:
        return self._build_id("media")

    def story_pk_from_url(self, url: str) -> str:
        return self._build_id("story")

    def user_highlights(self, user_id: str) -> List[SimpleNamespace]:
        highlights = self._highlights.setdefault(user_id, [])
        if not highlights:
            highlights.append(SimpleNamespace(id=self._build_id("highlight"), title="Highlights"))
        return highlights

    def highlight_delete(self, highlight_id: str) -> bool:
        for user_id, items in self._highlights.items():
            remaining = [item for item in items if getattr(item, "id", "") != highlight_id]
            self._highlights[user_id] = remaining
        self._record_event("highlight_delete", {"highlight_id": highlight_id})
        return True

    def user_medias(self, user_id: str, amount: int = 0) -> List[SimpleNamespace]:
        media_list = list(self._medias.values()) or [SimpleNamespace(id=self._build_id("media"))]
        return media_list[: amount or len(media_list)]

    def media_delete(self, media_id: str) -> bool:
        self._medias = {key: value for key, value in self._medias.items() if getattr(value, "id", "") != media_id}
        self._record_event("media_delete", {"media_id": media_id})
        return True

    def account_edit(self, **kwargs) -> SimpleNamespace:
        self.account.update(kwargs)
        username = kwargs.get("username", self._username or "stub_user")
        if "username" in kwargs:
            self._mark_logged_in(username)
        self._record_event("account_edit", kwargs)
        return SimpleNamespace(username=username, **kwargs)

    def account_set_biography(self, biography: str) -> Dict[str, Any]:
        self.account["biography"] = biography
        self._record_event("account_set_bio", {"biography": biography})
        return {"status": "ok", "biography": biography}

    def account_change_picture(self, image_path: str) -> Dict[str, Any]:
        self.account["picture"] = image_path
        self._record_event("account_change_picture", {"image_path": image_path})
        return {"status": "ok", "image_path": image_path}

    def private_request(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"endpoint": endpoint, "data": data or {}}
        self._record_event("private_request", payload)
        return {"status": "ok", **payload}

    def with_default_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        scoped = {"_csrftoken": self._build_id("token")}
        scoped.update(data)
        return scoped

    def user_followers_gql(self, user_id: str, amount: int = 0) -> List[SimpleNamespace]:
        return self.user_followers(user_id, amount)

    def user_following_gql(self, user_id: str, amount: int = 0) -> List[SimpleNamespace]:
        return self.user_following(user_id, amount)

    # Upload placeholders ---------------------------------------------------
    def video_upload_to_story(self, path: str, **kwargs) -> SimpleNamespace:
        return self._register_media("story_video", path, kwargs)

    def photo_upload_to_story(self, path: str, **kwargs) -> SimpleNamespace:
        return self._register_media("story_photo", path, kwargs)

    def album_upload(self, paths: Sequence[str], caption: str = "", **kwargs) -> SimpleNamespace:
        return self._register_media("album", list(paths), {"caption": caption, **kwargs})

    def video_upload(self, path: str, caption: str = "", **kwargs) -> SimpleNamespace:
        return self._register_media("video", path, {"caption": caption, **kwargs})

    def photo_upload(self, path: str, caption: str = "", **kwargs) -> SimpleNamespace:
        return self._register_media("photo", path, {"caption": caption, **kwargs})

    def clip_upload(self, path: str, caption: str = "", **kwargs) -> SimpleNamespace:
        return self._register_media("clip", path, {"caption": caption, **kwargs})

    def media_comment(self, media_id: str, text: str) -> Dict[str, Any]:
        self._record_event("media_comment", {"media_id": media_id, "text": text})
        return {"status": "ok", "id": self._build_id("comment")}

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _ensure_thread(self, username: str) -> str:
        normalized = username.strip().lstrip("@") or self._random_username()
        thread_id = f"thread_{normalized}"
        self._thread_participants.setdefault(thread_id, [normalized])
        self._threads.setdefault(thread_id, [])
        return thread_id

    def _participants_for_thread(self, thread_id: str) -> List[str]:
        participants = set(self._thread_participants.get(thread_id, []))
        if self._username:
            participants.add(self._username)
        return sorted(participants)

    def _register_media(self, kind: str, payload: Any, extra: Optional[Dict[str, Any]]) -> SimpleNamespace:
        media_id = self._build_id(kind)
        media = SimpleNamespace(id=media_id, kind=kind, payload=payload, meta=extra or {})
        self._medias[media_id] = media
        self._record_event("media_upload", {"kind": kind, "payload": str(payload), "meta": extra or {}})
        return media
