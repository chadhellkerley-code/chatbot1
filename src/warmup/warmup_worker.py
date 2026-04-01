from __future__ import annotations

from typing import Any, Awaitable, Callable

from .actions import WarmupActionContext, WarmupActionResult
from .actions import warmup_comment_post
from .actions import warmup_follow_accounts
from .actions import warmup_like_posts
from .actions import warmup_reply_story
from .actions import warmup_send_message
from .actions import warmup_watch_reels


ActionHandler = Callable[[WarmupActionContext], Awaitable[WarmupActionResult]]


class WarmupWorker:
    def __init__(self) -> None:
        self._handlers: dict[str, ActionHandler] = {
            "watch_reels": warmup_watch_reels.run,
            "like_posts": warmup_like_posts.run,
            "follow_accounts": warmup_follow_accounts.run,
            "comment_post": warmup_comment_post.run,
            "reply_story": warmup_reply_story.run,
            "send_message": warmup_send_message.run,
        }

    async def execute(self, action_type: str, account: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        clean_type = str(action_type or "").strip().lower()
        handler = self._handlers.get(clean_type)
        if handler is None:
            raise RuntimeError(f"Accion warm up no soportada: {clean_type}")
        try:
            result = await handler(WarmupActionContext(account=account, payload=dict(payload or {}), action_type=clean_type))
        except Exception as exc:
            return {
                "action_type": clean_type,
                "ok": False,
                "performed": 0,
                "message": str(exc) or clean_type,
                "details": [str(exc) or clean_type],
            }
        return {
            "action_type": clean_type,
            "ok": bool(result.ok),
            "performed": int(result.performed or 0),
            "message": str(result.message or "").strip(),
            "details": [str(item or "").strip() for item in result.details if str(item or "").strip()],
        }
