from __future__ import annotations

from typing import Any, Callable

from .warmup_scheduler import WarmupCursor, WarmupScheduler
from .warmup_worker import WarmupWorker


ProgressCallback = Callable[[dict[str, Any]], None]
StateCallback = Callable[[str, int, int, str, str], None]
StopCallback = Callable[[], bool]


class WarmupEngine:
    def __init__(
        self,
        *,
        scheduler: WarmupScheduler | None = None,
        worker: WarmupWorker | None = None,
        progress_callback: ProgressCallback | None = None,
        state_callback: StateCallback | None = None,
        stop_callback: StopCallback | None = None,
    ) -> None:
        self._scheduler = scheduler or WarmupScheduler()
        self._worker = worker or WarmupWorker()
        self._progress_callback = progress_callback
        self._state_callback = state_callback
        self._stop_callback = stop_callback

    async def run_flow(
        self,
        flow: dict[str, Any],
        accounts: list[dict[str, Any]],
        *,
        cursor_by_account: dict[str, WarmupCursor] | None = None,
    ) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        for account in accounts:
            username = str(account.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            cursor = (cursor_by_account or {}).get(username) or WarmupCursor()
            summaries.append(await self.run_account(flow, account, cursor=cursor))
        return summaries

    async def run_account(
        self,
        flow: dict[str, Any],
        account: dict[str, Any],
        *,
        cursor: WarmupCursor | None = None,
    ) -> dict[str, Any]:
        username = str(account.get("username") or "").strip().lstrip("@")
        results: list[dict[str, Any]] = []
        for stage, action in self._scheduler.iter_actions(flow, cursor=cursor):
            if callable(self._stop_callback) and self._stop_callback():
                break
            stage_order = max(1, int(stage.get("stage_order") or 1))
            action_order = max(1, int(action.get("action_order") or 1))
            action_type = str(action.get("action_type") or "").strip().lower()
            if callable(self._state_callback):
                self._state_callback(username, stage_order, action_order, action_type, "running")
            outcome = await self._worker.execute(action_type, account, dict(action.get("payload") or {}))
            results.append(
                {
                    "stage_order": stage_order,
                    "stage_title": str(stage.get("title") or "").strip(),
                    "action_order": action_order,
                    **outcome,
                }
            )
            if callable(self._progress_callback):
                self._progress_callback({"username": username, **results[-1]})
            if callable(self._state_callback):
                self._state_callback(username, stage_order, action_order, action_type, "paused")
            if not bool(outcome.get("ok", True)):
                break
        return {"username": username, "results": results}
