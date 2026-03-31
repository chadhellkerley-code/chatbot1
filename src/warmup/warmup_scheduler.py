from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator


@dataclass(frozen=True)
class WarmupCursor:
    stage_order: int = 1
    action_order: int = 1


class WarmupScheduler:
    def iter_actions(self, flow: dict[str, Any], *, cursor: WarmupCursor | None = None) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
        stages = [dict(item) for item in flow.get("stages") or [] if isinstance(item, dict)]
        current = cursor or WarmupCursor()
        for stage in stages:
            if not bool(stage.get("enabled", True)):
                continue
            stage_order = int(stage.get("stage_order") or 0)
            if stage_order < current.stage_order:
                continue
            actions = [dict(item) for item in stage.get("actions") or [] if isinstance(item, dict)]
            for action in actions:
                action_order = int(action.get("action_order") or 0)
                if stage_order == current.stage_order and action_order < current.action_order:
                    continue
                yield stage, action

    def advance_cursor(
        self,
        flow: dict[str, Any],
        *,
        stage_order: int,
        action_order: int,
    ) -> WarmupCursor:
        stages = [dict(item) for item in flow.get("stages") or [] if isinstance(item, dict) and bool(item.get("enabled", True))]
        found_current = False
        last_stage_order = max(1, int(stage_order or 1))
        last_action_order = max(1, int(action_order or 1))
        for stage in stages:
            current_stage_order = max(1, int(stage.get("stage_order") or 1))
            actions = [dict(item) for item in stage.get("actions") or [] if isinstance(item, dict)]
            if not actions:
                continue
            for action in actions:
                current_action_order = max(1, int(action.get("action_order") or 1))
                last_stage_order = current_stage_order
                last_action_order = current_action_order
                if found_current:
                    return WarmupCursor(
                        stage_order=current_stage_order,
                        action_order=current_action_order,
                    )
                if current_stage_order == max(1, int(stage_order or 1)) and current_action_order == max(1, int(action_order or 1)):
                    found_current = True
        if found_current:
            return WarmupCursor(
                stage_order=last_stage_order,
                action_order=last_action_order + 1,
            )
        return WarmupCursor(
            stage_order=max(1, int(stage_order or 1)),
            action_order=max(1, int(action_order or 1)),
        )
