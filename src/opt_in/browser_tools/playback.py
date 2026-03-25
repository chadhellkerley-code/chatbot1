from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional

from . import audit
from .browser_manager import BrowserManager
from .config import get_settings
from .utils import (
    ask_hidden,
    click_first,
    random_human_delay,
    sample_delay,
    wait_first_selector,
)


PLACEHOLDER_RE = re.compile(r"\{\{([A-Z0-9_]+)\}\}")


class FlowPlayback:
    def __init__(self, alias: str, variables: Optional[Dict[str, str]] = None, account_alias: Optional[str] = None) -> None:
        self.alias = alias
        self.variables = variables or {}
        self.account_alias = account_alias
        self.settings = get_settings()
        self.document = self._load_document()

    @property
    def path(self) -> Path:
        return self.settings.flows_dir / f"{self.alias}.json"

    def _load_document(self) -> Dict:
        if not self.path.exists():
            raise FileNotFoundError(f"No existe el flujo '{self.alias}'")
        with self.path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    @property
    def steps(self) -> List[Dict]:
        return self.document.get("steps", [])

    def run(self) -> None:
        audit.log_event("playback.start", account=self.account_alias, details={"alias": self.alias})
        with BrowserManager(account_alias=self.account_alias, persist_session=bool(self.account_alias)) as manager:
            page = manager.ensure_page()
            for step in self.steps:
                action = step.get("action")
                if action == "goto":
                    url = self._resolve_template(step.get("url", ""))
                    manager.go(url)
                elif action == "click":
                    selector = step.get("selector")
                    if selector:
                        click_first(page, [selector])
                elif action == "fill":
                    selector = step.get("selector")
                    if selector:
                        value = self._resolve_value(step.get("value", ""))
                        locator = wait_first_selector(page, [selector])
                        locator.click()
                        locator.fill("")
                        for char in value:
                            delay_ms = max(int(sample_delay(self.settings.keyboard_delay) * 1000), 15)
                            locator.type(char, delay=delay_ms)
                        random_human_delay(self.settings.action_delay)
                else:
                    audit.log_event("playback.skip_step", account=self.account_alias, details={"action": action})
            random_human_delay(self.settings.action_delay)
        audit.log_event("playback.end", account=self.account_alias, details={"alias": self.alias})

    def _resolve_template(self, text: str) -> str:
        def repl(match):
            key = match.group(1)
            return self._resolve_placeholder(key)

        return PLACEHOLDER_RE.sub(repl, text)

    def _resolve_value(self, value: str) -> str:
        if not value:
            return ""
        return self._resolve_template(value)

    def _resolve_placeholder(self, key: str) -> str:
        if key in {"SECRET", "CODE"}:
            return ask_hidden(f"Ingrese valor para {key}: ")
        existing = self.variables.get(key)
        if existing is not None:
            return existing
        prompt = input(f"Ingrese valor para {key}: ").strip()
        self.variables[key] = prompt
        return prompt


def cli_play(alias: str, variables: Optional[Dict[str, str]] = None, account: Optional[str] = None) -> None:
    playback = FlowPlayback(alias=alias, variables=variables, account_alias=account or None)
    playback.run()
