from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List

from playwright.sync_api import Page

from . import audit
from .browser_manager import BrowserManager
from .config import get_settings
from .utils import random_human_delay, sanitize_placeholder


RECORDER_SCRIPT = """
(() => {
  if (window.__optInRecorderInstalled) {
    return;
  }
  window.__optInRecorderInstalled = true;
  const toSelector = (element) => {
    if (!element || element.nodeType !== 1) {
      return '';
    }
    if (element.id) {
      return `#${element.id}`;
    }
    const parts = [];
    let el = element;
    while (el && el.nodeType === 1) {
      let part = el.nodeName.toLowerCase();
      if (el.classList && el.classList.length) {
        part += '.' + Array.from(el.classList).slice(0, 3).join('.');
      }
      const parent = el.parentElement;
      if (parent) {
        const siblings = Array.from(parent.children).filter(child => child.nodeName === el.nodeName);
        if (siblings.length > 1) {
          const index = siblings.indexOf(el) + 1;
          part += `:nth-of-type(${index})`;
        }
      }
      parts.unshift(part);
      el = parent;
    }
    return parts.join(' > ');
  };

  const send = (payload) => {
    try {
      window.optInRecord(payload);
    } catch (error) {
      console.warn('optInRecord failed', error);
    }
  };

  document.addEventListener('click', (event) => {
    send({
      action: 'click',
      selector: toSelector(event.target),
    });
  }, true);

  const sendInput = (event) => {
    const target = event.target;
    const selector = toSelector(target);
    let value = '';
    if ('value' in target) {
      value = target.value || '';
    } else if (target.textContent) {
      value = target.textContent;
    }
    if (value.length > 160) {
      value = value.slice(0, 160);
    }
    send({
      action: 'fill',
      selector,
      value,
      inputType: target.type || '',
      name: target.name || '',
    });
  };

  document.addEventListener('input', (event) => {
    const target = event.target;
    const type = (target.type || '').toLowerCase();
    if (type === 'password') {
      send({
        action: 'fill',
        selector: toSelector(target),
        value: '{{SECRET}}',
        inputType: 'password',
        name: target.name || '',
      });
      return;
    }
    sendInput(event);
  }, true);

  document.addEventListener('change', sendInput, true);
})();
"""


class FlowRecorder:
    def __init__(self, alias: str) -> None:
        self.alias = alias
        self.settings = get_settings()
        self.steps: List[Dict[str, Any]] = []
        self._last: Dict[str, Any] | None = None

    @property
    def output_path(self) -> Path:
        return self.settings.flows_dir / f"{self.alias}.json"

    def ensure_not_exists(self) -> None:
        if self.output_path.exists():
            raise FileExistsError(f"El flujo '{self.alias}' ya existe")

    def record(self) -> None:
        self.ensure_not_exists()
        audit.log_event("recorder.start", account=None, details={"alias": self.alias})

        with BrowserManager(account_alias=None, persist_session=False) as manager:
            page = manager.ensure_page()
            self._wire(page)
            page.goto("https://www.instagram.com/", wait_until="domcontentloaded")
            random_human_delay(self.settings.action_delay)
            print("[OPT-IN] Se inició la grabación. Realiza las acciones manualmente en la ventana.")
            print("[OPT-IN] Presiona Enter aquí cuando desees finalizar...")
            input()
            manager.wait_idle(1.0)

        self._save()
        audit.log_event("recorder.end", account=None, details={"alias": self.alias, "steps": len(self.steps)})

    def _wire(self, page: Page) -> None:
        page.expose_binding("optInRecord", lambda source, data: self._handle_event(data), handle=True)
        page.add_init_script(RECORDER_SCRIPT)
        page.on("framenavigated", self._nav_handler)

    def _nav_handler(self, frame) -> None:
        page = frame.page
        if page and frame == page.main_frame:
            self._push({"action": "goto", "url": frame.url})

    def _handle_event(self, data: Dict[str, Any]) -> None:
        action = data.get("action")
        selector = data.get("selector", "")
        step: Dict[str, Any] = {"action": action, "selector": selector}
        if action == "fill":
            raw_value = data.get("value", "")
            sanitized = sanitize_placeholder(str(raw_value))
            step["value"] = sanitized
        self._push(step)

    def _push(self, step: Dict[str, Any]) -> None:
        if not step.get("action"):
            return
        if self._last and all(step.get(key) == self._last.get(key) for key in ("action", "selector", "value")):
            return
        enriched = dict(step)
        enriched["ts"] = time.time()
        self.steps.append(enriched)
        self._last = enriched

    def _save(self) -> None:
        document = {
            "version": 1,
            "alias": self.alias,
            "recorded_at": time.time(),
            "steps": self.steps,
        }
        with self.output_path.open("w", encoding="utf-8") as handle:
            json.dump(document, handle, ensure_ascii=True, indent=2)


def cli_record(alias: str) -> None:
    recorder = FlowRecorder(alias)
    recorder.record()
    print(f"[OPT-IN] Flujo grabado en {recorder.output_path}")
