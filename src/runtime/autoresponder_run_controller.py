from __future__ import annotations

import logging
import threading
import uuid
from typing import Any

from src.runtime.autoresponder_run_service import AutoresponderRunService
from src.runtime.run_config import RunConfig


logger = logging.getLogger(__name__)


class AutoresponderRunController:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._service = AutoresponderRunService()

    def start_run(self, config: RunConfig) -> str:
        print("AutoresponderRunController.start_run invoked", flush=True)
        logger.info("RunController.start_run invoked")
        active = self._service.get_active_run()
        if isinstance(active, dict) and str(active.get("run_id") or "").strip():
            active_id = str(active["run_id"])
            print("AutoresponderRunController.start_run: already active run_id=", active_id, flush=True)
            logger.info("Autoresponder run already active (run_id=%s)", active_id)
            return active_id

        run_id = str(uuid.uuid4())
        print("AutoresponderRunController.start_run generated run_id:", run_id, flush=True)
        logger.info("Run created with id %s", run_id)
        with self._lock:
            logger.info("Starting AutoresponderRunService")
            try:
                print("AutoresponderRunController calling AutoresponderRunService.start", flush=True)
                self._service.start(run_id, config)
            except Exception:
                logger.exception("AutoresponderRunService.start failed (run_id=%s)", run_id)
                return ""
        return run_id

    def stop_run(self, run_id: str) -> bool:
        return self._service.stop(str(run_id or "").strip())

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._service.get_run(str(run_id or "").strip())

    def get_active_run(self) -> dict[str, Any] | None:
        return self._service.get_active_run()
