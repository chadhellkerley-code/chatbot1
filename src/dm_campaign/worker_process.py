from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any

from src.dm_campaign.proxy_workers_runner import (
    connect_worker_ipc,
    run_proxy_worker_from_config,
    write_worker_manifest,
)


logger = logging.getLogger(__name__)


def _load_worker_config(path_arg: str) -> dict[str, Any]:
    cfg_path = Path(str(path_arg or "").strip()).expanduser()
    if not cfg_path.exists():
        raise FileNotFoundError(f"worker config not found: {cfg_path}")
    payload = json.loads(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("worker config must be a JSON object")
    return dict(payload)


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv)
    if len(args) < 2:
        raise SystemExit("usage: python -m src.dm_campaign.worker_process <config.json>")

    worker_cfg = _load_worker_config(args[1])
    ipc_handles = connect_worker_ipc(worker_cfg)
    event_sink = ipc_handles.get("event_sink")

    def _runtime_event_callback(event: dict[str, Any]) -> None:
        if event_sink is None:
            return
        try:
            event_sink.record_event(dict(event))
        except Exception:
            logger.exception("No se pudo enviar runtime_event desde worker process.")

    write_worker_manifest(worker_cfg)
    run_proxy_worker_from_config(
        worker_cfg,
        scheduler=ipc_handles["scheduler"],
        health_monitor=ipc_handles["health_monitor"],
        stats=ipc_handles["stats"],
        stats_lock=ipc_handles["stats_lock"],
        runtime_event_callback=_runtime_event_callback,
        control_registry=ipc_handles.get("control_registry"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
