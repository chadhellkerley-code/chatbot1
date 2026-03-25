from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from core import responder as responder_module
from runtime.runtime import EngineCancellationToken, bind_stop_token, request_stop, restore_stop_token
from src.runtime.account_runtime_lease import AccountRuntimeLeaseManager
from src.runtime.run_config import RunConfig

logger = logging.getLogger(__name__)


_ACTIVE_RUN_STATUSES = {"starting", "running", "stopping"}
_TERMINAL_RUN_STATUSES = {"stopped", "failed", "completed"}


class AutoresponderRunService:
    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def start(self, run_id: str, config: RunConfig) -> None:
        print("AutoresponderRunService.start entered (run_id=%s)" % (run_id,), flush=True)
        logger.info("AutoresponderRunService.start() entered (run_id=%s)", run_id)
        if isinstance(config, RunConfig):
            print("Run accounts:", list(config.accounts or []), flush=True)
            logger.info("Run accounts: %s", list(config.accounts or []))
            logger.info("Threads limit: %s", config.threads_limit)
        token = EngineCancellationToken(f"autoresponder:{run_id}")
        thread = threading.Thread(
            target=self._run_thread,
            args=(run_id,),
            daemon=True,
        )

        with self._lock:
            self._runs[run_id] = {
                "run_id": run_id,
                "status": "starting",
                "config": config,
                "thread": thread,
                "token": token,
                "created_at": time.time(),
                "error": "",
            }

        thread.start()
        print("Autoresponder run thread started (run_id=%s)" % (run_id,), flush=True)
        logger.info("Autoresponder run thread started (run_id=%s)", run_id)

    def stop(self, run_id: str) -> bool:
        token: EngineCancellationToken | None = None
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return False
            if str(run.get("status") or "") in _TERMINAL_RUN_STATUSES:
                return True
            run["status"] = "stopping"
            token = run.get("token")

        try:
            if token is not None:
                request_stop("stop_run requested", token=token)
        except Exception:
            pass

        return True

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return None
            config = run.get("config")
            payload = {
                "run_id": run.get("run_id"),
                "status": run.get("status"),
                "config": config.to_dict() if isinstance(config, RunConfig) else dict(config or {}),
            }
            if run.get("error"):
                payload["error"] = str(run.get("error") or "").strip()
            return payload

    def get_active_run(self) -> dict[str, Any] | None:
        with self._lock:
            candidates = [
                run
                for run in self._runs.values()
                if str(run.get("status") or "") in _ACTIVE_RUN_STATUSES
            ]
            if not candidates:
                return None
            active = max(candidates, key=lambda item: float(item.get("created_at") or 0.0))
            config = active.get("config")
            payload = {
                "run_id": active.get("run_id"),
                "status": active.get("status"),
                "config": config.to_dict() if isinstance(config, RunConfig) else dict(config or {}),
            }
            if active.get("error"):
                payload["error"] = str(active.get("error") or "").strip()
            return payload

    def _run_thread(self, run_id: str) -> None:
        print("AutoresponderRunService._run_thread entered (run_id=%s)" % (run_id,), flush=True)
        logger.info("Autoresponder run thread entered (run_id=%s)", run_id)
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                print("Run thread started but run_id not registered (run_id=%s)" % (run_id,), flush=True)
                logger.error("Run thread started but run_id not registered (run_id=%s)", run_id)
                return
            config = run.get("config")
            token = run.get("token")

        if not isinstance(config, RunConfig) or not isinstance(token, EngineCancellationToken):
            print(
                "Invalid run config/token (run_id=%s config_type=%s token_type=%s)"
                % (run_id, type(config).__name__, type(token).__name__),
                flush=True,
            )
            logger.error("Invalid run config/token (run_id=%s config_type=%s token_type=%s)", run_id, type(config).__name__, type(token).__name__)
            self._set_status(run_id, "failed", error="invalid_run_config_or_token")
            return

        previous = bind_stop_token(token)
        try:
            accounts = list(config.accounts or [])
            if not accounts:
                print("No accounts requested (run_id=%s)" % (run_id,), flush=True)
                logger.error("No accounts requested (run_id=%s)", run_id)
                self._set_status(run_id, "failed", error="no_accounts_requested")
                return

            print("Entering account processing loop (run_id=%s)" % (run_id,), flush=True)
            logger.info("Run accounts: %s", accounts)
            logger.info("Threads limit: %s", config.threads_limit)
            stop_callback: Callable[[], bool] = lambda: self._is_stopping(run_id) or token.is_cancelled()
            first_cycle = True
            run_started = False

            logger.info("Entering account processing loop")
            while not self._is_stopping(run_id) and not token.is_cancelled():
                leased_any = False
                ran_any = False
                activation_blocked_reason = ""

                for account in accounts:
                    is_stopping = self._is_stopping(run_id)
                    is_cancelled = token.is_cancelled()
                    if is_stopping or is_cancelled:
                        logger.info(
                            "Cancellation token state checked (run_id=%s stopping=%s cancelled=%s)",
                            run_id,
                            is_stopping,
                            is_cancelled,
                        )
                        break

                    try:
                        with AccountRuntimeLeaseManager.lease(account):
                            leased_any = True
                            if not run_started:
                                run_started = True
                                self._set_status(run_id, "running")

                            print("Processing account:", account, flush=True)
                            logger.info("Processing account %s", account)
                            print("Calling responder engine", flush=True)
                            logger.info("Calling core.responder.run_autoresponder_service")
                            result = responder_module.run_autoresponder_service(
                                [account],
                                config.alias,
                                config.threads_limit,
                                config.delay_min,
                                config.delay_max,
                                False,
                                stop_callback=stop_callback,
                            )
                            logger.info("Finished account %s", account)
                            print("Finished account:", account, flush=True)
                    except RuntimeError:
                        logger.info("Account runtime lease unavailable (account=%s)", account)
                        continue

                    result_status = str((result or {}).get("status") or "").strip().lower()
                    if self._is_stopping(run_id) or token.is_cancelled() or result_status == "stopped":
                        logger.info("Run stopping after account result (run_id=%s status=%s)", run_id, result_status or "stopped")
                        self._set_status(run_id, "stopped")
                        return

                    if result_status in {"failed"}:
                        reason = str((result or {}).get("reason") or "").strip() or result_status
                        logger.error("Autoresponder failed (run_id=%s reason=%s)", run_id, reason)
                        self._set_status(run_id, "failed", error=reason)
                        return

                    if result_status == "activation_blocked":
                        activation_blocked_reason = str((result or {}).get("reason") or "").strip() or "activation_blocked"
                        continue
                    ran_any = True

                if self._is_stopping(run_id) or token.is_cancelled():
                    break

                if not leased_any:
                    if first_cycle:
                        logger.error("No accounts leased on first cycle (run_id=%s)", run_id)
                        self._set_status(run_id, "failed", error="no_accounts_leased")
                        return
                    time.sleep(1.0)
                elif first_cycle and not ran_any:
                    logger.error("No accounts ran on first cycle (run_id=%s reason=%s)", run_id, activation_blocked_reason or "activation_blocked")
                    self._set_status(run_id, "failed", error=activation_blocked_reason or "activation_blocked")
                    return

                logger.info("Autoresponder cycle completed (run_id=%s leased_any=%s ran_any=%s)", run_id, leased_any, ran_any)
                first_cycle = False

                if not bool(config.continuous):
                    break

            if self._is_stopping(run_id) or token.is_cancelled():
                self._set_status(run_id, "stopped")
            else:
                self._set_status(run_id, "completed")

        except Exception as exc:
            logger.exception("Run thread crashed (run_id=%s)", run_id)
            self._set_status(run_id, "failed", error=str(exc) or exc.__class__.__name__)
        finally:
            restore_stop_token(previous)

    def _is_stopping(self, run_id: str) -> bool:
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return True
            return str(run.get("status") or "") == "stopping"

    def _set_status(self, run_id: str, status: str, *, error: str = "") -> None:
        next_status = str(status or "").strip()
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return
            prev_status = str(run.get("status") or "").strip()
            run["status"] = next_status or prev_status or "failed"
            if error:
                run["error"] = str(error or "").strip()
        if next_status and next_status != prev_status:
            logger.info("Run status changed (run_id=%s %s -> %s)", run_id, prev_status or "-", next_status)
        if error:
            logger.error("Run error (run_id=%s status=%s error=%s)", run_id, next_status or prev_status or "-", str(error or "").strip())
