from __future__ import annotations

import logging
import traceback
from dataclasses import dataclass
from itertools import count
from typing import Any, Callable

from PySide6.QtCore import QObject, QRunnable, QThreadPool, Signal


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueryError:
    message: str
    error_type: str = ""
    traceback_text: str = ""


class _QuerySignals(QObject):
    succeeded = Signal(int, object)
    failed = Signal(int, object)


class _QueryRunnable(QRunnable):
    def __init__(self, request_id: int, target: Callable[[], Any]) -> None:
        super().__init__()
        self._request_id = int(request_id)
        self._target = target
        self.signals = _QuerySignals()
        self.setAutoDelete(True)

    def run(self) -> None:  # type: ignore[override]
        try:
            payload = self._target()
        except Exception as exc:
            logger.error("Background query failed", exc_info=(type(exc), exc, exc.__traceback__))
            try:
                self.signals.failed.emit(
                    self._request_id,
                    QueryError(
                        message=str(exc) or exc.__class__.__name__,
                        error_type=exc.__class__.__name__,
                        traceback_text=traceback.format_exc(),
                    ),
                )
            except RuntimeError:
                return
            return
        try:
            self.signals.succeeded.emit(self._request_id, payload)
        except RuntimeError:
            return


class QueryManager(QObject):
    def __init__(
        self,
        parent: QObject | None = None,
        *,
        max_thread_count: int = 4,
    ) -> None:
        super().__init__(parent)
        self._pool = QThreadPool(parent if isinstance(parent, QObject) else None)
        self._pool.setMaxThreadCount(max(1, int(max_thread_count or 1)))
        self._request_ids = count(1)
        self._active: dict[int, _QueryRunnable] = {}
        self._success_callbacks: dict[int, Callable[[int, Any], None]] = {}
        self._error_callbacks: dict[int, Callable[[int, QueryError], None]] = {}
        self._shutdown = False

    def submit(
        self,
        target: Callable[[], Any],
        *,
        on_success: Callable[[int, Any], None] | None = None,
        on_error: Callable[[int, QueryError], None] | None = None,
    ) -> int:
        if self._shutdown:
            return 0
        request_id = next(self._request_ids)
        runnable = _QueryRunnable(request_id, target)
        self._active[request_id] = runnable
        if on_success is not None:
            self._success_callbacks[request_id] = on_success
        if on_error is not None:
            self._error_callbacks[request_id] = on_error
        runnable.signals.succeeded.connect(self._handle_success)
        runnable.signals.failed.connect(self._handle_error)
        self._pool.start(runnable)
        return request_id

    def _handle_success(self, request_id: int, payload: Any) -> None:
        clean_id = int(request_id)
        self._active.pop(clean_id, None)
        error_callback = self._error_callbacks.pop(clean_id, None)
        del error_callback
        callback = self._success_callbacks.pop(clean_id, None)
        if callback is not None:
            callback(clean_id, payload)

    def _handle_error(self, request_id: int, error: QueryError) -> None:
        clean_id = int(request_id)
        self._active.pop(clean_id, None)
        success_callback = self._success_callbacks.pop(clean_id, None)
        del success_callback
        callback = self._error_callbacks.pop(clean_id, None)
        if callback is not None:
            callback(clean_id, error)

    def shutdown(self, *, wait_ms: int = 30000) -> None:
        self._shutdown = True
        self._pool.clear()
        self._pool.waitForDone(max(0, int(wait_ms or 0)))
        self._active.clear()
        self._success_callbacks.clear()
        self._error_callbacks.clear()
