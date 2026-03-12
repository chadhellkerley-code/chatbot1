from __future__ import annotations

import io
import logging
import sys
import threading
import time
import traceback
from typing import Any, Callable

from PySide6.QtCore import QObject, Signal

from runtime.runtime import (
    EngineCancellationToken,
    bind_stop_token,
    request_stop,
    restore_stop_token,
)


class LogStore(QObject):
    logAdded = Signal(str)
    cleared = Signal()

    def __init__(self, *, max_chunks: int = 8000, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._chunks: list[str] = []
        self._max_chunks = max(500, int(max_chunks or 8000))
        self._lock = threading.RLock()
        self._base_cursor = 0
        self._next_cursor = 0

    def append(self, text: str) -> None:
        chunk = str(text or "")
        if not chunk:
            return
        with self._lock:
            self._chunks.append(chunk)
            self._next_cursor += 1
            if len(self._chunks) > self._max_chunks:
                overflow = len(self._chunks) - self._max_chunks
                del self._chunks[:overflow]
                self._base_cursor += overflow
        self.logAdded.emit(chunk)

    def clear(self) -> None:
        with self._lock:
            self._chunks.clear()
            self._base_cursor = self._next_cursor
        self.cleared.emit()

    def text(self) -> str:
        with self._lock:
            return "".join(self._chunks)

    def cursor(self) -> int:
        with self._lock:
            return self._next_cursor

    def read_since(self, cursor: int | None) -> tuple[int, str, bool]:
        with self._lock:
            start_cursor = self._base_cursor
            end_cursor = self._next_cursor
            if cursor is None:
                return end_cursor, "".join(self._chunks), True
            try:
                requested = int(cursor)
            except Exception:
                requested = start_cursor
            if requested < start_cursor or requested > end_cursor:
                return end_cursor, "".join(self._chunks), True
            if requested == end_cursor:
                return end_cursor, "", False
            offset = requested - start_cursor
            return end_cursor, "".join(self._chunks[offset:]), False


class _SignalWriter(io.TextIOBase):
    def __init__(self, emit_fn: Callable[[str], None]) -> None:
        super().__init__()
        self._emit_fn = emit_fn
        self._buffer = ""

    def write(self, text: str) -> int:
        chunk = str(text or "")
        if not chunk:
            return 0
        self._buffer += chunk
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._emit_fn(line + "\n")
        return len(chunk)

    def flush(self) -> None:
        if self._buffer:
            self._emit_fn(self._buffer)
            self._buffer = ""
        return None


class _ThreadStreamRouter(io.TextIOBase):
    def __init__(self, fallback: Any) -> None:
        super().__init__()
        self._fallback = fallback
        self._targets: dict[int, io.TextIOBase] = {}
        self._lock = threading.RLock()

    def attach_current_thread(self, target: io.TextIOBase) -> None:
        with self._lock:
            self._targets[threading.get_ident()] = target

    def detach_current_thread(self) -> None:
        with self._lock:
            self._targets.pop(threading.get_ident(), None)

    def _target(self) -> io.TextIOBase | None:
        with self._lock:
            return self._targets.get(threading.get_ident())

    def write(self, text: str) -> int:
        chunk = str(text or "")
        if not chunk:
            return 0
        target = self._target()
        if target is not None:
            return target.write(chunk)
        fallback = self._fallback
        if fallback is not None and fallback is not self:
            try:
                return fallback.write(chunk)
            except Exception:
                return len(chunk)
        return len(chunk)

    def flush(self) -> None:
        target = self._target()
        if target is not None:
            target.flush()
            return
        fallback = self._fallback
        if fallback is not None and fallback is not self:
            try:
                fallback.flush()
            except Exception:
                return

    @property
    def encoding(self) -> str:
        return str(getattr(self._fallback, "encoding", "utf-8"))

    def isatty(self) -> bool:
        try:
            return bool(getattr(self._fallback, "isatty", lambda: False)())
        except Exception:
            return False


class _LogSignalHandler(logging.Handler):
    def __init__(self, emit_fn: Callable[[str], None], *, thread_id: int | None = None) -> None:
        super().__init__()
        self._emit_fn = emit_fn
        self._thread_id = int(thread_id) if thread_id is not None else None
        self.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        if self._thread_id is not None and int(getattr(record, "thread", -1)) != self._thread_id:
            return
        try:
            message = self.format(record).strip()
        except Exception:
            message = record.getMessage()
        if message:
            self._emit_fn(message + "\n")


_STREAM_ROUTER_LOCK = threading.RLock()
_STDOUT_ROUTER: _ThreadStreamRouter | None = None
_STDERR_ROUTER: _ThreadStreamRouter | None = None


def _install_thread_streams() -> tuple[_ThreadStreamRouter, _ThreadStreamRouter]:
    global _STDOUT_ROUTER, _STDERR_ROUTER
    with _STREAM_ROUTER_LOCK:
        if not isinstance(sys.stdout, _ThreadStreamRouter):
            _STDOUT_ROUTER = _ThreadStreamRouter(sys.stdout)
            sys.stdout = _STDOUT_ROUTER
        elif _STDOUT_ROUTER is None:
            _STDOUT_ROUTER = sys.stdout

        if not isinstance(sys.stderr, _ThreadStreamRouter):
            _STDERR_ROUTER = _ThreadStreamRouter(sys.stderr)
            sys.stderr = _STDERR_ROUTER
        elif _STDERR_ROUTER is None:
            _STDERR_ROUTER = sys.stderr

        if _STDOUT_ROUTER is None or _STDERR_ROUTER is None:
            raise RuntimeError("No se pudieron instalar los routers de salida GUI.")
        return _STDOUT_ROUTER, _STDERR_ROUTER


class TaskManager(QObject):
    taskStarted = Signal(str)
    taskFinished = Signal(str, bool, str)
    taskCompleted = Signal(str, bool, str, object)
    taskStateChanged = Signal()

    def __init__(self, logs: LogStore, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._logs = logs
        self._threads: dict[str, threading.Thread] = {}
        self._started_at: dict[str, float] = {}
        self._metadata: dict[str, dict[str, Any]] = {}
        self._stop_tokens: dict[str, EngineCancellationToken] = {}
        self._lock = threading.RLock()

    def log(self, text: str) -> None:
        self._logs.append(text)

    def log_store(self) -> LogStore:
        return self._logs

    def is_running(self, name: str) -> bool:
        clean_name = str(name or "").strip()
        if not clean_name:
            return False
        with self._lock:
            thread = self._threads.get(clean_name)
            return bool(thread and thread.is_alive())

    def started_at(self, name: str) -> float | None:
        with self._lock:
            return self._started_at.get(str(name or "").strip())

    def running_tasks(self) -> list[str]:
        with self._lock:
            active: list[str] = []
            stale: list[str] = []
            for name, thread in self._threads.items():
                if thread.is_alive():
                    active.append(name)
                else:
                    stale.append(name)
            for name in stale:
                self._threads.pop(name, None)
                self._started_at.pop(name, None)
                self._metadata.pop(name, None)
            return sorted(active)

    def running_task_metadata(self) -> list[dict[str, Any]]:
        with self._lock:
            active: list[dict[str, Any]] = []
            stale: list[str] = []
            for name, thread in self._threads.items():
                if not thread.is_alive():
                    stale.append(name)
                    continue
                payload = dict(self._metadata.get(name) or {})
                payload.setdefault("name", name)
                started_at = self._started_at.get(name)
                if started_at is not None:
                    payload.setdefault("started_at", started_at)
                payload.setdefault("isolated_stop", name in self._stop_tokens)
                active.append(payload)
            for name in stale:
                self._threads.pop(name, None)
                self._started_at.pop(name, None)
                self._metadata.pop(name, None)
                self._stop_tokens.pop(name, None)
            active.sort(key=lambda item: str(item.get("name") or ""))
            return active

    def start_task(
        self,
        name: str,
        target: Callable[[], Any],
        *,
        metadata: dict[str, Any] | None = None,
        isolated_stop: bool = False,
    ) -> None:
        clean_name = str(name or "").strip()
        if not clean_name:
            raise RuntimeError("Task name is required.")
        if self.is_running(clean_name):
            raise RuntimeError(f"La tarea '{clean_name}' ya esta en ejecucion.")

        stdout_router, stderr_router = _install_thread_streams()
        task_stop_token = EngineCancellationToken(f"gui-task:{clean_name}") if isolated_stop else None
        emit_buffer: list[str] = []
        emit_chars = 0
        last_flush_at = time.perf_counter()
        flush_interval_seconds = 0.15
        flush_chunk_chars = 2048
        flush_chunk_count = 24

        def _flush_emit_buffer(*, force: bool = False) -> None:
            nonlocal emit_buffer, emit_chars, last_flush_at
            if not emit_buffer:
                return
            now = time.perf_counter()
            if (
                not force
                and emit_chars < flush_chunk_chars
                and len(emit_buffer) < flush_chunk_count
                and (now - last_flush_at) < flush_interval_seconds
            ):
                return
            chunk = "".join(emit_buffer)
            emit_buffer = []
            emit_chars = 0
            last_flush_at = now
            self._logs.append(chunk)

        def _emit(text: str) -> None:
            nonlocal emit_chars
            chunk = str(text or "")
            if not chunk:
                return
            emit_buffer.append(chunk)
            emit_chars += len(chunk)
            _flush_emit_buffer()

        def _runner() -> None:
            writer = _SignalWriter(_emit)
            handler = _LogSignalHandler(_emit, thread_id=threading.get_ident())
            root_logger = logging.getLogger()
            ok = True
            message = ""
            result: Any = None
            previous_stop_token = None

            stdout_router.attach_current_thread(writer)
            stderr_router.attach_current_thread(writer)
            root_logger.addHandler(handler)
            if task_stop_token is not None:
                previous_stop_token = bind_stop_token(task_stop_token)
            try:
                result = target()
                if result not in (None, "", [], {}):
                    _emit(f"[task:{clean_name}] {result}\n")
            except Exception as exc:
                ok = False
                message = str(exc) or exc.__class__.__name__
                _emit(f"[task:{clean_name}] ERROR: {message}\n")
                _emit(traceback.format_exc())
            finally:
                if previous_stop_token is not None:
                    restore_stop_token(previous_stop_token)
                _flush_emit_buffer(force=True)
                root_logger.removeHandler(handler)
                stdout_router.detach_current_thread()
                stderr_router.detach_current_thread()
                with self._lock:
                    self._threads.pop(clean_name, None)
                    self._started_at.pop(clean_name, None)
                    self._metadata.pop(clean_name, None)
                    self._stop_tokens.pop(clean_name, None)
                self.taskCompleted.emit(clean_name, ok, message, result)
                self.taskFinished.emit(clean_name, ok, message)
                self.taskStateChanged.emit()

        thread = threading.Thread(target=_runner, name=f"gui-task-{clean_name}", daemon=True)
        with self._lock:
            self._threads[clean_name] = thread
            self._started_at[clean_name] = time.time()
            self._metadata[clean_name] = dict(metadata or {})
            if task_stop_token is not None:
                self._stop_tokens[clean_name] = task_stop_token
        self.taskStarted.emit(clean_name)
        self.taskStateChanged.emit()
        thread.start()

    def request_task_stop(self, name: str, reason: str) -> None:
        clean_name = str(name or "").strip()
        if not clean_name:
            self.request_stop(reason)
            return
        with self._lock:
            token = self._stop_tokens.get(clean_name)
            thread = self._threads.get(clean_name)
        if token is not None:
            request_stop(str(reason or "").strip() or f"stop requested for {clean_name}", token=token)
            return
        # A targeted stop for a finished/missing task must not spill into the global stop event.
        if thread is None or not thread.is_alive():
            return
        if token is None:
            self.request_stop(reason)
            return

    def request_stop(self, reason: str) -> None:
        request_stop(str(reason or "").strip() or "stop requested from GUI")

    def shutdown(self, reason: str = "GUI shutdown") -> None:
        self.request_stop(reason)
        with self._lock:
            threads = list(self._threads.values())
        deadline = time.time() + 1.5
        for thread in threads:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            thread.join(timeout=remaining)
