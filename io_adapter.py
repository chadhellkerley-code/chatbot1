from __future__ import annotations

import builtins
import os
import re
import sys
import threading
from collections import deque
from dataclasses import dataclass
from itertools import count
from typing import Any, Deque, Optional

from PySide6.QtCore import QObject, Signal

DEBUG_UI_FLOW = True


@dataclass
class MenuOption:
    value: str
    label: str


@dataclass
class InputRequest:
    request_id: int
    prompt: str
    sensitive: bool
    is_menu: bool
    menu_title: str
    menu_options: list[MenuOption]


@dataclass
class _PendingInput:
    request: InputRequest
    event: threading.Event
    value: str = ""


class _StreamProxy:
    """Text stream proxy that mirrors writes into IOAdapter log signals."""

    def __init__(self, adapter: "IOAdapter", stream: Any) -> None:
        self._adapter = adapter
        self._fallback_stream = open(os.devnull, "w", encoding="utf-8")
        self._owns_fallback_stream = True
        self._owns_stream = False
        if stream is None:
            # In windowed EXE builds stderr/stdout may be None.
            # Playwright requires fileno(), so provide a real writable stream.
            self._stream = self._fallback_stream
            self._owns_stream = True
        else:
            self._stream = stream

    def write(self, data: Any) -> int:
        text = self._adapter._stringify_arg(data)
        written: Any = len(text)
        if self._stream is not None:
            try:
                written = self._stream.write(text)
            except Exception:
                try:
                    written = self._fallback_stream.write(text)
                except Exception:
                    written = len(text)
        self._adapter._capture_stream_text(text)
        return written if isinstance(written, int) else len(text)

    def flush(self) -> None:
        for stream in (self._stream, self._fallback_stream):
            if stream is None:
                continue
            try:
                stream.flush()
            except Exception:
                continue

    def writelines(self, lines: Any) -> None:
        for line in lines:
            self.write(line)

    def _safe_isatty(self, stream: Any) -> bool:
        if stream is None:
            return False
        try:
            return bool(stream.isatty())
        except Exception:
            return False

    def isatty(self) -> bool:
        return self._safe_isatty(self._stream)

    def fileno(self) -> int:
        for stream in (self._stream, self._fallback_stream):
            if stream is None:
                continue
            try:
                return stream.fileno()
            except Exception:
                continue
        raise OSError("No backing stream available")

    def close(self) -> None:
        if self._owns_stream and self._stream is not None:
            try:
                self._stream.close()
            except Exception:
                pass
            finally:
                self._stream = None
        if not self._owns_fallback_stream or self._fallback_stream is None:
            return
        try:
            self._fallback_stream.close()
        except Exception:
            return
        finally:
            self._fallback_stream = None

    @property
    def encoding(self) -> str:
        return getattr(self._stream or self._fallback_stream, "encoding", "utf-8")

    @property
    def errors(self) -> str:
        return getattr(self._stream or self._fallback_stream, "errors", "replace")

    @property
    def closed(self) -> bool:
        stream = self._stream or self._fallback_stream
        return bool(getattr(stream, "closed", False))

    def __getattr__(self, name: str) -> Any:
        if self._stream is None:
            raise AttributeError(name)
        return getattr(self._stream, name)


class IOAdapter(QObject):
    """Bridges global CLI IO into Qt while preserving backend behavior."""

    log_chunk = Signal(str)
    input_requested = Signal(object)
    menu_detected = Signal(object)

    _ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
    _MENU_OPTION_RE = re.compile(
        r"^\s*(?:\[(?P<bracket>\d{1,3})\]|(?P<paren>\d{1,3})\))\s+(?P<label>.+?)\s*$"
    )
    _SEPARATOR_RE = re.compile(r"^[=\-_*~\s]{3,}$")
    _MOJIBAKE_MARKERS = ("Ã", "â", "ð", "Ÿ", "ï¿½", "\ufffd")
    _ACTIVE_PATCH_LOCK = threading.Lock()
    _ACTIVE_ADAPTER: Optional["IOAdapter"] = None

    def __init__(self) -> None:
        super().__init__()
        self._original_input = builtins.input
        self._original_print = builtins.print
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        self._stdout_proxy: Optional[_StreamProxy] = None
        self._stderr_proxy: Optional[_StreamProxy] = None
        self._install_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._history_lock = threading.Lock()
        self._pending_inputs: Deque[_PendingInput] = deque()
        self._prefilled_inputs: Deque[str] = deque()
        self._line_history: Deque[str] = deque(maxlen=600)
        self._line_fragment = ""
        self._request_seq = count(1)
        self._warned_main_thread_input = False
        self._installed = False
        self._shutting_down = False

    def install(self) -> None:
        with self._install_lock:
            if self._installed:
                return
            with self._ACTIVE_PATCH_LOCK:
                active = self._ACTIVE_ADAPTER
                if active and active is not self:
                    raise RuntimeError("Another IOAdapter instance already monkeypatched builtins.")
                self._ACTIVE_ADAPTER = self
            self._original_stdout = sys.stdout
            self._original_stderr = sys.stderr
            self._stdout_proxy = _StreamProxy(self, self._original_stdout)
            self._stderr_proxy = _StreamProxy(self, self._original_stderr)
            sys.stdout = self._stdout_proxy
            sys.stderr = self._stderr_proxy
            builtins.input = self._input_override
            builtins.print = self._print_override
            self._installed = True

    def uninstall(self) -> None:
        with self._install_lock:
            if not self._installed:
                return
            builtins.input = self._original_input
            builtins.print = self._original_print
            if self._stdout_proxy is not None and sys.stdout is self._stdout_proxy:
                sys.stdout = self._original_stdout
            if self._stderr_proxy is not None and sys.stderr is self._stderr_proxy:
                sys.stderr = self._original_stderr
            if self._stdout_proxy is not None:
                self._stdout_proxy.close()
            if self._stderr_proxy is not None:
                self._stderr_proxy.close()
            self._stdout_proxy = None
            self._stderr_proxy = None
            with self._ACTIVE_PATCH_LOCK:
                if self._ACTIVE_ADAPTER is self:
                    self._ACTIVE_ADAPTER = None
            self._installed = False

    def shutdown(self) -> None:
        self._shutting_down = True
        with self._pending_lock:
            pending = list(self._pending_inputs)
            self._pending_inputs.clear()
            self._prefilled_inputs.clear()
        for item in pending:
            item.value = ""
            item.event.set()

    def provide_input(self, value: str, request_id: Optional[int] = None) -> bool:
        text = self._stringify_arg(value)
        with self._pending_lock:
            if not self._pending_inputs:
                self._prefilled_inputs.append(text)
                return True
            target: Optional[_PendingInput] = None
            if request_id is None:
                target = self._pending_inputs.popleft()
            else:
                for candidate in list(self._pending_inputs):
                    if candidate.request.request_id == request_id:
                        target = candidate
                        self._pending_inputs.remove(candidate)
                        break
                if target is None:
                    target = self._pending_inputs.popleft()
        target.value = text
        target.event.set()
        return True

    # Alias used by UI routing.
    def fulfill_input(self, value: str, request_id: Optional[int] = None) -> bool:
        return self.provide_input(value, request_id=request_id)

    def emit_log(self, text: str) -> None:
        self.log_chunk.emit(self._normalize_text(text))

    def _dbg(self, message: str) -> None:
        if not DEBUG_UI_FLOW:
            return
        self.log_chunk.emit(f"[DBG] {message}\n")

    def _print_override(self, *args: Any, **kwargs: Any) -> None:
        # Delegate to the original print so stdout/stderr behavior remains intact.
        # Stream proxies mirror writes back into GUI log signals.
        self._original_print(*args, **kwargs)

    def _input_override(self, prompt: Any = "") -> str:
        if threading.current_thread() is threading.main_thread():
            if not self._warned_main_thread_input:
                self._warned_main_thread_input = True
                self.log_chunk.emit(
                    "[gui] input() called from UI thread; returning empty string to avoid deadlock.\n"
                )
            return ""
        if self._shutting_down:
            raise SystemExit(0)

        with self._pending_lock:
            if self._prefilled_inputs:
                return self._prefilled_inputs.popleft()

        prompt_text = self._normalize_text(self._stringify_arg("" if prompt is None else prompt))
        # PHASE-0 pipeline: backend waits on input() -> this function builds InputRequest for GUI.
        menu_title, menu_options = self._detect_menu_from_history(prompt_text)
        request = InputRequest(
            request_id=next(self._request_seq),
            prompt=prompt_text,
            sensitive=self._looks_sensitive(prompt_text),
            is_menu=bool(menu_options),
            menu_title=menu_title,
            menu_options=menu_options,
        )
        self._dbg(
            "backend_response_type=InputRequest "
            f"request_id={request.request_id} prompt={prompt_text!r} "
            f"is_menu={request.is_menu} detected_options_count={len(request.menu_options)}"
        )
        pending = _PendingInput(
            request=request,
            event=threading.Event(),
        )
        with self._pending_lock:
            self._pending_inputs.append(pending)

        if prompt_text:
            self.log_chunk.emit(prompt_text)
        if request.is_menu:
            self.menu_detected.emit(request)
        self.input_requested.emit(request)

        pending.event.wait()
        if self._shutting_down:
            raise SystemExit(0)
        self._dbg(f"input_released request_id={request.request_id} value_len={len(pending.value)}")
        return pending.value

    def _capture_stream_text(self, chunk: str) -> None:
        text = self._normalize_text(chunk)
        if not text:
            return
        self._record_for_menu_detection(text)
        self.log_chunk.emit(text)

    def _record_for_menu_detection(self, chunk: str) -> None:
        normalized = self._normalize_text(self._ANSI_ESCAPE_RE.sub("", chunk.replace("\r", "\n")))
        if not normalized:
            return
        with self._history_lock:
            text = self._line_fragment + normalized
            lines = text.split("\n")
            self._line_fragment = lines.pop() if lines else ""
            for line in lines:
                clean = line.strip()
                if not clean:
                    self._line_history.append("")
                    continue
                self._line_history.append(clean[:320])

    def _detect_menu_from_history(self, prompt: str) -> tuple[str, list[MenuOption]]:
        with self._history_lock:
            history = list(self._line_history)

        if not history:
            return "", []

        end = len(history) - 1
        while end >= 0 and not history[end].strip():
            end -= 1
        if end < 0:
            return "", []

        options_rev: list[MenuOption] = []
        first_idx = -1
        idx = end
        while idx >= 0:
            line = history[idx].strip()
            match = self._MENU_OPTION_RE.match(line)
            if match:
                first_idx = idx
                value = match.group("paren") or match.group("bracket") or ""
                options_rev.append(MenuOption(value=value, label=match.group("label").strip()))
                idx -= 1
                continue
            if options_rev:
                # Keep only the nearest menu block to avoid mixing old options after blank separators.
                break
            idx -= 1

        options = list(reversed(options_rev))
        self._dbg(f"menu_scan prompt={prompt!r} raw_options_count={len(options)}")
        if len(options) < 2:
            return "", []
        if not self._prompt_expects_menu(prompt, history):
            self._dbg("menu_scan rejected_by_prompt_gate=True")
            return "", []

        deduped: list[MenuOption] = []
        seen_values: set[str] = set()
        for option in options:
            if option.value in seen_values:
                continue
            seen_values.add(option.value)
            deduped.append(option)

        if len(deduped) < 2:
            return "", []

        title = self._extract_menu_title(history, first_idx)
        self._dbg(f"menu_scan accepted title={title!r} deduped_options_count={len(deduped)}")
        return title, deduped

    def _extract_menu_title(self, history: list[str], first_option_idx: int) -> str:
        if first_option_idx <= 0:
            return "Detected menu"
        for idx in range(first_option_idx - 1, max(-1, first_option_idx - 8), -1):
            line = history[idx].strip()
            if not line:
                continue
            if self._SEPARATOR_RE.match(line):
                continue
            if self._MENU_OPTION_RE.match(line):
                continue
            return line[:120]
        return "Detected menu"

    def _prompt_expects_menu(self, prompt: str, history: list[str]) -> bool:
        text = self._plain_text(prompt).strip()
        keywords = (
            "opcion",
            "option",
            "elige",
            "choice",
            "seleccion",
            "numero",
            "num",
            "indice",
            "menu",
            "volver",
            "volv",
        )
        if any(keyword in text for keyword in keywords):
            return True
        if text in ("", ">", ">>", ":", "->", "=>"):
            return True
        if len(text) <= 4 and any(ch in text for ch in (":", ">")):
            return True
        tail = " ".join(self._plain_text(line) for line in history[-4:] if line)
        return any(keyword in tail for keyword in keywords)

    @classmethod
    def _looks_sensitive(cls, prompt: str) -> bool:
        text = cls._plain_text(prompt)
        keywords = (
            "password",
            "pass",
            "token",
            "secret",
            "otp",
            "2fa",
            "pin",
            "contrase",
            "clave",
            "codigo",
            "code",
        )
        return any(key in text for key in keywords)

    @classmethod
    def _stringify_arg(cls, value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, (bytes, bytearray)):
            raw = bytes(value)
            # Prefer explicit UTF-8 decode for CLI/subprocess streams.
            best = raw.decode("utf-8", errors="replace")
            best_score = cls._mojibake_score(best) + (best.count("\ufffd") * 3)
            for encoding in ("utf-8-sig", "cp1252", "latin-1"):
                try:
                    candidate = raw.decode(encoding, errors="replace")
                except Exception:
                    continue
                score = cls._mojibake_score(candidate) + (candidate.count("\ufffd") * 3)
                if score < best_score:
                    best = candidate
                    best_score = score
            return best
        try:
            return str(value)
        except Exception:
            return repr(value)

    @classmethod
    def _plain_text(cls, value: str) -> str:
        text = cls._normalize_text(value).lower()
        return (
            text.replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ú", "u")
            .replace("ü", "u")
            .replace("ñ", "n")
        )

    @classmethod
    def _normalize_text(cls, value: str) -> str:
        text = cls._stringify_arg(value)
        if not text:
            return ""

        candidate = text
        for _ in range(2):
            if not any(marker in candidate for marker in cls._MOJIBAKE_MARKERS):
                break
            repaired = cls._repair_mojibake(candidate)
            if repaired == candidate:
                break
            candidate = repaired
        return candidate

    @classmethod
    def _repair_mojibake(cls, value: str) -> str:
        best = value
        best_score = cls._mojibake_score(value)

        for source_encoding in ("latin-1", "cp1252"):
            try:
                candidate = value.encode(source_encoding).decode("utf-8")
            except Exception:
                continue
            score = cls._mojibake_score(candidate)
            if score < best_score:
                best = candidate
                best_score = score
        return best

    @classmethod
    def _mojibake_score(cls, value: str) -> int:
        return sum(value.count(marker) for marker in cls._MOJIBAKE_MARKERS)
