"""UI helpers for colored console output and live tables."""
from __future__ import annotations

import builtins
import contextlib
import os
import shutil
import sys
import time
from collections import deque
from dataclasses import dataclass, field
import threading
from typing import List, Sequence

try:  # pragma: no cover - depends on optional dependency
    from colorama import Fore, Style, init as _colorama_init

    _colorama_init(convert=True, autoreset=True)
except Exception:  # pragma: no cover - graceful fallback
    class _Dummy:
        BLACK = BLUE = CYAN = GREEN = MAGENTA = RED = WHITE = YELLOW = ""
        RESET_ALL = ""
        BRIGHT = ""

    Fore = Style = _Dummy()  # type: ignore


_MOJIBAKE_MARKERS = (
    "\ufffd",
    "\u00c3",
    "\u00c2",
    "\u00e2",
    "\u00f0",
    "\u0178",
    "\u00ef",
    "\u0192",
    "\u201a",
    "\u201e",
    "\u2020",
    "\u2021",
    "\u02c6",
    "\u2030",
    "\u0160",
    "\u0152",
    "\u017d",
    "\u02dc",
    "\u0161",
    "\u0153",
    "\u017e",
)
_ORIGINAL_PRINT = builtins.print
_ORIGINAL_INPUT = builtins.input
_CONSOLE_FIXES_INSTALLED = False


def _mojibake_score(text: str) -> int:
    return (text.count("\ufffd") * 4) + sum(text.count(token) for token in _MOJIBAKE_MARKERS)


def _repair_mojibake(text: str) -> str:
    if not text:
        return text
    score_before = _mojibake_score(text)
    if score_before == 0:
        return text

    best = text
    best_score = score_before
    queue: deque[tuple[str, int]] = deque([(text, 0)])
    seen: set[str] = {text}
    while queue:
        current, depth = queue.popleft()
        if depth >= 6:
            continue
        for source_encoding in ("latin1", "cp1252"):
            try:
                candidate = current.encode(source_encoding).decode("utf-8")
            except Exception:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            candidate_score = _mojibake_score(candidate)
            if candidate_score < best_score:
                best = candidate
                best_score = candidate_score
                if best_score == 0:
                    return best
            queue.append((candidate, depth + 1))
    return best


def _configure_stdio_utf8() -> None:
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        is_tty = False
        with contextlib.suppress(Exception):
            is_tty = bool(stream.isatty())
        if not is_tty:
            continue
        with contextlib.suppress(Exception):
            stream.reconfigure(encoding="utf-8", errors="replace")
    if os.name != "nt":
        return
    stdout_tty = False
    stdin_tty = False
    with contextlib.suppress(Exception):
        stdout_tty = bool(sys.stdout.isatty())
    with contextlib.suppress(Exception):
        stdin_tty = bool(sys.stdin.isatty())
    if not stdout_tty and not stdin_tty:
        return
    with contextlib.suppress(Exception):
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        if stdout_tty:
            kernel32.SetConsoleOutputCP(65001)
        if stdin_tty:
            kernel32.SetConsoleCP(65001)


def _patched_print(*args, **kwargs):
    fixed_args = tuple(_repair_mojibake(arg) if isinstance(arg, str) else arg for arg in args)
    try:
        return _ORIGINAL_PRINT(*fixed_args, **kwargs)
    except OSError:
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        text = sep.join(str(item) for item in fixed_args) + end
        fallback_stream = kwargs.get("file") or getattr(sys, "__stdout__", None)
        if fallback_stream is not None:
            with contextlib.suppress(Exception):
                fallback_stream.write(text)
                fallback_stream.flush()
        return None


def _patched_input(prompt: str = "") -> str:
    if isinstance(prompt, str):
        prompt = _repair_mojibake(prompt)
    return _ORIGINAL_INPUT(prompt)


def install_console_fixes() -> None:
    global _CONSOLE_FIXES_INSTALLED
    if _CONSOLE_FIXES_INSTALLED:
        return
    _configure_stdio_utf8()
    builtins.print = _patched_print
    builtins.input = _patched_input
    _CONSOLE_FIXES_INSTALLED = True


install_console_fixes()


def supports_emojis_default() -> bool:
    value = os.environ.get("INSTACLI_EMOJI")
    if value is not None:
        return value.strip().lower() not in {"0", "false", "no"}
    return True


EMOJI_ON = supports_emojis_default()


DEFAULT_BOLD = True


def em(text: str) -> str:
    if EMOJI_ON:
        return text
    return (
        text.replace("🏆", "*")
        .replace("🟢", "[ON]")
        .replace("⚪", "[OFF]")
        .replace("✅", "OK")
        .replace("❌", "ERR")
        .replace("🤖", "BOT")
    )


def terminal_width(fallback: int = 80) -> int:
    try:
        return shutil.get_terminal_size((fallback, 20)).columns
    except Exception:
        return fallback


def panel_width() -> int:
    width = terminal_width()
    target = 60
    if width <= target:
        return max(40, width)
    return target


def panel_padding() -> str:
    return ""


def full_line(char: str = "─", *, color: str | None = None, bold: bool = False) -> str:
    width = max(20, panel_width())
    base = char * width
    styled = style_text(base, color=color, bold=bold)
    return f"{panel_padding()}{styled}"


def _align_label_value(text: str) -> str:
    if ":" not in text:
        return text
    if "\n" in text:
        return text
    label, remainder = text.split(":", 1)
    if not remainder:
        return text
    formatted_label = f"{label.strip()}:"
    if not formatted_label:
        return text
    padded = formatted_label.ljust(24)
    value = remainder.lstrip()
    return f"{padded}{value}"


def style_text(text: str, *, color: str | None = None, bold: bool = False) -> str:
    text = _align_label_value(text)
    prefix = ""
    use_bold = bold or DEFAULT_BOLD
    suffix = Style.RESET_ALL if color or use_bold else ""
    if color:
        prefix += color
    if use_bold:
        prefix += Style.BRIGHT
    return f"{prefix}{text}{suffix}"


def clear_console() -> None:
    if os.environ.get("INSTACLI_DISABLE_CONSOLE_CLEAR", "").strip() in {"1", "true", "yes"}:
        return
    stdin_tty = False
    stdout_tty = False
    with contextlib.suppress(Exception):
        stdin_tty = bool(sys.stdin.isatty())
    with contextlib.suppress(Exception):
        stdout_tty = bool(sys.stdout.isatty())
    # In windowed GUI/EXE there is no real console; running "cls" spawns cmd.exe.
    if not (stdin_tty or stdout_tty):
        return
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        print("\n" * 2)


def print_header() -> None:
    clear_console()
    pad = panel_padding()
    print()
    heading = style_text(
        em("🏆  HERRAMIENTA DE MENSAJERÍA DE IG  -  PROPIEDAD DE MATIDIAZLIFE/ELITE 🏆"),
        color=Fore.MAGENTA,
        bold=True,
    )
    print(f"{pad}{heading}")
    print(full_line("─", color=Fore.MAGENTA, bold=True))
    print()


def print_metrics(sent: int, errors: int) -> None:
    # Maintained for compatibility with older calls; no output needed in the new layout.
    return


def print_daily_metrics(sent: int, errors: int, tz_label: str, last_reset: str) -> None:
    pad = panel_padding()
    title = style_text(em("📨  MENSAJERÍA (HOY)"), color=Fore.CYAN, bold=True)
    divider = full_line(color=Fore.BLUE, bold=True)
    print(f"{pad}{title}")
    print(divider)
    ok = style_text(f"Mensajes enviados (HOY): {sent}", color=Fore.GREEN, bold=True)
    fail = style_text(f"Mensajes con error (HOY): {errors}", color=Fore.RED, bold=True)
    tz_line = style_text(f"Zona horaria: Último reset: {last_reset}", color=Fore.WHITE)
    print(f"{pad}{ok}")
    print(f"{pad}{fail}")
    print(f"{pad}{tz_line}")
    print(divider)


def print_section(title: str) -> None:
    print(full_line(color=Fore.BLUE))
    print(f"{panel_padding()}{style_text(title, color=Fore.CYAN, bold=True)}")
    print(full_line(color=Fore.BLUE))


def banner() -> None:
    print_header()


def highlight(message: str, *, color: str = Fore.YELLOW) -> str:
    return f"{Style.BRIGHT}{color}{message}{Style.RESET_ALL}"


def format_table(rows: Sequence[Sequence[str]]) -> List[str]:
    if not rows:
        return []
    widths = [max(len(str(cell)) for cell in column) for column in zip(*rows)]
    formatted: List[str] = []
    for row in rows:
        formatted.append("  ".join(str(cell).ljust(width) for cell, width in zip(row, widths)))
    return formatted


@dataclass
class LiveEntry:
    account: str
    lead: str
    started_at: float
    status_icon: str = field(default=em("⏳"))
    detail: str = ""
    finished_at: float | None = None

    def as_row(self) -> Sequence[str]:
        started = time.strftime("%H:%M:%S", time.localtime(self.started_at))
        return (self.account, self.lead, started, self.status_icon, self.detail)


class LiveTable:
    """Keeps a snapshot of accounts currently processing messages."""

    def __init__(self, *, max_entries: int = 10, expiry_seconds: int = 6) -> None:
        self._entries: dict[str, LiveEntry] = {}
        self._lock = threading.Lock()
        self._max_entries = max_entries
        self._expiry = expiry_seconds

    def begin(self, account: str, lead: str) -> None:
        with self._lock:
            self._entries[account] = LiveEntry(
                account=account, lead=lead, started_at=time.time()
            )

    def complete(self, account: str, success: bool, detail: str = "") -> None:
        with self._lock:
            entry = self._entries.get(account)
            if not entry:
                return
            entry.status_icon = em("✅") if success else em("❌")
            entry.detail = detail
            entry.finished_at = time.time()

    def abandon(self, account: str, detail: str) -> None:
        with self._lock:
            entry = self._entries.get(account)
            if not entry:
                return
            entry.status_icon = em("❌")
            entry.detail = detail
            entry.finished_at = time.time()

    def prune(self) -> None:
        with self._lock:
            now = time.time()
            to_delete = [
                key
                for key, entry in self._entries.items()
                if entry.finished_at and now - entry.finished_at > self._expiry
            ]
            for key in to_delete:
                self._entries.pop(key, None)

    def rows(self) -> List[Sequence[str]]:
        self.prune()
        with self._lock:
            if not self._entries:
                return []
            ordered = sorted(self._entries.values(), key=lambda e: e.started_at)
        header = (
            style_text("Cuenta", bold=True),
            style_text("Lead", bold=True),
            style_text("Hora", bold=True),
            style_text("Res", bold=True),
            style_text("Detalle", bold=True),
        )
        rows = [header]
        rows.extend(entry.as_row() for entry in ordered[: self._max_entries])
        return rows

    def render(self) -> str:
        rows = self.rows()
        if not rows:
            return f"{panel_padding()}{style_text('Sin envíos en vuelo', color=Fore.WHITE)}"
        formatted = [panel_padding() + row for row in format_table(rows)]
        return "\n".join(formatted)
