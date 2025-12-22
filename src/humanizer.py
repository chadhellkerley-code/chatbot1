# src/humanizer.py
import asyncio
import random
import time
from typing import Optional, Union

from playwright.sync_api import Locator, Page

def human_wait(min_s: float = 0.2, max_s: float = 1.0) -> None:
    """Pausa aleatoria para simular tiempos humanos."""
    time.sleep(random.uniform(min_s, max_s))

def _keystroke_delay_ms(base: float = 0.07, jitter: float = 0.03) -> int:
    """Retorna un delay por tecla en milisegundos, con pequeña variación."""
    delay = max(0.01, random.gauss(base, jitter))
    return int(delay * 1000)

def human_type(locator, text: str, clear_first: bool = True) -> None:
    """
    Escribe texto carácter por carácter con pausas humanas.
    locator: Playwright Locator (ej: page.locator("input[...]")).
    """
    if clear_first:
        locator.click()
        locator.fill("")
        human_wait(0.05, 0.2)

    for ch in text:
        locator.type(ch, delay=_keystroke_delay_ms())
        # micro-pausas esporádicas
        if random.random() < 0.06:
            human_wait(0.08, 0.3)

    human_wait(0.1, 0.4)

def _resolve_locator(target: Union[Page, Locator], selector: Optional[str]) -> Locator:
    if selector is not None:
        if not hasattr(target, "locator"):
            raise TypeError("human_click: cuando se pasa selector, target debe exponer .locator().")
        locator = target.locator(selector)
    else:
        locator = target  # type: ignore[assignment]
    try:
        return locator.first
    except Exception:
        return locator


def human_click(target: Union[Page, Locator], selector: Optional[str] = None) -> None:
    """
    Hover previo + click con pausa.
    Se acepta tanto human_click(page, "button[type=submit]") como human_click(locator).
    """
    locator = _resolve_locator(target, selector)
    try:
        locator.scroll_into_view_if_needed(timeout=2_000)
    except Exception:
        pass
    try:
        locator.hover()
    except Exception:
        pass
    human_wait(0.05, 0.4)
    locator.click()
    human_wait(0.1, 0.6)

def human_scroll(page, amount_px: int = 1200, steps: int = 6) -> None:
    """Desplazamiento suave por la página."""
    step = max(1, int(amount_px / max(1, steps)))
    for _ in range(max(1, steps)):
        page.mouse.wheel(0, step)
        human_wait(0.2, 0.7)

def human_mouse_trace(page, x: Optional[float] = None, y: Optional[float] = None, steps: int = 8) -> None:
    """
    Movimiento de mouse suave hacia una posición (si no se pasa, hace un pequeño ‘wiggle’).
    """
    try:
        if x is None or y is None:
            # pequeño “zig-zag” corto en el área actual
            pos = page.mouse
            for dx, dy in [(4, 2), (-3, -1), (5, 1), (-2, 0)]:
                pos.move(pos.position[0] + dx, pos.position[1] + dy, steps=random.randint(2, 6))
                human_wait(0.02, 0.08)
        else:
            page.mouse.move(x, y, steps=max(2, steps))
            human_wait(0.05, 0.2)
    except Exception:
        # Es opcional; si falla, no debe romper el flujo principal
        pass


# --- Async helpers ------------------------------------------------------- #

async def random_wait(min_ms: int = 150, max_ms: int = 450) -> None:
    """Asynchronous versión: espera entre min_ms y max_ms milisegundos."""
    delay = max(0, random.uniform(min_ms, max_ms) / 1000.0)
    await asyncio.sleep(delay)


async def type_text(
    locator,
    text: str,
    *,
    min_delay: float = 0.04,
    max_delay: float = 0.18,
    occasional_pause: float = 0.12,
) -> None:
    """Tipea carácter por carácter usando .type(), ideal para contenteditables."""
    try:
        await locator.click()
    except Exception:
        pass

    for ch in text:
        delay_ms = max(10, int(random.uniform(min_delay, max_delay) * 1000))
        await locator.type(ch, delay=delay_ms)
        if random.random() < 0.12:
            await asyncio.sleep(occasional_pause)
