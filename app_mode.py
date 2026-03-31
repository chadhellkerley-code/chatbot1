from __future__ import annotations

import os
import sys

_VALID_MODES = ("client", "owner")


def _normalize_mode(mode: str | None) -> str:
    normalized = str(mode or "").strip().lower()
    return normalized if normalized in _VALID_MODES else "client"


APP_MODE = _normalize_mode(os.environ.get("INSTACRM_APP_MODE"))


def set_app_mode(mode: str | None) -> str:
    global APP_MODE
    APP_MODE = _normalize_mode(mode)
    os.environ["INSTACRM_APP_MODE"] = APP_MODE

    main_window = sys.modules.get("gui.main_window")
    if main_window is not None:
        try:
            setattr(main_window, "APP_MODE", APP_MODE)
        except Exception:
            pass
    return APP_MODE

