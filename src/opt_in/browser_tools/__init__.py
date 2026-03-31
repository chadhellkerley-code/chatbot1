"""
Opt-in Playwright automation tools.

This package groups helper modules that provide:

* Configuration loading guarded by environment flags.
* A minimal audit log so usage stays traceable.
* Session storage with optional encryption to avoid leaking secrets.
* Browser management helpers that wrap Playwright in a reusable context.
* High level workflows (login, DM sending, replying, recorder, playback).

Only import these modules when `OPTIN_ENABLE=1` to keep the core app untouched.
"""

from . import audit, browser_manager, config, dm, login, playback, recorder, replies, session_store, utils

__all__ = [
    "audit",
    "browser_manager",
    "config",
    "dm",
    "login",
    "playback",
    "recorder",
    "replies",
    "session_store",
    "utils",
]
