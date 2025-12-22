"""
tools/test_integration_send.py

Quick harness to test the integrated send path. This script uses the app's
account loader and calls `integraciones.adapter.send_message` for the first
available account. It is intentionally minimal.
"""
from __future__ import annotations

import sys
import logging
from pathlib import Path

# Ensure project root is importable when running from tools/
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO)

from accounts import list_all  # type: ignore

try:
    from integraciones.adapter import send_message
except Exception as exc:  # pragma: no cover - adapter optional
    print("Adapter import failed:", exc)
    send_message = None  # type: ignore


def main() -> int:
    accs = list_all()
    if not accs:
        print("No accounts found in data/accounts.json. Add one using the app menu.")
        return 2
    acct = accs[0]
    recipient = input("Target username (without @): ").strip() or "test_target"
    message = input("Message to send: ").strip() or "Test message from integration harness"

    if send_message is None:
        print("Adapter not available. Exiting with code 3.")
        return 3

    ok, detail = send_message(acct, recipient, message, {"simulate_typing": True})
    print("Result:", ok, detail)
    return 0 if ok else 4


if __name__ == "__main__":
    raise SystemExit(main())
