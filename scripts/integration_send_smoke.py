"""
scripts/integration_send_smoke.py

Quick harness to test the integrated send path. This script uses the app's
account loader and calls ``adapters.integrations.adapter.send_message`` for the
first available account.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO)

from core.accounts import list_all  # type: ignore

try:
    from adapters.integrations.adapter import send_message
except Exception as exc:  # pragma: no cover - adapter optional
    print("Adapter import failed:", exc)
    send_message = None  # type: ignore


def main() -> int:
    accounts = list_all()
    if not accounts:
        print("No accounts found in storage/accounts/accounts.json. Add one using the app menu.")
        return 2

    account = accounts[0]
    recipient = input("Target username (without @): ").strip() or "test_target"
    message = input("Message to send: ").strip() or "Test message from integration harness"

    if send_message is None:
        print("Adapter not available. Exiting with code 3.")
        return 3

    ok, detail = send_message(account, recipient, message, {"simulate_typing": True})
    print("Result:", ok, detail)
    return 0 if ok else 4


if __name__ == "__main__":
    raise SystemExit(main())
