from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.integrations.android_sim_adapter import AndroidSimAdapter

if __name__ == "__main__":
    bot = AndroidSimAdapter("myaccount")  # mismo nombre que usaste en 'gramaddict init'
    bot.start_session(["--mode", "interact-users-list"])
