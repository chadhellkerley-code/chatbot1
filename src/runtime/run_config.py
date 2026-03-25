from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RunConfig:
    alias: str = "inbox_run"

    # Multi-account support (preferred)
    accounts: list[str] | None = None

    # Legacy single-account field (backwards compatible input/output)
    account: str = ""

    threads_limit: int = 70

    delay_min: int = 10

    delay_max: int = 30

    continuous: bool = True

    def __post_init__(self) -> None:
        self.alias = str(self.alias or "inbox_run").strip() or "inbox_run"
        self.account = str(self.account or "").strip()

        if self.accounts is None and self.account:
            self.accounts = [self.account]

        self.accounts = [str(item or "").strip() for item in (self.accounts or []) if str(item or "").strip()]
        if self.accounts and not self.account:
            self.account = self.accounts[0]
        self.threads_limit = max(1, int(self.threads_limit or 1))
        self.delay_min = max(1, int(self.delay_min or 1))
        self.delay_max = max(self.delay_min, int(self.delay_max or self.delay_min))
        self.continuous = bool(self.continuous)

    def to_dict(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "accounts": list(self.accounts),
            "account": self.account,
            "threads_limit": int(self.threads_limit),
            "delay_min": int(self.delay_min),
            "delay_max": int(self.delay_max),
            "continuous": bool(self.continuous),
        }
