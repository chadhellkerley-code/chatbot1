import time
from dataclasses import dataclass


@dataclass
class BackoffState:
    consecutive_429: int = 0
    last_429_timestamp: float = 0.0
    base_sleep: float = 2.0
    max_sleep: float = 60.0


class AdaptiveBackoff:

    def __init__(self):
        self.state = BackoffState()

    def record_429(self):

        self.state.consecutive_429 += 1
        self.state.last_429_timestamp = time.time()

    def record_success(self):

        if self.state.consecutive_429 > 0:
            self.state.consecutive_429 -= 1

    def compute_sleep(self):

        if self.state.consecutive_429 == 0:
            return 0

        sleep_time = min(
            self.state.base_sleep * (2 ** max(0, self.state.consecutive_429 - 1)),
            self.state.max_sleep
        )

        return sleep_time
