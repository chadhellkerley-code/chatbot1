"""External service integrations for optional send backends."""

from .adapter import send_message
from .android_sim_adapter import AndroidSimAdapter

__all__ = ["AndroidSimAdapter", "send_message"]
