from __future__ import annotations

import logging
import threading
from typing import Callable

from core.inbox.message_queue import MessageQueue, MessageQueueTask

logger = logging.getLogger(__name__)


class MessageSenderWorker(threading.Thread):
    def __init__(
        self,
        *,
        index: int,
        task_queue: MessageQueue,
        handler: Callable[[MessageQueueTask], None],
        name: str = "inbox-sender",
    ) -> None:
        super().__init__(name=f"{name}-{index}", daemon=True)
        self._task_queue = task_queue
        self._handler = handler
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            task = self._task_queue.get(timeout=0.5)
            if task is None:
                continue
            try:
                self._handler(task)
            except Exception:
                logger.exception("Message sender worker failed on task=%s", task.task_type)
            finally:
                self._task_queue.task_done(task)
