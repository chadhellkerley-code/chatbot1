import threading
from contextlib import contextmanager


class AccountRuntimeLeaseManager:
    """
    Garantiza que solo un runtime pueda operar una cuenta a la vez.
    """

    _locks = {}
    _global_lock = threading.Lock()

    @classmethod
    def _get_lock(cls, account_username: str) -> threading.Lock:
        with cls._global_lock:
            if account_username not in cls._locks:
                cls._locks[account_username] = threading.Lock()
            return cls._locks[account_username]

    @classmethod
    @contextmanager
    def lease(cls, account_username: str):
        lock = cls._get_lock(account_username)
        acquired = lock.acquire(blocking=False)

        if not acquired:
            raise RuntimeError(
                f"Account '{account_username}' already has an active runtime."
            )

        try:
            yield
        finally:
            lock.release()
