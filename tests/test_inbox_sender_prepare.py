from __future__ import annotations

from core.inbox.account_worker import AccountWorker
from core.inbox.browser_pool import BrowserPool


def test_browser_pool_revalidates_active_thread_before_reusing_worker() -> None:
    class _FakeWorker:
        def __init__(self) -> None:
            self.account_id = "acc1"
            self.prepare_calls: list[dict[str, object]] = []

        def prepare(self, thread_row: dict[str, object]) -> dict[str, object]:
            self.prepare_calls.append(dict(thread_row))
            return {"ok": True, "reason": "revalidated"}

    worker = _FakeWorker()
    pool = BrowserPool(lambda _account_id: {"username": "acc1"})
    pool._worker = worker  # type: ignore[assignment]
    pool._active_thread_key = "acc1:thread-a"

    result = pool.prepare(
        {
            "thread_key": "acc1:thread-a",
            "thread_id": "thread-a",
            "account_id": "acc1",
        }
    )

    assert result == {"ok": True, "reason": "revalidated"}
    assert worker.prepare_calls == [
        {
            "thread_key": "acc1:thread-a",
            "thread_id": "thread-a",
            "account_id": "acc1",
        }
    ]


def test_account_worker_rebuilds_same_thread_client_when_cached_prepare_is_stale(monkeypatch) -> None:
    created_clients: list[object] = []
    focus_calls: list[str] = []
    ready_sequences = iter(
        [
            [(True, "ok"), (False, "composer_not_found")],
            [(True, "ok")],
        ]
    )

    class _FakePreparedRuntime:
        def __init__(self, account: dict[str, object]) -> None:
            self.account = dict(account)
            self.shutdown_calls = 0

        def shutdown(self) -> None:
            self.shutdown_calls += 1

        def run_async(self, _coro, *, timeout: float | None = None):
            del timeout
            return None

        def open_page(self, _account: dict[str, object], *, timeout: float | None = None):
            del timeout
            return object()

        def close_page(self, _page, *, timeout: float | None = None) -> None:
            del timeout
            return None

    class _FakeClient:
        def __init__(self, *_args, thread_id: str, **_kwargs) -> None:
            self.thread_id = thread_id
            self.closed = False
            self._ready_results = list(next(ready_sequences))
            created_clients.append(self)

        def ensure_thread_ready_strict(self, _thread_id: str) -> tuple[bool, str]:
            return self._ready_results.pop(0)

        def close(self) -> None:
            self.closed = True

        def _ensure_page(self):
            return object()

    monkeypatch.setattr("core.inbox.account_worker._PreparedRuntime", _FakePreparedRuntime)
    monkeypatch.setattr("core.inbox.account_worker.TaskDirectClient", _FakeClient)
    monkeypatch.setattr(
        "core.inbox.account_worker.AccountWorker._focus_composer",
        lambda self: focus_calls.append(str(self.thread_key or "")),
    )

    worker = AccountWorker({"username": "acc1"})
    thread_row = {
        "thread_key": "acc1:thread-a",
        "thread_id": "thread-a",
        "thread_href": "https://www.instagram.com/direct/t/thread-a/",
    }

    first = worker.prepare(thread_row)
    second = worker.prepare(thread_row)

    assert first["ok"] is True
    assert second["ok"] is True
    assert len(created_clients) == 2
    assert created_clients[0].closed is True
    assert created_clients[1].closed is False
    assert len(focus_calls) == 2
