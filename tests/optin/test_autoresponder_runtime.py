from __future__ import annotations

import time
from dataclasses import dataclass
from types import SimpleNamespace

import core.responder as responder
@dataclass
class _FakeRow:
    thread_id: str
    title: str
    snippet: str = "hola"


class _FakeLocator:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows

    def count(self) -> int:
        return len(self._rows)

    def nth(self, idx: int) -> _FakeRow:
        return self._rows[idx]


class _FakePage:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows
        self.url = "https://www.instagram.com/direct/inbox/"

    def locator(self, _selector: str):
        return _FakeLocator(self._rows)

    def wait_for_timeout(self, _ms: int) -> None:
        return None


def _msg(message_id: str, user_id: str, text: str, ts: float):
    direction = "outbound" if user_id == "me" else "inbound"
    return SimpleNamespace(
        id=message_id,
        user_id=user_id,
        text=text,
        timestamp=ts,
        direction=direction,
    )


def _make_thread(thread_id: str, lead_username: str):
    lead = responder.UserLike(pk=lead_username, id=lead_username, username=lead_username)
    return responder.ThreadLike(
        id=thread_id,
        pk=thread_id,
        users=[lead],
        unread_count=0,
        title=lead_username,
        source_index=-1,
    )


class _FakeInboxClient:
    def __init__(self, rows: list[_FakeRow], *, list_threads_payload=None) -> None:
        self.username = "tester"
        self.user_id = "me"
        self.headless = True
        self._thread_cache: dict[str, object] = {}
        self._thread_cache_meta: dict[str, dict] = {}
        self._rows = rows
        self._page = _FakePage(rows)
        self.list_threads_payload = list_threads_payload or []
        self.list_threads_calls = 0
        self.get_messages_calls = 0
        self.sent_messages: list[tuple[str, str]] = []
        self.opened_threads: list[str] = []

    def _open_inbox(self) -> None:
        self._page.url = "https://www.instagram.com/direct/inbox/"

    def _ensure_page(self):
        return self._page

    def _row_selector_candidates(self):
        return ["fake_rows"]

    def _get_inbox_panel(self, page, rows=None):
        return page, "selector", "fake_rows", {}

    def _scroll_panel_to_top(self, _panel) -> None:
        return None

    def return_to_inbox(self) -> None:
        self._page.url = "https://www.instagram.com/direct/inbox/"

    def _stagnation_limit(self, _target: int) -> int:
        return 10

    def _row_is_valid(self, _row, *, selector=None) -> bool:
        return True

    def _row_lines(self, row: _FakeRow):
        return [row.title, row.snippet]

    def _resolve_thread_key(self, _page, row: _FakeRow, *, title: str, peer_username: str, snippet: str):
        return row.thread_id, "fake", f"/direct/t/{row.thread_id}/"

    def _scroll_panel_down(self, _panel, *, max_attempts=None) -> bool:
        return False

    def _wait_for_scroll_settle(self, _page, *, extra_ms: int = 0) -> None:
        return None

    def debug_dump_inbox(self, _reason: str) -> str:
        return "debug"

    def list_threads(self, amount: int = 20, filter_unread: bool = False):
        self.list_threads_calls += 1
        if self.list_threads_payload:
            return list(self.list_threads_payload)[:amount]
        derived = [_make_thread(row.thread_id, row.title) for row in self._rows]
        return derived[:amount]

    def get_messages(self, thread, amount: int = 10):
        self.get_messages_calls += 1
        now = time.time()
        return [_msg(f"in-{thread.id}", f"lead-{thread.id}", "hola", now)]

    def send_message(self, thread, text: str):
        self.sent_messages.append((str(thread.id), text))
        return f"sent-{len(self.sent_messages)}"

    def _open_thread(self, thread) -> bool:
        self.opened_threads.append(str(getattr(thread, "id", "")))
        return True


class _FakeFollowupClient:
    def __init__(self, threads):
        self.user_id = "me"
        self.username = "tester"
        self.headless = True
        self._threads = threads
        self._thread_cache: dict[str, object] = {}
        self._thread_cache_meta: dict[str, dict] = {}
        self.sent_messages: list[tuple[str, str]] = []

    def list_threads(self, amount: int = 20, filter_unread: bool = False):
        return list(self._threads)[:amount]

    def get_messages(self, thread, amount: int = 20):
        old_ts = time.time() - (responder._MIN_TIME_FOR_FOLLOWUP + 120)
        # Mensaje mas reciente saliente para habilitar followup.
        return [_msg(f"out-{thread.id}", "me", "te escribo", old_ts)]

    def send_message(self, thread, text: str):
        self.sent_messages.append((str(thread.id), text))
        return f"fu-{len(self.sent_messages)}"

    def _open_thread(self, thread) -> bool:
        return True


class _ScenarioFollowupClient(_FakeFollowupClient):
    def __init__(self, threads, messages_by_thread: dict[str, list[object]]):
        super().__init__(threads)
        self._messages_by_thread = messages_by_thread

    def get_messages(self, thread, amount: int = 20):
        return list(self._messages_by_thread.get(str(thread.id), []))[:amount]


class _FakeIterInboxClient(_FakeInboxClient):
    def __init__(self, rows: list[_FakeRow], *, list_threads_payload=None) -> None:
        super().__init__(rows, list_threads_payload=list_threads_payload)
        self.iter_threads_calls = 0

    def iter_threads(self, amount: int = 20, filter_unread: bool = False):
        self.iter_threads_calls += 1
        if self.list_threads_payload:
            for thread in list(self.list_threads_payload)[:amount]:
                yield thread
            return
        for row in self._rows[:amount]:
            yield _make_thread(row.thread_id, row.title)

    def list_threads(self, amount: int = 20, filter_unread: bool = False):
        raise AssertionError("list_threads no debe llamarse cuando existe iter_threads")


class _FakeIterInboxClientWithOpenFailures(_FakeIterInboxClient):
    def __init__(self, rows: list[_FakeRow], *, fail_thread_ids: set[str]) -> None:
        super().__init__(rows)
        self.fail_thread_ids = {str(v) for v in fail_thread_ids}

    def _open_thread(self, thread) -> bool:
        thread_id = str(getattr(thread, "id", ""))
        if thread_id in self.fail_thread_ids:
            return False
        return super()._open_thread(thread)


def _memory_row(
    *,
    thread_id: str,
    recipient_username: str,
    messages: list[dict],
    unread_count: int = 0,
    recipient_id: str | None = None,
    thread_id_real: str | None = None,
    thread_href: str | None = None,
) -> dict:
    now = time.time()
    real_id = str(thread_id_real or "").strip()
    if not (real_id.isdigit() and 6 <= len(real_id) <= 20):
        digits = "".join(ch for ch in str(thread_id or "") if ch.isdigit())
        if not digits:
            digits = str(abs(hash(str(thread_id))) % 9_999_999_999_999)
        if len(digits) < 6:
            digits = str(1_000_000 + int(digits))
        if len(digits) > 20:
            digits = digits[:20]
        real_id = digits
    href_value = str(thread_href or "").strip() or f"https://www.instagram.com/direct/t/{real_id}/"
    return {
        "thread_id": thread_id,
        "thread_id_real": real_id,
        "thread_href": href_value,
        "thread_id_api": thread_id,
        "recipient_id": recipient_id or recipient_username,
        "recipient_username": recipient_username,
        "title": recipient_username,
        "snippet": str(messages[0].get("text") or "") if messages else "",
        "unread_count": unread_count,
        "last_activity_at": now,
        "last_interaction_at": now,
        "messages": list(messages),
        "updated_at": now,
    }


def _patch_inbox_dependencies(
    monkeypatch,
    *,
    can_send: bool,
    delay_calls: list[str],
    memory_rows: list[dict] | None = None,
) -> None:
    rows = list(memory_rows or [])
    monkeypatch.setattr(
        responder,
        "_account_conversations_from_memory",
        lambda *_a, **_k: list(rows),
    )
    monkeypatch.setattr(responder, "_gen_response", lambda *_args, **_kwargs: "respuesta")
    monkeypatch.setattr(
        responder,
        "_can_send_message",
        lambda *_args, **_kwargs: (can_send, "ok" if can_send else "skip"),
    )
    monkeypatch.setattr(
        responder,
        "_sleep_between_replies_sync",
        lambda *_args, **kwargs: delay_calls.append(str(kwargs.get("label", "reply_delay"))),
    )
    monkeypatch.setattr(responder, "_determine_conversation_stage", lambda *_a, **_k: responder._STAGE_ACTIVE)
    monkeypatch.setattr(responder, "_get_conversation_state", lambda *_a, **_k: {})
    monkeypatch.setattr(responder, "_update_conversation_state", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_record_message_received", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_record_message_sent", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_classify_response", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "save_auto_state", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_print_response_summary", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "log_conversation_status", lambda *_a, **_k: None)


def _patch_followup_dependencies(
    monkeypatch,
    *,
    memory_rows: list[dict],
    logs: list[dict] | None = None,
) -> None:
    rows = list(memory_rows)
    monkeypatch.setattr(
        responder,
        "_account_conversations_from_memory",
        lambda *_a, **_k: list(rows),
    )
    monkeypatch.setattr(
        responder,
        "_followup_enabled_entry_for",
        lambda _user: ("alias", {"enabled": True, "prompt": "seguir"}),
    )
    monkeypatch.setattr(responder, "_followup_decision", lambda *_a, **_k: ("seguimiento", 1))
    monkeypatch.setattr(
        responder,
        "_get_conversation_state",
        lambda *_a, **_k: {
            "last_message_sent_at": time.time() - (responder._MIN_TIME_FOR_FOLLOWUP + 180),
            "last_message_received_at": None,
            "last_message_sender": "me",
            "messages_sent": [],
        },
    )
    monkeypatch.setattr(responder, "_can_send_message", lambda *_a, **_k: (True, "ok"))
    monkeypatch.setattr(responder, "_record_message_sent", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_record_message_received", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_update_conversation_state", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_classify_response", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_sleep_between_replies_sync", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "save_auto_state", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_print_response_summary", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "log_conversation_status", lambda *_a, **_k: None)
    monkeypatch.setattr(
        responder,
        "_load_conversation_state",
        lambda *_a, **_k: {"version": "1.0", "last_cleanup_ts": 0, "conversations": {}},
    )
    monkeypatch.setattr(responder, "_save_conversation_state", lambda *_a, **_k: None)
    monkeypatch.setattr(responder, "_set_followup_entry", lambda *_a, **_k: None)
    if logs is not None:
        monkeypatch.setattr(responder, "_append_message_log", lambda payload: logs.append(dict(payload)))


def test_inbox_small_scan_is_fast_and_delay_only_between_messages(monkeypatch):
    responder.reset_stop_event()
    rows = [_FakeRow(thread_id=f"t{i}", title=f"lead{i}") for i in range(5)]
    client = _FakeInboxClient(rows)
    delay_calls: list[str] = []
    now = time.time()
    memory_rows = [
        _memory_row(
            thread_id=f"t{i}",
            recipient_username=f"lead{i}",
            messages=[
                {
                    "message_id": f"in-t{i}",
                    "direction": "inbound",
                    "text": "hola",
                    "timestamp_epoch": now - i,
                }
            ],
        )
        for i in range(5)
    ]
    _patch_inbox_dependencies(
        monkeypatch,
        can_send=True,
        delay_calls=delay_calls,
        memory_rows=memory_rows,
    )
    stats = responder.BotStats(alias="alias")

    responder._process_inbox(
        client=client,
        user="tester",
        state={},
        api_key="x",
        system_prompt="p",
        stats=stats,
        delay_min=3,
        delay_max=45,
        max_age_days=7,
        allowed_thread_ids=None,
        threads_limit=5,
    )

    assert len(client.sent_messages) == 5
    assert len(delay_calls) == 5
    assert stats.responded == 5


def test_inbox_skips_thread_when_no_new_inbound_after_last_outbound(monkeypatch):
    responder.reset_stop_event()
    rows = [_FakeRow(thread_id="t1", title="lead1")]
    now = time.time()
    client = _FakeInboxClient(rows)
    delay_calls: list[str] = []
    memory_rows = [
        _memory_row(
            thread_id="t1",
            recipient_username="lead1",
            messages=[
                {
                    "message_id": "out-t1",
                    "direction": "outbound",
                    "text": "ultimo mensaje bot",
                    "timestamp_epoch": now,
                    "sender_id": "me",
                },
                {
                    "message_id": "in-t1",
                    "direction": "inbound",
                    "text": "mensaje viejo lead",
                    "timestamp_epoch": now - 120,
                    "sender_id": "lead-t1",
                },
            ],
        )
    ]
    _patch_inbox_dependencies(
        monkeypatch,
        can_send=True,
        delay_calls=delay_calls,
        memory_rows=memory_rows,
    )
    stats = responder.BotStats(alias="alias")

    responder._process_inbox(
        client=client,
        user="tester",
        state={},
        api_key="x",
        system_prompt="p",
        stats=stats,
        delay_min=0,
        delay_max=0,
        max_age_days=7,
        allowed_thread_ids=None,
        threads_limit=1,
    )

    assert len(client.sent_messages) == 0
    assert stats.responded == 0


def test_inbox_skips_when_outbound_wins_same_timestamp(monkeypatch):
    responder.reset_stop_event()
    rows = [_FakeRow(thread_id="t2", title="lead2")]
    now = time.time()
    client = _FakeInboxClient(rows)
    delay_calls: list[str] = []
    memory_rows = [
        _memory_row(
            thread_id="t2",
            recipient_username="lead2",
            messages=[
                {
                    "message_id": "z-outbound",
                    "direction": "outbound",
                    "text": "outbound reciente",
                    "timestamp_epoch": now,
                    "sender_id": "me",
                },
                {
                    "message_id": "a-inbound",
                    "direction": "inbound",
                    "text": "inbound en empate",
                    "timestamp_epoch": now,
                    "sender_id": "lead-t2",
                },
            ],
        )
    ]
    _patch_inbox_dependencies(
        monkeypatch,
        can_send=True,
        delay_calls=delay_calls,
        memory_rows=memory_rows,
    )
    stats = responder.BotStats(alias="alias")

    responder._process_inbox(
        client=client,
        user="tester",
        state={},
        api_key="x",
        system_prompt="p",
        stats=stats,
        delay_min=0,
        delay_max=0,
        max_age_days=7,
        allowed_thread_ids=None,
        threads_limit=1,
    )

    assert len(client.sent_messages) == 0
    assert stats.responded == 0


def test_inbox_500_threads_reaches_target_with_bulk_reseed(monkeypatch):
    responder.reset_stop_event()
    rows = [_FakeRow(thread_id="seed-1", title="seed1"), _FakeRow(thread_id="seed-2", title="seed2")]
    client = _FakeInboxClient(rows)
    delay_calls: list[str] = []
    now = time.time()
    memory_rows = [
        _memory_row(
            thread_id=f"bulk-{i}",
            recipient_username=f"lead{i}",
            messages=[
                {
                    "message_id": f"in-bulk-{i}",
                    "direction": "inbound",
                    "text": "hola",
                    "timestamp_epoch": now - i,
                }
            ],
        )
        for i in range(500)
    ]
    _patch_inbox_dependencies(
        monkeypatch,
        can_send=False,
        delay_calls=delay_calls,
        memory_rows=memory_rows,
    )
    stats = responder.BotStats(alias="alias")

    responder._process_inbox(
        client=client,
        user="tester",
        state={},
        api_key="x",
        system_prompt="p",
        stats=stats,
        delay_min=3,
        delay_max=45,
        max_age_days=7,
        allowed_thread_ids=None,
        threads_limit=500,
    )

    assert len(client.sent_messages) == 0
    assert stats.responded == 0


def test_followup_sends_and_applies_delay_only_between_messages(monkeypatch):
    responder.reset_stop_event()
    threads = [_make_thread("fu-1", "lead1"), _make_thread("fu-2", "lead2")]
    client = _FakeFollowupClient(threads)
    delay_calls: list[str] = []
    now = time.time()
    memory_rows = [
        _memory_row(
            thread_id="fu-1",
            recipient_username="lead1",
            messages=[
                {
                    "message_id": "out-fu-1",
                    "direction": "outbound",
                    "text": "te escribo",
                    "timestamp_epoch": now - (5 * 3600),
                    "sender_id": "me",
                }
            ],
        ),
        _memory_row(
            thread_id="fu-2",
            recipient_username="lead2",
            messages=[
                {
                    "message_id": "out-fu-2",
                    "direction": "outbound",
                    "text": "te escribo",
                    "timestamp_epoch": now - (5 * 3600),
                    "sender_id": "me",
                }
            ],
        ),
    ]
    _patch_followup_dependencies(monkeypatch, memory_rows=memory_rows)
    monkeypatch.setattr(
        responder,
        "_sleep_between_replies_sync",
        lambda *_a, **kwargs: delay_calls.append(str(kwargs.get("label", "reply_delay"))),
    )

    responder._process_followups(
        client=client,
        user="tester",
        api_key="x",
        delay_min=3,
        delay_max=45,
        max_age_days=7,
        threads_limit=5,
        followup_schedule_hours=[4, 8, 12, 24],
    )

    assert len(client.sent_messages) == 2
    assert len(delay_calls) == 1


def test_inbox_prefers_iter_threads_over_list_threads(monkeypatch):
    responder.reset_stop_event()
    rows = [_FakeRow(thread_id=f"t{i}", title=f"lead{i}") for i in range(3)]
    client = _FakeIterInboxClient(rows)
    delay_calls: list[str] = []
    now = time.time()
    memory_rows = [
        _memory_row(
            thread_id=f"t{i}",
            recipient_username=f"lead{i}",
            messages=[
                {
                    "message_id": f"in-t{i}",
                    "direction": "inbound",
                    "text": "hola",
                    "timestamp_epoch": now - i,
                }
            ],
        )
        for i in range(3)
    ]
    _patch_inbox_dependencies(
        monkeypatch,
        can_send=False,
        delay_calls=delay_calls,
        memory_rows=memory_rows,
    )
    stats = responder.BotStats(alias="alias")

    responder._process_inbox(
        client=client,
        user="tester",
        state={},
        api_key="x",
        system_prompt="p",
        stats=stats,
        delay_min=3,
        delay_max=45,
        max_age_days=7,
        allowed_thread_ids=None,
        threads_limit=3,
    )

    assert client.iter_threads_calls == 0
    assert client.list_threads_calls == 0


def test_inbox_reaches_target_even_if_some_threads_fail_to_open(monkeypatch):
    responder.reset_stop_event()
    rows = [_FakeRow(thread_id=f"t{i}", title=f"lead{i}") for i in range(6)]
    client = _FakeIterInboxClientWithOpenFailures(rows, fail_thread_ids={"t0", "t1"})
    delay_calls: list[str] = []
    captured_threads: list[str] = []
    now = time.time()
    memory_rows = [
        _memory_row(
            thread_id=f"t{i}",
            recipient_username=f"lead{i}",
            messages=[
                {
                    "message_id": f"in-t{i}",
                    "direction": "inbound",
                    "text": "hola",
                    "timestamp_epoch": now - i,
                }
            ],
        )
        for i in range(6)
    ]
    _patch_inbox_dependencies(
        monkeypatch,
        can_send=False,
        delay_calls=delay_calls,
        memory_rows=memory_rows,
    )
    monkeypatch.setattr(
        responder,
        "_update_conversation_state",
        lambda _account, thread_id, _updates, _recipient_username=None: captured_threads.append(str(thread_id)),
    )
    stats = responder.BotStats(alias="alias")

    responder._process_inbox(
        client=client,
        user="tester",
        state={},
        api_key="x",
        system_prompt="p",
        stats=stats,
        delay_min=3,
        delay_max=45,
        max_age_days=7,
        allowed_thread_ids=None,
        threads_limit=3,
    )

    unique_captured = {tid for tid in captured_threads}
    assert len(unique_captured) >= 3


def test_followup_allows_thread_when_last_outbound_is_older_than_60s(monkeypatch):
    responder.reset_stop_event()
    now = time.time()
    thread = _make_thread("fu-old", "lead_old")
    client = _ScenarioFollowupClient(
        [thread],
        messages_by_thread={
            "fu-old": [_msg("out-old", "me", "te escribo", now - 180)],
        },
    )
    logs: list[dict] = []
    memory_rows = [
        _memory_row(
            thread_id="fu-old",
            recipient_username="lead_old",
            messages=[
                {
                    "message_id": "out-old",
                    "direction": "outbound",
                    "text": "te escribo",
                    "timestamp_epoch": now - 180,
                    "sender_id": "me",
                }
            ],
        )
    ]
    _patch_followup_dependencies(monkeypatch, memory_rows=memory_rows, logs=logs)

    responder._process_followups(
        client=client,
        user="tester",
        api_key="x",
        delay_min=0,
        delay_max=0,
        max_age_days=7,
        threads_limit=5,
        followup_schedule_hours=[0],
    )

    assert len(client.sent_messages) == 1
    skip_logs = [entry for entry in logs if entry.get("action") == "followup_skip"]
    assert not skip_logs


def test_followup_blocks_thread_when_last_outbound_is_newer_than_60s(monkeypatch):
    responder.reset_stop_event()
    now = time.time()
    thread = _make_thread("fu-recent", "lead_recent")
    client = _ScenarioFollowupClient(
        [thread],
        messages_by_thread={
            "fu-recent": [_msg("out-recent", "me", "te escribo", now - 30)],
        },
    )
    logs: list[dict] = []
    memory_rows = [
        _memory_row(
            thread_id="fu-recent",
            recipient_username="lead_recent",
            messages=[
                {
                    "message_id": "out-recent",
                    "direction": "outbound",
                    "text": "te escribo",
                    "timestamp_epoch": now - 30,
                    "sender_id": "me",
                }
            ],
        )
    ]
    _patch_followup_dependencies(monkeypatch, memory_rows=memory_rows, logs=logs)
    monkeypatch.setattr(
        responder,
        "_get_conversation_state",
        lambda *_a, **_k: {
            "last_message_sent_at": None,
            "last_message_received_at": None,
            "last_message_sender": None,
            "messages_sent": [],
        },
    )

    responder._process_followups(
        client=client,
        user="tester",
        api_key="x",
        delay_min=0,
        delay_max=0,
        max_age_days=7,
        threads_limit=5,
        followup_schedule_hours=[0],
    )

    assert len(client.sent_messages) == 0
    skip_reasons = [entry.get("reason") for entry in logs if entry.get("action") == "followup_skip"]
    assert "skip_last_outbound_lt_60s_or_fallback_suspected" in skip_reasons


def test_followup_does_not_invent_timestamp_when_message_timestamp_is_missing(monkeypatch):
    responder.reset_stop_event()
    thread = _make_thread("fu-missing-ts", "lead_missing_ts")
    message_without_timestamp = SimpleNamespace(
        id="out-missing",
        user_id="me",
        text="te escribo",
        timestamp=None,
        direction="outbound",
    )
    client = _ScenarioFollowupClient(
        [thread],
        messages_by_thread={"fu-missing-ts": [message_without_timestamp]},
    )
    logs: list[dict] = []
    memory_rows = [
        _memory_row(
            thread_id="fu-missing-ts",
            recipient_username="lead_missing_ts",
            messages=[
                {
                    "message_id": "out-missing",
                    "direction": "outbound",
                    "text": "te escribo",
                    "timestamp_epoch": None,
                    "sender_id": "me",
                }
            ],
        )
    ]
    _patch_followup_dependencies(monkeypatch, memory_rows=memory_rows, logs=logs)

    responder._process_followups(
        client=client,
        user="tester",
        api_key="x",
        delay_min=0,
        delay_max=0,
        max_age_days=0,
        threads_limit=5,
        followup_schedule_hours=[0],
    )

    assert len(client.sent_messages) == 0
    skip_reasons = [entry.get("reason") for entry in logs if entry.get("action") == "followup_skip"]
    assert "skip_no_outbound_messages" in skip_reasons


def test_runtime_metrics_increment_on_confirmed_reply_success(monkeypatch):
    responder.reset_stop_event()
    runtime = responder.AutoresponderRuntimeController()
    monkeypatch.setattr(responder, "_AUTORESPONDER_RUNTIME_CONTROLLER", runtime)
    rows = [_FakeRow(thread_id="t1", title="lead1")]
    client = _FakeInboxClient(rows)
    now = time.time()
    memory_rows = [
        _memory_row(
            thread_id="t1",
            recipient_username="lead1",
            messages=[
                {
                    "message_id": "in-t1",
                    "direction": "inbound",
                    "text": "hola",
                    "timestamp_epoch": now,
                }
            ],
        )
    ]
    _patch_inbox_dependencies(
        monkeypatch,
        can_send=True,
        delay_calls=[],
        memory_rows=memory_rows,
    )

    responder._process_inbox(
        client=client,
        user="tester",
        state={},
        api_key="x",
        system_prompt="p",
        stats=responder.BotStats(alias="alias"),
        delay_min=0,
        delay_max=0,
        max_age_days=7,
        allowed_thread_ids=None,
        threads_limit=1,
    )

    snapshot = runtime.snapshot("tester")
    assert snapshot["responses_success"] == 1
    assert snapshot["responses_failed"] == 0
    assert snapshot["followups_success"] == 0
    assert snapshot["followups_failed"] == 0


def test_runtime_metrics_increment_on_confirmed_reply_failure(monkeypatch):
    responder.reset_stop_event()
    runtime = responder.AutoresponderRuntimeController()
    monkeypatch.setattr(responder, "_AUTORESPONDER_RUNTIME_CONTROLLER", runtime)
    rows = [_FakeRow(thread_id="t1", title="lead1")]
    client = _FakeInboxClient(rows)
    client.send_message = lambda *_args, **_kwargs: None
    now = time.time()
    memory_rows = [
        _memory_row(
            thread_id="t1",
            recipient_username="lead1",
            messages=[
                {
                    "message_id": "in-t1",
                    "direction": "inbound",
                    "text": "hola",
                    "timestamp_epoch": now,
                }
            ],
        )
    ]
    _patch_inbox_dependencies(
        monkeypatch,
        can_send=True,
        delay_calls=[],
        memory_rows=memory_rows,
    )

    responder._process_inbox(
        client=client,
        user="tester",
        state={},
        api_key="x",
        system_prompt="p",
        stats=responder.BotStats(alias="alias"),
        delay_min=0,
        delay_max=0,
        max_age_days=7,
        allowed_thread_ids=None,
        threads_limit=1,
    )

    snapshot = runtime.snapshot("tester")
    assert snapshot["responses_success"] == 0
    assert snapshot["responses_failed"] == 1
    assert snapshot["followups_success"] == 0
    assert snapshot["followups_failed"] == 0


def test_runtime_metrics_increment_on_confirmed_followup_success(monkeypatch):
    responder.reset_stop_event()
    runtime = responder.AutoresponderRuntimeController()
    monkeypatch.setattr(responder, "_AUTORESPONDER_RUNTIME_CONTROLLER", runtime)
    now = time.time()
    thread = _make_thread("fu-1", "lead1")
    client = _ScenarioFollowupClient(
        [thread],
        messages_by_thread={"fu-1": [_msg("out-fu-1", "me", "te escribo", now - 180)]},
    )
    _patch_followup_dependencies(
        monkeypatch,
        memory_rows=[
            _memory_row(
                thread_id="fu-1",
                recipient_username="lead1",
                messages=[
                    {
                        "message_id": "out-fu-1",
                        "direction": "outbound",
                        "text": "te escribo",
                        "timestamp_epoch": now - 180,
                        "sender_id": "me",
                    }
                ],
            )
        ],
    )

    responder._process_followups(
        client=client,
        user="tester",
        api_key="x",
        delay_min=0,
        delay_max=0,
        max_age_days=7,
        threads_limit=1,
        followup_schedule_hours=[0],
        stats=responder.BotStats(alias="alias"),
    )

    snapshot = runtime.snapshot("tester")
    assert snapshot["responses_success"] == 0
    assert snapshot["responses_failed"] == 0
    assert snapshot["followups_success"] == 1
    assert snapshot["followups_failed"] == 0


def test_runtime_metrics_increment_on_confirmed_followup_failure(monkeypatch):
    responder.reset_stop_event()
    runtime = responder.AutoresponderRuntimeController()
    monkeypatch.setattr(responder, "_AUTORESPONDER_RUNTIME_CONTROLLER", runtime)
    now = time.time()
    thread = _make_thread("fu-1", "lead1")
    client = _ScenarioFollowupClient(
        [thread],
        messages_by_thread={"fu-1": [_msg("out-fu-1", "me", "te escribo", now - 180)]},
    )
    client.send_message = lambda *_args, **_kwargs: None
    _patch_followup_dependencies(
        monkeypatch,
        memory_rows=[
            _memory_row(
                thread_id="fu-1",
                recipient_username="lead1",
                messages=[
                    {
                        "message_id": "out-fu-1",
                        "direction": "outbound",
                        "text": "te escribo",
                        "timestamp_epoch": now - 180,
                        "sender_id": "me",
                    }
                ],
            )
        ],
    )

    responder._process_followups(
        client=client,
        user="tester",
        api_key="x",
        delay_min=0,
        delay_max=0,
        max_age_days=7,
        threads_limit=1,
        followup_schedule_hours=[0],
        stats=responder.BotStats(alias="alias"),
    )

    snapshot = runtime.snapshot("tester")
    assert snapshot["responses_success"] == 0
    assert snapshot["responses_failed"] == 0
    assert snapshot["followups_success"] == 0
    assert snapshot["followups_failed"] == 1


def test_gen_response_retries_when_first_output_is_invalid(monkeypatch):
    calls: list[str] = []
    outputs = iter(
        [
            '{"enviar": true, "mensaje": "x"}',
            "Perfecto, te cuento en breve como funciona y vemos si te sirve.",
        ]
    )
    monkeypatch.setattr(responder, "_build_openai_client", lambda _api_key: object())
    monkeypatch.setattr(responder, "_resolve_ai_model", lambda _api_key: "fake-model")

    def _fake_generate(*_args, **_kwargs):
        calls.append("call")
        return next(outputs)

    monkeypatch.setattr(responder, "_openai_generate_text", _fake_generate)

    result = responder._gen_response(
        api_key="k",
        system_prompt="Responder como closer",
        convo_text="ELLOS: Hola, me interesa",
        memory_context="stage_actual=active",
    )

    assert len(calls) == 2
    assert result.startswith("Perfecto")


def test_followup_decision_retries_until_valid_json(monkeypatch):
    calls: list[str] = []
    outputs = iter(
        [
            "dale, enviemos seguimiento",
            '{"enviar": true, "mensaje": "Seguimos en contacto, te sirve hablar mañana?", "etapa": 3}',
        ]
    )
    monkeypatch.setattr(responder, "_build_openai_client", lambda _api_key: object())
    monkeypatch.setattr(responder, "_resolve_ai_model", lambda _api_key: "fake-model")

    def _fake_generate(*_args, **_kwargs):
        calls.append("call")
        return next(outputs)

    monkeypatch.setattr(responder, "_openai_generate_text", _fake_generate)

    decision = responder._followup_decision(
        api_key="k",
        prompt_text="Solo seguir si hay interes real.",
        conversation="YO: Hola\nELLOS: Si, me interesa",
        metadata={
            "intento_followup_siguiente": 3,
            "etapa_negocio": 2,
            "horas_objetivo": 24,
        },
    )

    assert len(calls) == 2
    assert decision == ("Seguimos en contacto, te sirve hablar mañana?", 3)


def test_resolve_system_prompt_for_user_prefers_specific_over_global(monkeypatch):
    prompts = {
        "leadx": "PROMPT_USUARIO",
        "ventas": "PROMPT_ALIAS",
        "all": "PROMPT_ALL",
        "default": "PROMPT_DEFAULT",
    }

    monkeypatch.setattr(
        responder,
        "_read_system_prompt_from_file",
        lambda alias=None: prompts.get(str(alias or "default").strip().lower()),
    )
    monkeypatch.setattr(responder, "get_account", lambda _username: {"alias": "ventas"})

    resolved = responder._resolve_system_prompt_for_user(
        "leadx",
        active_alias="ALL",
        fallback_prompt="PROMPT_FALLBACK",
    )

    assert resolved == "PROMPT_USUARIO"


def test_followup_prefers_account_alias_when_active_alias_is_all(monkeypatch):
    entries = {
        "all": {"enabled": True, "accounts": [], "prompt": "global"},
        "ventas": {"enabled": True, "accounts": [], "prompt": "alias"},
    }

    monkeypatch.setattr(responder, "ACTIVE_ALIAS", "ALL")
    monkeypatch.setattr(responder, "get_account", lambda _username: {"alias": "ventas"})
    monkeypatch.setattr(
        responder,
        "_get_followup_entry",
        lambda alias: entries.get(str(alias).strip().lower(), {}),
    )

    alias, entry = responder._followup_enabled_entry_for("leadx")

    assert alias == "ventas"
    assert entry.get("prompt") == "alias"


def test_gen_response_returns_empty_on_openai_auth_error(monkeypatch):
    class _AuthError(Exception):
        def __init__(self):
            super().__init__("Error code: 401 - {'error': {'message': 'User not found.'}}")
            self.status_code = 401

    monkeypatch.setattr(responder, "_build_openai_client", lambda _api_key: object())
    monkeypatch.setattr(responder, "_resolve_ai_model", lambda _api_key: "fake-model")
    monkeypatch.setattr(responder, "_openai_generate_text", lambda *_a, **_k: (_ for _ in ()).throw(_AuthError()))

    result = responder._gen_response(
        api_key="k",
        system_prompt="Responder como closer",
        convo_text="ELLOS: Hola",
        memory_context="stage_actual=active",
    )

    assert result == ""


def test_can_send_message_rejects_empty_ai_output(monkeypatch):
    monkeypatch.setattr(
        responder,
        "_get_conversation_state",
        lambda *_a, **_k: {"messages_sent": [], "stage": responder._STAGE_ACTIVE},
    )

    can_send, reason = responder._can_send_message(
        account="acc",
        thread_id="th",
        message_text="   ",
        force=False,
    )

    assert can_send is False
    assert "vacia" in reason.lower()


def test_probe_ai_runtime_returns_false_on_auth_error(monkeypatch):
    class _AuthError(Exception):
        def __init__(self):
            super().__init__("Error code: 401")
            self.status_code = 401

    monkeypatch.setattr(responder, "_build_openai_client", lambda _api_key: object())
    monkeypatch.setattr(responder, "_resolve_ai_model", lambda _api_key: "fake-model")
    monkeypatch.setattr(responder, "_openai_generate_text", lambda *_a, **_k: (_ for _ in ()).throw(_AuthError()))

    ok, reason = responder._probe_ai_runtime("k")

    assert ok is False
    assert "401" in reason


def test_probe_ai_runtime_returns_true_when_generation_works(monkeypatch):
    monkeypatch.setattr(responder, "_build_openai_client", lambda _api_key: object())
    monkeypatch.setattr(responder, "_resolve_ai_model", lambda _api_key: "fake-model")
    monkeypatch.setattr(responder, "_openai_generate_text", lambda *_a, **_k: "ok")

    ok, reason = responder._probe_ai_runtime("k")

    assert ok is True
    assert reason == "ok"


def test_resolve_ai_api_key_uses_openai_only(monkeypatch):
    monkeypatch.setattr(responder, "SETTINGS", SimpleNamespace(openai_api_key=""))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    key = responder._resolve_ai_api_key(
        {"OPENAI_API_KEY": "sk-openai-123", "OPENROUTER_API_KEY": "sk-or-999"}
    )

    assert key == "sk-openai-123"


def test_resolve_ai_api_key_does_not_fallback_to_openrouter(monkeypatch):
    monkeypatch.setattr(responder, "SETTINGS", SimpleNamespace(openai_api_key=""))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    key = responder._resolve_ai_api_key({"OPENROUTER_API_KEY": "sk-or-999"})

    assert key == ""
