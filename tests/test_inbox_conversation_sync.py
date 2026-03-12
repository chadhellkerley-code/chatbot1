from __future__ import annotations

from src.inbox.conversation_sync import (
    _append_cache_bust_query,
    _build_inbox_candidate_urls,
    _snapshot_to_thread_row,
)


def test_snapshot_to_thread_row_uses_last_activity_when_preview_has_no_timestamp() -> None:
    row = _snapshot_to_thread_row(
        {
            "thread_id": "thread-1",
            "recipient_username": "cliente1",
            "title": "Cliente 1",
            "snippet": "Mensaje entrante",
            "unread_count": 2,
            "last_activity_at": 1234.5,
            "messages": [],
        },
        account_id="acc1",
        account_alias="ventas",
    )

    assert row is not None
    assert row["thread_key"] == "acc1:thread-1"
    assert row["last_message_timestamp"] == 1234.5
    assert row["last_message_direction"] == "inbound"
    assert row["latest_customer_message_at"] == 1234.5
    assert row["last_message_text"] == "Mensaje entrante"


def test_build_inbox_candidate_urls_appends_cache_bust_nonce() -> None:
    urls = _build_inbox_candidate_urls(cursor="", limit=10, message_limit=5, nonce=123456)

    assert urls
    assert all("_cb=123456" in url for url in urls)


def test_build_inbox_candidate_urls_preserves_large_limit_requests() -> None:
    urls = _build_inbox_candidate_urls(cursor="", limit=70, message_limit=12, nonce=123456)

    assert urls
    assert any("limit=70" in url for url in urls)
    assert any("/api/v1/direct_v2/threads/" in url for url in urls)


def test_append_cache_bust_query_replaces_previous_nonce() -> None:
    url = _append_cache_bust_query("/api/v1/direct_v2/threads/1/?limit=20&_cb=111", nonce=222)

    assert url.endswith("limit=20&_cb=222")
