from __future__ import annotations

import json

from src.inbox import endpoint_reader
from src.inbox.endpoint_reader import InboxEndpointError, _AccountEndpointClient, _response_error_kind


class _FakeResponse:
    def __init__(self, *, status_code: int, url: str, payload: dict[str, object]) -> None:
        self.status_code = status_code
        self.url = url
        self._payload = dict(payload)
        self.text = json.dumps(payload)

    def json(self) -> dict[str, object]:
        return dict(self._payload)


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, *, headers: dict[str, str], timeout: tuple[float, float], allow_redirects: bool) -> _FakeResponse:
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "timeout": tuple(timeout),
                "allow_redirects": bool(allow_redirects),
            }
        )
        return self._response

    def close(self) -> None:
        return None


class _SequenceSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, *, headers: dict[str, str], timeout: tuple[float, float], allow_redirects: bool) -> _FakeResponse:
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "timeout": tuple(timeout),
                "allow_redirects": bool(allow_redirects),
            }
        )
        if not self._responses:
            raise AssertionError("no more fake responses queued")
        return self._responses.pop(0)

    def close(self) -> None:
        return None


def _build_client(response: _FakeResponse) -> _AccountEndpointClient:
    client = _AccountEndpointClient.__new__(_AccountEndpointClient)
    client._account = {"username": "acc-1"}
    client._profiles_root = None
    client._account_id = "acc-1"
    client._session = _FakeSession(response)
    return client


def test_response_error_kind_ignores_read_receipts_disabled_in_valid_payload() -> None:
    payload = {
        "thread": {
            "items": [{"item_id": "1", "message_id": "mid.1", "text": "hola"}],
            "read_receipts_disabled": 0,
        }
    }

    kind = _response_error_kind(
        status_code=200,
        response_url="https://www.instagram.com/api/v1/direct_v2/threads/123/",
        response_text=json.dumps(payload),
    )

    assert kind is None


def test_fetch_json_candidates_returns_valid_thread_payload_with_read_receipts_disabled() -> None:
    payload = {
        "thread": {
            "items": [{"item_id": "1", "message_id": "mid.1", "text": "hola"}],
            "read_receipts_disabled": 0,
        }
    }
    client = _build_client(
        _FakeResponse(
            status_code=200,
            url="https://www.instagram.com/api/v1/direct_v2/threads/123/",
            payload=payload,
        )
    )

    result = client.fetch_json_candidates(
        ["https://www.instagram.com/api/v1/direct_v2/threads/123/"],
        referer="https://www.instagram.com/direct/inbox/",
        timeout_seconds=5.0,
    )

    assert result == payload
    assert len(client._session.calls) == 1


def test_fetch_json_candidates_still_detects_disabled_account_surface() -> None:
    payload = {"message": "Your account has been disabled for violating our terms."}
    client = _build_client(
        _FakeResponse(
            status_code=200,
            url="https://www.instagram.com/accounts/disabled/",
            payload=payload,
        )
    )

    try:
        client.fetch_json_candidates(
            ["https://www.instagram.com/accounts/disabled/"],
            referer="https://www.instagram.com/direct/inbox/",
            timeout_seconds=5.0,
        )
    except InboxEndpointError as exc:
        assert exc.kind == "banned"
    else:
        raise AssertionError("expected InboxEndpointError")


def test_sync_account_threads_from_storage_uses_requested_limit_above_40(monkeypatch) -> None:
    captured_urls: list[str] = []

    class _FakeEndpointClient:
        def __init__(self, account, *, profiles_root=None) -> None:
            del account, profiles_root
            self.account_id = "acc-1"

        def fetch_json_candidates(self, urls, *, referer, timeout_seconds):
            del referer, timeout_seconds
            captured_urls.extend(str(url) for url in urls)
            return {}

        def close(self) -> None:
            return None

    monkeypatch.setattr(endpoint_reader, "_AccountEndpointClient", _FakeEndpointClient)

    rows = endpoint_reader.sync_account_threads_from_storage(
        {"username": "acc-1"},
        thread_limit=70,
        max_pages=1,
    )

    assert rows == []
    assert captured_urls
    assert any("limit=70" in url for url in captured_urls)


def test_fetch_json_candidates_retries_rate_limit_and_recovers(monkeypatch) -> None:
    payload = {
        "thread": {
            "items": [{"item_id": "1", "message_id": "mid.1", "text": "hola"}],
            "read_receipts_disabled": 0,
        }
    }
    client = _AccountEndpointClient.__new__(_AccountEndpointClient)
    client._account = {"username": "acc-1"}
    client._profiles_root = None
    client._account_id = "acc-1"
    client._session = _SequenceSession(
        [
            _FakeResponse(
                status_code=429,
                url="https://www.instagram.com/api/v1/direct_v2/threads/123/",
                payload={"message": "Please wait a few minutes before you try again."},
            ),
            _FakeResponse(
                status_code=200,
                url="https://www.instagram.com/api/v1/direct_v2/threads/123/",
                payload=payload,
            ),
        ]
    )
    monkeypatch.setattr(endpoint_reader.time, "sleep", lambda *_args, **_kwargs: None)

    result = client.fetch_json_candidates(
        ["https://www.instagram.com/api/v1/direct_v2/threads/123/"],
        referer="https://www.instagram.com/direct/inbox/",
        timeout_seconds=5.0,
    )

    assert result == payload
    assert len(client._session.calls) == 2


def test_sync_account_threads_from_storage_uses_cookie_identity_for_payload_parsing(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeEndpointClient:
        def __init__(self, account, *, profiles_root=None) -> None:
            del account, profiles_root
            self.account_id = "acc-1"
            self.self_user_id = "42"

        def fetch_json_candidates(self, urls, *, referer, timeout_seconds):
            del urls, referer, timeout_seconds
            return {"threads": []}

        def close(self) -> None:
            return None

    def _fake_extract(payload, *, self_user_id, self_username, message_limit, thread_limit):
        captured["payload"] = payload
        captured["self_user_id"] = self_user_id
        captured["self_username"] = self_username
        captured["message_limit"] = message_limit
        captured["thread_limit"] = thread_limit
        return []

    monkeypatch.setattr(endpoint_reader, "_AccountEndpointClient", _FakeEndpointClient)
    monkeypatch.setattr(endpoint_reader, "_extract_inbox_threads_from_payload", _fake_extract)

    rows = endpoint_reader.sync_account_threads_from_storage(
        {"username": "acc-1"},
        thread_limit=25,
        message_limit=9,
        max_pages=1,
    )

    assert rows == []
    assert captured["self_user_id"] == "42"
    assert captured["self_username"] == "acc-1"
    assert captured["message_limit"] == 9
    assert captured["thread_limit"] == 25


def test_read_thread_from_storage_uses_cookie_identity_for_message_parsing(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeEndpointClient:
        def __init__(self, account, *, profiles_root=None) -> None:
            del account, profiles_root
            self.account_id = "acc-1"
            self.self_user_id = "42"

        def fetch_json_candidates(self, urls, *, referer, timeout_seconds):
            del urls, referer, timeout_seconds
            return {"thread": {"items": []}}

        def close(self) -> None:
            return None

    def _fake_payload_to_messages(payload, *, self_user_id):
        captured["payload"] = payload
        captured["self_user_id"] = self_user_id
        return []

    monkeypatch.setattr(endpoint_reader, "_AccountEndpointClient", _FakeEndpointClient)
    monkeypatch.setattr(endpoint_reader, "_payload_to_messages", _fake_payload_to_messages)

    result = endpoint_reader.read_thread_from_storage(
        {"username": "acc-1"},
        thread_id="340282366841710300949128198730550503704",
    )

    assert result["messages"] == []
    assert captured["self_user_id"] == "42"
