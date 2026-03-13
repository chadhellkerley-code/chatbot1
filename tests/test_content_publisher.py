import base64
import importlib
import json
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.content_publisher.content_api import app as content_api_app
from src.content_publisher.content_extract_service import ContentExtractService
from src.content_publisher.content_library_service import ContentLibraryService, ContentPublisherError
from src.content_publisher.content_publish_service import ContentPublishService
from src.content_publisher.session_client import create_authenticated_client


_PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"
)


def _write_png(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_PNG_BYTES)
    return path


def test_create_authenticated_client_reuses_playwright_storage_state(tmp_path: Path) -> None:
    profiles_root = tmp_path / "profiles"
    storage_state = profiles_root / "worker_one" / "storage_state.json"
    storage_state.parent.mkdir(parents=True, exist_ok=True)
    storage_state.write_text(
        json.dumps(
            {
                "cookies": [
                    {"name": "sessionid", "value": "session_value", "domain": ".instagram.com", "path": "/"},
                    {"name": "csrftoken", "value": "csrf_value", "domain": ".instagram.com", "path": "/"},
                    {"name": "ds_user_id", "value": "123456", "domain": ".instagram.com", "path": "/"},
                    {"name": "mid", "value": "mid_value", "domain": ".instagram.com", "path": "/"},
                ],
                "origins": [],
            }
        ),
        encoding="utf-8",
    )

    client = create_authenticated_client(
        {"username": "worker_one", "alias": "default"},
        reason="test",
        profiles_root=profiles_root,
    )
    try:
        assert client.username == "worker_one"
        assert client.cookie_map["sessionid"] == "session_value"
        assert client.cookie_map["csrftoken"] == "csrf_value"
        assert client.session.headers["X-CSRFToken"] == "csrf_value"
        assert client.session.cookies.get("sessionid", domain=".instagram.com", path="/") == "session_value"
    finally:
        client.close()


def test_create_authenticated_client_adds_instagram_headers_and_proxy_auth(tmp_path: Path) -> None:
    profiles_root = tmp_path / "profiles"
    storage_state = profiles_root / "worker_one" / "storage_state.json"
    storage_state.parent.mkdir(parents=True, exist_ok=True)
    storage_state.write_text(
        json.dumps(
            {
                "cookies": [
                    {"name": "sessionid", "value": "session_value", "domain": ".instagram.com", "path": "/"},
                    {"name": "csrftoken", "value": "csrf_value", "domain": ".instagram.com", "path": "/"},
                    {"name": "ds_user_id", "value": "123456", "domain": ".instagram.com", "path": "/"},
                ],
                "origins": [],
            }
        ),
        encoding="utf-8",
    )

    client = create_authenticated_client(
        {
            "username": "worker_one",
            "alias": "default",
            "accept_language": "es-UY,es;q=0.9",
            "x_ig_app_id": "12345",
            "x_asbd_id": "67890",
            "proxy_url": "http://127.0.0.1:9000",
            "proxy_user": "alice",
            "proxy_pass": "secret",
        },
        reason="test",
        profiles_root=profiles_root,
    )
    try:
        assert client.session.headers["Accept-Language"] == "es-UY,es;q=0.9"
        assert client.session.headers["X-IG-App-ID"] == "12345"
        assert client.session.headers["X-ASBD-ID"] == "67890"
        assert client.session.proxies == {
            "http": "http://alice:secret@127.0.0.1:9000",
            "https": "http://alice:secret@127.0.0.1:9000",
        }
    finally:
        client.close()


def test_create_authenticated_client_prefers_assigned_proxy_over_legacy_fields(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    profiles_root = tmp_path / "profiles"
    storage_state = profiles_root / "worker_one" / "storage_state.json"
    storage_state.parent.mkdir(parents=True, exist_ok=True)
    storage_state.write_text(
        json.dumps(
            {
                "cookies": [
                    {"name": "sessionid", "value": "session_value", "domain": ".instagram.com", "path": "/"},
                    {"name": "csrftoken", "value": "csrf_value", "domain": ".instagram.com", "path": "/"},
                    {"name": "ds_user_id", "value": "123456", "domain": ".instagram.com", "path": "/"},
                ],
                "origins": [],
            }
        ),
        encoding="utf-8",
    )
    proxies_path = tmp_path / "storage" / "accounts" / "proxies.json"
    proxies_path.parent.mkdir(parents=True, exist_ok=True)
    proxies_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "proxies": [
                    {
                        "id": "proxy-1",
                        "server": "http://127.0.0.1:9000",
                        "user": "alice",
                        "pass": "secret",
                        "active": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    client = create_authenticated_client(
        {
            "username": "worker_one",
            "alias": "default",
            "assigned_proxy_id": "proxy-1",
            "proxy_url": "http://127.0.0.1:9999",
            "proxy_user": "wrong",
            "proxy_pass": "wrong",
        },
        reason="test",
        profiles_root=profiles_root,
    )
    try:
        assert client.session.proxies == {
            "http": "http://alice:secret@127.0.0.1:9000",
            "https": "http://alice:secret@127.0.0.1:9000",
        }
    finally:
        client.close()


def test_content_library_service_exports_json_csv_zip(tmp_path: Path) -> None:
    library = ContentLibraryService(root_dir=tmp_path)
    image_entry = library.store_media_entry(
        source_profile="profile_one",
        media_type="image",
        media_files=[_write_png(tmp_path / "fixtures" / "image.png")],
        caption="Caption one",
        entry_key="img_one",
    )
    carousel_entry = library.store_media_entry(
        source_profile="profile_two",
        media_type="carousel",
        media_files=[
            _write_png(tmp_path / "fixtures" / "carousel_1.png"),
            _write_png(tmp_path / "fixtures" / "carousel_2.png"),
        ],
        caption="Caption two",
        entry_key="carousel_two",
    )

    json_path = library.export_json([image_entry["id"], carousel_entry["id"]], tmp_path / "exports" / "content.json")
    csv_path = library.export_csv([image_entry["id"], carousel_entry["id"]], tmp_path / "exports" / "content.csv")
    zip_path = library.export_zip([image_entry["id"], carousel_entry["id"]], tmp_path / "exports" / "content.zip")

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert len(payload) == 2
    assert payload[0]["media_path"].startswith("data/content_library/")
    assert "source_profile" in csv_path.read_text(encoding="utf-8")

    with zipfile.ZipFile(zip_path) as archive:
        names = set(archive.namelist())
    assert "metadata.json" in names
    assert any(name.endswith(".png") for name in names)


def test_content_extract_service_stores_image_and_carousel_entries(tmp_path: Path, monkeypatch) -> None:
    library = ContentLibraryService(root_dir=tmp_path)
    service = ContentExtractService(root_dir=tmp_path, library_service=library)

    class _FakeClient:
        def get_settings(self) -> dict:
            return {"cookies": {"sessionid": "s1", "csrftoken": "c1", "ds_user_id": "42"}}

    class _FakeProfileResponse:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = '{"ok": true}'

        def json(self) -> dict:
            return {
                "data": {
                    "user": {
                        "edge_owner_to_timeline_media": {
                            "edges": [
                                {
                                    "node": {
                                        "__typename": "GraphImage",
                                        "id": "101",
                                        "shortcode": "img101",
                                        "display_url": "https://cdn.example.com/image_101.jpg",
                                        "edge_media_to_caption": {
                                            "edges": [{"node": {"text": "Image caption"}}]
                                        },
                                    }
                                },
                                {
                                    "node": {
                                        "__typename": "GraphSidecar",
                                        "id": "202",
                                        "shortcode": "car202",
                                        "edge_media_to_caption": {
                                            "edges": [{"node": {"text": "Carousel caption"}}]
                                        },
                                        "edge_sidecar_to_children": {
                                            "edges": [
                                                {"node": {"display_url": "https://cdn.example.com/carousel_1.jpg"}},
                                                {"node": {"display_url": "https://cdn.example.com/carousel_2.jpg"}},
                                            ]
                                        },
                                    }
                                },
                            ]
                        }
                    }
                }
            }

    class _FakeDownloadResponse:
        status_code = 200
        headers = {"content-type": "image/jpeg"}

        def iter_content(self, chunk_size: int = 0):
            del chunk_size
            yield _PNG_BYTES

    class _FakeSession:
        def __init__(self) -> None:
            self.cookies = {}
            self.headers = {}
            self.trust_env = False

        def close(self) -> None:
            return None

        def get(self, url: str, **kwargs):
            params = kwargs.get("params") or {}
            if params:
                assert params["username"] == "profile_target"
                assert self.cookies["sessionid"] == "s1"
                assert kwargs["headers"]["X-CSRFToken"] == "c1"
                return _FakeProfileResponse()
            return _FakeDownloadResponse()

    monkeypatch.setattr(
        service,
        "_resolve_accounts",
        lambda account_ids, alias="": [{"username": "worker_one", "alias": alias or "default"}],
    )
    monkeypatch.setattr(service, "_create_authenticated_client", lambda account: _FakeClient())
    monkeypatch.setattr(service, "_close_authenticated_client", lambda account, client: None)
    monkeypatch.setattr("src.content_publisher.content_extract_service.requests.Session", _FakeSession)

    result = service.extract(
        alias="default",
        account_ids=["worker_one"],
        profile_urls=["https://instagram.com/profile_target"],
        posts_per_profile=2,
    )

    entries = library.list_entries()
    assert result["stored_count"] == 2
    assert len(entries) == 2
    assert {entry["media_type"] for entry in entries} == {"image", "carousel"}
    assert any(str(entry["media_path"]).endswith("manifest.json") for entry in entries)


def test_content_extract_service_endpoint_parses_captions_and_media(monkeypatch) -> None:
    service = ContentExtractService()

    class _FakeClient:
        def get_settings(self) -> dict:
            return {"cookies": {"sessionid": "s1", "csrftoken": "c1", "ds_user_id": "42"}}

    class _FakeResponse:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = '{"ok": true}'

        def json(self) -> dict:
            return {
                "data": {
                    "user": {
                        "edge_owner_to_timeline_media": {
                            "edges": [
                                {
                                    "node": {
                                        "__typename": "GraphImage",
                                        "id": "100",
                                        "shortcode": "ABC100",
                                        "display_url": "https://cdn.example.com/abc100.jpg",
                                        "edge_media_to_caption": {
                                            "edges": [{"node": {"text": "First caption"}}]
                                        },
                                    }
                                },
                                {
                                    "node": {
                                        "__typename": "GraphSidecar",
                                        "id": "200",
                                        "shortcode": "XYZ200",
                                        "edge_media_to_caption": {
                                            "edges": [{"node": {"text": "Second caption"}}]
                                        },
                                        "edge_sidecar_to_children": {
                                            "edges": [
                                                {"node": {"display_url": "https://cdn.example.com/xyz200_1.jpg"}},
                                                {"node": {"display_url": "https://cdn.example.com/xyz200_2.jpg"}},
                                            ]
                                        },
                                    }
                                },
                            ]
                        }
                    }
                }
            }

    calls: list[tuple] = []

    class _FakeSession:
        def __init__(self) -> None:
            self.cookies = {}
            self.headers = {}
            self.trust_env = False

        def close(self) -> None:
            return None

        def get(self, url: str, **kwargs):
            calls.append((url, kwargs.get("params"), kwargs.get("headers"), kwargs.get("timeout"), dict(self.cookies)))
            return _FakeResponse()

    monkeypatch.setattr("src.content_publisher.content_extract_service.requests.Session", _FakeSession)

    result = service._fetch_supported_media(_FakeClient(), "profile_target", desired_count=2)
    assert len(result) == 2
    assert result[0][1] == "image"
    assert result[0][0].caption_text == "First caption"
    assert result[0][0].media_urls == ["https://cdn.example.com/abc100.jpg"]
    assert result[1][1] == "carousel"
    assert result[1][0].caption_text == "Second caption"
    assert result[1][0].media_urls == [
        "https://cdn.example.com/xyz200_1.jpg",
        "https://cdn.example.com/xyz200_2.jpg",
    ]
    assert calls[0][1]["username"] == "profile_target"
    assert calls[0][4]["sessionid"] == "s1"


def test_content_extract_service_endpoint_handles_invalid_responses(monkeypatch) -> None:
    service = ContentExtractService()

    class _FakeClient:
        def get_settings(self) -> dict:
            return {"cookies": {"sessionid": "s1", "csrftoken": "c1", "ds_user_id": "42"}}

    class _FakeSession:
        def __init__(self, response) -> None:
            self.cookies = {}
            self.headers = {}
            self.trust_env = False
            self._response = response

        def close(self) -> None:
            return None

        def get(self, url: str, **kwargs):
            del url, kwargs
            return self._response

    class _HtmlResponse:
        status_code = 200
        headers = {"content-type": "text/html"}
        text = "<html>blocked</html>"

        def json(self) -> dict:
            raise ValueError("not json")

    class _RateLimitedResponse:
        status_code = 429
        headers = {"content-type": "text/plain"}
        text = "Too Many Requests"

        def json(self) -> dict:
            raise ValueError("not json")

    monkeypatch.setattr(
        "src.content_publisher.content_extract_service.requests.Session",
        lambda: _FakeSession(_HtmlResponse()),
    )
    try:
        service._fetch_supported_media(_FakeClient(), "profile_target", desired_count=1)
        assert False, "Expected ContentPublisherError for HTML response"
    except ContentPublisherError as exc:
        assert "invalid response" in str(exc).lower()

    monkeypatch.setattr(
        "src.content_publisher.content_extract_service.requests.Session",
        lambda: _FakeSession(_RateLimitedResponse()),
    )
    try:
        service._fetch_supported_media(_FakeClient(), "profile_target", desired_count=1)
        assert False, "Expected ContentPublisherError for HTTP 429 response"
    except ContentPublisherError as exc:
        assert "429" in str(exc)


def test_content_extract_service_multiple_profiles_do_not_crash_on_partial_failures(tmp_path: Path, monkeypatch) -> None:
    service = ContentExtractService(root_dir=tmp_path)

    class _FakeClient:
        def get_settings(self) -> dict:
            return {}

    monkeypatch.setattr(
        service,
        "_resolve_accounts",
        lambda account_ids, alias="": [{"username": "worker_one", "alias": alias or "default"}],
    )
    monkeypatch.setattr(service, "_create_authenticated_client", lambda account: _FakeClient())
    monkeypatch.setattr(service, "_close_authenticated_client", lambda account, client: None)

    def _fake_fetch(client, profile_username: str, *, desired_count: int):
        del client, desired_count
        if profile_username == "profile_a":
            raise ContentPublisherError("controlled endpoint error")
        return []

    monkeypatch.setattr(service, "_fetch_supported_media", _fake_fetch)

    result = service.extract(
        alias="default",
        account_ids=["worker_one"],
        profile_urls=["https://instagram.com/profile_a", "https://instagram.com/profile_b"],
        posts_per_profile=1,
    )

    assert result["profiles_processed"] == 2
    assert len(result["profiles"]) == 2
    assert result["profiles"][0]["status"] == "error"
    assert result["profiles"][1]["status"] == "ok"


def test_content_extract_service_skips_accounts_blocked_by_proxy_preflight(tmp_path: Path, monkeypatch) -> None:
    service = ContentExtractService(root_dir=tmp_path)
    created_accounts: list[str] = []

    class _FakeClient:
        def get_settings(self) -> dict:
            return {}

    monkeypatch.setattr(
        service,
        "_resolve_accounts",
        lambda account_ids, alias="": [
            {"username": "blocked", "alias": alias or "default", "assigned_proxy_id": "proxy-a"},
            {"username": "ready", "alias": alias or "default"},
        ],
    )
    monkeypatch.setattr(
        "src.content_publisher.content_extract_service.preflight_accounts_for_proxy_runtime",
        lambda accounts, **_kwargs: {
            "ready_accounts": [dict(accounts[1])],
            "blocked_accounts": [
                {"username": "blocked", "status": "quarantined", "message": "proxy quarantined"}
            ],
        },
    )
    monkeypatch.setattr(
        service,
        "_create_authenticated_client",
        lambda account: created_accounts.append(str(account.get("username") or "")) or _FakeClient(),
    )
    monkeypatch.setattr(service, "_close_authenticated_client", lambda account, client: None)
    monkeypatch.setattr(service, "_fetch_supported_media", lambda client, profile_username, *, desired_count: [])

    result = service.extract(
        alias="default",
        account_ids=["blocked", "ready"],
        profile_urls=["https://instagram.com/profile_target"],
        posts_per_profile=1,
    )

    assert created_accounts == ["ready"]
    assert any("@blocked" in line and "proxy quarantined" in line for line in result["logs"])


def test_content_publish_service_opens_manual_publish_session(tmp_path: Path, monkeypatch) -> None:
    library = ContentLibraryService(root_dir=tmp_path)
    entry = library.store_media_entry(
        source_profile="profile_publish",
        media_type="carousel",
        media_files=[
            _write_png(tmp_path / "publish" / "slide_1.png"),
            _write_png(tmp_path / "publish" / "slide_2.png"),
        ],
        caption="Publish caption",
        entry_key="carousel_publish",
    )
    service = ContentPublishService(root_dir=tmp_path, library_service=library)

    opened: list[tuple[dict, str, str]] = []

    monkeypatch.setattr(service, "_resolve_account", lambda account_id: {"username": account_id, "alias": "default"})
    monkeypatch.setattr(
        "src.content_publisher.content_publish_service._open_playwright_manual_session",
        lambda account, *, start_url, action_label: opened.append((dict(account), start_url, action_label)),
    )

    result = service.publish(
        account_id="target_account",
        media_path=str(entry["media_path"]),
        caption="Manual caption",
    )

    assert result["media_type"] == "carousel"
    assert result["published_media"] == {}
    assert "manual" in result["summary"].lower()
    assert opened[0][0]["username"] == "target_account"
    assert opened[0][1] == "https://www.instagram.com/create/select/"
    assert "manual" in opened[0][2].lower()


def test_content_publish_service_rejects_blocked_proxy_account(tmp_path: Path, monkeypatch) -> None:
    library = ContentLibraryService(root_dir=tmp_path)
    entry = library.store_media_entry(
        source_profile="profile_publish",
        media_type="image",
        media_files=[_write_png(tmp_path / "publish" / "single.png")],
        caption="Publish caption",
        entry_key="blocked_publish",
    )
    service = ContentPublishService(root_dir=tmp_path, library_service=library)

    monkeypatch.setattr(
        service,
        "_resolve_account",
        lambda account_id: {"username": account_id, "alias": "default", "assigned_proxy_id": "proxy-a"},
    )
    monkeypatch.setattr(
        "src.content_publisher.content_publish_service.account_proxy_preflight",
        lambda account: {
            "username": str(account.get("username") or "").strip(),
            "status": "quarantined",
            "message": "proxy quarantined",
            "blocking": True,
        },
    )

    with pytest.raises(ContentPublisherError, match="proxy quarantined"):
        service.publish(
            account_id="target_account",
            media_path=str(entry["media_path"]),
            caption="Manual caption",
        )


def test_content_api_routes_and_backend_mount(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "src.content_publisher.content_extract_service.ContentExtractService.extract",
        lambda self, **kwargs: {"summary": "extract ok", "logs": ["extract"], "stored_count": 1, **kwargs},
    )
    monkeypatch.setattr(
        "src.content_publisher.content_publish_service.ContentPublishService.publish",
        lambda self, **kwargs: {"summary": "publish ok", "logs": ["publish"], **kwargs},
    )

    client = TestClient(content_api_app)
    try:
        extract_response = client.post(
            "/content/extract",
            json={
                "alias": "default",
                "account_ids": ["worker_one"],
                "profile_urls": ["https://instagram.com/profile_target"],
                "posts_per_profile": 2,
                "root_dir": str(tmp_path),
            },
        )
        publish_response = client.post(
            "/content/publish",
            json={
                "account_id": "worker_one",
                "media_path": "data/content_library/sample.png",
                "caption": "caption",
                "root_dir": str(tmp_path),
            },
        )
    finally:
        client.close()

    assert extract_response.status_code == 200
    assert publish_response.status_code == 200
    assert extract_response.json()["summary"] == "extract ok"
    assert publish_response.json()["summary"] == "publish ok"

    backend_main = importlib.import_module("backend.main")
    routes = {route.path for route in backend_main.app.routes}
    assert "/content/extract" in routes
    assert "/content/publish" in routes
