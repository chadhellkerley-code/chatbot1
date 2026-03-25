from __future__ import annotations

import asyncio
from pathlib import Path

from src.content_publisher.content_extract_service import ContentExtractService
from src.content_publisher.content_library_service import ContentLibraryService


_PNG_BYTES = b"\x89PNG\r\n\x1a\n"
_MP4_BYTES = b"\x00\x00\x00\x18ftypmp42"


class _FakePage:
    def __init__(self, html_by_url: dict[str, str]) -> None:
        self._html_by_url = dict(html_by_url)
        self.url = ""

    async def goto(self, url: str, **_kwargs) -> None:
        self.url = str(url)

    async def wait_for_load_state(self, *_args, **_kwargs) -> None:
        return None

    async def content(self) -> str:
        return self._html_by_url.get(self.url, "")


class _FakeContext:
    def __init__(self, page: _FakePage) -> None:
        self.pages = [page]
        self.closed = False

    async def new_page(self) -> _FakePage:
        return self.pages[0]

    async def close(self) -> None:
        self.closed = True


class _FakePlaywrightService:
    def __init__(self, *args, **kwargs) -> None:
        del args, kwargs
        self._ctx = None

    async def new_context_for_account(self, *args, **kwargs):
        del args, kwargs
        assert self._ctx is not None
        return self._ctx

    async def close(self) -> None:
        return None


def _write_media_file(media_url: str, target_path: Path) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(_MP4_BYTES if str(media_url).endswith(".mp4") else _PNG_BYTES)
    return target_path


def test_content_extract_service_prefers_browser_flow_and_supports_video(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles_root = tmp_path / "profiles"
    storage_state = profiles_root / "worker_one" / "storage_state.json"
    storage_state.parent.mkdir(parents=True, exist_ok=True)
    storage_state.write_text('{"cookies":[],"origins":[]}', encoding="utf-8")

    profile_html = """
    <html>
      <body>
        <script type="application/json">
          {"data":{"user":{"edge_owner_to_timeline_media":{"count":27}}}}
        </script>
        <a href="/p/IMG001/">image</a>
        <a href="/p/CAR002/">carousel</a>
        <a href="/reel/VID003/">video</a>
      </body>
    </html>
    """
    image_html = """
    <html><body>
      <script type="application/json">
        {"shortcode_media":{"id":"1","shortcode":"IMG001","display_url":"https://cdn.example.com/image_1.jpg","caption":{"text":"Image caption"}}}
      </script>
    </body></html>
    """
    carousel_html = """
    <html><body>
      <script type="application/json">
        {"shortcode_media":{"id":"2","shortcode":"CAR002","caption":{"text":"Carousel caption"},"edge_sidecar_to_children":{"edges":[{"node":{"display_url":"https://cdn.example.com/car_1.jpg"}},{"node":{"display_url":"https://cdn.example.com/car_2.jpg"}}]}}}
      </script>
    </body></html>
    """
    video_html = """
    <html><body>
      <script type="application/json">
        {"shortcode_media":{"id":"3","shortcode":"VID003","video_url":"https://cdn.example.com/video_3.mp4","caption":{"text":"Video caption"}}}
      </script>
    </body></html>
    """
    page = _FakePage(
        {
            "https://www.instagram.com/profile_target/": profile_html,
            "https://www.instagram.com/p/IMG001/": image_html,
            "https://www.instagram.com/p/CAR002/": carousel_html,
            "https://www.instagram.com/reel/VID003/": video_html,
        }
    )
    fake_service = _FakePlaywrightService()
    fake_service._ctx = _FakeContext(page)

    library = ContentLibraryService(root_dir=tmp_path)
    service = ContentExtractService(root_dir=tmp_path, library_service=library)

    monkeypatch.setattr("src.content_publisher.content_extract_service.BASE_PROFILES", profiles_root)
    monkeypatch.setattr("src.content_publisher.content_extract_service.PlaywrightService", lambda *args, **kwargs: fake_service)
    monkeypatch.setattr(
        "src.content_publisher.content_extract_service.run_coroutine_sync",
        lambda coro, **_kwargs: asyncio.run(coro),
    )
    monkeypatch.setattr(
        service,
        "_resolve_accounts",
        lambda account_ids, alias="": [{"username": "worker_one", "alias": alias or "default"}],
    )
    monkeypatch.setattr(service, "_create_authenticated_client", lambda account: object())
    monkeypatch.setattr(service, "_close_authenticated_client", lambda account, client: None)
    monkeypatch.setattr(service, "_session_from_client", lambda client: (object(), False))
    monkeypatch.setattr(
        service,
        "_download_to_path",
        lambda _session, media_url, target_path: _write_media_file(str(media_url), target_path),
    )
    monkeypatch.setattr(
        service,
        "_fetch_supported_media",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("endpoint fallback should not run")),
    )

    result = service.extract(
        alias="default",
        account_ids=["worker_one"],
        profile_urls=["https://instagram.com/profile_target"],
        posts_per_profile=3,
    )

    entries = library.list_entries()
    assert result["stored_count"] == 3
    assert result["profiles"][0]["extract_method"] == "browser"
    assert result["profiles"][0]["post_count"] == 27
    assert {entry["media_type"] for entry in entries} == {"image", "carousel", "video"}
