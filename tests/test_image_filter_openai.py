import asyncio
import types
import unittest
from unittest.mock import AsyncMock, patch

import leads


class ImageFilterOpenAITests(unittest.TestCase):
    def test_image_filter_calls_openai(self):
        last_request = {}

        class FakeResponses:
            def create(self, **kwargs):
                last_request.update(kwargs)
                return types.SimpleNamespace(output_text="CALIFICA")

        class FakeOpenAI:
            def __init__(self, api_key):
                self.api_key = api_key
                self.responses = FakeResponses()

        user = leads.ScrapedUser(
            username="test",
            biography="",
            full_name="",
            follower_count=0,
            media_count=0,
            is_private=False,
            profile_pic_url="http://example.com/p.jpg",
        )
        with patch("openai.OpenAI", FakeOpenAI):
            ok, reason = leads._image_ai_decision("key", user, "persona con sombrero", image_bytes=b"123")
        self.assertTrue(ok)
        self.assertIn("model", last_request)
        self.assertEqual(last_request.get("model"), "gpt-4o-mini")
        content = last_request.get("input", [])[1]["content"]
        self.assertTrue(any(item.get("type") == "input_image" for item in content))

    def test_image_filter_skips_without_api_key(self):
        cfg = leads.LeadFilterConfig(
            classic=leads.ClassicFilterConfig(
                min_followers=0,
                min_posts=0,
                privacy="any",
                link_in_bio="any",
                include_keywords=[],
                exclude_keywords=[],
                language="any",
            ),
            text=leads.TextFilterConfig(enabled=False, criteria="", model_path=""),
            image=leads.ImageFilterConfig(enabled=True, prompt="persona con sombrero"),
        )
        user = leads.ScrapedUser(
            username="test",
            biography="",
            full_name="",
            follower_count=0,
            media_count=0,
            is_private=False,
            profile_pic_url="http://example.com/p.jpg",
        )

        async def run():
            with patch.object(leads, "_pw_fetch_profile_snapshot", new=AsyncMock(return_value=(user, ""))):
                with patch.object(leads, "_passes_classic_filters", return_value=(True, "")):
                    with patch.object(leads, "_get_profile_image_bytes", new=AsyncMock(return_value=b"123")):
                        with patch.object(leads, "_get_openai_api_key", return_value=""):
                            called = False

                            def fake_image_ai(*args, **kwargs):
                                nonlocal called
                                called = True
                                return True, "ok"

                            with patch.object(leads, "_image_ai_decision", side_effect=fake_image_ai):
                                ok, reason = await leads._evaluate_username(
                                    None,
                                    "test",
                                    cfg,
                                    asyncio.Lock(),
                                    asyncio.Lock(),
                                )
            return ok, called

        ok, called = asyncio.run(run())
        self.assertTrue(ok)
        self.assertFalse(called)

    def test_image_config_serialization(self):
        cfg = leads.LeadFilterConfig(
            classic=leads.ClassicFilterConfig(
                min_followers=0,
                min_posts=0,
                privacy="any",
                link_in_bio="any",
                include_keywords=[],
                exclude_keywords=[],
                language="any",
            ),
            text=leads.TextFilterConfig(enabled=False, criteria="", model_path=""),
            image=leads.ImageFilterConfig(enabled=True, prompt="algo"),
        )
        payload = leads._filter_config_to_dict(cfg)
        self.assertEqual(set(payload.get("image", {}).keys()), {"enabled", "prompt"})


if __name__ == "__main__":
    unittest.main()
