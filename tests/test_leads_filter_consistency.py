import unittest
from types import SimpleNamespace

import core.leads as leads
from src import leads_filter_pipeline


class LeadsFilterConsistencyTests(unittest.TestCase):
    def test_classic_keywords_match_full_term_not_substring(self):
        profile = leads_filter_pipeline.ProfileSnapshot(
            username="lead",
            biography="familia feliz",
            full_name="Lead Test",
            follower_count=100,
            media_count=10,
            is_private=False,
            profile_pic_url="https://example.com/img.jpg",
            user_id="1",
            external_url="",
            is_verified=False,
        )
        cfg = SimpleNamespace(
            min_followers=0,
            min_posts=0,
            privacy="any",
            link_in_bio="any",
            include_keywords=["ia"],
            exclude_keywords=[],
            language="any",
            min_followers_state="disabled",
            min_posts_state="disabled",
            privacy_state="disabled",
            link_in_bio_state="disabled",
            include_keywords_state="required",
            exclude_keywords_state="disabled",
            language_state="disabled",
        )

        ok, reason = leads_filter_pipeline.passes_classic_filters(profile, cfg)
        self.assertFalse(ok)
        self.assertEqual(reason, "keyword_faltante")

    def test_classic_keywords_use_semantic_aliases(self):
        profile = leads_filter_pipeline.ProfileSnapshot(
            username="lead",
            biography="Ayudo a hombres con sobrepeso",
            full_name="Lead Test",
            follower_count=100,
            media_count=10,
            is_private=False,
            profile_pic_url="https://example.com/img.jpg",
            user_id="1",
            external_url="",
            is_verified=False,
        )
        cfg = SimpleNamespace(
            min_followers=0,
            min_posts=0,
            privacy="any",
            link_in_bio="any",
            include_keywords=["peso"],
            exclude_keywords=[],
            language="any",
            min_followers_state="disabled",
            min_posts_state="disabled",
            privacy_state="disabled",
            link_in_bio_state="disabled",
            include_keywords_state="required",
            exclude_keywords_state="disabled",
            language_state="disabled",
        )

        ok, reason = leads_filter_pipeline.passes_classic_filters(profile, cfg)
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_language_filter_uses_only_biography_text(self):
        profile = leads_filter_pipeline.ProfileSnapshot(
            username="the.and.for.with",
            biography="para la salud y el bienestar",
            full_name="The And For With",
            follower_count=100,
            media_count=10,
            is_private=False,
            profile_pic_url="https://example.com/img.jpg",
            user_id="1",
            external_url="",
            is_verified=False,
        )
        cfg = SimpleNamespace(
            min_followers=0,
            min_posts=0,
            privacy="any",
            link_in_bio="any",
            include_keywords=[],
            exclude_keywords=[],
            language="es",
            min_followers_state="disabled",
            min_posts_state="disabled",
            privacy_state="disabled",
            link_in_bio_state="disabled",
            include_keywords_state="disabled",
            exclude_keywords_state="disabled",
            language_state="required",
        )

        ok, reason = leads_filter_pipeline.passes_classic_filters(profile, cfg)
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_language_filter_requires_biography_when_enabled(self):
        profile = leads_filter_pipeline.ProfileSnapshot(
            username="lead",
            biography="",
            full_name="Lead Test",
            follower_count=100,
            media_count=10,
            is_private=False,
            profile_pic_url="https://example.com/img.jpg",
            user_id="1",
            external_url="",
            is_verified=False,
        )
        cfg = SimpleNamespace(
            min_followers=0,
            min_posts=0,
            privacy="any",
            link_in_bio="any",
            include_keywords=[],
            exclude_keywords=[],
            language="es",
            min_followers_state="disabled",
            min_posts_state="disabled",
            privacy_state="disabled",
            link_in_bio_state="disabled",
            include_keywords_state="disabled",
            exclude_keywords_state="disabled",
            language_state="required",
        )

        ok, reason = leads_filter_pipeline.passes_classic_filters(profile, cfg)
        self.assertFalse(ok)
        self.assertEqual(reason, "biografia_vacia")

    def test_config_roundtrip_disables_noop_classic_states(self):
        payload = {
            "classic": {
                "min_followers": 0,
                "min_posts": 0,
                "privacy": "any",
                "link_in_bio": "any",
                "include_keywords": [],
                "exclude_keywords": [],
                "language": "any",
                "min_followers_state": "required",
                "min_posts_state": "indifferent",
                "privacy_state": "required",
                "link_in_bio_state": "required",
                "include_keywords_state": "required",
                "exclude_keywords_state": "required",
                "language_state": "required",
            },
            "text": {"enabled": False, "criteria": "", "state": "disabled"},
            "image": {"enabled": False, "prompt": "", "state": "disabled"},
        }

        cfg = leads._filter_config_from_dict(payload)
        self.assertIsNotNone(cfg)
        roundtrip = leads._filter_config_to_dict(cfg)
        classic = roundtrip["classic"]
        self.assertEqual(classic["min_followers_state"], "disabled")
        self.assertEqual(classic["min_posts_state"], "disabled")
        self.assertEqual(classic["privacy_state"], "disabled")
        self.assertEqual(classic["link_in_bio_state"], "disabled")
        self.assertEqual(classic["include_keywords_state"], "disabled")
        self.assertEqual(classic["exclude_keywords_state"], "disabled")
        self.assertEqual(classic["language_state"], "disabled")

    def test_username_normalization_strips_invisible_chars(self):
        values = ["\ufeff@LeadUno", "  @leadDos\u200b  ", "", " \u200c@leadTres "]
        normalized = leads._normalize_usernames(values)
        self.assertEqual(normalized, ["LeadUno", "leadDos", "leadTres"])

    def test_fresh_rerun_reset_clears_runtime_fields(self):
        item = {
            "username": "leadtest",
            "status": "DISCARDED",
            "result": "NO CALIFICA",
            "reason": "keyword_excluida",
            "account": "CuentaA",
            "updated_at": "2026-03-01T10:00:00Z",
            "decision_final": "fail",
            "reasons": ["phase1_ok"],
            "scores": {"text_similarity": 0.2},
            "extracted": {"username": "leadtest"},
            "profile_retry_count": 2,
            "profile_next_attempt_at": "2026-03-01T10:05:00Z",
            "retry_count": 2,
            "next_attempt_at": "2026-03-01T10:05:00Z",
            "last_retry_task_type": "profile",
            "last_rate_limit_reason": "http_429",
            "image_retry_count": 1,
            "image_next_attempt_at": "2026-03-01T10:07:00Z",
            "image_status": "rate_limited",
            "image_reason": "image_http_429",
        }

        leads_filter_pipeline._v2_reset_item_for_fresh_run(item)  # pylint: disable=protected-access

        self.assertEqual(item["status"], "PENDING")
        self.assertEqual(item["result"], "")
        self.assertEqual(item["reason"], "")
        self.assertEqual(item["account"], "")
        self.assertEqual(item["updated_at"], "")
        self.assertNotIn("decision_final", item)
        self.assertNotIn("reasons", item)
        self.assertNotIn("scores", item)
        self.assertNotIn("extracted", item)
        self.assertNotIn("profile_retry_count", item)
        self.assertNotIn("profile_next_attempt_at", item)
        self.assertNotIn("retry_count", item)
        self.assertNotIn("next_attempt_at", item)
        self.assertNotIn("last_retry_task_type", item)
        self.assertNotIn("last_rate_limit_reason", item)
        self.assertNotIn("image_retry_count", item)
        self.assertNotIn("image_next_attempt_at", item)
        self.assertNotIn("image_status", item)
        self.assertNotIn("image_reason", item)


if __name__ == "__main__":
    unittest.main()
