import unittest
from unittest import mock
import asyncio
import time

import core.leads as leads
from src.image_attribute_filter import ImageAnalysisResult
from src.image_prompt_parser import parse_image_prompt
from src.image_rule_evaluator import (
    AGE_MIN_TOLERANCE_YEARS,
    OVERWEIGHT_THRESHOLD,
    OVERWEIGHT_MALE35_THRESHOLD,
    OVERWEIGHT_TOLERANCE,
    evaluate_image_rules,
)
from src import leads_filter_pipeline


class LeadsLocalPipelineTests(unittest.TestCase):
    def test_startup_wait_default_matches_production_timeout(self):
        self.assertEqual(leads_filter_pipeline.DEFAULT_STARTUP_WAIT_SECONDS, 25.0)

    def test_startup_retry_targets_only_accounts_without_ready_runtime(self):
        accounts = [
            {"username": "cuenta_a"},
            {"username": "cuenta_b"},
            {"username": "cuenta_c"},
            {"username": "cuenta_b"},
        ]

        pending = leads_filter_pipeline._v2_pending_accounts_for_startup_retry(
            accounts,
            {"cuenta_a": object()},
        )

        self.assertEqual(
            [str(item.get("username") or "") for item in pending],
            ["cuenta_b", "cuenta_c"],
        )

    def test_worker_selection_without_proxies_keeps_single_local_worker(self):
        accounts = [
            {"username": "cuenta_a", "assigned_proxy_id": "", "proxy_url": ""},
            {"username": "cuenta_b", "assigned_proxy_id": "", "proxy_url": ""},
            {"username": "cuenta_c", "assigned_proxy_id": "", "proxy_url": ""},
        ]

        with mock.patch("src.leads_filter_pipeline._v2_account_has_storage_state", return_value=False):
            selected_accounts, worker_keys = leads_filter_pipeline._v2_select_accounts_for_workers(
                accounts,
                requested_workers=1,
            )

        self.assertEqual(worker_keys, ["__no_proxy__"])
        self.assertEqual(
            [str(item.get("username") or "") for item in selected_accounts],
            ["cuenta_a", "cuenta_b", "cuenta_c"],
        )

    def test_worker_selection_keeps_all_accounts_inside_selected_proxy_groups(self):
        accounts = [
            {"username": "proxy1_a", "assigned_proxy_id": "proxy-1"},
            {"username": "proxy1_b", "assigned_proxy_id": "proxy-1"},
            {"username": "proxy2_a", "assigned_proxy_id": "proxy-2"},
            {"username": "proxy3_a", "assigned_proxy_id": "proxy-3"},
        ]

        with mock.patch(
            "src.leads_filter_pipeline._v2_account_has_storage_state",
            side_effect=lambda account: str(account.get("username") or "") == "proxy2_a",
        ):
            selected_accounts, worker_keys = leads_filter_pipeline._v2_select_accounts_for_workers(
                accounts,
                requested_workers=2,
            )

        self.assertEqual(worker_keys, ["proxy-2", "proxy-1"])
        self.assertEqual(
            [str(item.get("username") or "") for item in selected_accounts],
            ["proxy2_a", "proxy1_a", "proxy1_b"],
        )

    def test_proxy_preflight_filter_excludes_blocked_accounts(self):
        accounts = [
            {"username": "cuenta_ok"},
            {"username": "cuenta_bad", "assigned_proxy_id": "proxy-a"},
        ]

        with mock.patch(
            "src.leads_filter_pipeline.preflight_accounts_for_proxy_runtime",
            return_value={
                "ready_accounts": [dict(accounts[0])],
                "blocked_accounts": [{"username": "cuenta_bad", "status": "quarantined"}],
            },
        ):
            ready, blocked = leads_filter_pipeline._v2_filter_accounts_by_proxy_runtime(accounts)

        self.assertEqual([str(item.get("username") or "") for item in ready], ["cuenta_ok"])
        self.assertEqual(blocked[0]["username"], "cuenta_bad")

    def test_text_inteligente_local(self):
        user = leads.ScrapedUser(
            username="coachmaria",
            biography="Coach de negocios para emprendedoras en espanol",
            full_name="Maria Coach",
            follower_count=12000,
            media_count=250,
            is_private=False,
            profile_pic_url="https://example.com/avatar.jpg",
            user_id="123",
        )
        decision, reason = leads._text_ai_decision("unused", user, "coach de negocios")
        self.assertTrue(decision)
        self.assertIn("score_text=", reason)

    @mock.patch.dict("os.environ", {"LEADS_DISABLE_EMBEDDINGS": "1"}, clear=False)
    def test_text_inteligente_hibrido_combina_embeddings_y_regex(self):
        engine = leads_filter_pipeline.LocalTextEngine(
            "coach de negocios",
            thresholds={
                "embeddings_threshold": 0.70,
                "hybrid_embeddings_weight": 0.80,
                "regex_floor_threshold": 0.35,
                "regex_ceiling_threshold": 0.85,
                "regex_coverage_base": 0.20,
                "regex_coverage_per_term": 0.10,
                "regex_coverage_max_terms": 4,
            },
        )

        class _FakeModel:
            def encode(self, _texts, normalize_embeddings=True):  # pylint: disable=unused-argument
                return [[0.90, 0.10]]

        engine._model = _FakeModel()  # pylint: disable=protected-access
        engine._criteria_embedding = [1.0, 0.0]  # pylint: disable=protected-access
        engine.mode = "hybrid"

        profile = leads_filter_pipeline.ProfileSnapshot(
            username="coach_max",
            biography="Coach para emprendedores de alto rendimiento",
            full_name="Max Coach",
            follower_count=5000,
            media_count=200,
            is_private=False,
            profile_pic_url="https://example.com/avatar.jpg",
            user_id="1",
            external_url="",
            is_verified=False,
        )
        decision = engine.score(profile)
        self.assertTrue(decision.qualified)
        self.assertEqual(decision.mode, "hybrid")
        self.assertIn("score_emb=", decision.reason)
        self.assertIn("score_regex=", decision.reason)

    @mock.patch.dict("os.environ", {"LEADS_DISABLE_EMBEDDINGS": "1"}, clear=False)
    def test_text_inteligente_detecta_perder_peso_semantico(self):
        user = leads.ScrapedUser(
            username="robertoperez.coach",
            biography=(
                "Te Garantizo Que vas a Perder De 10 a 20 kg de 3 a 6 meses O No Pagas. "
                "Solo para HOMBRES +40 con SOBREPESO."
            ),
            full_name="Roberto Perez",
            follower_count=42000,
            media_count=500,
            is_private=False,
            profile_pic_url="https://example.com/avatar.jpg",
            user_id="999",
        )
        decision, reason = leads._text_ai_decision(
            "unused",
            user,
            "que ayuden a personas a adelgazar y bajar de peso",
        )
        self.assertTrue(decision)
        self.assertIn("score_text=", reason)

    @mock.patch.dict("os.environ", {"LEADS_DISABLE_EMBEDDINGS": "1"}, clear=False)
    def test_text_inteligente_fallback_regex_si_falla_embeddings(self):
        engine = leads_filter_pipeline.LocalTextEngine("coach de negocios")

        class _BrokenModel:
            def encode(self, _texts, normalize_embeddings=True):  # pylint: disable=unused-argument
                raise RuntimeError("forced_failure")

        engine._model = _BrokenModel()  # pylint: disable=protected-access
        engine._criteria_embedding = [1.0, 0.0]  # pylint: disable=protected-access
        engine.mode = "hybrid"

        profile = leads_filter_pipeline.ProfileSnapshot(
            username="coach_lucas",
            biography="Mentor y coach para negocios digitales",
            full_name="Lucas Coach",
            follower_count=7000,
            media_count=90,
            is_private=False,
            profile_pic_url="https://example.com/avatar.jpg",
            user_id="2",
            external_url="",
            is_verified=False,
        )
        decision = engine.score(profile)
        self.assertEqual(decision.mode, "regex")
        self.assertIn("score_text=", decision.reason)

    def test_image_local_requires_real_image(self):
        user = leads.ScrapedUser(
            username="nofoto",
            biography="",
            full_name="",
            follower_count=0,
            media_count=0,
            is_private=False,
            profile_pic_url="",
        )
        ok, reason = leads._image_ai_decision("unused", user, "hombre adulto")
        self.assertFalse(ok)
        self.assertEqual(reason, "image_download_failed")

    def test_verify_dependencies_no_openai_key(self):
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
            text=leads.TextFilterConfig(enabled=True, criteria="coach", model_path="", state="required"),
            image=leads.ImageFilterConfig(enabled=False, prompt="", state="disabled"),
        )
        leads._verify_dependencies_for_run(cfg)

    def test_config_serialization_preserves_state(self):
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
            text=leads.TextFilterConfig(enabled=True, criteria="coach", model_path="", state="indifferent"),
            image=leads.ImageFilterConfig(enabled=True, prompt="hombre adulto", state="required"),
        )
        payload = leads._filter_config_to_dict(cfg)
        image_payload = payload.get("image", {})
        self.assertTrue({"enabled", "prompt", "state"}.issubset(set(image_payload.keys())))
        self.assertIn("engine_thresholds", image_payload)
        self.assertIsInstance(image_payload.get("engine_thresholds"), dict)
        self.assertEqual(payload["image"]["state"], "required")

    def test_phase1_payload_requires_followers_and_posts(self):
        payload = {
            "data": {
                "user": {
                    "username": "leadtest",
                    "biography": "bio",
                    "full_name": "Lead Test",
                    "edge_followed_by": {},
                    "edge_owner_to_timeline_media": {"count": 12},
                    "profile_pic_url_hd": "https://example.com/pic.jpg",
                    "id": "100",
                }
            }
        }
        profile = leads_filter_pipeline.extract_profile_from_payload(payload, "leadtest")
        self.assertIsNone(profile)

    def test_validate_image_payload_rejects_non_image_payload(self):
        ok, reason = leads_filter_pipeline.validate_image_payload(b"x" * 2048, "")
        self.assertFalse(ok)
        self.assertEqual(reason, "invalid_real")

    def test_validate_image_payload_accepts_magic_without_content_type(self):
        jpeg_header = b"\xff\xd8\xff\xe0" + (b"x" * 6000)
        ok, reason = leads_filter_pipeline.validate_image_payload(jpeg_header, "")
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

    @mock.patch(
        "core.leads.list_all",
        return_value=[
            {"username": "CuentaA", "proxy_url": "http://1"},
            {"username": "CuentaB", "proxy_url": "http://2"},
        ],
    )
    def test_resolve_accounts_uses_requested_order(self, _mock_list_all):
        resolved = leads._resolve_accounts(["cuentab", "cuentaa"])
        self.assertEqual([item["username"] for item in resolved], ["CuentaB", "CuentaA"])

    def test_requeue_discards_with_max_retries_exceeded(self):
        list_data = {
            "items": [
                {
                    "username": "leadtest",
                    "status": "PENDING",
                    "result": "",
                    "reason": "",
                }
            ]
        }

        async def _run() -> bool:
            lock = asyncio.Lock()
            for _ in range(3):
                should_retry_inner = await leads_filter_pipeline._v2_mark_retry_pending(  # pylint: disable=protected-access
                    list_data,
                    idx=0,
                    account="CuentaA",
                    reason="http_429",
                    next_attempt_epoch=1_700_000_000.0,
                    lock=lock,
                    max_retries=3,
                )
                self.assertTrue(should_retry_inner)

            return await leads_filter_pipeline._v2_mark_retry_pending(  # pylint: disable=protected-access
                list_data,
                idx=0,
                account="CuentaA",
                reason="http_429",
                next_attempt_epoch=1_700_000_000.0,
                lock=lock,
                max_retries=3,
            )

        should_retry = asyncio.run(_run())
        self.assertFalse(should_retry)
        item = list_data["items"][0]
        self.assertEqual(item.get("status"), "PENDING")
        self.assertEqual(item.get("reason"), "http_429")
        self.assertEqual(item.get("profile_retry_count"), 4)
        self.assertIsNone(item.get("profile_next_attempt_at"))

    def test_profile_image_limiters_are_independent(self):
        profile = leads_filter_pipeline.ProfileLimiter(
            "cuentaa",
            daily_budget=0,
            delay_min_seconds=1.0,
            delay_max_seconds=1.5,
        )
        image = leads_filter_pipeline.ImageLimiter("proxy://x", daily_budget=0)
        messages = []

        asyncio.run(profile.apply_rate_limit(status=429, warn=messages.append))

        self.assertGreater(profile.cooling_until, 0.0)
        self.assertEqual(image.cooling_until, 0.0)
        self.assertTrue(any("LIMITE_TASA" in msg for msg in messages))

    def test_macro_rate_limit_adaptation_reduces_concurrency_temporarily(self):
        runtime = leads_filter_pipeline.AccountRuntime(
            account={"username": "CuentaA"},
            username="CuentaA",
            svc=None,
            ctx=None,
            page=object(),
            profile_gate=leads_filter_pipeline.SlotGate(3),
            image_gate=leads_filter_pipeline.SlotGate(1),
            profile_limiter=leads_filter_pipeline.ProfileLimiter(
                "CuentaA",
                daily_budget=0,
                delay_min_seconds=1.0,
                delay_max_seconds=1.5,
            ),
            macro_default_profile_capacity=3,
            macro_base_interval_seconds=1.0,
        )

        async def _run():
            await leads_filter_pipeline._macro_register_rate_limit(runtime)  # pylint: disable=protected-access
            await leads_filter_pipeline._macro_register_rate_limit(runtime)  # pylint: disable=protected-access
            reduced_capacity = runtime.profile_gate.capacity
            runtime.macro_reduced_concurrency_until = time.monotonic() - 1
            await leads_filter_pipeline._macro_restore_profile_capacity_if_due(runtime)  # pylint: disable=protected-access
            restored_capacity = runtime.profile_gate.capacity
            return reduced_capacity, restored_capacity

        reduced, restored = asyncio.run(_run())
        self.assertGreaterEqual(runtime.macro_base_interval_seconds, 1.3)
        self.assertEqual(reduced, 2)
        self.assertEqual(restored, 3)

    def test_checkpoint_runtime_state_roundtrip(self):
        runtime = leads_filter_pipeline.AccountRuntime(
            account={"username": "CuentaA"},
            username="CuentaA",
            svc=None,
            ctx=None,
            page=object(),
            profile_gate=leads_filter_pipeline.SlotGate(2),
            image_gate=leads_filter_pipeline.SlotGate(1),
            profile_limiter=leads_filter_pipeline.ProfileLimiter(
                "CuentaA",
                daily_budget=0,
                delay_min_seconds=1.0,
                delay_max_seconds=1.5,
            ),
            image_limiter=leads_filter_pipeline.ImageLimiter("proxy://1", daily_budget=0),
            macro_base_interval_seconds=1.2,
            macro_block_target=80,
            macro_block_progress=15,
            macro_default_profile_capacity=2,
        )
        runtime.account_processed = 42
        runtime.macro_pause_until = time.monotonic() + 12
        runtime.profile_gate.capacity = 2

        payload = leads_filter_pipeline._serialize_runtime_state(runtime)  # pylint: disable=protected-access
        clone = leads_filter_pipeline.AccountRuntime(
            account={"username": "CuentaA"},
            username="CuentaA",
            svc=None,
            ctx=None,
            page=object(),
            profile_gate=leads_filter_pipeline.SlotGate(1),
            image_gate=leads_filter_pipeline.SlotGate(1),
            profile_limiter=leads_filter_pipeline.ProfileLimiter(
                "CuentaA",
                daily_budget=0,
                delay_min_seconds=1.0,
                delay_max_seconds=1.5,
            ),
        )
        leads_filter_pipeline._restore_runtime_state(clone, payload)  # pylint: disable=protected-access

        self.assertEqual(clone.account_processed, 42)
        self.assertEqual(clone.macro_block_target, 80)
        self.assertEqual(clone.macro_block_progress, 15)
        self.assertEqual(clone.profile_gate.capacity, 2)
        self.assertGreater(clone.macro_pause_until, time.monotonic())

    def test_fetch_profile_via_playwright_parses_json(self):
        class FakePage:
            async def evaluate(self, _script, _arg):
                return {
                    "status": 200,
                    "body": '{"data":{"user":{"username":"ok"}}}',
                    "reason": "",
                }

        payload = asyncio.run(
            leads_filter_pipeline.fetch_profile_via_playwright(  # pylint: disable=protected-access
                FakePage(),
                "ok",
                timeout_ms=8_000,
            )
        )
        self.assertIsInstance(payload, dict)
        self.assertIn("data", payload)

    def test_fetch_profile_via_playwright_raises_rate_limit(self):
        class FakePage:
            async def evaluate(self, _script, _arg):
                return {
                    "status": 429,
                    "body": "",
                    "reason": "",
                }

        with self.assertRaises(leads_filter_pipeline.ProfileRateLimit):
            asyncio.run(
                leads_filter_pipeline.fetch_profile_via_playwright(  # pylint: disable=protected-access
                    FakePage(),
                    "ok",
                    timeout_ms=8_000,
                )
            )

    def test_fetch_profile_via_playwright_includes_profile_headers(self):
        captured = {}

        class FakePage:
            async def evaluate(self, _script, arg):
                captured.update(arg or {})
                return {
                    "status": 200,
                    "body": '{"data":{"user":{"username":"ok"}}}',
                    "reason": "",
                }

        payload = asyncio.run(
            leads_filter_pipeline.fetch_profile_via_playwright(
                FakePage(),
                "ok",
                timeout_ms=8_000,
                ig_app_id="936619743392459",
                asbd_id="198387",
                accept_language="es-AR,es;q=0.9",
            )
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(captured.get("igAppId"), "936619743392459")
        self.assertEqual(captured.get("asbdId"), "198387")
        self.assertEqual(captured.get("acceptLanguage"), "es-AR,es;q=0.9")

    def test_build_profile_http_reason_includes_message_slug(self):
        reason = leads_filter_pipeline.build_profile_http_reason(
            400,
            '{"message":"useragent mismatch","status":"fail"}',
        )
        self.assertEqual(reason, "http_400_useragent_mismatch")

    def test_missing_essential_fields_is_dynamic_by_image_requirement(self):
        profile = leads_filter_pipeline.ProfileSnapshot(
            username="leadtest",
            biography="",
            full_name="Lead Test",
            follower_count=10,
            media_count=5,
            is_private=False,
            profile_pic_url="",
            user_id="123",
            external_url="",
            is_verified=False,
        )
        without_image = leads_filter_pipeline.missing_essential_fields(
            profile,
            require_image_url=False,
        )
        self.assertEqual(without_image, [])

        with_image = leads_filter_pipeline.missing_essential_fields(
            profile,
            require_image_url=True,
        )
        self.assertIn("profile_pic_url", with_image)

    def test_profile_endpoint_preflight_rejects_http_error(self):
        class FakePage:
            async def evaluate(self, _script, _arg):
                return {
                    "status": 400,
                    "body": '{"message":"useragent mismatch","status":"fail"}',
                    "reason": "",
                }

        runtime = leads_filter_pipeline.AccountRuntime(
            account={"username": "CuentaA"},
            username="CuentaA",
            svc=None,
            ctx=None,
            page=FakePage(),
            profile_gate=leads_filter_pipeline.SlotGate(1),
            image_gate=leads_filter_pipeline.SlotGate(1),
            ig_app_id="936619743392459",
            asbd_id="198387",
        )

        ok, status, reason = asyncio.run(
            leads_filter_pipeline.profile_endpoint_preflight(
                runtime,
                probe_username="instagram",
            )
        )
        self.assertFalse(ok)
        self.assertEqual(status, 400)
        self.assertEqual(reason, "http_400_useragent_mismatch")

    def test_download_profile_image_accepts_jpeg_without_content_type(self):
        class FakeResponse:
            def __init__(self, *, status: int, body: bytes, content_type: str = ""):
                self.status = status
                self._body = body
                self._content_type = content_type

            async def all_headers(self):
                if self._content_type:
                    return {"content-type": self._content_type}
                return {}

            async def body(self):
                return self._body

            async def text(self):
                return ""

        class FakeRequest:
            def __init__(self, response):
                self._response = response

            async def get(self, *_args, **_kwargs):
                return self._response

        class FakePage:
            def __init__(self, response):
                self.context = type("Ctx", (), {"request": FakeRequest(response)})()

        image_bytes = asyncio.run(
            leads_filter_pipeline.download_profile_image(
                "https://cdn.example/image.jpg",
                proxy="http://proxy",
                headers={},
                timeout=10_000,
                page=FakePage(
                    FakeResponse(
                        status=200,
                        body=(b"\xff\xd8\xff" + (b"x" * 6000)),
                        content_type="",
                    )
                ),
                proxy_key="proxy://1",
            )
        )
        self.assertTrue(image_bytes.startswith(b"\xff\xd8\xff"))

    def test_download_profile_image_detects_soft_block_html(self):
        class FakeResponse:
            def __init__(self, *, status: int, body: bytes):
                self.status = status
                self._body = body

            async def all_headers(self):
                return {"content-type": "text/html"}

            async def body(self):
                return self._body

            async def text(self):
                return self._body.decode("utf-8", errors="ignore")

        class FakeRequest:
            def __init__(self, response):
                self._response = response

            async def get(self, *_args, **_kwargs):
                return self._response

        class FakePage:
            def __init__(self, response):
                self.context = type("Ctx", (), {"request": FakeRequest(response)})()

        with self.assertRaises(leads_filter_pipeline.ImageDownloadError) as soft_block_ctx:
            asyncio.run(
                leads_filter_pipeline.download_profile_image(
                    "https://cdn.example/image.jpg",
                    proxy="http://proxy",
                    headers={},
                    timeout=10_000,
                    page=FakePage(
                        FakeResponse(
                            status=200,
                            body=b"<html><title>Please wait</title></html>",
                        )
                    ),
                    proxy_key="proxy://1",
                )
            )
        self.assertEqual(soft_block_ctx.exception.reason, "image_soft_block")

    def test_response_headers_map_reads_headers_property(self):
        class FakeResponse:
            status = 200
            headers = {"Content-Type": "image/jpeg", "X-Test": "1"}

            async def body(self):
                return b"\xff\xd8\xff" + (b"x" * 6000)

        headers = asyncio.run(leads_filter_pipeline.response_headers_map(FakeResponse()))
        self.assertEqual(headers.get("content-type"), "image/jpeg")
        self.assertEqual(headers.get("x-test"), "1")

    def test_image_rate_limit_sets_proxy_cooldown_and_requeues(self):
        runtime = leads_filter_pipeline.AccountRuntime(
            account={"username": "CuentaA"},
            username="CuentaA",
            svc=None,
            ctx=None,
            page=object(),
            profile_gate=leads_filter_pipeline.SlotGate(1),
            image_gate=leads_filter_pipeline.SlotGate(1),
            proxy_url="http://proxy",
            proxy_key="proxy://1",
            image_limiter=leads_filter_pipeline.ImageLimiter("proxy://1", daily_budget=0),
        )
        state = leads_filter_pipeline.LeadWorkState(
            idx=0,
            username="leadtest",
            account="CuentaA",
            profile=leads_filter_pipeline.ProfileSnapshot(
                username="leadtest",
                biography="bio",
                full_name="Lead Test",
                follower_count=10,
                media_count=5,
                is_private=False,
                profile_pic_url="https://cdn.example/profile.jpg",
                user_id="1",
                external_url="",
                is_verified=False,
            ),
        )
        task = leads_filter_pipeline.ScheduledTask(
            idx=0,
            username="leadtest",
            task_type=leads_filter_pipeline.TASK_IMAGE_DOWNLOAD,
            attempts=0,
        )
        settings = leads_filter_pipeline.PipelineFilterSettings(
            classic_cfg=None,
            text_state="disabled",
            text_criteria="",
            image_state="required",
            image_prompt="hombre adulto",
        )
        logs = []

        async def _run():
            with mock.patch(
                "src.leads_filter_pipeline.download_profile_image_for_runtime",
                side_effect=leads_filter_pipeline.ImageRateLimit("proxy://1", 429),
            ):
                with mock.patch("src.leads_filter_pipeline.random.uniform", return_value=180.0):
                    return await leads_filter_pipeline._v2_task_image_download(  # pylint: disable=protected-access
                        task,
                        state,
                        runtime,
                        filter_settings=settings,
                        warn=logs.append,
                    )

        outcome = asyncio.run(_run())
        self.assertTrue(outcome.requeue)
        self.assertEqual(outcome.requeue_delay_seconds, 180.0)
        self.assertIn("image_http_429", outcome.requeue_reason)
        self.assertEqual(outcome.stats.get("image_rate_limited"), 1)
        self.assertEqual(state.image_status, "rate_limited")
        self.assertEqual(state.image_reason, "image_http_429")
        self.assertEqual(state.image_retry_count, 1)
        self.assertGreater(runtime.image_limiter.cooling_until, time.monotonic())
        self.assertTrue(any("ENFRIAMIENTO_IMAGEN" in line for line in logs))

    def test_image_404_refresh_profile_retries_with_new_url(self):
        runtime = leads_filter_pipeline.AccountRuntime(
            account={"username": "CuentaA"},
            username="CuentaA",
            svc=None,
            ctx=None,
            page=object(),
            profile_gate=leads_filter_pipeline.SlotGate(1),
            image_gate=leads_filter_pipeline.SlotGate(1),
            proxy_url="http://proxy",
            proxy_key="proxy://1",
            image_limiter=leads_filter_pipeline.ImageLimiter("proxy://1", daily_budget=0),
        )
        state = leads_filter_pipeline.LeadWorkState(
            idx=0,
            username="leadtest",
            account="CuentaA",
            profile=leads_filter_pipeline.ProfileSnapshot(
                username="leadtest",
                biography="bio",
                full_name="Lead Test",
                follower_count=10,
                media_count=5,
                is_private=False,
                profile_pic_url="https://cdn.example/old.jpg",
                user_id="1",
                external_url="",
                is_verified=False,
            ),
        )
        task = leads_filter_pipeline.ScheduledTask(
            idx=0,
            username="leadtest",
            task_type=leads_filter_pipeline.TASK_IMAGE_DOWNLOAD,
            attempts=0,
        )
        settings = leads_filter_pipeline.PipelineFilterSettings(
            classic_cfg=None,
            text_state="disabled",
            text_criteria="",
            image_state="required",
            image_prompt="hombre adulto",
        )
        refreshed_profile = leads_filter_pipeline.ProfileSnapshot(
            username="leadtest",
            biography="bio",
            full_name="Lead Test",
            follower_count=10,
            media_count=5,
            is_private=False,
            profile_pic_url="https://cdn.example/new.jpg",
            user_id="1",
            external_url="",
            is_verified=False,
        )
        download_mock = mock.AsyncMock(
            side_effect=[
                leads_filter_pipeline.ImageDownloadError("image_not_found", status=404),
                b"\xff\xd8\xff" + (b"x" * 2048),
            ]
        )

        async def _run():
            with mock.patch(
                "src.leads_filter_pipeline.download_profile_image_for_runtime",
                download_mock,
            ):
                with mock.patch(
                    "src.leads_filter_pipeline.fetch_profile_json_with_meta",
                    mock.AsyncMock(return_value=(refreshed_profile, 200, "")),
                ):
                    return await leads_filter_pipeline._v2_task_image_download(  # pylint: disable=protected-access
                        task,
                        state,
                        runtime,
                        filter_settings=settings,
                        warn=lambda _line: None,
                    )

        outcome = asyncio.run(_run())
        self.assertEqual(len(outcome.next_tasks), 1)
        self.assertEqual(outcome.next_tasks[0].task_type, leads_filter_pipeline.TASK_IMAGE_SCORE)
        self.assertEqual(download_mock.await_count, 2)
        first_url = download_mock.await_args_list[0].kwargs.get("image_url")
        second_url = download_mock.await_args_list[1].kwargs.get("image_url")
        self.assertEqual(first_url, "https://cdn.example/old.jpg")
        self.assertEqual(second_url, "https://cdn.example/new.jpg")
        self.assertEqual(state.profile.profile_pic_url, "https://cdn.example/new.jpg")

    def test_invalid_real_respects_retry_cap(self):
        runtime = leads_filter_pipeline.AccountRuntime(
            account={"username": "CuentaA"},
            username="CuentaA",
            svc=None,
            ctx=None,
            page=object(),
            profile_gate=leads_filter_pipeline.SlotGate(1),
            image_gate=leads_filter_pipeline.SlotGate(1),
            proxy_url="http://proxy",
            proxy_key="proxy://1",
            image_limiter=leads_filter_pipeline.ImageLimiter("proxy://1", daily_budget=0),
        )
        base_profile = leads_filter_pipeline.ProfileSnapshot(
            username="leadtest",
            biography="bio",
            full_name="Lead Test",
            follower_count=10,
            media_count=5,
            is_private=False,
            profile_pic_url="https://cdn.example/profile.jpg",
            user_id="1",
            external_url="",
            is_verified=False,
        )
        settings = leads_filter_pipeline.PipelineFilterSettings(
            classic_cfg=None,
            text_state="disabled",
            text_criteria="",
            image_state="required",
            image_prompt="hombre adulto",
        )

        async def _run_task(attempts: int):
            task = leads_filter_pipeline.ScheduledTask(
                idx=0,
                username="leadtest",
                task_type=leads_filter_pipeline.TASK_IMAGE_DOWNLOAD,
                attempts=attempts,
            )
            state = leads_filter_pipeline.LeadWorkState(
                idx=0,
                username="leadtest",
                account="CuentaA",
                profile=base_profile,
            )
            with mock.patch(
                "src.leads_filter_pipeline.download_profile_image_for_runtime",
                side_effect=leads_filter_pipeline.ImageDownloadError("invalid_real", status=200),
            ):
                outcome = await leads_filter_pipeline._v2_task_image_download(  # pylint: disable=protected-access
                    task,
                    state,
                    runtime,
                    filter_settings=settings,
                    warn=lambda _line: None,
                )
            return outcome, state

        first_outcome, first_state = asyncio.run(_run_task(attempts=0))
        self.assertTrue(first_outcome.requeue)
        self.assertEqual(first_outcome.requeue_reason, "image_invalid_real")
        self.assertEqual(first_state.image_status, "invalid_real")
        self.assertEqual(first_state.image_retry_count, 1)

        final_outcome, final_state = asyncio.run(_run_task(attempts=2))
        self.assertFalse(final_outcome.requeue)
        self.assertEqual(len(final_outcome.next_tasks), 1)
        self.assertEqual(final_outcome.next_tasks[0].task_type, leads_filter_pipeline.TASK_FINALIZE)
        self.assertEqual(final_state.image_status, "invalid_real")
        self.assertEqual(final_state.image_reason, "invalid_real")

    def test_gating_image_not_requested_when_not_needed(self):
        state = leads_filter_pipeline.LeadWorkState(
            idx=0,
            username="leadtest",
            account="CuentaA",
            profile=leads_filter_pipeline.ProfileSnapshot(
                username="leadtest",
                biography="bio",
                full_name="Lead Test",
                follower_count=10,
                media_count=5,
                is_private=False,
                profile_pic_url="https://cdn.example/profile.jpg",
                user_id="1",
                external_url="",
                is_verified=False,
            ),
        )
        task = leads_filter_pipeline.ScheduledTask(
            idx=0,
            username="leadtest",
            task_type=leads_filter_pipeline.TASK_TEXT_SCORE,
        )
        settings = leads_filter_pipeline.PipelineFilterSettings(
            classic_cfg=None,
            text_state="disabled",
            text_criteria="",
            image_state="disabled",
            image_prompt="",
        )

        outcome = asyncio.run(
            leads_filter_pipeline._v2_task_text_score(  # pylint: disable=protected-access
                task,
                state,
                filter_settings=settings,
                text_engine=leads_filter_pipeline.LocalTextEngine("", threshold=0.64),
                text_gate=asyncio.Semaphore(1),
            )
        )

        self.assertEqual(len(outcome.next_tasks), 1)
        self.assertEqual(outcome.next_tasks[0].task_type, leads_filter_pipeline.TASK_FINALIZE)
        self.assertEqual(outcome.stats.get("image_skipped"), 1)
        self.assertEqual(state.image_status, "skipped")
        self.assertEqual(state.image_reason, "image_not_requested")
        self.assertTrue(bool(state.pending_evaluation and state.pending_evaluation.passed))

    def test_gating_image_runs_only_after_text_pass(self):
        class FakeTextEngine:
            threshold = 0.64
            thresholds = leads_filter_pipeline.default_text_engine_thresholds()

            def score(self, _profile):
                return leads_filter_pipeline.TextDecision(
                    qualified=False,
                    score=0.2,
                    threshold=0.64,
                    mode="regex",
                    reason="text_mismatch",
                )

        state = leads_filter_pipeline.LeadWorkState(
            idx=0,
            username="leadtest",
            account="CuentaA",
            profile=leads_filter_pipeline.ProfileSnapshot(
                username="leadtest",
                biography="bio",
                full_name="Lead Test",
                follower_count=10,
                media_count=5,
                is_private=False,
                profile_pic_url="https://cdn.example/profile.jpg",
                user_id="1",
                external_url="",
                is_verified=False,
            ),
        )
        task = leads_filter_pipeline.ScheduledTask(
            idx=0,
            username="leadtest",
            task_type=leads_filter_pipeline.TASK_TEXT_SCORE,
        )
        settings = leads_filter_pipeline.PipelineFilterSettings(
            classic_cfg=None,
            text_state="required",
            text_criteria="coach",
            image_state="required",
            image_prompt="hombre adulto",
        )

        outcome = asyncio.run(
            leads_filter_pipeline._v2_task_text_score(  # pylint: disable=protected-access
                task,
                state,
                filter_settings=settings,
                text_engine=FakeTextEngine(),
                text_gate=asyncio.Semaphore(1),
            )
        )

        self.assertEqual(len(outcome.next_tasks), 1)
        self.assertEqual(outcome.next_tasks[0].task_type, leads_filter_pipeline.TASK_FINALIZE)
        self.assertEqual(state.image_status, "skipped")
        self.assertEqual(state.image_reason, "text_not_qualified")
        self.assertFalse(bool(state.pending_evaluation and state.pending_evaluation.passed))
        self.assertEqual(state.pending_evaluation.primary_reason, "texto_inteligente_no_califica")

    def test_parse_image_prompt_extracts_constraints(self):
        rules = parse_image_prompt("hombre menor de 30 con barba y sobrepeso")
        self.assertEqual(rules.gender, "male")
        self.assertEqual(rules.max_age, 30)
        self.assertTrue(rules.max_age_strict)
        self.assertTrue(rules.require_beard)
        self.assertTrue(rules.require_overweight)
        rules_min = parse_image_prompt("hombre 35+")
        self.assertEqual(rules_min.min_age, 35)
        self.assertFalse(rules_min.min_age_strict)
        rules_min_strict = parse_image_prompt("hombre mayor de 35")
        self.assertEqual(rules_min_strict.min_age, 35)
        self.assertTrue(rules_min_strict.min_age_strict)
        rules_no_beard = parse_image_prompt("hombre sin barba")
        self.assertFalse(rules_no_beard.require_beard)
        self.assertTrue(rules_no_beard.forbid_beard)

    def test_parse_image_prompt_handles_negative_examples_without_inverting_rules(self):
        prompt = (
            "Califica SOLO si se ve claramente un hombre 35+ con sobrepeso visible. "
            "No califican mujeres y no califica si parece menor de 35 o si no se ve sobrepeso."
        )
        rules = parse_image_prompt(prompt)
        self.assertEqual(rules.gender, "male")
        self.assertEqual(rules.min_age, 35)
        self.assertIsNone(rules.max_age)
        self.assertTrue(rules.require_overweight)
        self.assertFalse(rules.forbid_overweight)

    def test_evaluate_image_rules_beard_required(self):
        rules = parse_image_prompt("hombre con barba")
        analysis = ImageAnalysisResult(
            face_detected=True,
            age=42,
            gender="male",
            beard_prob=0.2,
            attribute_probs={},
        )
        ok, reason = evaluate_image_rules(analysis, rules)
        self.assertFalse(ok)
        self.assertEqual(reason, "no_beard")

    def test_evaluate_image_rules_beard_forbidden(self):
        rules = parse_image_prompt("hombre sin barba")
        analysis = ImageAnalysisResult(
            face_detected=True,
            age=42,
            gender="male",
            beard_prob=0.9,
            attribute_probs={},
        )
        ok, reason = evaluate_image_rules(analysis, rules)
        self.assertFalse(ok)
        self.assertEqual(reason, "beard_forbidden")

    def test_evaluate_image_rules_min_age_strict(self):
        rules = parse_image_prompt("hombre mayor de 35 con barba")
        analysis = ImageAnalysisResult(
            face_detected=True,
            age=35,
            gender="male",
            beard_prob=0.9,
            attribute_probs={},
        )
        ok, reason = evaluate_image_rules(analysis, rules)
        self.assertFalse(ok)
        self.assertEqual(reason, "age_below_min")

    def test_evaluate_image_rules_min_age_tolerance_for_profile_noise(self):
        rules = parse_image_prompt("hombre 35+ con sobrepeso")
        analysis = ImageAnalysisResult(
            face_detected=True,
            age=34,
            gender="male",
            beard_prob=0.0,
            attribute_probs={
                "overweight": 0.80,
                "age_over_30_prob": 0.90,
            },
        )
        ok, reason = evaluate_image_rules(analysis, rules)
        if AGE_MIN_TOLERANCE_YEARS <= 0:
            self.assertFalse(ok)
            self.assertEqual(reason, "age_below_min")
        else:
            self.assertTrue(ok)
            self.assertEqual(reason, "image_match")

    def test_evaluate_image_rules_min_age_rejects_clearly_below(self):
        rules = parse_image_prompt("hombre 35+ con sobrepeso")
        analysis = ImageAnalysisResult(
            face_detected=True,
            age=32,
            gender="male",
            beard_prob=0.0,
            attribute_probs={
                "overweight": 0.80,
                "age_over_30_prob": 0.95,
            },
        )
        ok, reason = evaluate_image_rules(analysis, rules)
        self.assertFalse(ok)
        self.assertEqual(reason, "age_below_min")

    def test_evaluate_image_rules_max_age_strict(self):
        rules = parse_image_prompt("hombre menor de 30 con barba")
        analysis = ImageAnalysisResult(
            face_detected=True,
            age=30,
            gender="male",
            beard_prob=0.9,
            attribute_probs={},
        )
        ok, reason = evaluate_image_rules(analysis, rules)
        self.assertFalse(ok)
        self.assertEqual(reason, "age_above_max")

    def test_evaluate_image_rules_overweight_recalibrated_default(self):
        rules = parse_image_prompt("hombre mayor de 35 con sobrepeso")
        analysis = ImageAnalysisResult(
            face_detected=True,
            age=40,
            gender="male",
            beard_prob=0.2,
            attribute_probs={"overweight": 0.7557},
        )
        ok, reason = evaluate_image_rules(analysis, rules)
        self.assertTrue(OVERWEIGHT_THRESHOLD <= 0.72)
        self.assertTrue(ok)
        self.assertEqual(reason, "image_match")

    def test_evaluate_image_rules_overweight_tolerance_accepts_borderline(self):
        rules = parse_image_prompt("hombre 35+ con sobrepeso")
        borderline_prob = max(0.0, (OVERWEIGHT_THRESHOLD - OVERWEIGHT_TOLERANCE) + 0.001)
        analysis = ImageAnalysisResult(
            face_detected=True,
            age=45,
            gender="male",
            beard_prob=0.0,
            attribute_probs={"overweight": borderline_prob},
        )
        ok, reason = evaluate_image_rules(analysis, rules)
        self.assertTrue(ok)
        self.assertEqual(reason, "image_match")

    def test_evaluate_image_rules_overweight_male35_relaxation(self):
        rules = parse_image_prompt("hombre 35+ con sobrepeso")
        # Should be below the generic threshold but above the male35 fallback.
        overweight_prob = max(
            0.0,
            min(
                1.0,
                min(
                    OVERWEIGHT_MALE35_THRESHOLD + 0.01,
                    (OVERWEIGHT_THRESHOLD - OVERWEIGHT_TOLERANCE) - 0.02,
                ),
            ),
        )
        analysis = ImageAnalysisResult(
            face_detected=True,
            age=50,
            gender="male",
            beard_prob=0.0,
            attribute_probs={
                "overweight": overweight_prob,
                "age_over_30_prob": 0.95,
            },
        )
        ok, reason = evaluate_image_rules(analysis, rules)
        self.assertTrue(ok)
        self.assertEqual(reason, "image_match")

    def test_image_ai_decision_uses_local_rule_pipeline(self):
        user = leads.ScrapedUser(
            username="leadtest",
            biography="",
            full_name="Lead Test",
            follower_count=0,
            media_count=0,
            is_private=False,
            profile_pic_url="",
        )
        sample_jpeg = b"\xff\xd8\xff" + (b"x" * 2048)
        mocked_analysis = ImageAnalysisResult(
            face_detected=True,
            age=28,
            gender="male",
            beard_prob=0.9,
            attribute_probs={"overweight": 0.8},
        )

        with mock.patch(
            "src.leads_filter_pipeline.ImageAttributeFilter.analyze",
            return_value=mocked_analysis,
        ):
            ok, reason = leads_filter_pipeline.image_ai_decision(
                user,
                "hombre menor de 30 con barba",
                image_bytes=sample_jpeg,
            )

        self.assertTrue(ok)
        self.assertEqual(reason, "image_match")


if __name__ == "__main__":
    unittest.main()
