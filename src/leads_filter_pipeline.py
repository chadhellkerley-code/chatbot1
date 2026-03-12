from __future__ import annotations

import asyncio
import contextlib
import heapq
import json
import logging
import os
import random
import re
import time
import unicodedata
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from core.proxy_preflight import preflight_accounts_for_proxy_runtime
from core.storage_atomic import atomic_write_json, load_json_file
from paths import storage_root
from src.content_publisher.session_client import AuthenticatedSession, create_authenticated_client
from src.image_attribute_filter import ImageAnalysisResult, ImageAttributeFilter
from src.image_engine_thresholds import (
    build_image_engine_thresholds,
    image_engine_thresholds_to_dict,
    sanitize_image_engine_thresholds_payload,
)
from src.image_prompt_parser import parse_image_prompt
from src.image_rule_evaluator import evaluate_image_rules
from src.text_engine_thresholds import (
    TextEngineThresholds,
    build_text_engine_thresholds,
    default_text_engine_thresholds,
    sanitize_text_engine_thresholds_payload,
    text_engine_thresholds_to_dict,
)
FILTER_STATE_REQUIRED = "required"
FILTER_STATE_INDIFFERENT = "indifferent"
FILTER_STATE_DISABLED = "disabled"

logger = logging.getLogger(__name__)

SAFE_RESPONSE_HEADERS_MISSING = "__safe_headers_missing__"
IMAGE_MAGIC_PREFIX_BYTES = 16
IMAGE_INVALID_REAL_MAX_RETRIES = 2
LARGE_RUN_AUTO_THRESHOLD = 1_000
LARGE_RUN_CHECKPOINT_EVERY = 500
DEFAULT_MAX_RUNTIME_SECONDS = 3_600
DEFAULT_PROFILE_MAX_RETRIES = 6
DEFAULT_IMAGE_MAX_RETRIES = 4
DEFAULT_RATE_LIMIT_MAX_RETRIES = 8
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 4
DEFAULT_CIRCUIT_BREAKER_SECONDS = 900.0
DEFAULT_STARTUP_WAIT_SECONDS = 25.0
DEFAULT_STARTUP_RETRY_ATTEMPTS = 1
MACRO_BLOCK_MIN = 60
MACRO_BLOCK_MAX = 140
MACRO_SHORT_BREAK_MIN_SECONDS = 30.0
MACRO_SHORT_BREAK_MAX_SECONDS = 120.0
MACRO_LONG_BREAK_EVERY_MIN = 3
MACRO_LONG_BREAK_EVERY_MAX = 6
MACRO_LONG_BREAK_MIN_SECONDS = 180.0
MACRO_LONG_BREAK_MAX_SECONDS = 540.0
MACRO_BASE_INTERVAL_MIN_SECONDS = 0.9
MACRO_BASE_INTERVAL_MAX_SECONDS = 1.4
MACRO_REQUEST_JITTER_SECONDS = 0.3
MACRO_RATE_LIMIT_WINDOW_SECONDS = 20.0 * 60.0
MACRO_CONCURRENCY_REDUCTION_SECONDS = 20.0 * 60.0
CHECKPOINT_SCHEMA_VERSION = 1


@dataclass
class TextDecision:
    qualified: bool
    score: float
    threshold: float
    mode: str
    reason: str


@dataclass
class LeadEvaluation:
    passed: bool
    primary_reason: str
    reasons: List[str]
    scores: Dict[str, Any]
    extracted: Dict[str, Any]


@dataclass
class SafeResponse:
    status: int
    headers: Dict[str, str]
    body: bytes
    final_url: str = ""


@dataclass
class AccountRuntime:
    account: Dict[str, Any]
    username: str
    svc: Any
    ctx: Any
    page: Any
    profile_gate: "SlotGate"
    image_gate: "SlotGate"
    semaphore: Optional[asyncio.Semaphore] = None
    cooldown_until: float = 0.0
    rate_limit_hits: int = 0
    proxy_url: str = ""
    proxy_key: str = ""
    worker_key: str = "__no_proxy__"
    profile_limiter: Optional["ProfileLimiter"] = None
    image_limiter: Optional["ImageLimiter"] = None
    user_agent: str = ""
    accept_language: str = "es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7"
    cookie_jar: Dict[str, str] = field(default_factory=dict)
    cookie_header: str = ""
    ig_app_id: str = ""
    asbd_id: str = ""
    request_timeout_min_ms: int = 8_000
    request_timeout_max_ms: int = 12_000
    macro_base_interval_seconds: float = 1.0
    macro_next_request_not_before: float = 0.0
    macro_block_target: int = 0
    macro_block_progress: int = 0
    macro_blocks_since_long_pause: int = 0
    macro_next_long_break_after_blocks: int = 0
    macro_pause_until: float = 0.0
    macro_default_profile_capacity: int = 1
    macro_reduced_concurrency_until: float = 0.0
    macro_rate_limit_events: Deque[float] = field(default_factory=deque)
    account_processed: int = 0
    profile_retry_max: int = DEFAULT_PROFILE_MAX_RETRIES
    image_retry_max: int = DEFAULT_IMAGE_MAX_RETRIES
    rate_limit_retry_max: int = DEFAULT_RATE_LIMIT_MAX_RETRIES
    profile_circuit_breaker_threshold: int = DEFAULT_CIRCUIT_BREAKER_THRESHOLD
    profile_circuit_breaker_seconds: float = DEFAULT_CIRCUIT_BREAKER_SECONDS
    image_circuit_breaker_threshold: int = DEFAULT_CIRCUIT_BREAKER_THRESHOLD
    image_circuit_breaker_seconds: float = DEFAULT_CIRCUIT_BREAKER_SECONDS
    disabled_reason: str = ""
    http_client: AuthenticatedSession | None = None
    session_manager: Any = None
    session: Any = None


@dataclass
class ProfileSnapshot:
    username: str
    biography: str
    full_name: str
    follower_count: int
    media_count: int
    is_private: bool
    profile_pic_url: str
    user_id: str
    external_url: str
    is_verified: bool


class LeadRetryLater(Exception):
    def __init__(self, reason: str, delay_seconds: float) -> None:
        super().__init__(reason)
        self.reason = str(reason or "retry_later")
        self.delay_seconds = max(1.0, float(delay_seconds))


class ProfileRateLimit(RuntimeError):
    def __init__(self, status: int, body: str = "") -> None:
        super().__init__(f"profile_rate_limit:{int(status)}")
        self.status = int(status)
        self.body = str(body or "")


class ProfileHttpError(RuntimeError):
    def __init__(self, status: int, reason: str = "", body: str = "") -> None:
        normalized_status = int(status or 0)
        normalized_reason = str(reason or "profile_http_error")
        super().__init__(f"{normalized_reason}:{normalized_status}")
        self.status = normalized_status
        self.reason = normalized_reason
        self.body = str(body or "")


class ImageRateLimit(RuntimeError):
    def __init__(self, proxy_key: str, status: int, body: str = "") -> None:
        normalized_proxy = str(proxy_key or "-")
        normalized_status = int(status or 0)
        super().__init__(f"image_rate_limit:{normalized_proxy}:{normalized_status}")
        self.proxy_key = normalized_proxy
        self.status = normalized_status
        self.body = str(body or "")


class ImageDownloadError(RuntimeError):
    def __init__(self, reason: str, *, status: int = 0) -> None:
        normalized_reason = str(reason or "image_download_failed")
        normalized_status = int(status or 0)
        super().__init__(f"{normalized_reason}:{normalized_status}")
        self.reason = normalized_reason
        self.status = normalized_status


TASK_PROFILE = "profile"
TASK_TEXT_SCORE = "text_score"
TASK_IMAGE_DOWNLOAD = "image_download"
TASK_IMAGE_SCORE = "image_score"
TASK_FINALIZE = "finalize"

TASK_PRIORITY: Dict[str, int] = {
    # Downstream-first priority keeps the pipeline flowing and avoids
    # starvation where profile tasks monopolize dispatch.
    TASK_FINALIZE: 0,
    TASK_IMAGE_SCORE: 1,
    TASK_IMAGE_DOWNLOAD: 2,
    TASK_TEXT_SCORE: 3,
    TASK_PROFILE: 4,
}
ACCOUNT_RUNTIME_TASK_TYPES = frozenset({TASK_PROFILE, TASK_IMAGE_DOWNLOAD})

@dataclass
class ScheduledTask:
    idx: int
    username: str
    task_type: str
    attempts: int = 0
    next_attempt_at: float = 0.0


@dataclass
class TaskExecution:
    task: ScheduledTask
    account: str = ""
    finalized: bool = False
    requeue: bool = False
    requeue_delay_seconds: float = 0.0
    requeue_reason: str = ""
    max_retries: int = 0
    next_tasks: List[ScheduledTask] = field(default_factory=list)
    stats: Dict[str, int] = field(default_factory=dict)


@dataclass
class LeadWorkState:
    idx: int
    username: str
    account: str = ""
    profile: Optional["ProfileSnapshot"] = None
    extracted: Dict[str, Any] = field(default_factory=dict)
    scores: Dict[str, Any] = field(default_factory=dict)
    reasons: List[str] = field(default_factory=list)
    image_bytes: Optional[bytes] = None
    image_status: str = "skipped"
    image_reason: str = "not_requested"
    image_retry_count: int = 0
    image_next_attempt_at: str = ""
    pending_evaluation: Optional["LeadEvaluation"] = None


@dataclass
class PipelineFilterSettings:
    classic_cfg: Any
    text_state: str
    text_criteria: str
    image_state: str
    image_prompt: str
    text_engine_thresholds: Dict[str, Any] = field(default_factory=dict)
    image_engine_thresholds: Dict[str, Any] = field(default_factory=dict)


class SlotGate:
    def __init__(self, capacity: int) -> None:
        self.capacity = max(1, int(capacity))
        self._in_use = 0
        self._lock = asyncio.Lock()

    async def try_acquire(self) -> bool:
        async with self._lock:
            if self._in_use >= self.capacity:
                return False
            self._in_use += 1
            return True

    async def release(self) -> None:
        async with self._lock:
            if self._in_use > 0:
                self._in_use -= 1

    async def set_capacity(self, capacity: int) -> None:
        async with self._lock:
            self.capacity = max(1, int(capacity))

    async def snapshot(self) -> Tuple[int, int]:
        async with self._lock:
            return self.capacity, self._in_use


class AdaptiveTokenLimiter:
    def __init__(
        self,
        *,
        key_label: str,
        key_value: str,
        min_interval_seconds: float,
        burst_max: int,
        cooldown_min_seconds: float,
        cooldown_max_seconds: float,
        tune_floor_seconds: float,
        tune_ceiling_seconds: float,
        tune_up_step_seconds: float = 0.5,
        daily_budget: int,
    ) -> None:
        self.key_label = str(key_label)
        self.key_value = str(key_value or "-")
        self.min_interval_seconds = max(0.1, float(min_interval_seconds))
        self.burst_max = max(1, int(burst_max))
        self.cooldown_min_seconds = max(1.0, float(cooldown_min_seconds))
        self.cooldown_max_seconds = max(self.cooldown_min_seconds, float(cooldown_max_seconds))
        self.tune_floor_seconds = max(0.1, float(tune_floor_seconds))
        self.tune_ceiling_seconds = max(self.tune_floor_seconds, float(tune_ceiling_seconds))
        self.tune_up_step_seconds = max(0.05, float(tune_up_step_seconds))
        self.daily_budget = max(0, int(daily_budget))

        self.bucket_tokens = float(self.burst_max)
        self.bucket_updated_at = time.monotonic()
        self.cooling_until = 0.0
        self.exhausted = False
        self._budget_day = datetime.utcnow().date().isoformat()
        self._budget_used = 0
        self._budget_exhausted_logged = False
        self._rate_limit_events: deque[float] = deque()
        self._last_tune_down_at = 0.0
        self.consecutive_429 = 0
        self.circuit_open_until = 0.0
        self._lock = asyncio.Lock()

    def _seconds_until_budget_reset(self) -> float:
        now_dt = datetime.utcnow()
        reset_dt = (now_dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return max(1.0, (reset_dt - now_dt).total_seconds())

    def _refresh_daily_budget_locked(self) -> None:
        today = datetime.utcnow().date().isoformat()
        if today != self._budget_day:
            self._budget_day = today
            self._budget_used = 0
            self.exhausted = False
            self._budget_exhausted_logged = False

    def _prune_rate_limits_locked(self, now_mono: float) -> None:
        window_seconds = 600.0
        while self._rate_limit_events and (now_mono - self._rate_limit_events[0]) > window_seconds:
            self._rate_limit_events.popleft()

    def _refill_tokens_locked(self, now_mono: float) -> None:
        elapsed = max(0.0, now_mono - self.bucket_updated_at)
        self.bucket_updated_at = now_mono
        refill = elapsed / max(0.1, self.min_interval_seconds)
        self.bucket_tokens = min(float(self.burst_max), self.bucket_tokens + refill)

    def _peek_wait_locked(self, now_mono: float) -> Tuple[float, str]:
        self._refresh_daily_budget_locked()
        if self.daily_budget > 0 and self._budget_used >= self.daily_budget:
            self.exhausted = True
            return self._seconds_until_budget_reset(), "budget_exhausted"
        if self.cooling_until > now_mono:
            return self.cooling_until - now_mono, "cooldown"
        self._refill_tokens_locked(now_mono)
        if self.bucket_tokens >= 1.0:
            return 0.0, "ready"
        wait_seconds = (1.0 - self.bucket_tokens) * max(0.1, self.min_interval_seconds)
        return max(0.05, wait_seconds), "throttle"

    async def peek_wait_seconds(self) -> Tuple[float, str]:
        async with self._lock:
            now_mono = time.monotonic()
            return self._peek_wait_locked(now_mono)

    async def try_acquire_slot(self) -> Tuple[bool, float, str]:
        async with self._lock:
            now_mono = time.monotonic()
            wait_seconds, reason = self._peek_wait_locked(now_mono)
            if wait_seconds > 0.0:
                return False, wait_seconds, reason
            self.bucket_tokens = max(0.0, self.bucket_tokens - 1.0)
            if self.daily_budget > 0:
                self._budget_used += 1
                if self._budget_used >= self.daily_budget:
                    self.exhausted = True
            return True, 0.0, "ok"

    async def wait_for_slot(self) -> None:
        while True:
            acquired, wait_seconds, _reason = await self.try_acquire_slot()
            if acquired:
                return
            await asyncio.sleep(max(0.05, min(1.0, wait_seconds)))

    async def circuit_wait_seconds(self) -> float:
        async with self._lock:
            now_mono = time.monotonic()
            if self.circuit_open_until <= now_mono:
                self.circuit_open_until = 0.0
                return 0.0
            return float(self.circuit_open_until - now_mono)

    async def record_http_429(
        self,
        *,
        threshold: int,
        circuit_seconds: float,
        warn: Optional[Callable[[str], None]] = None,
    ) -> None:
        normalized_threshold = max(1, int(threshold or 1))
        normalized_circuit_seconds = max(1.0, float(circuit_seconds or 1.0))
        opened = False
        until_epoch = 0.0
        async with self._lock:
            self.consecutive_429 += 1
            if self.consecutive_429 >= normalized_threshold:
                now_mono = time.monotonic()
                candidate_until = now_mono + normalized_circuit_seconds
                if candidate_until > self.circuit_open_until:
                    self.circuit_open_until = candidate_until
                    opened = True
                    until_epoch = _mono_to_epoch(candidate_until)
                self.consecutive_429 = 0
        if opened and warn is not None:
            warn(
                (
                    "[LEADS][CIRCUITO_ABIERTO] "
                    f"tipo={self.key_label} | clave={self.key_value} | "
                    f"duracion={normalized_circuit_seconds:.0f}s | "
                    f"hasta={_v2_epoch_to_iso(until_epoch)}"
                )
            )

    async def record_success(self) -> None:
        async with self._lock:
            self.consecutive_429 = 0

    async def apply_rate_limit(self, *, status: int, warn: Callable[[str], None]) -> float:
        async with self._lock:
            now_mono = time.monotonic()
            self._rate_limit_events.append(now_mono)
            self._prune_rate_limits_locked(now_mono)
            previous_interval = self.min_interval_seconds
            self.min_interval_seconds = min(
                self.tune_ceiling_seconds,
                self.min_interval_seconds + self.tune_up_step_seconds,
            )
            cooldown_seconds = random.uniform(self.cooldown_min_seconds, self.cooldown_max_seconds)
            self.cooling_until = max(self.cooling_until, now_mono + cooldown_seconds)
            tuned = self.min_interval_seconds != previous_interval
        warn(
            (
                "[LEADS][LIMITE_TASA] "
                f"tipo={self.key_label} | clave={self.key_value} | codigo={status}"
            )
        )
        warn(
            (
                "[LEADS][ENFRIAMIENTO] "
                f"tipo={self.key_label} | clave={self.key_value} | enfriamiento={cooldown_seconds:.0f}s"
            )
        )
        if tuned:
            warn(
                (
                    "[LEADS][AUTO_AJUSTE] "
                    f"tipo={self.key_label} | clave={self.key_value} | "
                    f"intervalo_minimo={self.min_interval_seconds:.2f}s"
                )
            )
        return cooldown_seconds

    async def apply_success_tuning(self, *, warn: Callable[[str], None]) -> None:
        tuned = False
        previous = 0.0
        async with self._lock:
            now_mono = time.monotonic()
            self._prune_rate_limits_locked(now_mono)
            if self._rate_limit_events:
                return
            if (now_mono - self._last_tune_down_at) < 60.0:
                return
            previous = self.min_interval_seconds
            next_interval = max(self.tune_floor_seconds, self.min_interval_seconds - 0.1)
            if next_interval < self.min_interval_seconds:
                self.min_interval_seconds = next_interval
                self._last_tune_down_at = now_mono
                tuned = True
        if tuned:
            warn(
                (
                    "[LEADS][AUTO_AJUSTE] "
                    f"tipo={self.key_label} | clave={self.key_value} | "
                    f"intervalo_minimo={self.min_interval_seconds:.2f}s"
                )
            )


class ProfileLimiter(AdaptiveTokenLimiter):
    def __init__(
        self,
        account_username: str,
        *,
        daily_budget: int,
        delay_min_seconds: float,
        delay_max_seconds: float,
    ) -> None:
        normalized_delay_min = max(0.1, float(delay_min_seconds))
        normalized_delay_max = max(normalized_delay_min, float(delay_max_seconds))
        super().__init__(
            key_label="account",
            key_value=f"@{account_username}",
            min_interval_seconds=random.uniform(normalized_delay_min, normalized_delay_max),
            burst_max=2,
            cooldown_min_seconds=300.0,
            cooldown_max_seconds=900.0,
            tune_floor_seconds=normalized_delay_min,
            tune_ceiling_seconds=max(normalized_delay_max, normalized_delay_min + 0.5),
            tune_up_step_seconds=0.3,
            daily_budget=daily_budget,
        )


class ImageLimiter(AdaptiveTokenLimiter):
    def __init__(self, proxy_key: str, *, daily_budget: int) -> None:
        super().__init__(
            key_label="proxy",
            key_value=proxy_key,
            min_interval_seconds=random.uniform(0.8, 1.5),
            burst_max=random.randint(2, 3),
            cooldown_min_seconds=120.0,
            cooldown_max_seconds=480.0,
            tune_floor_seconds=0.6,
            tune_ceiling_seconds=2.5,
            tune_up_step_seconds=0.3,
            daily_budget=daily_budget,
        )


TEXT_REGEX_STOPWORDS: Set[str] = {
    "de",
    "la",
    "el",
    "los",
    "las",
    "para",
    "por",
    "con",
    "sin",
    "una",
    "uno",
    "que",
    "como",
    "the",
    "and",
    "for",
    "with",
    "this",
    "that",
    "y",
    "o",
    "a",
    "en",
    "to",
    "of",
    "is",
    "are",
}

LANG_STOPWORDS: Dict[str, Set[str]] = {
    "es": {
        "de",
        "la",
        "que",
        "el",
        "en",
        "y",
        "a",
        "los",
        "del",
        "se",
        "las",
        "por",
        "un",
        "para",
        "con",
        "no",
        "una",
    },
    "pt": {
        "de",
        "a",
        "o",
        "que",
        "e",
        "do",
        "da",
        "em",
        "um",
        "para",
        "com",
        "nao",
        "uma",
    },
    "en": {
        "the",
        "be",
        "to",
        "of",
        "and",
        "a",
        "in",
        "that",
        "have",
        "i",
        "it",
        "for",
        "not",
        "on",
        "with",
    },
}

for _bucket in LANG_STOPWORDS.values():
    TEXT_REGEX_STOPWORDS.update(_bucket)


class LocalTextEngine:
    def __init__(
        self,
        criteria: str,
        *,
        threshold: Optional[float] = None,
        thresholds: TextEngineThresholds | Dict[str, Any] | None = None,
    ) -> None:
        self.criteria = (criteria or "").strip()
        if thresholds is None:
            if threshold is None:
                thresholds = default_text_engine_thresholds()
            else:
                thresholds = {"embeddings_threshold": threshold}
        self.thresholds = build_text_engine_thresholds(thresholds)
        self.threshold = float(self.thresholds.embeddings_threshold)
        self.mode = "regex"
        self._model = None
        self._criteria_embedding = None
        self._positive_terms, self._negative_terms = self._extract_regex_terms(self.criteria)
        self._maybe_load_embeddings()

    def _maybe_load_embeddings(self) -> None:
        if not self.criteria:
            return
        if env_truthy("LEADS_DISABLE_EMBEDDINGS", False):
            return
        model_name = os.getenv(
            "LEADS_TEXT_MODEL",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        )
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
        except Exception:
            return
        try:
            model = SentenceTransformer(model_name)
            criteria_vec = model.encode([self.criteria], normalize_embeddings=True)
            self._model = model
            self._criteria_embedding = criteria_vec[0]
            self.mode = "hybrid"
            logger.info(
                "Texto inteligente local: embeddings activos con modelo '%s' (modo hibrido).",
                model_name,
            )
        except Exception as exc:
            logger.warning(
                "Texto inteligente local: embeddings no disponibles (%s). Se usa regex scoring.",
                exc,
            )

    def score(self, profile: ProfileSnapshot) -> TextDecision:
        if not self.criteria:
            return TextDecision(True, 1.0, self.threshold, self.mode, "criteria_empty")

        combined = " ".join(
            [
                profile.username or "",
                profile.full_name or "",
                profile.biography or "",
            ]
        ).strip()
        if not combined:
            return TextDecision(False, 0.0, self.threshold, self.mode, "text_empty")

        regex_decision = self._score_regex(profile)
        if self._model is not None and self._criteria_embedding is not None:
            try:
                import numpy as np  # type: ignore

                vector = self._model.encode([combined], normalize_embeddings=True)[0]
                similarity = float(np.dot(self._criteria_embedding, vector))
                blend_weight = max(0.0, min(1.0, float(self.thresholds.hybrid_embeddings_weight)))
                blended_score = (similarity * blend_weight) + (
                    float(regex_decision.score) * (1.0 - blend_weight)
                )
                qualified = (blended_score + 1e-9) >= self.threshold
                return TextDecision(
                    qualified=qualified,
                    score=blended_score,
                    threshold=self.threshold,
                    mode="hybrid",
                    reason=(
                        f"score_text={blended_score:.3f} threshold={self.threshold:.3f} "
                        f"decision_text={'pass' if qualified else 'fail'} "
                        f"score_emb={similarity:.3f} score_regex={float(regex_decision.score):.3f} "
                        f"w_emb={blend_weight:.2f}"
                    ),
                )
            except Exception as exc:
                logger.warning(
                    "Texto inteligente local: fallo embeddings (%s). Se usa regex scoring.",
                    exc,
                )
                self.mode = "regex"

        return regex_decision

    def _score_regex(self, profile: ProfileSnapshot) -> TextDecision:
        haystack_bio = normalize_text(" ".join([profile.full_name or "", profile.biography or ""]))
        haystack_user = normalize_text(profile.username or "")

        if not self._positive_terms:
            return TextDecision(True, 1.0, self.threshold, "regex", "no_positive_terms")

        points = 0
        max_points = max(1, len(self._positive_terms) * 2)
        for term in self._positive_terms:
            if contains_term(term, haystack_bio):
                points += 2
            elif contains_term(term, haystack_user):
                points += 1

        for term in self._negative_terms:
            if contains_term(term, haystack_bio) or contains_term(term, haystack_user):
                points -= 3

        normalized = max(0.0, min(1.0, points / float(max_points)))
        # Regex fallback is less expressive than embeddings; avoid forcing an overly strict threshold.
        coverage_hint = self.thresholds.regex_coverage_base + (
            self.thresholds.regex_coverage_per_term
            * min(self.thresholds.regex_coverage_max_terms, len(self._positive_terms))
        )
        lower_bound = min(
            self.thresholds.regex_floor_threshold,
            self.thresholds.regex_ceiling_threshold,
        )
        upper_bound = max(
            self.thresholds.regex_floor_threshold,
            self.thresholds.regex_ceiling_threshold,
        )
        threshold = max(lower_bound, min(upper_bound, self.threshold, coverage_hint))
        qualified = (normalized + 1e-9) >= threshold
        return TextDecision(
            qualified=qualified,
            score=normalized,
            threshold=threshold,
            mode="regex",
            reason=(
                f"score_text={normalized:.3f} threshold={threshold:.3f} "
                f"decision_text={'pass' if qualified else 'fail'}"
            ),
        )

    @staticmethod
    def _extract_regex_terms(criteria: str) -> Tuple[Set[str], Set[str]]:
        normalized = normalize_text(criteria)
        if not normalized:
            return set(), set()

        tokens = [
            tok
            for tok in re.findall(r"[a-z0-9_]+", normalized)
            if len(tok) >= 3 and tok not in TEXT_REGEX_STOPWORDS
        ]
        positive = set(tokens)

        negative: Set[str] = set()
        for pattern in (
            r"(?:no|sin|evitar|descartar|excluir)\s+([a-z0-9_]{3,})",
            r"todo\s+lo\s+contrario\s+(?:de\s+)?([a-z0-9_]{3,})",
        ):
            for match in re.finditer(pattern, normalized):
                term = match.group(1).strip()
                if term and term not in TEXT_REGEX_STOPWORDS:
                    negative.add(term)

        return positive, negative


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    # Normalize accents and combining marks to improve deterministic matching.
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9_\s./-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def detect_language(text: str) -> str:
    normalized = normalize_text(text)
    if not normalized:
        return "unknown"
    tokens = normalized.split()
    scores: Dict[str, int] = {}
    for lang, vocab in LANG_STOPWORDS.items():
        scores[lang] = sum(1 for token in tokens if token in vocab)
    best_lang = max(scores, key=scores.get)
    best_score = scores.get(best_lang, 0)
    if best_score <= 0:
        return "unknown"
    tied = [lang for lang, score in scores.items() if score == best_score]
    if len(tied) > 1:
        return "unknown"
    return best_lang


def contains_term(term: str, haystack: str) -> bool:
    normalized_term = normalize_text(term)
    normalized_haystack = normalize_text(haystack)
    if not normalized_term or not normalized_haystack:
        return False
    semantic_aliases = {
        "adelgazar": (
            r"\badelgaz\w*",
            r"\bbajar\w*\b(?:\s+de)?\s+\bpeso\b",
            r"\bperd\w*\b(?:\s+de)?\s+(?:\bpeso\b|\bkg\b|\bkilos?\b)",
            r"\bsobrepeso\b",
            r"\bobes\w*",
            r"\bkg\b",
            r"\bkilos?\b",
        ),
        "bajar": (
            r"\bbajar\w*",
            r"\breduc\w*",
            r"\bperd\w*\b(?:\s+de)?\s+(?:\bpeso\b|\bkg\b|\bkilos?\b)",
            r"\bsobrepeso\b",
        ),
        "peso": (
            r"\bpeso\b",
            r"\bsobrepeso\b",
            r"\bobes\w*",
            r"\bkg\b",
            r"\bkilos?\b",
        ),
        "ayuda": (r"\bayud\w*",),
        "ayudar": (r"\bayud\w*",),
        "ayuden": (r"\bayud\w*",),
        "ayudo": (r"\bayud\w*",),
    }
    aliases = semantic_aliases.get(normalized_term)
    if aliases:
        for pattern in aliases:
            if re.search(pattern, normalized_haystack):
                return True
    if " " in normalized_term:
        return normalized_term in normalized_haystack
    if re.search(rf"\b{re.escape(normalized_term)}\b", normalized_haystack):
        return True
    # Light stemming for plural/verb variants and compound words (e.g. "sobrepeso" ~= "peso").
    if len(normalized_term) >= 4:
        tokens = [tok for tok in normalized_haystack.split() if tok]
        root = normalized_term[:4]
        for token in tokens:
            if token == normalized_term:
                return True
            if token.startswith(normalized_term) or token.endswith(normalized_term):
                return True
            if len(token) >= 4 and token[:4] == root and abs(len(token) - len(normalized_term)) <= 4:
                return True
    return False


def normalize_filter_state(raw: Any, *, default: str = FILTER_STATE_DISABLED) -> str:
    value = str(raw or "").strip().lower()
    mapping = {
        "required": FILTER_STATE_REQUIRED,
        "requerido": FILTER_STATE_REQUIRED,
        "requerida": FILTER_STATE_REQUIRED,
        "indifferent": FILTER_STATE_INDIFFERENT,
        "indiferente": FILTER_STATE_INDIFFERENT,
        "disabled": FILTER_STATE_DISABLED,
        "deshabilitado": FILTER_STATE_DISABLED,
        "desactivado": FILTER_STATE_DISABLED,
        "off": FILTER_STATE_DISABLED,
    }
    normalized = mapping.get(value, value)
    valid_states = {
        FILTER_STATE_REQUIRED,
        FILTER_STATE_INDIFFERENT,
        FILTER_STATE_DISABLED,
    }
    if normalized not in valid_states:
        fallback = str(default or FILTER_STATE_DISABLED).strip().lower()
        fallback = mapping.get(fallback, fallback)
        if fallback in valid_states:
            return fallback
        return FILTER_STATE_DISABLED
    return normalized


def _config_value(config: Any, key: str, default: Any = None) -> Any:
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


def _safe_int(value: Any, *, default: int = 0, minimum: Optional[int] = None) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    if minimum is not None:
        parsed = max(int(minimum), parsed)
    return parsed


def _normalize_choice(value: Any, *, allowed: Set[str], default: str) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in allowed:
        return candidate
    return str(default or "").strip().lower()


def _normalize_keywords(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    elif value is None:
        raw_items = []
    else:
        raw_items = [value]
    deduped: List[str] = []
    seen: Set[str] = set()
    for raw_item in raw_items:
        candidate = str(raw_item or "").strip()
        normalized = normalize_text(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(candidate)
    return deduped


def _normalize_classic_filter_cfg(cfg: Any) -> Dict[str, Any]:
    min_followers = _safe_int(_config_value(cfg, "min_followers", 0), default=0, minimum=0)
    min_posts = _safe_int(_config_value(cfg, "min_posts", 0), default=0, minimum=0)
    privacy = _normalize_choice(
        _config_value(cfg, "privacy", "any"),
        allowed={"public", "private", "any"},
        default="any",
    )
    link_in_bio = _normalize_choice(
        _config_value(cfg, "link_in_bio", "any"),
        allowed={"yes", "no", "any"},
        default="any",
    )
    include_keywords = _normalize_keywords(_config_value(cfg, "include_keywords", []))
    exclude_keywords = _normalize_keywords(_config_value(cfg, "exclude_keywords", []))
    language = _normalize_choice(
        _config_value(cfg, "language", "any"),
        allowed={"es", "pt", "en", "any"},
        default="any",
    )

    followers_state = normalize_filter_state(
        _config_value(cfg, "min_followers_state", None),
        default=FILTER_STATE_REQUIRED if min_followers > 0 else FILTER_STATE_DISABLED,
    )
    posts_state = normalize_filter_state(
        _config_value(cfg, "min_posts_state", None),
        default=FILTER_STATE_REQUIRED if min_posts > 0 else FILTER_STATE_DISABLED,
    )
    privacy_state = normalize_filter_state(
        _config_value(cfg, "privacy_state", None),
        default=FILTER_STATE_REQUIRED if privacy != "any" else FILTER_STATE_DISABLED,
    )
    link_state = normalize_filter_state(
        _config_value(cfg, "link_in_bio_state", None),
        default=FILTER_STATE_REQUIRED if link_in_bio != "any" else FILTER_STATE_DISABLED,
    )
    include_state = normalize_filter_state(
        _config_value(cfg, "include_keywords_state", None),
        default=FILTER_STATE_REQUIRED if include_keywords else FILTER_STATE_DISABLED,
    )
    exclude_state = normalize_filter_state(
        _config_value(cfg, "exclude_keywords_state", None),
        default=FILTER_STATE_REQUIRED if exclude_keywords else FILTER_STATE_DISABLED,
    )
    language_state = normalize_filter_state(
        _config_value(cfg, "language_state", None),
        default=FILTER_STATE_REQUIRED if language != "any" else FILTER_STATE_DISABLED,
    )

    if min_followers <= 0:
        followers_state = FILTER_STATE_DISABLED
    if min_posts <= 0:
        posts_state = FILTER_STATE_DISABLED
    if privacy == "any":
        privacy_state = FILTER_STATE_DISABLED
    if link_in_bio == "any":
        link_state = FILTER_STATE_DISABLED
    if not include_keywords:
        include_state = FILTER_STATE_DISABLED
    if not exclude_keywords:
        exclude_state = FILTER_STATE_DISABLED
    if language == "any":
        language_state = FILTER_STATE_DISABLED

    return {
        "min_followers": min_followers,
        "min_posts": min_posts,
        "privacy": privacy,
        "link_in_bio": link_in_bio,
        "include_keywords": include_keywords,
        "exclude_keywords": exclude_keywords,
        "language": language,
        "followers_state": followers_state,
        "posts_state": posts_state,
        "privacy_state": privacy_state,
        "link_state": link_state,
        "include_state": include_state,
        "exclude_state": exclude_state,
        "language_state": language_state,
    }


def env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "si", "s"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    try:
        return float(str(raw).replace(",", "."))
    except Exception:
        return float(default)


def clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, int(value)))


HTTP_META_PATH = storage_root(Path(__file__).resolve().parents[1]) / "lead_filters" / "account_http_meta.json"
DEFAULT_IG_APP_ID = "936619743392459"
DEFAULT_ASBD_ID = "198387"
DEFAULT_HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
REQUIRED_IG_COOKIE_NAMES: Tuple[str, ...] = (
    "sessionid",
    "ds_user_id",
    "csrftoken",
    "mid",
    "ig_did",
)


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def load_http_meta_store() -> Dict[str, Any]:
    if not HTTP_META_PATH.exists():
        return {}
    try:
        payload = load_json_file(HTTP_META_PATH, {}, label="leads_filter.http_meta")
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def save_http_meta_store(payload: Dict[str, Any]) -> None:
    HTTP_META_PATH.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(HTTP_META_PATH, payload)


def read_account_http_meta(account_username: str) -> Dict[str, Any]:
    store = load_http_meta_store()
    raw = store.get(str(account_username or "").strip().lower())
    return dict(raw) if isinstance(raw, dict) else {}


def persist_account_http_meta(account_username: str, payload: Dict[str, Any]) -> None:
    if not account_username:
        return
    store = load_http_meta_store()
    key = str(account_username).strip().lower()
    current = store.get(key)
    merged: Dict[str, Any] = {}
    if isinstance(current, dict):
        merged.update(current)
    merged.update(payload or {})
    merged["updated_at"] = now_iso()
    store[key] = merged
    save_http_meta_store(store)


def extract_bootstrap_value(text: str, patterns: List[str]) -> str:
    if not text:
        return ""
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _slugify_reason(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized[:64]


def build_profile_http_reason(status: int, body: str) -> str:
    status_code = max(0, int(status or 0))
    if status_code <= 0:
        return "http_unknown"
    base = f"http_{status_code}"
    raw_body = str(body or "").strip()
    if not raw_body:
        return base
    try:
        payload = json.loads(raw_body)
    except Exception:
        return base
    if not isinstance(payload, dict):
        return base
    message = str(payload.get("message") or payload.get("error") or "").strip()
    detail = _slugify_reason(message)
    if not detail:
        return base
    return f"{base}_{detail}"


def _classify_runtime_health_failure(reason: str, *, status_code: int = 0) -> Tuple[str, str] | None:
    normalized_reason = _slugify_reason(str(reason or ""))
    normalized_status = max(0, int(status_code or 0))
    if not normalized_reason and normalized_status == 401:
        normalized_reason = "http_401"
    if not normalized_reason:
        return None

    dead_tokens = (
        "challenge",
        "checkpoint",
        "suspended",
        "disabled",
        "captcha",
        "two_factor",
        "confirm_email",
        "verification_required",
    )
    inactive_tokens = (
        "login_required",
        "logged_out",
        "session_expired",
        "session_invalid",
        "not_logged_in",
        "auth_required",
        "authentication_required",
    )

    if any(token in normalized_reason for token in dead_tokens):
        return "dead", normalized_reason
    if any(token in normalized_reason for token in inactive_tokens):
        return "inactive", normalized_reason
    if normalized_status == 401 or normalized_reason == "http_401":
        return "inactive", normalized_reason or "http_401"
    return None


def _isolate_runtime_for_account_health(
    runtime: Optional[AccountRuntime],
    *,
    status_code: int = 0,
    reason: str = "",
    warn: Optional[Callable[[str], None]] = None,
) -> bool:
    if runtime is None:
        return False
    classification = _classify_runtime_health_failure(reason, status_code=status_code)
    if classification is None:
        return False

    state_kind, normalized_reason = classification
    account_label = str(runtime.username or "").strip().lstrip("@") or "-"
    already_disabled = bool(str(runtime.disabled_reason or "").strip())
    runtime.disabled_reason = normalized_reason

    try:
        import health_store

        if state_kind == "dead":
            health_store.mark_blocked(account_label, reason=normalized_reason)
        else:
            health_store.mark_session_expired(account_label, reason=normalized_reason)
    except Exception as exc:
        if warn is not None:
            warn(
                (
                    "[LEADS][ACCOUNT_HEALTH] "
                    f"no se pudo persistir salud de @{account_label}: {exc}"
                )
            )

    if warn is not None and not already_disabled:
        warn(
            (
                "[LEADS][ACCOUNT_HEALTH] "
                f"cuenta aislada del run @{account_label}: {normalized_reason}"
            )
        )
    return True


async def _safe_shutdown_runtime_handle(runtime: Optional[AccountRuntime]) -> None:
    if runtime is None:
        return
    http_client = getattr(runtime, "http_client", None)
    if http_client is not None:
        with contextlib.suppress(Exception):
            http_client.close()
        runtime.http_client = None
    session_manager = getattr(runtime, "session_manager", None)
    session = getattr(runtime, "session", None)
    if session_manager is not None and session is not None:
        current_url = ""
        try:
            current_url = str(getattr(getattr(runtime, "page", None), "url", "") or "")
        except Exception:
            current_url = ""
        with contextlib.suppress(Exception):
            await session_manager.finalize_session(session, current_url=current_url)
        runtime.session = None
        return
    try:
        from src.playwright_service import shutdown
    except Exception:
        return
    with contextlib.suppress(Exception):
        await shutdown(runtime.svc, runtime.ctx)


def runtime_request_timeout_ms(runtime: Optional[AccountRuntime]) -> int:
    min_ms = max(1_000, int(getattr(runtime, "request_timeout_min_ms", 8_000) or 8_000))
    max_ms = max(min_ms, int(getattr(runtime, "request_timeout_max_ms", 12_000) or 12_000))
    return int(random.uniform(min_ms, max_ms))


async def refresh_runtime_cookie_jar(runtime: AccountRuntime) -> None:
    http_client = getattr(runtime, "http_client", None)
    if http_client is not None:
        jar = {
            str(name): str(value).strip()
            for name, value in dict(getattr(http_client, "cookie_map", {}) or {}).items()
            if str(name).strip() and str(value).strip() and str(name).strip() in REQUIRED_IG_COOKIE_NAMES
        }
        runtime.cookie_jar = jar
        runtime.cookie_header = "; ".join(
            f"{name}={jar[name]}" for name in REQUIRED_IG_COOKIE_NAMES if name in jar
        )
        return
    context = getattr(getattr(runtime, "page", None), "context", None)
    if context is None:
        return
    try:
        cookies = await context.cookies("https://www.instagram.com/")
    except Exception:
        return
    if not isinstance(cookies, list):
        return
    jar: Dict[str, str] = {}
    for row in cookies:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        value = str(row.get("value") or "").strip()
        if name and value and name in REQUIRED_IG_COOKIE_NAMES:
            jar[name] = value
    runtime.cookie_jar = jar
    runtime.cookie_header = "; ".join(f"{name}={jar[name]}" for name in REQUIRED_IG_COOKIE_NAMES if name in jar)


async def capture_runtime_http_meta(runtime: AccountRuntime) -> None:
    account = runtime.account or {}
    persisted = read_account_http_meta(runtime.username)
    http_client = getattr(runtime, "http_client", None)

    user_agent = str(persisted.get("user_agent") or account.get("user_agent") or "").strip()
    accept_language = str(
        persisted.get("accept_language")
        or account.get("accept_language")
        or "es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7"
    ).strip()
    ig_app_id = str(
        persisted.get("x_ig_app_id")
        or account.get("x_ig_app_id")
        or account.get("ig_app_id")
        or ""
    ).strip()
    asbd_id = str(
        persisted.get("x_asbd_id")
        or account.get("x_asbd_id")
        or account.get("asbd_id")
        or ""
    ).strip()

    page = runtime.page
    if http_client is not None and not user_agent:
        try:
            user_agent = str(
                http_client.session.headers.get("User-Agent") or DEFAULT_HTTP_USER_AGENT
            ).strip()
        except Exception:
            user_agent = ""
    if page is not None and not user_agent:
        try:
            candidate = await page.evaluate("() => navigator.userAgent")
            user_agent = str(candidate or "").strip()
        except Exception:
            user_agent = ""

    await refresh_runtime_cookie_jar(runtime)
    csrf_token = str(runtime.cookie_jar.get("csrftoken") or "").strip()
    if http_client is not None:
        session_headers = getattr(http_client.session, "headers", {}) or {}
        if not ig_app_id:
            ig_app_id = str(session_headers.get("X-IG-App-ID") or "").strip()
        if not asbd_id:
            asbd_id = str(session_headers.get("X-ASBD-ID") or "").strip()
        if not accept_language:
            accept_language = str(
                session_headers.get("Accept-Language") or "es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7"
            ).strip()

    if page is not None and (not ig_app_id or not asbd_id):
        try:
            response = await page.context.request.get(
                "https://www.instagram.com/",
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": accept_language,
                    "User-Agent": user_agent or DEFAULT_HTTP_USER_AGENT,
                    "Referer": "https://www.instagram.com/",
                },
                timeout=runtime_request_timeout_ms(runtime),
            )
            headers = await response_headers_map(response)
            header_app = str((headers or {}).get("x-ig-app-id") or "").strip()
            header_asbd = str((headers or {}).get("x-asbd-id") or "").strip()
            if header_app and not ig_app_id:
                ig_app_id = header_app
            if header_asbd and not asbd_id:
                asbd_id = header_asbd

            text = ""
            try:
                text = await response.text()
            except Exception:
                text = ""
            if not ig_app_id:
                ig_app_id = extract_bootstrap_value(
                    text,
                    [
                        r'"x-ig-app-id"\s*:\s*"(\d+)"',
                        r'"X-IG-App-ID"\s*:\s*"(\d+)"',
                        r'"appId"\s*:\s*"(\d+)"',
                    ],
                )
            if not asbd_id:
                asbd_id = extract_bootstrap_value(
                    text,
                    [
                        r'"x-asbd-id"\s*:\s*"(\d+)"',
                        r'"X-ASBD-ID"\s*:\s*"(\d+)"',
                        r'"ASBD_ID"\s*:\s*"(\d+)"',
                    ],
                )
        except Exception:
            pass

    if not user_agent:
        user_agent = DEFAULT_HTTP_USER_AGENT
    if not ig_app_id:
        ig_app_id = DEFAULT_IG_APP_ID
    if not asbd_id:
        asbd_id = DEFAULT_ASBD_ID

    runtime.user_agent = user_agent
    runtime.accept_language = accept_language
    runtime.ig_app_id = ig_app_id
    runtime.asbd_id = asbd_id

    persist_account_http_meta(
        runtime.username,
        {
            "user_agent": runtime.user_agent,
            "accept_language": runtime.accept_language,
            "x_ig_app_id": runtime.ig_app_id,
            "x_asbd_id": runtime.asbd_id,
            "has_sessionid": bool(runtime.cookie_jar.get("sessionid")),
            "has_ds_user_id": bool(runtime.cookie_jar.get("ds_user_id")),
            "has_csrftoken": bool(runtime.cookie_jar.get("csrftoken")),
            "has_mid": bool(runtime.cookie_jar.get("mid")),
            "has_ig_did": bool(runtime.cookie_jar.get("ig_did")),
            "csrftoken_preview": (csrf_token[:8] + "...") if csrf_token else "",
        },
    )


async def fetch_profile_via_playwright(
    page: Any,
    username: str,
    *,
    timeout_ms: int,
    ig_app_id: str = "",
    asbd_id: str = "",
    accept_language: str = "",
) -> Dict[str, Any]:
    normalized = str(username or "").strip().lstrip("@")
    if page is None or not normalized:
        raise ProfileHttpError(0, "context_unavailable")

    script = """async ({ username, igAppId, asbdId, acceptLanguage }) => {
        const normalized = String(username || "").trim().replace(/^@+/, "");
        if (!normalized) {
            return { status: 0, body: "", reason: "username_vacio" };
        }
        const csrftoken = ((document.cookie || "").match(/(?:^|;\\s*)csrftoken=([^;]+)/) || [])[1] || "";
        const url = `https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(normalized)}`;
        const headers = {
            "accept": "*/*",
            "x-requested-with": "XMLHttpRequest",
            "x-csrftoken": csrftoken,
            "referer": `https://www.instagram.com/${normalized}/`
        };
        if (String(igAppId || "").trim()) {
            headers["x-ig-app-id"] = String(igAppId).trim();
        }
        if (String(asbdId || "").trim()) {
            headers["x-asbd-id"] = String(asbdId).trim();
        }
        if (String(acceptLanguage || "").trim()) {
            headers["accept-language"] = String(acceptLanguage).trim();
        }
        const res = await fetch(url, {
            method: "GET",
            headers,
            credentials: "include"
        });
        const body = await res.text();
        return { status: Number(res.status || 0), body: String(body || ""), reason: "" };
    }"""

    try:
        raw = await asyncio.wait_for(
            page.evaluate(
                script,
                {
                    "username": normalized,
                    "igAppId": str(ig_app_id or "").strip(),
                    "asbdId": str(asbd_id or "").strip(),
                    "acceptLanguage": str(accept_language or "").strip(),
                },
            ),
            timeout=max(1.0, float(timeout_ms) / 1000.0),
        )
    except asyncio.TimeoutError as exc:
        raise ProfileHttpError(0, "request_timeout") from exc
    except Exception as exc:
        raise ProfileHttpError(0, f"request_error:{type(exc).__name__}") from exc

    payload = dict(raw or {})
    status = int(payload.get("status") or 0)
    body = str(payload.get("body") or "")
    if status == 429:
        raise ProfileRateLimit(status, body=body)
    if status != 200:
        raise ProfileHttpError(status, build_profile_http_reason(status, body), body=body)
    try:
        return json.loads(body)
    except Exception as exc:
        raise ProfileHttpError(status, "invalid_json", body=body) from exc


async def _session_get_async(
    session: Any,
    url: str,
    *,
    headers: Dict[str, str],
    timeout_ms: int,
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    connect_timeout = max(2.0, float(timeout_ms) / 2000.0)
    read_timeout = max(2.0, float(timeout_ms) / 1000.0)
    return await asyncio.to_thread(
        session.get,
        str(url).strip(),
        params=params,
        headers=dict(headers or {}),
        timeout=(connect_timeout, read_timeout),
        allow_redirects=True,
    )


async def fetch_profile_via_session(
    session: Any,
    username: str,
    *,
    timeout_ms: int,
    ig_app_id: str = "",
    asbd_id: str = "",
    accept_language: str = "",
) -> Dict[str, Any]:
    normalized = str(username or "").strip().lstrip("@")
    if session is None or not normalized:
        raise ProfileHttpError(0, "session_unavailable")

    headers = {
        "Accept": "*/*",
        "Accept-Language": str(accept_language or "es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7").strip(),
        "Referer": f"https://www.instagram.com/{normalized}/",
        "User-Agent": str(
            getattr(session, "headers", {}).get("User-Agent")
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ).strip(),
        "X-ASBD-ID": str(asbd_id or DEFAULT_ASBD_ID).strip() or DEFAULT_ASBD_ID,
        "X-CSRFToken": str(getattr(session, "headers", {}).get("X-CSRFToken") or "").strip(),
        "X-IG-App-ID": str(ig_app_id or DEFAULT_IG_APP_ID).strip() or DEFAULT_IG_APP_ID,
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        response = await _session_get_async(
            session,
            "https://www.instagram.com/api/v1/users/web_profile_info/",
            params={"username": normalized},
            headers=headers,
            timeout_ms=timeout_ms,
        )
    except Exception as exc:
        raise ProfileHttpError(0, f"request_error:{type(exc).__name__}") from exc

    status = int(getattr(response, "status_code", 0) or 0)
    body = str(getattr(response, "text", "") or "")
    if status == 429:
        raise ProfileRateLimit(status, body=body)
    if status != 200:
        raise ProfileHttpError(status, build_profile_http_reason(status, body), body=body)
    try:
        payload = response.json()
    except Exception as exc:
        raise ProfileHttpError(status, "invalid_json", body=body) from exc
    if not isinstance(payload, dict):
        raise ProfileHttpError(status, "invalid_json", body=body)
    return payload


async def wait_for_account_request_slot(runtime: AccountRuntime) -> None:
    limiter = runtime.profile_limiter
    if limiter is None:
        return
    await limiter.wait_for_slot()


async def wait_for_image_request_slot(runtime: AccountRuntime) -> None:
    limiter = runtime.image_limiter
    if limiter is None:
        return
    await limiter.wait_for_slot()


def verify_dependencies_for_run(cfg: Any) -> None:
    text_cfg = _config_value(cfg, "text", None)
    text_enabled = bool(_config_value(text_cfg, "enabled", False))
    text_state = normalize_filter_state(
        _config_value(text_cfg, "state", FILTER_STATE_DISABLED),
        default=FILTER_STATE_REQUIRED if text_enabled else FILTER_STATE_DISABLED,
    )
    text_criteria = str(_config_value(text_cfg, "criteria", "") or "")
    if text_state == FILTER_STATE_REQUIRED and not text_criteria.strip():
        raise RuntimeError(
            "Filtro Texto inteligente en REQUIRED requiere un criterio no vacio."
        )

    image_cfg = _config_value(cfg, "image", None)
    image_enabled = bool(_config_value(image_cfg, "enabled", False))
    image_state = normalize_filter_state(
        _config_value(image_cfg, "state", FILTER_STATE_DISABLED),
        default=FILTER_STATE_REQUIRED if image_enabled else FILTER_STATE_DISABLED,
    )
    image_prompt = str(_config_value(image_cfg, "prompt", "") or "")
    if image_state == FILTER_STATE_REQUIRED and not image_prompt.strip():
        raise RuntimeError(
            "Filtro Imagen en REQUIRED requiere un prompt no vacio."
        )


async def fetch_profile_json_with_meta(
    runtime_or_page: Any,
    username: str,
) -> Tuple[Optional[ProfileSnapshot], int, str]:
    runtime: Optional[AccountRuntime]
    page: Any
    if isinstance(runtime_or_page, AccountRuntime):
        runtime = runtime_or_page
        page = runtime.page
    else:
        runtime = None
        page = runtime_or_page

    if not username:
        return None, 0, "context_unavailable"

    if runtime is not None:
        await wait_for_account_request_slot(runtime)
        timeout_ms = runtime_request_timeout_ms(runtime)
        ig_app_id = str(runtime.ig_app_id or DEFAULT_IG_APP_ID).strip()
        asbd_id = str(runtime.asbd_id or DEFAULT_ASBD_ID).strip()
        accept_language = str(runtime.accept_language or "").strip()
        session = getattr(getattr(runtime, "http_client", None), "session", None)
    else:
        timeout_ms = runtime_request_timeout_ms(None)
        ig_app_id = DEFAULT_IG_APP_ID
        asbd_id = DEFAULT_ASBD_ID
        accept_language = ""
        session = None

    if page is None and session is None:
        return None, 0, "context_unavailable"

    try:
        if session is not None:
            payload = await fetch_profile_via_session(
                session,
                username,
                timeout_ms=timeout_ms,
                ig_app_id=ig_app_id,
                asbd_id=asbd_id,
                accept_language=accept_language,
            )
        else:
            payload = await fetch_profile_via_playwright(
                page,
                username,
                timeout_ms=timeout_ms,
                ig_app_id=ig_app_id,
                asbd_id=asbd_id,
                accept_language=accept_language,
            )
        status_code = 200
    except ProfileRateLimit as exc:
        return None, int(exc.status), f"http_{int(exc.status)}"
    except ProfileHttpError as exc:
        status_code = int(exc.status or 0)
        reason = str(exc.reason or "profile_http_error")
        if status_code > 0 and not reason.startswith("http_"):
            reason = f"http_{status_code}"
        return None, status_code, reason

    profile = extract_profile_from_payload(payload, username)
    if profile is None:
        return None, status_code, "payload_incomplete"
    return profile, status_code, ""


async def profile_endpoint_preflight(
    runtime: AccountRuntime,
    *,
    probe_username: str,
) -> Tuple[bool, int, str]:
    if runtime is None:
        return False, 0, "context_unavailable"
    normalized_probe = str(probe_username or "").strip().lstrip("@") or "instagram"
    timeout_ms = max(4_000, min(12_000, runtime_request_timeout_ms(runtime)))
    session = getattr(getattr(runtime, "http_client", None), "session", None)
    page = getattr(runtime, "page", None)
    if session is None and page is None:
        return False, 0, "context_unavailable"
    try:
        if session is not None:
            await fetch_profile_via_session(
                session,
                normalized_probe,
                timeout_ms=timeout_ms,
                ig_app_id=str(runtime.ig_app_id or DEFAULT_IG_APP_ID).strip(),
                asbd_id=str(runtime.asbd_id or DEFAULT_ASBD_ID).strip(),
                accept_language=str(runtime.accept_language or "").strip(),
            )
        else:
            await fetch_profile_via_playwright(
                page,
                normalized_probe,
                timeout_ms=timeout_ms,
                ig_app_id=str(runtime.ig_app_id or DEFAULT_IG_APP_ID).strip(),
                asbd_id=str(runtime.asbd_id or DEFAULT_ASBD_ID).strip(),
                accept_language=str(runtime.accept_language or "").strip(),
            )
    except ProfileRateLimit as exc:
        return False, int(exc.status), f"http_{int(exc.status)}"
    except ProfileHttpError as exc:
        return False, int(exc.status or 0), str(exc.reason or "profile_http_error")
    return True, 200, "ok"


async def fetch_profile_json(page: Any, username: str) -> Optional[ProfileSnapshot]:
    profile, _status_code, _reason = await fetch_profile_json_with_meta(page, username)
    return profile


def extract_profile_from_payload(payload: Dict[str, Any], fallback_username: str) -> Optional[ProfileSnapshot]:
    if not isinstance(payload, dict):
        return None

    user_data = (payload.get("data") or {}).get("user")
    if not isinstance(user_data, dict):
        return None

    biography = str(user_data.get("biography") or "").strip()
    full_name = str(user_data.get("full_name") or "").strip()

    followers_raw = (user_data.get("edge_followed_by") or {}).get("count")
    posts_raw = (user_data.get("edge_owner_to_timeline_media") or {}).get("count")
    follower_count = parse_required_count(followers_raw)
    media_count = parse_required_count(posts_raw)
    if follower_count is None or media_count is None:
        return None

    is_private = bool(user_data.get("is_private", False))
    is_verified = bool(user_data.get("is_verified", False))
    profile_pic_url = str(
        user_data.get("profile_pic_url_hd")
        or user_data.get("profile_pic_url")
        or ""
    ).strip()
    external_url = str(user_data.get("external_url") or "").strip()
    username = str(user_data.get("username") or fallback_username).strip().lstrip("@")
    user_id = str(user_data.get("id") or user_data.get("pk") or "").strip()

    return ProfileSnapshot(
        username=username,
        biography=biography,
        full_name=full_name,
        follower_count=follower_count,
        media_count=media_count,
        is_private=is_private,
        profile_pic_url=profile_pic_url,
        user_id=user_id,
        external_url=external_url,
        is_verified=is_verified,
    )


def missing_essential_fields(
    profile: ProfileSnapshot,
    *,
    require_image_url: bool = False,
) -> List[str]:
    missing: List[str] = []
    if not str(profile.user_id or "").strip():
        missing.append("id")
    if not isinstance(profile.follower_count, int) or profile.follower_count < 0:
        missing.append("followers")
    if not isinstance(profile.media_count, int) or profile.media_count < 0:
        missing.append("posts")
    if require_image_url and not str(profile.profile_pic_url or "").strip():
        missing.append("profile_pic_url")
    return missing


def response_status_code(response: Any) -> int:
    if response is None:
        return 0
    status = getattr(response, "status", None)
    if callable(status):
        try:
            return int(status())
        except Exception:
            return 0
    try:
        return int(status)
    except Exception:
        return 0


async def response_headers_map(response: Any) -> Dict[str, str]:
    if response is None:
        return {}

    def _normalize_headers(raw: Any) -> Dict[str, str]:
        if not isinstance(raw, dict):
            return {}
        normalized: Dict[str, str] = {}
        for key, value in raw.items():
            header_key = str(key or "").strip().lower()
            if not header_key:
                continue
            normalized[header_key] = str(value or "").strip()
        return normalized

    try:
        headers_attr = getattr(response, "headers", None)
        if callable(headers_attr):
            maybe_headers = headers_attr()
        else:
            maybe_headers = headers_attr
        normalized = _normalize_headers(maybe_headers)
        if normalized:
            return normalized
    except Exception:
        pass

    try:
        all_headers = getattr(response, "all_headers", None)
        if callable(all_headers):
            maybe_headers = all_headers()
            if asyncio.iscoroutine(maybe_headers):
                maybe_headers = await maybe_headers
            normalized = _normalize_headers(maybe_headers)
            if normalized:
                return normalized
    except Exception:
        pass

    return {}


def detect_image_content_type(image_bytes: bytes) -> str:
    payload = bytes(image_bytes or b"")
    if len(payload) >= 3 and payload[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if len(payload) >= 8 and payload[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if len(payload) >= 12 and payload[:4] == b"RIFF" and payload[8:12] == b"WEBP":
        return "image/webp"
    if len(payload) >= 6 and payload[:6] in {b"GIF87a", b"GIF89a"}:
        return "image/gif"
    return ""


def _safe_response_content_type(headers: Dict[str, str]) -> str:
    normalized_headers = dict(headers or {})
    raw_value = str(normalized_headers.get("content-type") or "").strip()
    if raw_value:
        return raw_value
    if normalized_headers.get(SAFE_RESPONSE_HEADERS_MISSING) == "1":
        return "missing_headers"
    return "missing_content_type"


def _safe_response_final_url(response: Any, fallback_url: str) -> str:
    url_attr = getattr(response, "url", "")
    if callable(url_attr):
        try:
            url_attr = url_attr()
        except Exception:
            url_attr = ""
    final_url = str(url_attr or "").strip()
    if final_url:
        return final_url
    return str(fallback_url or "").strip()


async def build_safe_image_response(response: Any, *, fallback_url: str) -> SafeResponse:
    status = response_status_code(response)
    headers = await response_headers_map(response)
    safe_headers = dict(headers or {})
    if not safe_headers:
        safe_headers = {SAFE_RESPONSE_HEADERS_MISSING: "1"}
    try:
        body = bytes(await response.body() or b"")
    except Exception as exc:
        raise ImageDownloadError(f"image_body_error:{type(exc).__name__}", status=status) from exc
    return SafeResponse(
        status=status,
        headers=safe_headers,
        body=body,
        final_url=_safe_response_final_url(response, fallback_url),
    )


def _image_body_prefix_hint(payload: bytes) -> str:
    if not payload:
        return "empty"
    prefix = bytes(payload[:IMAGE_MAGIC_PREFIX_BYTES])
    ascii_hint = "".join(
        chr(value) if 32 <= value <= 126 else "."
        for value in prefix
    )
    return f"hex:{prefix.hex()}|txt:{ascii_hint}"


def _is_soft_block_payload(payload: bytes) -> bool:
    sample = bytes(payload[:2048])
    stripped = sample.lstrip()
    if stripped.startswith(b"<"):
        return True
    lowered = sample.lower()
    return any(
        token in lowered
        for token in (b"html", b"login", b"challenge", b"please wait")
    )


def _classify_image_payload(payload: bytes) -> Tuple[str, str]:
    header = bytes(payload[:IMAGE_MAGIC_PREFIX_BYTES])
    detected_type = detect_image_content_type(header)
    if detected_type:
        return "ok", detected_type
    if _is_soft_block_payload(payload):
        return "soft_block", ""
    return "invalid_real", ""


def _image_response_host(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return "-"
    try:
        parsed = urlparse(text)
        return str(parsed.netloc or "-")
    except Exception:
        return "-"


def _candidate_profile_image_urls(image_url: str) -> List[str]:
    normalized_url = str(image_url or "").strip()
    if not normalized_url:
        return []

    candidates: List[str] = [normalized_url]
    parsed = urlparse(normalized_url)
    host = str(parsed.netloc or "").strip().lower()
    if "instagram" not in host and "fbcdn.net" not in host:
        return candidates

    query_pairs = list(parse_qsl(parsed.query, keep_blank_values=True))
    if not query_pairs:
        return candidates

    stp_index = -1
    stp_value = ""
    for idx, (key, value) in enumerate(query_pairs):
        if str(key or "").strip().lower() == "stp":
            stp_index = idx
            stp_value = str(value or "")
            break
    if stp_index < 0 or not stp_value:
        return candidates

    stp_variants: List[str] = []
    for size in ("1080", "640", "480", "320"):
        variant = re.sub(r"s\d+x\d+", f"s{size}x{size}", stp_value)
        variant = re.sub(r"c0\.0\.\d+\.\d+a", f"c0.0.{size}.{size}a", variant)
        stp_variants.append(variant)

    seen: Set[str] = {normalized_url}
    for variant in stp_variants:
        updated_pairs = list(query_pairs)
        updated_pairs[stp_index] = (query_pairs[stp_index][0], variant)
        updated_query = urlencode(updated_pairs, doseq=True)
        candidate = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                updated_query,
                parsed.fragment,
            )
        )
        if candidate and candidate not in seen:
            seen.add(candidate)
            candidates.append(candidate)

    return candidates


def _log_image_diag(
    *,
    final_url: str,
    status: int,
    content_type: str,
    body: bytes,
    reason: str,
) -> None:
    logger.warning(
        (
            "[IMAGE_DIAG] "
            f"host={_image_response_host(final_url)} "
            f"status={int(status or 0)} "
            f"content_type={content_type or 'missing'} "
            f"size={len(body or b'')} "
            f"body_prefix_hint={_image_body_prefix_hint(body)} "
            f"reason={reason}"
        )
    )


def _classify_image_download(
    safe_response: SafeResponse,
    *,
    fallback_url: str,
) -> Tuple[str, str]:
    _ = fallback_url
    if int(safe_response.status or 0) != 200:
        return "http_error", ""
    return _classify_image_payload(safe_response.body)


def profile_to_output(profile: ProfileSnapshot) -> Dict[str, Any]:
    return {
        "username": profile.username,
        "id": profile.user_id,
        "followers_count": profile.follower_count,
        "posts_count": profile.media_count,
        "biography": profile.biography,
        "external_url": profile.external_url,
        "profile_pic_url": profile.profile_pic_url,
        "is_private": bool(profile.is_private),
        "is_verified": bool(profile.is_verified),
        "full_name": profile.full_name,
    }


async def download_image_bytes_with_retry(
    runtime: AccountRuntime,
    profile_pic_url: str,
    *,
    warn: Callable[[str], None],
) -> Tuple[Optional[bytes], str]:
    if not profile_pic_url:
        return None, "sin_foto"

    try:
        image_bytes = await download_profile_image_for_runtime(
            runtime,
            username=runtime.username,
            image_url=profile_pic_url,
        )
    except ImageRateLimit as exc:
        if runtime.image_limiter is not None:
            await runtime.image_limiter.apply_rate_limit(status=exc.status, warn=warn)
        return None, f"image_http_{exc.status}"
    except ImageDownloadError as exc:
        return None, exc.reason
    return image_bytes, "ok"


async def download_image_bytes_with_context(
    runtime_or_page: Any,
    url: str,
) -> Tuple[Optional[bytes], str, int, str]:
    runtime: Optional[AccountRuntime]
    page: Any
    if isinstance(runtime_or_page, AccountRuntime):
        runtime = runtime_or_page
        page = runtime.page
    else:
        runtime = None
        page = runtime_or_page
    session = getattr(getattr(runtime, "http_client", None), "session", None) if runtime is not None else None

    if (page is None and session is None) or not url:
        return None, "", 0, "image_download_failed"

    timeout_ms = random.randint(8_000, 15_000)
    headers = {
        "Referer": "https://www.instagram.com/",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    proxy_key = "-"
    if runtime is not None:
        headers = build_image_request_headers(runtime, runtime.username)
        timeout_ms = random.randint(8_000, 15_000)
        proxy_key = runtime.proxy_key or runtime.proxy_url or "-"

    try:
        if session is not None:
            image_bytes = await download_profile_image_via_session(
                session,
                url,
                headers=headers,
                timeout=timeout_ms,
                proxy_key=proxy_key,
            )
        else:
            image_bytes = await download_profile_image(
                url,
                proxy=(runtime.proxy_url if runtime is not None else ""),
                headers=headers,
                timeout=timeout_ms,
                page=page,
                proxy_key=proxy_key,
            )
    except ImageRateLimit as exc:
        return None, "", exc.status, f"image_http_{exc.status}"
    except ImageDownloadError as exc:
        return None, "", exc.status, exc.reason

    return image_bytes, detect_image_content_type(image_bytes), 200, ""


def build_image_request_headers(runtime: AccountRuntime, username: str) -> Dict[str, str]:
    normalized = str(username or "").strip().lstrip("@")
    headers = {
        "User-Agent": runtime.user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": runtime.accept_language or "en-US,en;q=0.9",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Referer": f"https://www.instagram.com/{normalized}/" if normalized else "https://www.instagram.com/",
        "Sec-Fetch-Dest": "image",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "cross-site",
    }
    return {key: value for key, value in headers.items() if str(value or "").strip()}


async def download_profile_image(
    image_url: str,
    proxy: str,
    headers: Dict[str, str],
    timeout: int,
    *,
    page: Any,
    proxy_key: str,
) -> bytes:
    if page is None or not str(image_url or "").strip():
        raise ImageDownloadError("image_download_failed")
    _ = proxy
    try:
        response = await page.context.request.get(
            str(image_url).strip(),
            headers=dict(headers or {}),
            timeout=max(1_000, int(timeout)),
        )
    except Exception as exc:
        raise ImageDownloadError(f"image_request_error:{type(exc).__name__}") from exc

    safe_response = await build_safe_image_response(response, fallback_url=str(image_url).strip())
    status_code = int(safe_response.status or 0)
    content_type = _safe_response_content_type(safe_response.headers)

    if status_code in {403, 429}:
        _log_image_diag(
            final_url=safe_response.final_url,
            status=status_code,
            content_type=content_type,
            body=safe_response.body,
            reason=f"http_{status_code}",
        )
        body_preview = ""
        try:
            body_preview = safe_response.body[:512].decode("utf-8", errors="ignore")
        except Exception:
            body_preview = ""
        raise ImageRateLimit(proxy_key, status_code, body=body_preview)
    if status_code in {404, 410}:
        _log_image_diag(
            final_url=safe_response.final_url,
            status=status_code,
            content_type=content_type,
            body=safe_response.body,
            reason="image_not_found",
        )
        raise ImageDownloadError("image_not_found", status=status_code)
    if status_code != 200:
        _log_image_diag(
            final_url=safe_response.final_url,
            status=status_code,
            content_type=content_type,
            body=safe_response.body,
            reason=f"image_http_{status_code}",
        )
        raise ImageDownloadError(f"image_http_{status_code}", status=status_code)

    image_state, _detected_type = _classify_image_download(safe_response, fallback_url=image_url)
    if image_state == "ok":
        return safe_response.body
    if image_state == "soft_block":
        raise ImageDownloadError("image_soft_block", status=status_code)
    _log_image_diag(
        final_url=safe_response.final_url,
        status=status_code,
        content_type=content_type,
        body=safe_response.body,
        reason="invalid_real",
    )
    raise ImageDownloadError("invalid_real", status=status_code)


async def download_profile_image_via_session(
    session: Any,
    image_url: str,
    headers: Dict[str, str],
    timeout: int,
    *,
    proxy_key: str,
) -> bytes:
    if session is None or not str(image_url or "").strip():
        raise ImageDownloadError("image_download_failed")
    try:
        response = await _session_get_async(
            session,
            str(image_url).strip(),
            headers=dict(headers or {}),
            timeout_ms=max(1_000, int(timeout)),
        )
    except Exception as exc:
        raise ImageDownloadError(f"image_request_error:{type(exc).__name__}") from exc

    safe_response = SafeResponse(
        status=int(getattr(response, "status_code", 0) or 0),
        headers={str(key).lower(): str(value) for key, value in dict(getattr(response, "headers", {}) or {}).items()},
        body=bytes(getattr(response, "content", b"") or b""),
        final_url=str(getattr(response, "url", "") or str(image_url).strip()),
    )
    if not safe_response.headers:
        safe_response.headers = {SAFE_RESPONSE_HEADERS_MISSING: "1"}
    status_code = int(safe_response.status or 0)
    content_type = _safe_response_content_type(safe_response.headers)

    if status_code in {403, 429}:
        _log_image_diag(
            final_url=safe_response.final_url,
            status=status_code,
            content_type=content_type,
            body=safe_response.body,
            reason=f"http_{status_code}",
        )
        body_preview = safe_response.body[:512].decode("utf-8", errors="ignore")
        raise ImageRateLimit(proxy_key, status_code, body=body_preview)
    if status_code in {404, 410}:
        _log_image_diag(
            final_url=safe_response.final_url,
            status=status_code,
            content_type=content_type,
            body=safe_response.body,
            reason="image_not_found",
        )
        raise ImageDownloadError("image_not_found", status=status_code)
    if status_code != 200:
        _log_image_diag(
            final_url=safe_response.final_url,
            status=status_code,
            content_type=content_type,
            body=safe_response.body,
            reason=f"image_http_{status_code}",
        )
        raise ImageDownloadError(f"image_http_{status_code}", status=status_code)

    image_state, _detected_type = _classify_image_download(
        safe_response,
        fallback_url=image_url,
    )
    if image_state == "ok":
        return safe_response.body
    if image_state == "soft_block":
        raise ImageDownloadError("image_soft_block", status=status_code)
    _log_image_diag(
        final_url=safe_response.final_url,
        status=status_code,
        content_type=content_type,
        body=safe_response.body,
        reason="invalid_real",
    )
    raise ImageDownloadError("invalid_real", status=status_code)


async def download_profile_image_for_runtime(
    runtime: AccountRuntime,
    *,
    username: str,
    image_url: str,
) -> bytes:
    if runtime is None:
        raise ImageDownloadError("runtime_unavailable")
    await wait_for_image_request_slot(runtime)
    headers = build_image_request_headers(runtime, username)
    candidate_urls = _candidate_profile_image_urls(image_url)
    if not candidate_urls:
        raise ImageDownloadError("image_download_failed")
    max_attempts = clamp(env_int("LEADS_IMAGE_REQUEST_ATTEMPTS", 1), 1, 2)
    timeout_ms = random.randint(8_000, 15_000)
    last_error: Optional[ImageDownloadError] = None
    session = getattr(getattr(runtime, "http_client", None), "session", None)

    for attempt in range(1, max_attempts + 1):
        for candidate_url in candidate_urls:
            try:
                if session is not None:
                    return await download_profile_image_via_session(
                        session,
                        candidate_url,
                        headers=headers,
                        timeout=timeout_ms,
                        proxy_key=runtime.proxy_key or runtime.proxy_url or "-",
                    )
                return await download_profile_image(
                    candidate_url,
                    proxy=runtime.proxy_url,
                    headers=headers,
                    timeout=timeout_ms,
                    page=runtime.page,
                    proxy_key=runtime.proxy_key or runtime.proxy_url or "-",
                )
            except ImageRateLimit:
                raise
            except ImageDownloadError as exc:
                last_error = exc
                if exc.reason == "image_soft_block":
                    raise

        if attempt >= max_attempts:
            break
        backoff = float(attempt)
        await asyncio.sleep(backoff)

    raise last_error or ImageDownloadError("image_download_failed")


def validate_image_payload(image_bytes: bytes, content_type: str) -> Tuple[bool, str]:
    _ = content_type
    state, _detected_type = _classify_image_payload(bytes(image_bytes or b""))
    if state == "ok":
        return True, "ok"
    if state == "soft_block":
        return False, "soft_block"
    return False, "invalid_real"


def parse_required_count(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    try:
        parsed = int(raw)
    except Exception:
        return None
    if parsed < 0:
        return None
    return parsed


def passes_classic_filters(profile: ProfileSnapshot, cfg: Any) -> Tuple[bool, str]:
    if cfg is None:
        return True, ""

    normalized_cfg = _normalize_classic_filter_cfg(cfg)
    haystack = " ".join([profile.username, profile.full_name, profile.biography]).strip()
    link_present = bool((profile.external_url or "").strip()) or bio_has_link(profile.biography)

    min_followers = int(normalized_cfg["min_followers"])
    min_posts = int(normalized_cfg["min_posts"])
    privacy = str(normalized_cfg["privacy"])
    link_in_bio = str(normalized_cfg["link_in_bio"])
    include_keywords = list(normalized_cfg["include_keywords"])
    exclude_keywords = list(normalized_cfg["exclude_keywords"])
    language = str(normalized_cfg["language"])
    followers_state = str(normalized_cfg["followers_state"])
    posts_state = str(normalized_cfg["posts_state"])
    privacy_state = str(normalized_cfg["privacy_state"])
    link_state = str(normalized_cfg["link_state"])
    include_state = str(normalized_cfg["include_state"])
    exclude_state = str(normalized_cfg["exclude_state"])
    language_state = str(normalized_cfg["language_state"])

    if followers_state == FILTER_STATE_REQUIRED and profile.follower_count < min_followers:
        return False, "seguidores_min"
    if posts_state == FILTER_STATE_REQUIRED and profile.media_count < min_posts:
        return False, "posts_min"

    if privacy_state == FILTER_STATE_REQUIRED:
        if privacy == "public" and profile.is_private:
            return False, "perfil_privado"
        if privacy == "private" and not profile.is_private:
            return False, "perfil_publico"

    if link_state == FILTER_STATE_REQUIRED:
        if link_in_bio == "yes" and not link_present:
            return False, "sin_link_bio"
        if link_in_bio == "no" and link_present:
            return False, "con_link_bio"

    if include_state == FILTER_STATE_REQUIRED and include_keywords:
        matched = False
        for term in include_keywords:
            if contains_term(term, haystack):
                matched = True
                break
        if not matched:
            return False, "keyword_faltante"

    if exclude_state == FILTER_STATE_REQUIRED and exclude_keywords:
        for term in exclude_keywords:
            if contains_term(term, haystack):
                return False, "keyword_excluida"

    if language_state == FILTER_STATE_REQUIRED and language and language != "any":
        biography_text = str(profile.biography or "").strip()
        if not biography_text:
            return False, "biografia_vacia"
        detected = detect_language(biography_text)
        if detected != language:
            return False, "idioma_no_coincide"

    return True, ""


def bio_has_link(bio: str) -> bool:
    if not bio:
        return False
    return bool(re.search(r"(https?://|www\.)", bio, flags=re.IGNORECASE))


def playwright_proxy_from_account(account: Dict[str, Any]) -> Optional[Dict[str, str]]:
    try:
        from src.proxy_payload import proxy_from_account
    except Exception:
        return None
    return proxy_from_account(account)


def _v2_worker_key_for_account(account: Dict[str, Any]) -> str:
    assigned_proxy_id = str(account.get("assigned_proxy_id") or "").strip()
    if assigned_proxy_id:
        return assigned_proxy_id
    proxy_payload = playwright_proxy_from_account(account) or {}
    proxy_key = str(proxy_payload.get("server") or proxy_payload.get("url") or "").strip()
    return proxy_key or "__no_proxy__"


def _v2_account_has_storage_state(account: Dict[str, Any]) -> bool:
    username = str(account.get("username") or "").strip().lstrip("@")
    if not username:
        return False
    try:
        from core.accounts import has_playwright_storage_state
    except Exception:
        return False
    return has_playwright_storage_state(username)


def _v2_order_accounts_for_startup(accounts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ready_accounts: List[Dict[str, Any]] = []
    pending_accounts: List[Dict[str, Any]] = []
    for account in accounts:
        if _v2_account_has_storage_state(account):
            ready_accounts.append(account)
            continue
        pending_accounts.append(account)
    return ready_accounts + pending_accounts


def _v2_pending_accounts_for_startup_retry(
    accounts: List[Dict[str, Any]],
    runtime_by_username: Dict[str, AccountRuntime],
) -> List[Dict[str, Any]]:
    pending_accounts: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for account in accounts:
        username = str(account.get("username") or "").strip().lstrip("@").lower()
        if not username or username in seen:
            continue
        seen.add(username)
        if username in runtime_by_username:
            continue
        pending_accounts.append(account)
    return pending_accounts


def _v2_select_accounts_for_workers(
    accounts: List[Dict[str, Any]],
    *,
    requested_workers: int,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for account in accounts:
        if not isinstance(account, dict):
            continue
        grouped.setdefault(_v2_worker_key_for_account(account), []).append(account)
    ranked_groups: List[Tuple[str, List[Dict[str, Any]], int]] = []
    for worker_key, grouped_accounts in grouped.items():
        ordered_accounts = _v2_order_accounts_for_startup(grouped_accounts)
        ready_count = sum(1 for account in ordered_accounts if _v2_account_has_storage_state(account))
        ranked_groups.append((worker_key, ordered_accounts, ready_count))
    ranked_groups.sort(
        key=lambda item: (
            0 if item[2] > 0 else 1,
            -item[2],
            -len(item[1]),
            str(item[0] or ""),
        )
    )
    selected_groups = ranked_groups[: max(1, int(requested_workers or 1))]
    selected_accounts: List[Dict[str, Any]] = []
    selected_worker_keys: List[str] = []
    for worker_key, grouped_accounts, _ready_count in selected_groups:
        selected_worker_keys.append(worker_key)
        selected_accounts.extend(grouped_accounts)
    return selected_accounts, selected_worker_keys


def _v2_filter_accounts_by_proxy_runtime(
    accounts: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    payload = preflight_accounts_for_proxy_runtime(accounts)
    ready_accounts = [
        dict(account)
        for account in (payload.get("ready_accounts") or [])
        if isinstance(account, dict)
    ]
    blocked_accounts = [
        dict(item)
        for item in (payload.get("blocked_accounts") or [])
        if isinstance(item, dict)
    ]
    return ready_accounts, blocked_accounts


def text_ai_decision(
    user: Any,
    criteria: str,
    *,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[bool], str]:
    profile = ProfileSnapshot(
        username=str(getattr(user, "username", "") or ""),
        biography=str(getattr(user, "biography", "") or ""),
        full_name=str(getattr(user, "full_name", "") or ""),
        follower_count=int(getattr(user, "follower_count", 0) or 0),
        media_count=int(getattr(user, "media_count", 0) or 0),
        is_private=bool(getattr(user, "is_private", False)),
        profile_pic_url=str(getattr(user, "profile_pic_url", "") or ""),
        user_id=str(getattr(user, "user_id", "") or ""),
        external_url=str(getattr(user, "external_url", "") or ""),
        is_verified=bool(getattr(user, "is_verified", False)),
    )
    engine = LocalTextEngine(criteria, thresholds=thresholds)
    decision = engine.score(profile)
    return decision.qualified, decision.reason


def image_ai_decision(
    user: Any,
    prompt: str,
    *,
    image_bytes: Optional[bytes] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    _ = user
    if not image_bytes:
        return False, "image_download_failed"
    rules = parse_image_prompt(prompt)
    analysis = ImageAttributeFilter().analyze(image_bytes)
    return evaluate_image_rules(analysis, rules, thresholds=thresholds)


def _v2_iso_to_epoch(raw_value: Any) -> float:
    text = str(raw_value or "").strip()
    if not text:
        return 0.0
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return max(0.0, parsed.timestamp())
    except Exception:
        return 0.0


def _v2_epoch_to_iso(epoch_seconds: float) -> str:
    try:
        return datetime.utcfromtimestamp(max(0.0, float(epoch_seconds))).isoformat(timespec="seconds") + "Z"
    except Exception:
        return now_iso()


async def _v2_sleep_with_stop(
    seconds: float,
    stop_event: asyncio.Event,
    should_stop: Callable[[asyncio.Event], bool],
) -> None:
    remaining = max(0.0, float(seconds))
    while remaining > 0:
        if should_stop(stop_event):
            return
        step = min(0.25, remaining)
        await asyncio.sleep(step)
        remaining -= step


def _v2_build_filter_settings(filter_cfg: Any) -> PipelineFilterSettings:
    text_cfg = _config_value(filter_cfg, "text", None)
    text_enabled = bool(_config_value(text_cfg, "enabled", False))
    text_state_default = FILTER_STATE_REQUIRED if text_enabled else FILTER_STATE_DISABLED
    text_state = normalize_filter_state(_config_value(text_cfg, "state", None), default=text_state_default)
    text_criteria = str(_config_value(text_cfg, "criteria", "") or "").strip()
    if not text_criteria and text_state != FILTER_STATE_REQUIRED:
        text_state = FILTER_STATE_DISABLED
    raw_text_thresholds = _config_value(text_cfg, "engine_thresholds", {})
    text_engine_thresholds = (
        sanitize_text_engine_thresholds_payload(raw_text_thresholds)
        if isinstance(raw_text_thresholds, dict)
        else sanitize_text_engine_thresholds_payload({})
    )

    image_cfg = _config_value(filter_cfg, "image", None)
    image_enabled = bool(_config_value(image_cfg, "enabled", False))
    image_state_default = FILTER_STATE_REQUIRED if image_enabled else FILTER_STATE_DISABLED
    image_state = normalize_filter_state(_config_value(image_cfg, "state", None), default=image_state_default)
    image_prompt = str(_config_value(image_cfg, "prompt", "") or "").strip()
    if not image_prompt and image_state != FILTER_STATE_REQUIRED:
        image_state = FILTER_STATE_DISABLED
    raw_image_thresholds = _config_value(image_cfg, "engine_thresholds", {})
    image_engine_thresholds = (
        sanitize_image_engine_thresholds_payload(raw_image_thresholds)
        if isinstance(raw_image_thresholds, dict)
        else sanitize_image_engine_thresholds_payload({})
    )

    return PipelineFilterSettings(
        classic_cfg=_normalize_classic_filter_cfg(_config_value(filter_cfg, "classic", None)),
        text_state=text_state,
        text_criteria=text_criteria,
        image_state=image_state,
        image_prompt=image_prompt,
        text_engine_thresholds=text_engine_thresholds,
        image_engine_thresholds=image_engine_thresholds,
    )


def _v2_is_image_requested(filter_settings: PipelineFilterSettings) -> bool:
    if filter_settings.image_state == FILTER_STATE_DISABLED:
        return False
    return bool(str(filter_settings.image_prompt or "").strip())


def _v2_is_image_required(filter_settings: PipelineFilterSettings) -> bool:
    return filter_settings.image_state == FILTER_STATE_REQUIRED


def _v2_build_fail_evaluation(
    state: LeadWorkState,
    primary_reason: str,
    *,
    extra_reasons: Optional[List[str]] = None,
) -> LeadEvaluation:
    reasons = list(state.reasons)
    if extra_reasons:
        reasons.extend([str(item) for item in extra_reasons if str(item).strip()])
    elif primary_reason:
        reasons.append(str(primary_reason))
    cleaned_reasons = reasons if reasons else [str(primary_reason or "descartado")]
    return LeadEvaluation(
        passed=False,
        primary_reason=str(primary_reason or "descartado"),
        reasons=cleaned_reasons,
        scores=dict(state.scores),
        extracted=dict(state.extracted),
    )


def _v2_build_pass_evaluation(state: LeadWorkState) -> LeadEvaluation:
    reasons = list(state.reasons)
    reasons.append("decision_final:pass")
    return LeadEvaluation(
        passed=True,
        primary_reason="ok",
        reasons=reasons,
        scores=dict(state.scores),
        extracted=dict(state.extracted),
    )


def _humanize_fail_reason(primary_reason: str) -> str:
    reason = str(primary_reason or "").strip()
    if not reason:
        return "no califica"
    mapping = {
        "seguidores_min": "no cumple con la cantidad minima de seguidores",
        "posts_min": "no cumple con la cantidad minima de posts",
        "perfil_privado": "el perfil es privado y se requiere publico",
        "perfil_publico": "el perfil es publico y se requiere privado",
        "sin_link_bio": "no tiene link en la biografia",
        "con_link_bio": "tiene link en biografia y ese caso se excluye",
        "keyword_faltante": "la biografia no incluye palabras requeridas",
        "keyword_excluida": "la biografia contiene palabras excluidas",
        "biografia_vacia": "no tiene biografia",
        "idioma_no_coincide": "el idioma de la biografia no coincide",
        "texto_inteligente_no_califica": "no cumple con el filtro de biografia",
        "sin_foto": "no tiene foto de perfil disponible",
        "no_face": "no se detecto rostro en la imagen",
        "gender_mismatch": "el genero detectado no coincide con el prompt",
        "age_below_min": "la edad detectada es menor al minimo pedido",
        "age_above_max": "la edad detectada supera el maximo pedido",
        "image_not_found": "la imagen de perfil no se encontro",
        "not_found": "la imagen de perfil no se encontro",
        "image_soft_block": "instagram devolvio un soft-block al descargar la imagen",
        "invalid_real": "la imagen descargada no es valida",
        "image_failed_max_retries": "se agotaron los reintentos de imagen",
        "gender_unknown": "no se pudo estimar el genero en la imagen",
        "age_unknown": "no se pudo estimar la edad en la imagen",
        "no_beard": "la imagen no muestra barba",
        "beard_forbidden": "la imagen muestra barba y se pidio sin barba",
        "not_overweight": "la imagen no muestra sobrepeso",
        "overweight_forbidden": "la imagen muestra sobrepeso y se pidio sin sobrepeso",
        "not_slim": "la imagen no muestra una contextura delgada",
        "slim_forbidden": "la imagen muestra contextura delgada y se pidio no delgada",
        "not_sharp": "la imagen no cumple el nivel minimo de nitidez",
        "sharp_forbidden": "la imagen tiene mas nitidez de la permitida por el prompt",
        "profile_missing": "no se pudo obtener el perfil",
        "payload_incomplete": "instagram devolvio datos de perfil incompletos",
        "perfil_no_disponible": "el perfil no esta disponible",
        "username_vacio": "el username esta vacio",
        "max_retries_exceeded": "se agotaron los reintentos",
        "finalize_without_evaluation": "la evaluacion final quedo incompleta",
    }
    normalized = mapping.get(reason)
    if normalized:
        return normalized
    if reason.startswith("incomplete_data:"):
        missing = reason.split(":", 1)[-1].replace(",", ", ")
        return f"faltan datos obligatorios del perfil: {missing}"
    if reason.startswith("http_"):
        return f"error HTTP en perfil ({reason})"
    if reason.startswith("image_http_"):
        return f"error HTTP al descargar imagen ({reason})"
    return reason.replace("_", " ")


def _build_decision_reason_for_log(evaluation: LeadEvaluation) -> str:
    if not evaluation.passed:
        return _humanize_fail_reason(evaluation.primary_reason)

    extracted = dict(evaluation.extracted or {})
    followers = extracted.get("followers_count")
    posts = extracted.get("posts_count")
    biography = str(extracted.get("biography") or "").strip()
    parts: List[str] = []
    if followers is not None:
        parts.append(f"cumple seguidores ({followers})")
    else:
        parts.append("cumple seguidores")
    if posts is not None:
        parts.append(f"cumple posts ({posts})")
    else:
        parts.append("cumple posts")
    if biography:
        parts.append("cumple biografia")
    else:
        parts.append("biografia vacia (sin bloqueo)")
    text_similarity = (evaluation.scores or {}).get("text_similarity")
    text_threshold = (evaluation.scores or {}).get("text_threshold")
    if text_similarity is not None and text_threshold is not None:
        parts.append(f"texto={text_similarity:.3f}/{text_threshold:.3f}")
    return ", ".join(parts)


def _v2_proxy_key(proxy_url: str, account_username: str) -> str:
    cleaned = str(proxy_url or "").strip()
    if cleaned:
        return cleaned
    return f"account:@{account_username}"


def _mono_to_epoch(mono_deadline: float) -> float:
    if mono_deadline <= 0.0:
        return 0.0
    remaining = max(0.0, mono_deadline - time.monotonic())
    return time.time() + remaining


def _epoch_to_mono(epoch_deadline: float) -> float:
    if epoch_deadline <= 0.0:
        return 0.0
    remaining = max(0.0, epoch_deadline - time.time())
    return time.monotonic() + remaining


def _macro_random_block_target() -> int:
    return random.randint(MACRO_BLOCK_MIN, MACRO_BLOCK_MAX)


def _macro_random_blocks_to_long_break() -> int:
    return random.randint(MACRO_LONG_BREAK_EVERY_MIN, MACRO_LONG_BREAK_EVERY_MAX)


def _macro_random_short_break_seconds() -> float:
    return random.uniform(MACRO_SHORT_BREAK_MIN_SECONDS, MACRO_SHORT_BREAK_MAX_SECONDS)


def _macro_random_long_break_seconds() -> float:
    return random.uniform(MACRO_LONG_BREAK_MIN_SECONDS, MACRO_LONG_BREAK_MAX_SECONDS)


def _macro_runtime_wait_seconds(runtime: AccountRuntime) -> Tuple[float, str]:
    now_mono = time.monotonic()
    if runtime.macro_pause_until > now_mono:
        return runtime.macro_pause_until - now_mono, "macro_pause"
    if runtime.macro_next_request_not_before > now_mono:
        return runtime.macro_next_request_not_before - now_mono, "macro_interval"
    return 0.0, "ok"


def _macro_note_request_dispatched(runtime: AccountRuntime) -> None:
    interval = runtime.macro_base_interval_seconds + random.uniform(
        -MACRO_REQUEST_JITTER_SECONDS,
        MACRO_REQUEST_JITTER_SECONDS,
    )
    interval = max(0.1, interval)
    runtime.macro_next_request_not_before = time.monotonic() + interval


def _macro_note_profile_processed(
    runtime: AccountRuntime,
    *,
    warn: Optional[Callable[[str], None]] = None,
) -> None:
    runtime.account_processed += 1
    runtime.macro_block_progress += 1
    if runtime.macro_block_progress < max(1, runtime.macro_block_target):
        return
    runtime.macro_block_progress = 0
    runtime.macro_block_target = _macro_random_block_target()
    runtime.macro_blocks_since_long_pause += 1
    pause_seconds = _macro_random_short_break_seconds()
    if runtime.macro_blocks_since_long_pause >= max(1, runtime.macro_next_long_break_after_blocks):
        pause_seconds += _macro_random_long_break_seconds()
        runtime.macro_blocks_since_long_pause = 0
        runtime.macro_next_long_break_after_blocks = _macro_random_blocks_to_long_break()
    runtime.macro_pause_until = max(runtime.macro_pause_until, time.monotonic() + pause_seconds)
    if warn is not None:
        warn(
            (
                "[LEADS][MACRO_PAUSA] "
                f"cuenta=@{runtime.username} | "
                f"pausa={pause_seconds:.0f}s | "
                f"bloque_objetivo={runtime.macro_block_target}"
            )
        )


def _macro_prune_rate_limit_events(runtime: AccountRuntime, now_mono: Optional[float] = None) -> None:
    current = float(now_mono if now_mono is not None else time.monotonic())
    window = MACRO_RATE_LIMIT_WINDOW_SECONDS
    while runtime.macro_rate_limit_events and (current - runtime.macro_rate_limit_events[0]) > window:
        runtime.macro_rate_limit_events.popleft()


async def _macro_restore_profile_capacity_if_due(runtime: AccountRuntime) -> None:
    now_mono = time.monotonic()
    if runtime.macro_reduced_concurrency_until <= 0.0:
        return
    if runtime.macro_reduced_concurrency_until > now_mono:
        return
    runtime.macro_reduced_concurrency_until = 0.0
    await runtime.profile_gate.set_capacity(max(1, runtime.macro_default_profile_capacity))


async def _macro_register_rate_limit(
    runtime: AccountRuntime,
    *,
    warn: Optional[Callable[[str], None]] = None,
) -> None:
    now_mono = time.monotonic()
    runtime.macro_rate_limit_events.append(now_mono)
    _macro_prune_rate_limit_events(runtime, now_mono=now_mono)
    if len(runtime.macro_rate_limit_events) < 2:
        return
    if runtime.macro_reduced_concurrency_until > now_mono:
        return
    runtime.macro_base_interval_seconds = min(
        runtime.macro_base_interval_seconds + 0.3,
        4.0,
    )
    if runtime.profile_limiter is not None:
        runtime.profile_limiter.min_interval_seconds = max(
            runtime.profile_limiter.min_interval_seconds,
            runtime.macro_base_interval_seconds,
        )
    reduced_capacity = max(1, runtime.macro_default_profile_capacity - 1)
    await runtime.profile_gate.set_capacity(reduced_capacity)
    runtime.macro_reduced_concurrency_until = now_mono + MACRO_CONCURRENCY_REDUCTION_SECONDS
    if warn is not None:
        warn(
            (
                "[LEADS][MACRO_ADAPT] "
                f"cuenta=@{runtime.username} | "
                f"intervalo_base={runtime.macro_base_interval_seconds:.2f}s | "
                f"concurrencia={reduced_capacity}"
            )
        )


def _deterministic_retry_delay_seconds(
    *,
    reason: str,
    attempts: int,
    cooldown_seconds: float = 0.0,
) -> float:
    normalized_reason = str(reason or "").lower()
    attempt_count = max(1, int(attempts))
    if "soft_block" in normalized_reason:
        return 90.0
    if "invalid_real" in normalized_reason:
        return float(20 * attempt_count)
    if "429" in normalized_reason or "403" in normalized_reason:
        if cooldown_seconds > 0:
            return max(30.0, min(240.0, float(cooldown_seconds)))
        return 90.0
    return float(15 * attempt_count)


def _serialize_limiter_state(limiter: AdaptiveTokenLimiter) -> Dict[str, Any]:
    return {
        "min_interval_seconds": float(limiter.min_interval_seconds),
        "bucket_tokens": float(limiter.bucket_tokens),
        "cooling_until_epoch": _mono_to_epoch(float(limiter.cooling_until)),
        "budget_day": str(getattr(limiter, "_budget_day", "") or ""),
        "budget_used": int(getattr(limiter, "_budget_used", 0) or 0),
        "exhausted": bool(getattr(limiter, "exhausted", False)),
        "rate_limit_events_epoch": [
            _mono_to_epoch(float(event_mono))
            for event_mono in list(getattr(limiter, "_rate_limit_events", []) or [])
        ],
        "consecutive_429": int(getattr(limiter, "consecutive_429", 0) or 0),
        "circuit_open_until_epoch": _mono_to_epoch(float(getattr(limiter, "circuit_open_until", 0.0) or 0.0)),
    }


def _restore_limiter_state(limiter: AdaptiveTokenLimiter, payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return
    limiter.min_interval_seconds = max(
        0.1,
        float(payload.get("min_interval_seconds") or limiter.min_interval_seconds),
    )
    limiter.bucket_tokens = max(
        0.0,
        min(float(limiter.burst_max), float(payload.get("bucket_tokens") or limiter.bucket_tokens)),
    )
    limiter.bucket_updated_at = time.monotonic()
    limiter.cooling_until = _epoch_to_mono(float(payload.get("cooling_until_epoch") or 0.0))
    limiter._budget_day = str(payload.get("budget_day") or limiter._budget_day)
    limiter._budget_used = max(0, int(payload.get("budget_used") or 0))
    limiter.exhausted = bool(payload.get("exhausted", limiter.exhausted))
    restored_events = deque()
    for event_epoch in list(payload.get("rate_limit_events_epoch") or []):
        try:
            event_mono = _epoch_to_mono(float(event_epoch))
        except Exception:
            continue
        if event_mono > 0:
            restored_events.append(event_mono)
    limiter._rate_limit_events = restored_events
    limiter.consecutive_429 = max(0, int(payload.get("consecutive_429") or 0))
    limiter.circuit_open_until = _epoch_to_mono(float(payload.get("circuit_open_until_epoch") or 0.0))


def _serialize_runtime_state(runtime: AccountRuntime) -> Dict[str, Any]:
    return {
        "macro_base_interval_seconds": float(runtime.macro_base_interval_seconds),
        "macro_next_request_not_before_epoch": _mono_to_epoch(runtime.macro_next_request_not_before),
        "macro_block_target": int(runtime.macro_block_target),
        "macro_block_progress": int(runtime.macro_block_progress),
        "macro_blocks_since_long_pause": int(runtime.macro_blocks_since_long_pause),
        "macro_next_long_break_after_blocks": int(runtime.macro_next_long_break_after_blocks),
        "macro_pause_until_epoch": _mono_to_epoch(runtime.macro_pause_until),
        "macro_default_profile_capacity": int(runtime.macro_default_profile_capacity),
        "macro_reduced_concurrency_until_epoch": _mono_to_epoch(runtime.macro_reduced_concurrency_until),
        "macro_rate_limit_events_epoch": [
            _mono_to_epoch(float(event_mono))
            for event_mono in list(runtime.macro_rate_limit_events or [])
        ],
        "account_processed": int(runtime.account_processed),
        "profile_limiter": _serialize_limiter_state(runtime.profile_limiter)
        if runtime.profile_limiter is not None
        else {},
        "profile_gate_capacity": int(runtime.profile_gate.capacity),
        "profile_retry_max": int(runtime.profile_retry_max),
        "image_retry_max": int(runtime.image_retry_max),
        "rate_limit_retry_max": int(runtime.rate_limit_retry_max),
        "profile_circuit_breaker_threshold": int(runtime.profile_circuit_breaker_threshold),
        "profile_circuit_breaker_seconds": float(runtime.profile_circuit_breaker_seconds),
        "image_circuit_breaker_threshold": int(runtime.image_circuit_breaker_threshold),
        "image_circuit_breaker_seconds": float(runtime.image_circuit_breaker_seconds),
    }


def _restore_runtime_state(
    runtime: AccountRuntime,
    payload: Dict[str, Any],
) -> None:
    if not isinstance(payload, dict):
        return
    runtime.macro_base_interval_seconds = max(
        MACRO_BASE_INTERVAL_MIN_SECONDS,
        float(payload.get("macro_base_interval_seconds") or runtime.macro_base_interval_seconds),
    )
    runtime.macro_next_request_not_before = _epoch_to_mono(
        float(payload.get("macro_next_request_not_before_epoch") or 0.0)
    )
    runtime.macro_block_target = max(1, int(payload.get("macro_block_target") or runtime.macro_block_target or 1))
    runtime.macro_block_progress = max(0, int(payload.get("macro_block_progress") or 0))
    runtime.macro_blocks_since_long_pause = max(0, int(payload.get("macro_blocks_since_long_pause") or 0))
    runtime.macro_next_long_break_after_blocks = max(
        1,
        int(payload.get("macro_next_long_break_after_blocks") or runtime.macro_next_long_break_after_blocks or 1),
    )
    runtime.macro_pause_until = _epoch_to_mono(float(payload.get("macro_pause_until_epoch") or 0.0))
    runtime.macro_default_profile_capacity = max(
        1,
        int(payload.get("macro_default_profile_capacity") or runtime.macro_default_profile_capacity),
    )
    runtime.macro_reduced_concurrency_until = _epoch_to_mono(
        float(payload.get("macro_reduced_concurrency_until_epoch") or 0.0)
    )
    restored_events = deque()
    for event_epoch in list(payload.get("macro_rate_limit_events_epoch") or []):
        try:
            event_mono = _epoch_to_mono(float(event_epoch))
        except Exception:
            continue
        if event_mono > 0:
            restored_events.append(event_mono)
    runtime.macro_rate_limit_events = restored_events
    runtime.account_processed = max(0, int(payload.get("account_processed") or 0))
    runtime.profile_gate.capacity = max(1, int(payload.get("profile_gate_capacity") or runtime.profile_gate.capacity))
    runtime.profile_retry_max = max(1, int(payload.get("profile_retry_max") or runtime.profile_retry_max))
    runtime.image_retry_max = max(1, int(payload.get("image_retry_max") or runtime.image_retry_max))
    runtime.rate_limit_retry_max = max(1, int(payload.get("rate_limit_retry_max") or runtime.rate_limit_retry_max))
    runtime.profile_circuit_breaker_threshold = max(
        1,
        int(payload.get("profile_circuit_breaker_threshold") or runtime.profile_circuit_breaker_threshold),
    )
    runtime.profile_circuit_breaker_seconds = max(
        1.0,
        float(payload.get("profile_circuit_breaker_seconds") or runtime.profile_circuit_breaker_seconds),
    )
    runtime.image_circuit_breaker_threshold = max(
        1,
        int(payload.get("image_circuit_breaker_threshold") or runtime.image_circuit_breaker_threshold),
    )
    runtime.image_circuit_breaker_seconds = max(
        1.0,
        float(payload.get("image_circuit_breaker_seconds") or runtime.image_circuit_breaker_seconds),
    )
    if runtime.profile_limiter is not None:
        _restore_limiter_state(runtime.profile_limiter, dict(payload.get("profile_limiter") or {}))


async def _v2_init_runtime(
    account: Dict[str, Any],
    *,
    run_cfg: Any,
    per_account_concurrency: int,
    image_concurrency_per_account: int,
    profile_daily_budget: int,
    profile_delay_min_seconds: float,
    profile_delay_max_seconds: float,
    profile_retry_max: int,
    image_retry_max: int,
    rate_limit_retry_max: int,
    profile_circuit_breaker_threshold: int,
    profile_circuit_breaker_seconds: float,
    image_circuit_breaker_threshold: int,
    image_circuit_breaker_seconds: float,
    warn: Callable[[str], None],
    worker_key: str = "__no_proxy__",
    shared_profile_gate: Optional["SlotGate"] = None,
    shared_image_gate: Optional["SlotGate"] = None,
    shared_semaphore: Optional[asyncio.Semaphore] = None,
) -> Optional[AccountRuntime]:
    account_username = str(account.get("username") or "").strip()
    if not account_username:
        return None

    try:
        http_client = create_authenticated_client(account, reason="leads-filter")
    except Exception as exc:
        warn(f"No se pudo iniciar cliente HTTP autenticado de @{account_username}: {exc}")
        return None

    proxy_url = str(account.get("proxy_url") or "").strip()
    normalized_delay_min = max(0.1, float(profile_delay_min_seconds))
    normalized_delay_max = max(normalized_delay_min, float(profile_delay_max_seconds))
    runtime = AccountRuntime(
        account=account,
        username=account_username,
        svc=None,
        ctx=None,
        page=None,
        profile_gate=shared_profile_gate or SlotGate(per_account_concurrency),
        image_gate=shared_image_gate or SlotGate(image_concurrency_per_account),
        semaphore=shared_semaphore or asyncio.Semaphore(per_account_concurrency),
        proxy_url=proxy_url,
        proxy_key=_v2_proxy_key(proxy_url, account_username),
        worker_key=str(worker_key or "__no_proxy__").strip() or "__no_proxy__",
        profile_limiter=ProfileLimiter(
            account_username,
            daily_budget=profile_daily_budget,
            delay_min_seconds=normalized_delay_min,
            delay_max_seconds=normalized_delay_max,
        ),
        request_timeout_min_ms=max(8_000, env_int("LEADS_REQUEST_TIMEOUT_MIN_MS", 8_000)),
        request_timeout_max_ms=max(8_000, env_int("LEADS_REQUEST_TIMEOUT_MAX_MS", 12_000)),
        macro_base_interval_seconds=random.uniform(normalized_delay_min, normalized_delay_max),
        macro_block_target=_macro_random_block_target(),
        macro_next_long_break_after_blocks=_macro_random_blocks_to_long_break(),
        macro_default_profile_capacity=max(1, int((shared_profile_gate.capacity if shared_profile_gate is not None else per_account_concurrency))),
        profile_retry_max=max(1, int(profile_retry_max)),
        image_retry_max=max(1, int(image_retry_max)),
        rate_limit_retry_max=max(1, int(rate_limit_retry_max)),
        profile_circuit_breaker_threshold=max(1, int(profile_circuit_breaker_threshold)),
        profile_circuit_breaker_seconds=max(1.0, float(profile_circuit_breaker_seconds)),
        image_circuit_breaker_threshold=max(1, int(image_circuit_breaker_threshold)),
        image_circuit_breaker_seconds=max(1.0, float(image_circuit_breaker_seconds)),
        http_client=http_client,
    )
    if runtime.request_timeout_max_ms < runtime.request_timeout_min_ms:
        runtime.request_timeout_min_ms, runtime.request_timeout_max_ms = (
            runtime.request_timeout_max_ms,
            runtime.request_timeout_min_ms,
        )
    if runtime.profile_limiter is not None:
        runtime.profile_limiter.min_interval_seconds = max(
            MACRO_BASE_INTERVAL_MIN_SECONDS,
            min(runtime.macro_base_interval_seconds, MACRO_BASE_INTERVAL_MAX_SECONDS),
        )
    try:
        await capture_runtime_http_meta(runtime)
    except Exception as exc:
        warn(f"No se pudo preparar el cliente HTTP de @{account_username}: {exc}")
        await _safe_shutdown_runtime_handle(runtime)
        return None
    return runtime


async def _v2_pick_profile_runtime(
    runtimes: deque[AccountRuntime],
    *,
    warn: Optional[Callable[[str], None]] = None,
) -> Tuple[Optional[AccountRuntime], float]:
    nearest_wait: Optional[float] = None
    if not runtimes:
        return None, 1.0
    for _ in range(len(runtimes)):
        runtime = runtimes[0]
        runtimes.rotate(-1)
        if str(runtime.disabled_reason or "").strip():
            continue
        await _macro_restore_profile_capacity_if_due(runtime)
        macro_wait, _macro_reason = _macro_runtime_wait_seconds(runtime)
        if macro_wait > 0.0:
            if nearest_wait is None or macro_wait < nearest_wait:
                nearest_wait = macro_wait
            continue
        limiter = runtime.profile_limiter
        if limiter is None:
            continue
        circuit_wait = await limiter.circuit_wait_seconds()
        if circuit_wait > 0.0:
            if nearest_wait is None or circuit_wait < nearest_wait:
                nearest_wait = circuit_wait
            continue
        if not await runtime.profile_gate.try_acquire():
            continue
        wait_seconds, reason = await limiter.peek_wait_seconds()
        if (
            reason == "budget_exhausted"
            and warn is not None
            and limiter.exhausted
            and not bool(getattr(limiter, "_budget_exhausted_logged", False))
        ):
            warn(
                (
                    "[LEADS][PRESUPUESTO] "
                    f"tipo={limiter.key_label} | clave={limiter.key_value} | agotado=si"
                )
            )
            limiter._budget_exhausted_logged = True
        if wait_seconds <= 0.0:
            _macro_note_request_dispatched(runtime)
            return runtime, 0.0
        await runtime.profile_gate.release()
        if nearest_wait is None or wait_seconds < nearest_wait:
            nearest_wait = wait_seconds
    return None, float(nearest_wait if nearest_wait is not None else 0.5)


async def _v2_pick_image_runtime(
    runtimes: deque[AccountRuntime],
    *,
    warn: Optional[Callable[[str], None]] = None,
) -> Tuple[Optional[AccountRuntime], float]:
    nearest_wait: Optional[float] = None
    if not runtimes:
        return None, 1.0
    for _ in range(len(runtimes)):
        runtime = runtimes[0]
        runtimes.rotate(-1)
        if str(runtime.disabled_reason or "").strip():
            continue
        macro_wait, _macro_reason = _macro_runtime_wait_seconds(runtime)
        if macro_wait > 0.0:
            if nearest_wait is None or macro_wait < nearest_wait:
                nearest_wait = macro_wait
            continue
        limiter = runtime.image_limiter
        if limiter is None:
            continue
        circuit_wait = await limiter.circuit_wait_seconds()
        if circuit_wait > 0.0:
            if nearest_wait is None or circuit_wait < nearest_wait:
                nearest_wait = circuit_wait
            continue
        if not await runtime.image_gate.try_acquire():
            continue
        wait_seconds, reason = await limiter.peek_wait_seconds()
        if (
            reason == "budget_exhausted"
            and warn is not None
            and limiter.exhausted
            and not bool(getattr(limiter, "_budget_exhausted_logged", False))
        ):
            warn(
                (
                    "[LEADS][PRESUPUESTO] "
                    f"tipo={limiter.key_label} | clave={limiter.key_value} | agotado=si"
                )
            )
            limiter._budget_exhausted_logged = True
        if wait_seconds <= 0.0:
            _macro_note_request_dispatched(runtime)
            return runtime, 0.0
        await runtime.image_gate.release()
        if nearest_wait is None or wait_seconds < nearest_wait:
            nearest_wait = wait_seconds
    return None, float(nearest_wait if nearest_wait is not None else 0.5)


async def _v2_mark_retry_pending(
    list_data: Dict[str, Any],
    *,
    idx: int,
    account: str,
    reason: str,
    next_attempt_epoch: float,
    lock: asyncio.Lock,
    task_type: str = TASK_PROFILE,
    max_retries: int,
) -> bool:
    def _retry_keys_for_task(task_kind: str) -> Tuple[str, str]:
        if task_kind == TASK_IMAGE_DOWNLOAD:
            return "image_retry_count", "image_next_attempt_at"
        return "profile_retry_count", "profile_next_attempt_at"

    def _retry_count_for_task(item_payload: Dict[str, Any], task_kind: str) -> int:
        count_key, _next_key = _retry_keys_for_task(task_kind)
        if task_kind == TASK_PROFILE:
            # Compatibilidad hacia atras: runs previos usaban retry_count global.
            return max(
                int(item_payload.get(count_key) or 0),
                int(item_payload.get("retry_count") or 0),
            )
        return int(item_payload.get(count_key) or 0)

    async with lock:
        item = list_data["items"][idx]
        retries = _retry_count_for_task(item, task_type) + 1
        normalized_reason = str(reason or "retry_later")
        next_attempt_at = _v2_epoch_to_iso(next_attempt_epoch)
        retry_count_key, retry_next_key = _retry_keys_for_task(task_type)
        item[retry_count_key] = retries
        item["account"] = account
        item["updated_at"] = now_iso()
        item["last_rate_limit_reason"] = normalized_reason
        item["last_retry_task_type"] = task_type
        if task_type == TASK_PROFILE:
            # Mantiene compatibilidad de lectura para listas antiguas.
            item["retry_count"] = retries
            item[retry_next_key] = next_attempt_at
            item["next_attempt_at"] = next_attempt_at
        if task_type == TASK_IMAGE_DOWNLOAD:
            lowered_reason = normalized_reason.lower()
            if "soft_block" in lowered_reason:
                item["image_status"] = "soft_block"
            elif "invalid_real" in lowered_reason:
                item["image_status"] = "invalid_real"
            else:
                item["image_status"] = "rate_limited"
            item["image_reason"] = normalized_reason
            item[retry_next_key] = next_attempt_at
        if retries > max(1, int(max_retries)):
            item.pop(retry_next_key, None)
            if task_type == TASK_PROFILE:
                item.pop("next_attempt_at", None)
            return False
        item["status"] = "PENDING"
        item["result"] = ""
        item["reason"] = normalized_reason
        item[retry_next_key] = next_attempt_at
        if task_type == TASK_PROFILE:
            item["next_attempt_at"] = next_attempt_at
        return True


async def _v2_clear_retry_metadata(
    list_data: Dict[str, Any],
    idx: int,
    lock: asyncio.Lock,
) -> None:
    async with lock:
        item = list_data["items"][idx]
        item.pop("profile_next_attempt_at", None)
        item.pop("profile_retry_count", None)
        item.pop("next_attempt_at", None)
        item.pop("retry_count", None)
        item.pop("last_retry_task_type", None)
        item.pop("last_rate_limit_reason", None)


def _v2_reset_item_for_fresh_run(item: Dict[str, Any]) -> None:
    item["status"] = "PENDING"
    item["result"] = ""
    item["reason"] = ""
    item["account"] = ""
    item["updated_at"] = ""
    for key in (
        "decision_final",
        "reasons",
        "scores",
        "extracted",
        "profile_next_attempt_at",
        "profile_retry_count",
        "next_attempt_at",
        "retry_count",
        "last_retry_task_type",
        "last_rate_limit_reason",
        "image_next_attempt_at",
        "image_retry_count",
        "image_status",
        "image_reason",
    ):
        item.pop(key, None)


async def _v2_task_profile(
    task: ScheduledTask,
    state: LeadWorkState,
    runtime: AccountRuntime,
    *,
    filter_settings: PipelineFilterSettings,
    warn: Callable[[str], None],
) -> TaskExecution:
    try:
        profile, status_code, reason = await fetch_profile_json_with_meta(runtime, state.username)
        if status_code == 429 and runtime.profile_limiter is not None:
            await runtime.profile_limiter.record_http_429(
                threshold=runtime.profile_circuit_breaker_threshold,
                circuit_seconds=runtime.profile_circuit_breaker_seconds,
                warn=warn,
            )
            cooldown = await runtime.profile_limiter.apply_rate_limit(status=status_code, warn=warn)
            await _macro_register_rate_limit(runtime, warn=warn)
            return TaskExecution(
                task=task,
                account=runtime.username,
                requeue=True,
                requeue_delay_seconds=_deterministic_retry_delay_seconds(
                    reason="profile_http_429",
                    attempts=int(task.attempts) + 1,
                    cooldown_seconds=cooldown,
                ),
                requeue_reason=f"profile_http_429_cooldown_{int(cooldown)}s",
                max_retries=runtime.rate_limit_retry_max,
            )

        if runtime.profile_limiter is not None and status_code and status_code < 400:
            await runtime.profile_limiter.record_success()
            await runtime.profile_limiter.apply_success_tuning(warn=warn)

        if profile is None:
            fail_reason = reason or (f"http_{status_code}" if status_code else "perfil_no_disponible")
            _isolate_runtime_for_account_health(
                runtime,
                status_code=status_code,
                reason=fail_reason,
                warn=warn,
            )
            state.account = runtime.username
            state.pending_evaluation = _v2_build_fail_evaluation(
                state,
                fail_reason,
                extra_reasons=[fail_reason],
            )
            return TaskExecution(
                task=task,
                account=runtime.username,
                next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
            )

        missing = missing_essential_fields(
            profile,
            require_image_url=_v2_is_image_requested(filter_settings),
        )
        if missing:
            fail_reason = f"incomplete_data:{','.join(missing)}"
            state.account = runtime.username
            state.pending_evaluation = _v2_build_fail_evaluation(
                state,
                fail_reason,
                extra_reasons=[fail_reason],
            )
            return TaskExecution(
                task=task,
                account=runtime.username,
                next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
            )

        state.account = runtime.username
        state.profile = profile
        state.extracted = profile_to_output(profile)
        state.scores = {}
        state.reasons = ["phase1_ok"]
        _macro_note_profile_processed(runtime, warn=warn)
        return TaskExecution(
            task=task,
            account=runtime.username,
            next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_TEXT_SCORE)],
        )
    finally:
        await runtime.profile_gate.release()


async def _v2_task_text_score(
    task: ScheduledTask,
    state: LeadWorkState,
    *,
    filter_settings: PipelineFilterSettings,
    text_engine: LocalTextEngine,
    text_gate: asyncio.Semaphore,
) -> TaskExecution:
    profile = state.profile
    image_requested = _v2_is_image_requested(filter_settings)
    if profile is None:
        state.pending_evaluation = _v2_build_fail_evaluation(
            state,
            "profile_missing",
            extra_reasons=["profile_missing"],
        )
        return TaskExecution(
            task=task,
            account=state.account,
            next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
        )

    phase2_ok, phase2_reason = passes_classic_filters(profile, filter_settings.classic_cfg)
    if not phase2_ok:
        if image_requested:
            state.image_status = "skipped"
            state.image_reason = "classic_filter_failed"
            state.image_next_attempt_at = ""
        state.reasons.append(f"phase2_fail:{phase2_reason}")
        state.pending_evaluation = _v2_build_fail_evaluation(
            state,
            phase2_reason or "classic_filters",
        )
        return TaskExecution(
            task=task,
            account=state.account,
            next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
        )

    state.reasons.append("phase2_ok")
    if filter_settings.text_state != FILTER_STATE_DISABLED:
        if not filter_settings.text_criteria:
            text_decision = TextDecision(
                qualified=False,
                score=0.0,
                threshold=text_engine.threshold,
                mode="none",
                reason="criteria_empty",
            )
        else:
            async with text_gate:
                text_decision = await asyncio.to_thread(text_engine.score, profile)

        state.scores["text_similarity"] = round(float(text_decision.score), 4)
        state.scores["text_threshold"] = round(float(text_decision.threshold), 4)
        state.scores["text_mode"] = text_decision.mode
        state.scores["text_engine_thresholds"] = text_engine_thresholds_to_dict(text_engine.thresholds)
        state.reasons.append(text_decision.reason)

        if filter_settings.text_state == FILTER_STATE_REQUIRED and not text_decision.qualified:
            if image_requested:
                state.image_status = "skipped"
                state.image_reason = "text_not_qualified"
                state.image_next_attempt_at = ""
            state.pending_evaluation = _v2_build_fail_evaluation(
                state,
                "texto_inteligente_no_califica",
            )
            return TaskExecution(
                task=task,
                account=state.account,
                next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
                )

    if not image_requested:
        state.image_status = "skipped"
        state.image_reason = "image_not_requested"
        state.image_next_attempt_at = ""
        state.pending_evaluation = _v2_build_pass_evaluation(state)
        return TaskExecution(
            task=task,
            account=state.account,
            next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
            stats={"image_skipped": 1},
        )

    return TaskExecution(
        task=task,
        account=state.account,
        next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_IMAGE_DOWNLOAD)],
    )


async def _v2_task_image_download(
    task: ScheduledTask,
    state: LeadWorkState,
    runtime: AccountRuntime,
    *,
    filter_settings: PipelineFilterSettings,
    warn: Callable[[str], None],
) -> TaskExecution:
    try:
        image_required = _v2_is_image_required(filter_settings)
        profile = state.profile
        if profile is None:
            state.image_status = "failed"
            state.image_reason = "profile_missing"
            state.image_next_attempt_at = ""
            state.pending_evaluation = _v2_build_fail_evaluation(
                state,
                "profile_missing",
                extra_reasons=["profile_missing"],
            )
            return TaskExecution(
                task=task,
                account=state.account,
                next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
                stats={"image_skipped": 1},
            )

        if not profile.profile_pic_url:
            fail_reason = "sin_foto"
            state.image_status = "failed" if image_required else "skipped"
            state.image_reason = fail_reason
            state.image_next_attempt_at = ""
            if image_required:
                state.pending_evaluation = _v2_build_fail_evaluation(
                    state,
                    fail_reason,
                    extra_reasons=[fail_reason],
                )
            else:
                state.reasons.append(fail_reason)
                state.pending_evaluation = _v2_build_pass_evaluation(state)
            return TaskExecution(
                task=task,
                account=state.account,
                next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
                stats={"image_skipped": 1},
            )

        async def _build_image_cooldown_requeue(status: int, reason: str) -> TaskExecution:
            state.image_status = "soft_block" if "soft_block" in str(reason or "").lower() else "rate_limited"
            state.image_reason = reason
            state.image_retry_count = max(state.image_retry_count, int(task.attempts) + 1)
            state.image_next_attempt_at = ""
            cooldown = 0.0
            if runtime.image_limiter is not None:
                await runtime.image_limiter.record_http_429(
                    threshold=runtime.image_circuit_breaker_threshold,
                    circuit_seconds=runtime.image_circuit_breaker_seconds,
                    warn=warn,
                )
                cooldown = await runtime.image_limiter.apply_rate_limit(status=status, warn=warn)
            await _macro_register_rate_limit(runtime, warn=warn)
            warn(
                (
                    "[LEADS][ENFRIAMIENTO_IMAGEN] "
                    f"proxy={runtime.proxy_key or runtime.proxy_url or '-'} | "
                    f"codigo={status} | "
                    f"enfriamiento={cooldown:.0f}s"
                )
            )
            return TaskExecution(
                task=task,
                account=runtime.username,
                requeue=True,
                requeue_delay_seconds=_deterministic_retry_delay_seconds(
                    reason=reason,
                    attempts=int(task.attempts) + 1,
                    cooldown_seconds=cooldown,
                ),
                requeue_reason=f"{reason}_cooldown_{int(cooldown)}s",
                max_retries=runtime.rate_limit_retry_max,
                stats={"image_rate_limited": 1},
            )

        image_bytes: Optional[bytes] = None
        download_error: Optional[ImageDownloadError] = None
        refresh_attempted = False
        image_url = str(profile.profile_pic_url or "").strip()

        while True:
            try:
                image_bytes = await download_profile_image_for_runtime(
                    runtime,
                    username=state.username,
                    image_url=image_url,
                )
                break
            except ImageRateLimit as exc:
                return await _build_image_cooldown_requeue(exc.status, f"image_http_{exc.status}")
            except ImageDownloadError as exc:
                if exc.reason == "image_not_found" and not refresh_attempted:
                    refresh_attempted = True
                    refreshed_profile, refreshed_status, _refreshed_reason = await fetch_profile_json_with_meta(
                        runtime,
                        state.username,
                    )
                    if refreshed_status == 429 and runtime.profile_limiter is not None:
                        await runtime.profile_limiter.record_http_429(
                            threshold=runtime.profile_circuit_breaker_threshold,
                            circuit_seconds=runtime.profile_circuit_breaker_seconds,
                            warn=warn,
                        )
                        cooldown = await runtime.profile_limiter.apply_rate_limit(
                            status=refreshed_status,
                            warn=warn,
                        )
                        await _macro_register_rate_limit(runtime, warn=warn)
                        return TaskExecution(
                            task=task,
                            account=runtime.username,
                            requeue=True,
                            requeue_delay_seconds=_deterministic_retry_delay_seconds(
                                reason="profile_refresh_http_429",
                                attempts=int(task.attempts) + 1,
                                cooldown_seconds=cooldown,
                            ),
                            requeue_reason=f"profile_refresh_http_429_cooldown_{int(cooldown)}s",
                            max_retries=runtime.rate_limit_retry_max,
                        )
                    if refreshed_profile is None:
                        refresh_fail_reason = (
                            _refreshed_reason
                            or (f"http_{refreshed_status}" if refreshed_status else "profile_refresh_failed")
                        )
                        _isolate_runtime_for_account_health(
                            runtime,
                            status_code=refreshed_status,
                            reason=refresh_fail_reason,
                            warn=warn,
                        )
                    if refreshed_profile is not None:
                        refreshed_url = str(refreshed_profile.profile_pic_url or "").strip()
                        if refreshed_url:
                            state.profile = refreshed_profile
                            state.extracted = profile_to_output(refreshed_profile)
                            image_url = refreshed_url
                            continue
                    download_error = ImageDownloadError("image_not_found", status=exc.status)
                    break
                download_error = exc
                break

        if image_bytes is None:
            fail_reason = str((download_error.reason if download_error is not None else "") or "image_download_failed")
            state.image_next_attempt_at = ""
            if fail_reason == "image_soft_block":
                return await _build_image_cooldown_requeue(429, "image_soft_block")
            if fail_reason == "invalid_real":
                current_attempt = int(task.attempts) + 1
                state.image_status = "invalid_real"
                state.image_reason = "invalid_real"
                state.image_retry_count = max(state.image_retry_count, current_attempt)
                if current_attempt <= IMAGE_INVALID_REAL_MAX_RETRIES:
                    delay = _deterministic_retry_delay_seconds(
                        reason="invalid_real",
                        attempts=current_attempt,
                    )
                    state.image_next_attempt_at = _v2_epoch_to_iso(time.time() + delay)
                    return TaskExecution(
                        task=task,
                        account=runtime.username,
                        requeue=True,
                        requeue_delay_seconds=delay,
                        requeue_reason="image_invalid_real",
                        max_retries=min(runtime.image_retry_max, IMAGE_INVALID_REAL_MAX_RETRIES),
                    )
                fail_reason = "invalid_real"

            if fail_reason == "image_not_found":
                state.image_status = "not_found"
                state.image_reason = "image_not_found"
            elif fail_reason == "invalid_real":
                state.image_status = "invalid_real"
                state.image_reason = "invalid_real"
            else:
                state.image_status = "failed"
                state.image_reason = fail_reason

            if image_required:
                state.pending_evaluation = _v2_build_fail_evaluation(
                    state,
                    state.image_reason,
                    extra_reasons=[state.image_reason],
                )
            else:
                state.reasons.append(state.image_reason)
                state.pending_evaluation = _v2_build_pass_evaluation(state)
            return TaskExecution(
                task=task,
                account=state.account,
                next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
                stats={"image_skipped": 1},
            )

        if runtime.image_limiter is not None:
            await runtime.image_limiter.record_success()
            await runtime.image_limiter.apply_success_tuning(warn=warn)

        state.image_status = "ok"
        state.image_reason = "ok"
        state.image_next_attempt_at = ""
        state.image_bytes = image_bytes
        return TaskExecution(
            task=task,
            account=state.account,
            next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_IMAGE_SCORE)],
            stats={"processed_images": 1},
        )
    finally:
        await runtime.image_gate.release()


async def _v2_task_image_score(
    task: ScheduledTask,
    state: LeadWorkState,
    *,
    filter_settings: PipelineFilterSettings,
    image_filter: ImageAttributeFilter,
    image_gate: asyncio.Semaphore,
) -> TaskExecution:
    image_required = _v2_is_image_required(filter_settings)
    if not state.image_bytes:
        reason = "image_missing"
        state.image_status = "failed" if image_required else "skipped"
        state.image_reason = reason
        state.image_next_attempt_at = ""
        if image_required:
            state.pending_evaluation = _v2_build_fail_evaluation(
                state,
                reason,
                extra_reasons=[reason],
            )
        else:
            state.reasons.append(reason)
            state.pending_evaluation = _v2_build_pass_evaluation(state)
        return TaskExecution(
            task=task,
            account=state.account,
            next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
        )

    rules = parse_image_prompt(filter_settings.image_prompt)
    effective_thresholds = build_image_engine_thresholds(filter_settings.image_engine_thresholds)
    async with image_gate:
        analysis: ImageAnalysisResult = await asyncio.to_thread(
            image_filter.analyze,
            state.image_bytes,
        )
    qualifies, reason = evaluate_image_rules(
        analysis,
        rules,
        thresholds=effective_thresholds,
    )

    state.scores["image_scores"] = {
        "prompt": filter_settings.image_prompt,
        "rules": {
            "gender": rules.gender,
            "min_age": rules.min_age,
            "min_age_strict": rules.min_age_strict,
            "max_age": rules.max_age,
            "max_age_strict": rules.max_age_strict,
            "require_beard": rules.require_beard,
            "forbid_beard": rules.forbid_beard,
            "require_overweight": rules.require_overweight,
            "forbid_overweight": rules.forbid_overweight,
            "require_slim": rules.require_slim,
            "forbid_slim": rules.forbid_slim,
            "require_sharp": rules.require_sharp,
            "forbid_sharp": rules.forbid_sharp,
        },
        "thresholds": image_engine_thresholds_to_dict(effective_thresholds),
        "analysis": {
            "face_detected": bool(analysis.face_detected),
            "age": analysis.age,
            "gender": analysis.gender,
            "beard_prob": float(analysis.beard_prob),
            "attribute_probs": dict(analysis.attribute_probs or {}),
        },
    }
    state.reasons.append(f"image:{reason}")
    if qualifies:
        state.image_status = "ok"
        state.image_reason = "ok"
        state.image_next_attempt_at = ""
    else:
        if reason == "no_face":
            state.image_status = "no_face"
        elif reason in {"age_unknown", "gender_unknown"}:
            state.image_status = "invalid_image"
        else:
            state.image_status = "failed"
        state.image_reason = reason
        state.image_next_attempt_at = ""

    if image_required and not qualifies:
        state.pending_evaluation = _v2_build_fail_evaluation(
            state,
            reason,
            extra_reasons=[reason],
        )
    else:
        state.pending_evaluation = _v2_build_pass_evaluation(state)

    return TaskExecution(
        task=task,
        account=state.account,
        next_tasks=[ScheduledTask(idx=task.idx, username=task.username, task_type=TASK_FINALIZE)],
    )


async def _v2_task_finalize(
    task: ScheduledTask,
    state: LeadWorkState,
    *,
    list_data: Dict[str, Any],
    list_lock: asyncio.Lock,
    update_item: Callable[[Dict[str, Any], int, str, LeadEvaluation, asyncio.Lock], Awaitable[None]],
    log_filter_result: Callable[[str, str, str, str], None],
) -> TaskExecution:
    evaluation = state.pending_evaluation or _v2_build_fail_evaluation(
        state,
        "finalize_without_evaluation",
        extra_reasons=["finalize_without_evaluation"],
    )
    account = state.account or "-"
    async with list_lock:
        item = list_data["items"][task.idx]
        item["image_status"] = state.image_status or "skipped"
        item["image_reason"] = state.image_reason or ""
        item["image_retry_count"] = int(state.image_retry_count or 0)
        if state.image_next_attempt_at:
            item["image_next_attempt_at"] = state.image_next_attempt_at
        else:
            item.pop("image_next_attempt_at", None)
    await update_item(list_data, task.idx, account, evaluation, list_lock)
    await _v2_clear_retry_metadata(list_data, task.idx, list_lock)
    log_reason = _build_decision_reason_for_log(evaluation)
    log_filter_result(
        state.username or "-",
        account,
        "CALIFICA" if evaluation.passed else "NO CALIFICA",
        log_reason,
    )
    return TaskExecution(task=task, account=account, finalized=True)


async def execute_filter_list_async(
    list_data: Dict[str, Any],
    filter_cfg: Any,
    run_cfg: Any,
    *,
    resolve_accounts: Callable[[List[str]], List[Dict[str, Any]]],
    refresh_list_stats: Callable[[Dict[str, Any]], None],
    save_filter_list: Callable[[Dict[str, Any]], None],
    reset_runtime_stop_event: Callable[[], None],
    should_stop: Callable[[asyncio.Event], bool],
    warn: Callable[[str], None],
    log_filter_result: Callable[[str, str, str, str], None],
    update_item: Callable[[Dict[str, Any], int, str, LeadEvaluation, asyncio.Lock], Awaitable[None]],
    save_filter_runtime_state: Optional[Callable[[Dict[str, Any], Optional[Set[int]]], None]] = None,
) -> bool:
    reset_runtime_stop_event()
    refresh_list_stats(list_data)
    force_fresh_start = bool(list_data.pop("_force_fresh_start", False))
    if force_fresh_start:
        items = list_data.get("items") or []
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    _v2_reset_item_for_fresh_run(item)
        list_data.pop("_pipeline_state", None)
        refresh_list_stats(list_data)
        save_filter_list(list_data)
        warn("reinicio total de lista aplicado: se limpiaron retries y estado previo.")
    total_leads = len(list_data.get("items") or [])
    checkpoint_auto_threshold = max(
        1,
        env_int("LEADS_CHECKPOINT_AUTO_THRESHOLD", LARGE_RUN_AUTO_THRESHOLD),
    )
    checkpoint_every = max(
        50,
        env_int("LEADS_CHECKPOINT_EVERY", LARGE_RUN_CHECKPOINT_EVERY),
    )
    large_run_enabled = total_leads >= checkpoint_auto_threshold
    checkpoint_payload = dict(list_data.get("_pipeline_state") or {}) if large_run_enabled else {}
    checkpoint_runtime_states: Dict[str, Dict[str, Any]] = dict(
        checkpoint_payload.get("account_states") or {}
    ) if isinstance(checkpoint_payload, dict) else {}
    checkpoint_proxy_states: Dict[str, Dict[str, Any]] = dict(
        checkpoint_payload.get("proxy_states") or {}
    ) if isinstance(checkpoint_payload, dict) else {}
    if large_run_enabled and isinstance(checkpoint_payload, dict):
        pending_retry_state = dict(checkpoint_payload.get("pending_retry_state") or {})
        items_ref = list_data.get("items") or []
        for idx_raw, retry_payload in pending_retry_state.items():
            try:
                idx = int(idx_raw)
            except Exception:
                continue
            if idx < 0 or idx >= len(items_ref):
                continue
            item = items_ref[idx]
            if str(item.get("status") or "").upper() != "PENDING":
                continue
            payload_dict = dict(retry_payload or {})
            profile_retry_count = int(
                payload_dict.get("profile_retry_count")
                or payload_dict.get("retry_count")
                or 0
            )
            profile_next_attempt_at = str(
                payload_dict.get("profile_next_attempt_at")
                or payload_dict.get("next_attempt_at")
                or ""
            )
            image_retry_count = int(payload_dict.get("image_retry_count") or 0)
            image_next_attempt_at = str(payload_dict.get("image_next_attempt_at") or "")
            if profile_retry_count > 0:
                item["profile_retry_count"] = profile_retry_count
                item["retry_count"] = profile_retry_count
            if profile_next_attempt_at:
                item["profile_next_attempt_at"] = profile_next_attempt_at
                item["next_attempt_at"] = profile_next_attempt_at
            if image_retry_count > 0:
                item["image_retry_count"] = image_retry_count
            if image_next_attempt_at:
                item["image_next_attempt_at"] = image_next_attempt_at
    next_large_checkpoint = (
        ((int(list_data.get("processed", 0) or 0) // checkpoint_every) + 1)
        * checkpoint_every
    ) if large_run_enabled else 0
    if large_run_enabled:
        warn(
            (
                "large run automatico activado: "
                f"total={total_leads}, umbral={checkpoint_auto_threshold}, "
                f"checkpoint cada {checkpoint_every}"
            )
        )

    pending_indices = [
        idx
        for idx, item in enumerate(list_data.get("items") or [])
        if item.get("status") == "PENDING"
    ]
    if not pending_indices:
        warn("No quedan usernames pendientes.")
        return False

    requested_accounts = list(getattr(run_cfg, "accounts", []) or [])
    accounts = resolve_accounts(requested_accounts)
    if not accounts:
        warn("No hay cuentas validas para ejecutar el filtrado.")
        return False

    run_concurrency = int(getattr(run_cfg, "concurrency", 1) or 1)
    requested_worker_count = max(1, run_concurrency)

    def _is_account_blocked_by_cache(account_payload: Dict[str, Any]) -> Tuple[bool, str]:
        username = str(account_payload.get("username") or "").strip().lstrip("@")
        if not username:
            return True, "username_vacio"
        try:
            import health_store
        except Exception:
            return False, ""
        state, expired = health_store.get_badge(username)
        health_state = str(state or "").strip().upper()
        if not health_state or expired:
            return False, ""
        if health_store.blocks_automation(health_state):
            return True, health_state
        return False, ""

    candidate_accounts: List[Dict[str, Any]] = []
    for account in accounts:
        blocked, badge_reason = _is_account_blocked_by_cache(account)
        if blocked:
            username = str(account.get("username") or "").strip().lstrip("@") or "-"
            warn(f"cuenta excluida por salud cacheada @{username}: {badge_reason or 'bloqueada'}")
            continue
        candidate_accounts.append(account)
    if not candidate_accounts:
        warn("No hay cuentas saludables para ejecutar el filtrado.")
        return False
    candidate_accounts, blocked_proxy_accounts = _v2_filter_accounts_by_proxy_runtime(candidate_accounts)
    for blocked in blocked_proxy_accounts:
        username = str(blocked.get("username") or "").strip().lstrip("@") or "-"
        reason = str(blocked.get("message") or blocked.get("status") or "proxy_blocked").strip()
        warn(f"cuenta excluida por preflight de proxy @{username}: {reason}")
    if not candidate_accounts:
        warn("No hay cuentas con proxy operativo para ejecutar el filtrado.")
        return False
    selected_accounts, selected_worker_keys = _v2_select_accounts_for_workers(
        candidate_accounts,
        requested_workers=requested_worker_count,
    )
    if not selected_accounts:
        warn("No hay cuentas utilizables para ejecutar el filtrado.")
        return False

    per_account_concurrency = clamp(env_int("LEADS_PER_ACCOUNT_CONCURRENCY", 2), 1, 8)
    image_concurrency_per_account = clamp(env_int("LEADS_IMAGE_CONCURRENCY_PER_ACCOUNT", 1), 1, 4)
    run_delay_min_seconds = max(
        0.1,
        float(getattr(run_cfg, "delay_min", 0) or env_float("LEADS_DELAY_MIN_SECONDS", 20.0)),
    )
    run_delay_max_seconds = max(
        run_delay_min_seconds,
        float(getattr(run_cfg, "delay_max", 0) or env_float("LEADS_DELAY_MAX_SECONDS", 40.0)),
    )
    profile_retry_max = max(
        1,
        env_int("LEADS_PROFILE_MAX_RETRIES", DEFAULT_PROFILE_MAX_RETRIES),
    )
    image_retry_max = max(
        1,
        env_int("LEADS_IMAGE_MAX_RETRIES", DEFAULT_IMAGE_MAX_RETRIES),
    )
    rate_limit_retry_max = max(
        1,
        env_int("LEADS_RATE_LIMIT_MAX_RETRIES", DEFAULT_RATE_LIMIT_MAX_RETRIES),
    )
    profile_circuit_breaker_threshold = max(
        1,
        env_int("LEADS_PROFILE_429_BREAKER_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD),
    )
    image_circuit_breaker_threshold = max(
        1,
        env_int("LEADS_IMAGE_429_BREAKER_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD),
    )
    profile_circuit_breaker_seconds = max(
        1.0,
        env_float("LEADS_PROFILE_429_BREAKER_SECONDS", DEFAULT_CIRCUIT_BREAKER_SECONDS),
    )
    image_circuit_breaker_seconds = max(
        1.0,
        env_float("LEADS_IMAGE_429_BREAKER_SECONDS", DEFAULT_CIRCUIT_BREAKER_SECONDS),
    )
    raw_max_runtime_seconds = getattr(run_cfg, "max_runtime_seconds", None)
    if raw_max_runtime_seconds is None:
        raw_max_runtime_seconds = env_float("LEADS_MAX_RUNTIME_SECONDS", DEFAULT_MAX_RUNTIME_SECONDS)
    max_runtime_seconds = max(
        0.0,
        float(raw_max_runtime_seconds or 0),
    )
    allow_tiny_runtime = env_truthy("LEADS_ALLOW_TINY_RUNTIME", False)
    if 0.0 < max_runtime_seconds < 30.0 and not allow_tiny_runtime:
        warn(
            (
                f"max_runtime_seconds={int(max_runtime_seconds)}s es demasiado bajo para produccion; "
                f"ajustado a {DEFAULT_MAX_RUNTIME_SECONDS}s "
                "(usa LEADS_ALLOW_TINY_RUNTIME=1 si queres smoke runs cortos)"
            )
        )
        max_runtime_seconds = float(DEFAULT_MAX_RUNTIME_SECONDS)
    max_items = max(0, int(getattr(run_cfg, "max_items", 0) or env_int("LEADS_MAX_ITEMS", 0)))
    progress_every_seconds = max(10.0, env_float("LEADS_PROGRESS_EVERY_SECONDS", 10.0))
    persist_interval_seconds = max(0.5, env_float("LEADS_PERSIST_INTERVAL_SECONDS", 2.0))
    persist_result_batch = max(1, env_int("LEADS_PERSIST_RESULT_BATCH", 10))
    profile_budget_per_day = max(0, env_int("LEADS_MAX_PROFILES_PER_DAY", 0))
    image_budget_per_day = max(0, env_int("LEADS_MAX_IMAGES_PER_DAY", 0))
    startup_wait_seconds = max(
        1.0,
        env_float("LEADS_STARTUP_WAIT_SECONDS", DEFAULT_STARTUP_WAIT_SECONDS),
    )
    startup_second_phase_default = max(45.0, min(240.0, float(max(1, len(selected_worker_keys))) * 20.0))
    startup_second_phase_seconds = max(
        0.0,
        env_float("LEADS_STARTUP_SECOND_PHASE_SECONDS", startup_second_phase_default),
    )
    startup_retry_attempts = max(
        0,
        env_int("LEADS_STARTUP_RETRY_ATTEMPTS", DEFAULT_STARTUP_RETRY_ATTEMPTS),
    )
    session_preflight_enabled = env_truthy("LEADS_SESSION_PREFLIGHT", True)
    session_preflight_concurrency = clamp(env_int("LEADS_SESSION_PREFLIGHT_CONCURRENCY", 2), 1, 8)
    session_preflight_timeout_seconds = max(
        5.0,
        env_float("LEADS_SESSION_PREFLIGHT_TIMEOUT_SECONDS", 20.0),
    )

    async def _preflight_session(account_payload: Dict[str, Any]) -> Tuple[bool, str]:
        username = str(account_payload.get("username") or "").strip().lstrip("@")
        if not username:
            return False, "username_vacio"
        try:
            from src.auth.persistent_login import check_session_async
        except Exception:
            return True, "preflight_unavailable"
        proxy_payload = playwright_proxy_from_account(account_payload)
        try:
            ok, reason = await asyncio.wait_for(
                check_session_async(
                    username=username,
                    proxy=proxy_payload,
                    headless=True,
                ),
                timeout=session_preflight_timeout_seconds,
            )
        except Exception as exc:
            # Errores transitorios no deben vaciar el pool completo.
            return True, f"preflight_error:{type(exc).__name__}"
        if ok:
            return True, str(reason or "ok")
        normalized_reason = str(reason or "").lower()
        if normalized_reason == "storage_state_missing":
            return True, normalized_reason
        blocking_tokens = (
            "auth_cookies_without_ui",
            "url_login_or_challenge",
            "chrome_error_page",
            "suspended",
            "checkpoint",
        )
        if any(token in normalized_reason for token in blocking_tokens):
            return False, normalized_reason or "session_unusable"
        return True, normalized_reason or "session_preflight_soft_fail"

    if session_preflight_enabled and selected_accounts:
        semaphore = asyncio.Semaphore(session_preflight_concurrency)

        async def _bounded_preflight(
            account_payload: Dict[str, Any],
        ) -> Tuple[Dict[str, Any], bool, str]:
            async with semaphore:
                ok, reason = await _preflight_session(account_payload)
                return account_payload, ok, reason

        preflight_results = await asyncio.gather(
            *[asyncio.create_task(_bounded_preflight(account)) for account in selected_accounts]
        )
        session_ready_accounts: List[Dict[str, Any]] = []
        for account_payload, ok, reason in preflight_results:
            username = str(account_payload.get("username") or "").strip().lstrip("@") or "-"
            if ok:
                session_ready_accounts.append(account_payload)
                continue
            warn(f"cuenta excluida por preflight de sesion @{username}: {reason}")
        if session_ready_accounts:
            selected_accounts, selected_worker_keys = _v2_select_accounts_for_workers(
                session_ready_accounts,
                requested_workers=requested_worker_count,
            )
            if not selected_accounts:
                warn("No hay cuentas con sesion util despues del preflight.")
                return False
        else:
            warn("No hay cuentas con sesion util despues del preflight.")
            return False

    warn(
        (
            f"delay efectivo perfil: min={run_delay_min_seconds:.1f}s "
            f"max={run_delay_max_seconds:.1f}s"
        )
    )
    warn(
        (
            f"retries maximos: profile={profile_retry_max} "
            f"image={image_retry_max} rate_limit={rate_limit_retry_max}"
        )
    )
    warn(
        (
            f"circuit breaker 429: profile={profile_circuit_breaker_threshold}/"
            f"{int(profile_circuit_breaker_seconds)}s "
            f"image={image_circuit_breaker_threshold}/{int(image_circuit_breaker_seconds)}s"
        )
    )

    filter_settings = _v2_build_filter_settings(filter_cfg)
    effective_text_thresholds = build_text_engine_thresholds(filter_settings.text_engine_thresholds)
    text_engine = LocalTextEngine(
        getattr(getattr(filter_cfg, "text", object()), "criteria", "") or "",
        thresholds=effective_text_thresholds,
    )
    image_filter: ImageAttributeFilter | None = None
    text_gate = asyncio.Semaphore(max(1, env_int("LEADS_TEXT_WORKERS", 2)))
    image_score_gate = asyncio.Semaphore(max(1, env_int("LEADS_IMAGE_WORKERS", 1)))
    worker_profile_gates = {worker_key: SlotGate(1) for worker_key in selected_worker_keys}
    worker_image_gates = {worker_key: SlotGate(1) for worker_key in selected_worker_keys}
    worker_semaphores = {worker_key: asyncio.Semaphore(1) for worker_key in selected_worker_keys}
    init_concurrency = max(1, len(selected_worker_keys))

    runtimes: List[AccountRuntime] = []
    image_limiters: Dict[str, ImageLimiter] = {}
    profile_rr: deque[AccountRuntime] = deque()
    image_rr: deque[AccountRuntime] = deque()
    runtime_by_username: Dict[str, AccountRuntime] = {}

    def register_runtime(runtime: AccountRuntime, *, origin: str) -> None:
        key = str(runtime.username or "").strip().lower()
        if not key or key in runtime_by_username:
            return
        proxy_key = runtime.proxy_key or f"account:@{runtime.username}"
        limiter = image_limiters.get(proxy_key)
        if limiter is None:
            limiter = ImageLimiter(proxy_key, daily_budget=image_budget_per_day)
            image_limiters[proxy_key] = limiter
        if large_run_enabled:
            limiter_state = checkpoint_proxy_states.get(str(proxy_key))
            if isinstance(limiter_state, dict):
                _restore_limiter_state(limiter, limiter_state)
        runtime.image_limiter = limiter
        if large_run_enabled:
            runtime_state = checkpoint_runtime_states.get(key)
            if isinstance(runtime_state, dict):
                _restore_runtime_state(runtime, runtime_state)
        runtime_by_username[key] = runtime
        runtimes.append(runtime)
        profile_rr.append(runtime)
        image_rr.append(runtime)
        _ = origin
        warn(f"sesion verificada para @{runtime.username}")

    init_tasks: Set[asyncio.Task] = set()
    init_task_owner: Dict[asyncio.Task, str] = {}
    selected_labels = [
        f"@{str(account.get('username') or '').strip() or '-'}"
        for account in selected_accounts
    ]

    init_semaphore = asyncio.Semaphore(init_concurrency)

    async def _bounded_init_runtime(account_payload: Dict[str, Any]) -> Optional[AccountRuntime]:
        worker_key = _v2_worker_key_for_account(account_payload)
        async with init_semaphore:
            return await _v2_init_runtime(
                account_payload,
                run_cfg=run_cfg,
                per_account_concurrency=per_account_concurrency,
                image_concurrency_per_account=image_concurrency_per_account,
                profile_daily_budget=profile_budget_per_day,
                profile_delay_min_seconds=run_delay_min_seconds,
                profile_delay_max_seconds=run_delay_max_seconds,
                profile_retry_max=profile_retry_max,
                image_retry_max=image_retry_max,
                rate_limit_retry_max=rate_limit_retry_max,
                profile_circuit_breaker_threshold=profile_circuit_breaker_threshold,
                profile_circuit_breaker_seconds=profile_circuit_breaker_seconds,
                image_circuit_breaker_threshold=image_circuit_breaker_threshold,
                image_circuit_breaker_seconds=image_circuit_breaker_seconds,
                warn=warn,
                worker_key=worker_key,
                shared_profile_gate=worker_profile_gates.get(worker_key),
                shared_image_gate=worker_image_gates.get(worker_key),
                shared_semaphore=worker_semaphores.get(worker_key),
            )

    def schedule_init_tasks(accounts: List[Dict[str, Any]]) -> Set[asyncio.Task]:
        scheduled: Set[asyncio.Task] = set()
        for account in accounts:
            account_username = str(account.get("username") or "").strip() or "-"
            warn(f"@{account_username} iniciando sesion")
            task = asyncio.create_task(_bounded_init_runtime(account))
            scheduled.add(task)
            init_task_owner[task] = account_username
        return scheduled

    async def cancel_init_tasks(tasks: Set[asyncio.Task]) -> None:
        if not tasks:
            return
        for init_task in list(tasks):
            init_task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    print("", flush=True)
    print("comenzando filtrado", flush=True)
    warn(f"cuentas seleccionadas: {', '.join(selected_labels) or '-'}")
    init_tasks = schedule_init_tasks(selected_accounts)

    async def consume_init_done(done_tasks: Set[asyncio.Task], *, origin: str) -> None:
        for init_task in done_tasks:
            owner = init_task_owner.pop(init_task, "-")
            try:
                runtime_result = init_task.result()
            except Exception as exc:
                error_detail = str(exc).strip() or type(exc).__name__
                warn(f"sesion no disponible para @{owner}: {error_detail}")
                continue
            if runtime_result is None:
                warn(f"sesion no disponible para @{owner}: runtime_no_disponible")
                continue
            register_runtime(runtime_result, origin=origin)

    print(
        (
            f"preparando sesiones: pendientes={len(pending_indices)}, "
            f"cuentas={len(selected_accounts)}, espera={int(startup_wait_seconds)}s"
        ),
        flush=True,
    )

    async def wait_for_ready_runtimes(tasks: Set[asyncio.Task], *, origin: str) -> Set[asyncio.Task]:
        pending_tasks = set(tasks)
        startup_deadline = time.monotonic() + startup_wait_seconds
        while pending_tasks and not runtimes and time.monotonic() < startup_deadline:
            timeout_left = max(0.05, startup_deadline - time.monotonic())
            done_init, pending_init = await asyncio.wait(
                pending_tasks,
                timeout=timeout_left,
                return_when=asyncio.FIRST_COMPLETED,
            )
            pending_tasks = set(pending_init)
            if done_init:
                await consume_init_done(set(done_init), origin=origin)
        if not runtimes and pending_tasks and startup_second_phase_seconds > 0.0:
            warn(
                (
                    f"sin cuentas listas en {int(startup_wait_seconds)}s, "
                    f"esperando extra {startup_second_phase_seconds:.1f}s"
                )
            )
            second_phase_deadline = time.monotonic() + startup_second_phase_seconds
            while pending_tasks and not runtimes and time.monotonic() < second_phase_deadline:
                done_init, pending_init = await asyncio.wait(
                    pending_tasks,
                    timeout=0.5,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                pending_tasks = set(pending_init)
                if done_init:
                    await consume_init_done(set(done_init), origin=f"{origin}_tardio")
        return pending_tasks

    init_tasks = await wait_for_ready_runtimes(init_tasks, origin="arranque")
    retry_attempt = 0
    while not runtimes and retry_attempt < startup_retry_attempts:
        retry_accounts = _v2_pending_accounts_for_startup_retry(selected_accounts, runtime_by_username)
        if not retry_accounts:
            break
        retry_attempt += 1
        await cancel_init_tasks(init_tasks)
        init_tasks = set()
        warn(
            (
                f"sin cuentas listas tras el arranque inicial; "
                f"reintentando inicializacion ({retry_attempt}/{startup_retry_attempts}) "
                f"con {len(retry_accounts)} cuenta(s)"
            )
        )
        init_tasks = schedule_init_tasks(retry_accounts)
        init_tasks = await wait_for_ready_runtimes(init_tasks, origin=f"reintento_{retry_attempt}")
    if not runtimes:
        await cancel_init_tasks(init_tasks)
        warn(
            (
                f"arranque_cancelado: ninguna cuenta quedo lista "
                f"(espera_inicial={startup_wait_seconds:.1f}s, reintentos={startup_retry_attempts}). "
                "revisa proxy/sesion o subi LEADS_STARTUP_SECOND_PHASE_SECONDS"
            )
        )
        return False

    lead_states: Dict[int, LeadWorkState] = {}
    task_heap: List[Tuple[float, int, int, ScheduledTask]] = []
    task_seq = 0

    def enqueue_task(task: ScheduledTask, ready_at: Optional[float] = None) -> None:
        nonlocal task_seq
        eta = max(0.0, float(ready_at if ready_at is not None else task.next_attempt_at or time.time()))
        task.next_attempt_at = eta
        heapq.heappush(task_heap, (eta, TASK_PRIORITY.get(task.task_type, 9), task_seq, task))
        task_seq += 1

    now_epoch = time.time()
    for idx in pending_indices:
        item = (list_data.get("items") or [])[idx]
        username = str(item.get("username") or "").strip().lstrip("@")
        state = LeadWorkState(idx=idx, username=username)
        lead_states[idx] = state

        if not username:
            state.pending_evaluation = _v2_build_fail_evaluation(
                state,
                "username_vacio",
                extra_reasons=["username_vacio"],
            )
            enqueue_task(ScheduledTask(idx=idx, username="-", task_type=TASK_FINALIZE))
            continue

        next_attempt = _v2_iso_to_epoch(
            item.get("profile_next_attempt_at") or item.get("next_attempt_at")
        )
        ready_at = max(now_epoch, next_attempt) if next_attempt > 0 else now_epoch
        retries = max(
            0,
            int(item.get("profile_retry_count") or item.get("retry_count") or 0),
        )
        enqueue_task(
            ScheduledTask(
                idx=idx,
                username=username,
                task_type=TASK_PROFILE,
                attempts=retries,
                next_attempt_at=ready_at,
            ),
            ready_at=ready_at,
        )

    active_tasks: Set[asyncio.Task] = set()
    list_lock = asyncio.Lock()
    stop_event = asyncio.Event()
    started_at = time.monotonic()
    next_progress_at = time.monotonic() + progress_every_seconds
    next_idle_notice_at = 0.0
    idle_notice_every_seconds = max(30.0, float(progress_every_seconds) * 3.0)

    def compute_max_inflight() -> int:
        profile_gate_capacity = 0
        profile_gate_ids: Set[int] = set()
        image_gate_capacity = 0
        image_gate_ids: Set[int] = set()
        for runtime in runtimes:
            if str(runtime.disabled_reason or "").strip():
                continue
            profile_gate_id = id(runtime.profile_gate)
            if profile_gate_id not in profile_gate_ids:
                profile_gate_ids.add(profile_gate_id)
                profile_gate_capacity += max(1, int(runtime.profile_gate.capacity))
            image_gate_id = id(runtime.image_gate)
            if image_gate_id not in image_gate_ids:
                image_gate_ids.add(image_gate_id)
                image_gate_capacity += max(1, int(runtime.image_gate.capacity))
        return max(
            1,
            profile_gate_capacity
            + image_gate_capacity
            + int(getattr(text_gate, "_value", 1) or 1)
            + int(getattr(image_score_gate, "_value", 1) or 1),
        )

    def has_usable_account_runtimes() -> bool:
        return any(not str(runtime.disabled_reason or "").strip() for runtime in runtimes)

    processed_in_run = 0
    end_reason = "queue_drained"
    failed_account_count = max(0, len(selected_accounts) - len(runtimes))
    image_stats: Dict[str, int] = {
        "processed_images": 0,
        "image_rate_limited": 0,
        "image_skipped": 0,
    }
    account_pool_unusable = False

    def _default_max_retries_for_task(task_type: str) -> int:
        if task_type == TASK_IMAGE_DOWNLOAD:
            return max(1, image_retry_max)
        return max(1, profile_retry_max)

    def _build_retry_exhausted_evaluation(
        retry_state: LeadWorkState,
        *,
        task_type: str,
        retry_reason: str,
    ) -> LeadEvaluation:
        normalized_reason = str(retry_reason or "retry_later")
        if task_type == TASK_IMAGE_DOWNLOAD:
            retry_state.image_status = "failed"
            retry_state.image_reason = "image_failed_max_retries"
            retry_state.image_next_attempt_at = ""
            if _v2_is_image_required(filter_settings):
                return _v2_build_fail_evaluation(
                    retry_state,
                    "image_failed_max_retries",
                    extra_reasons=[normalized_reason, "image_failed_max_retries"],
                )
            retry_state.reasons.extend([normalized_reason, "image_failed_max_retries"])
            return _v2_build_pass_evaluation(retry_state)
        retry_state.reasons.extend([normalized_reason, "max_retries_exceeded"])
        return _v2_build_fail_evaluation(
            retry_state,
            "max_retries_exceeded",
            extra_reasons=[normalized_reason, "max_retries_exceeded"],
        )

    def maybe_save_large_run_checkpoint(*, force: bool = False) -> bool:
        nonlocal next_large_checkpoint
        if not large_run_enabled:
            return False
        refresh_list_stats(list_data)
        processed_total = int(list_data.get("processed", 0) or 0)
        if not force and processed_total < next_large_checkpoint:
            return False
        account_states: Dict[str, Dict[str, Any]] = {}
        for runtime in runtimes:
            account_key = str(runtime.username or "").strip().lower()
            if not account_key:
                continue
            account_states[account_key] = _serialize_runtime_state(runtime)
        proxy_states: Dict[str, Dict[str, Any]] = {}
        for proxy_key, limiter in image_limiters.items():
            proxy_states[str(proxy_key)] = _serialize_limiter_state(limiter)
        pending_retry_state: Dict[str, Dict[str, Any]] = {}
        for idx, item in enumerate(list_data.get("items") or []):
            if str(item.get("status") or "").upper() != "PENDING":
                continue
            profile_retry_count = int(item.get("profile_retry_count") or item.get("retry_count") or 0)
            profile_next_attempt_at = str(
                item.get("profile_next_attempt_at") or item.get("next_attempt_at") or ""
            )
            image_retry_count = int(item.get("image_retry_count") or 0)
            image_next_attempt_at = str(item.get("image_next_attempt_at") or "")
            if (
                profile_retry_count <= 0
                and not profile_next_attempt_at
                and image_retry_count <= 0
                and not image_next_attempt_at
            ):
                continue
            pending_retry_state[str(idx)] = {
                "profile_retry_count": profile_retry_count,
                "profile_next_attempt_at": profile_next_attempt_at,
                "image_retry_count": image_retry_count,
                "image_next_attempt_at": image_next_attempt_at,
            }
        list_data["_pipeline_state"] = {
            "schema": CHECKPOINT_SCHEMA_VERSION,
            "large_run_enabled": True,
            "updated_at": now_iso(),
            "processed": processed_total,
            "checkpoint_every": checkpoint_every,
            "checkpoint_auto_threshold": checkpoint_auto_threshold,
            "retry_fields_saved_in_items": True,
            "pending_retry_state": pending_retry_state,
            "account_states": account_states,
            "proxy_states": proxy_states,
        }
        if callable(save_filter_runtime_state):
            save_filter_runtime_state(list_data, set(dirty_item_indexes))
        else:
            save_filter_list(list_data)
        if force:
            next_large_checkpoint = (
                ((processed_total // checkpoint_every) + 1) * checkpoint_every
            )
            return True
        while processed_total >= next_large_checkpoint:
            next_large_checkpoint += checkpoint_every
        return True

    pending_persist = False
    persisted_results_since_flush = 0
    last_persist_at = time.monotonic()
    dirty_item_indexes: Set[int] = set()

    def mark_list_dirty(*, finalized: bool = False, item_idx: Optional[int] = None) -> None:
        nonlocal pending_persist, persisted_results_since_flush, dirty_item_indexes
        pending_persist = True
        if item_idx is not None:
            dirty_item_indexes.add(int(item_idx))
        if finalized:
            persisted_results_since_flush += 1

    def flush_list_state(*, force: bool = False) -> None:
        nonlocal pending_persist, persisted_results_since_flush, last_persist_at, dirty_item_indexes
        if not pending_persist and not force:
            return
        now_mono = time.monotonic()
        due_by_time = (now_mono - last_persist_at) >= persist_interval_seconds
        due_by_batch = persisted_results_since_flush >= persist_result_batch
        if not force and not due_by_time and not due_by_batch:
            return
        refresh_list_stats(list_data)
        saved = bool(maybe_save_large_run_checkpoint(force=force))
        if not saved:
            if callable(save_filter_runtime_state):
                save_filter_runtime_state(list_data, set(dirty_item_indexes))
            else:
                save_filter_list(list_data)
        pending_persist = False
        persisted_results_since_flush = 0
        dirty_item_indexes.clear()
        last_persist_at = time.monotonic()

    def ensure_image_filter_ready() -> ImageAttributeFilter:
        nonlocal image_filter
        if image_filter is None:
            image_filter = ImageAttributeFilter()
            image_model_status = image_filter.warmup_models()
            scrfd_loaded = bool(image_model_status.get("scrfd_loaded"))
            fairface_loaded = bool(image_model_status.get("fairface_loaded"))
            genderage_loaded = bool(image_model_status.get("genderage_loaded"))
            scrfd_msg = f"[IMAGE_MODELS] scrfd_loaded={scrfd_loaded}"
            fairface_msg = f"[IMAGE_MODELS] fairface_loaded={fairface_loaded}"
            genderage_msg = f"[IMAGE_MODELS] genderage_loaded={genderage_loaded}"
            logger.info(scrfd_msg)
            logger.info(fairface_msg)
            logger.info(genderage_msg)
            warn(scrfd_msg)
            warn(fairface_msg)
            warn(genderage_msg)
        return image_filter

    print("", flush=True)
    print("comenzando filtrado...", flush=True)
    print(
        (
            f"cuentas listas: {len(runtimes)}/{len(selected_accounts)} | "
            f"fallidas={failed_account_count} | "
            f"presupuesto perfiles={profile_budget_per_day or 'sin limite'} | "
            f"presupuesto imagenes={image_budget_per_day or 'sin limite'}"
        ),
        flush=True,
    )

    try:
        while (task_heap or active_tasks) and not should_stop(stop_event):
            if init_tasks:
                done_init, pending_init = await asyncio.wait(
                    init_tasks,
                    timeout=0.0,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                init_tasks = set(pending_init)
                if done_init:
                    before_count = len(runtimes)
                    await consume_init_done(set(done_init), origin="en_ejecucion")
                    if len(runtimes) > before_count:
                        print(f"cuentas disponibles: {len(runtimes)}/{len(selected_accounts)}", flush=True)

            if max_runtime_seconds > 0 and (time.monotonic() - started_at) >= max_runtime_seconds:
                end_reason = f"max_runtime_reached:{int(max_runtime_seconds)}s"
                break
            if max_items > 0 and processed_in_run >= max_items:
                end_reason = f"max_items_reached:{max_items}"
                break

            now_epoch = time.time()
            max_inflight = compute_max_inflight()
            dispatched_any = False
            while task_heap and len(active_tasks) < max_inflight and not should_stop(stop_event):
                ready_at, _priority, _seq, scheduled = task_heap[0]
                if ready_at > now_epoch:
                    break
                heapq.heappop(task_heap)
                state = lead_states.get(scheduled.idx)
                if state is None:
                    continue

                if max_items > 0 and (processed_in_run + len(active_tasks)) >= max_items:
                    end_reason = f"max_items_reached:{max_items}"
                    break

                task_coro: Optional[Awaitable[TaskExecution]] = None
                if scheduled.task_type == TASK_PROFILE:
                    runtime, wait_seconds = await _v2_pick_profile_runtime(profile_rr, warn=warn)
                    if runtime is None:
                        if not init_tasks and not has_usable_account_runtimes():
                            account_pool_unusable = True
                            end_reason = "account_pool_unusable"
                            warn(
                                (
                                    "[LEADS][ACCOUNT_HEALTH] "
                                    "sin cuentas seguras/utilizables; el run se pausa para evitar "
                                    "reutilizar sesiones comprometidas"
                                )
                            )
                            break
                        enqueue_task(scheduled, ready_at=time.time() + max(0.2, wait_seconds))
                        # Keep scanning the heap for tasks that can run now
                        # (for example, text/finalize tasks that do not need
                        # an account runtime slot).
                        continue
                    task_coro = _v2_task_profile(
                        scheduled,
                        state,
                        runtime,
                        filter_settings=filter_settings,
                        warn=warn,
                    )
                elif scheduled.task_type == TASK_TEXT_SCORE:
                    task_coro = _v2_task_text_score(
                        scheduled,
                        state,
                        filter_settings=filter_settings,
                        text_engine=text_engine,
                        text_gate=text_gate,
                    )
                elif scheduled.task_type == TASK_IMAGE_DOWNLOAD:
                    runtime, wait_seconds = await _v2_pick_image_runtime(image_rr, warn=warn)
                    if runtime is None:
                        if not init_tasks and not has_usable_account_runtimes():
                            account_pool_unusable = True
                            end_reason = "account_pool_unusable"
                            warn(
                                (
                                    "[LEADS][ACCOUNT_HEALTH] "
                                    "sin cuentas seguras/utilizables; el run se pausa para evitar "
                                    "reutilizar sesiones comprometidas"
                                )
                            )
                            break
                        enqueue_task(scheduled, ready_at=time.time() + max(0.2, wait_seconds))
                        continue
                    task_coro = _v2_task_image_download(
                        scheduled,
                        state,
                        runtime,
                        filter_settings=filter_settings,
                        warn=warn,
                    )
                elif scheduled.task_type == TASK_IMAGE_SCORE:
                    current_image_filter = ensure_image_filter_ready()
                    task_coro = _v2_task_image_score(
                        scheduled,
                        state,
                        filter_settings=filter_settings,
                        image_filter=current_image_filter,
                        image_gate=image_score_gate,
                    )
                elif scheduled.task_type == TASK_FINALIZE:
                    task_coro = _v2_task_finalize(
                        scheduled,
                        state,
                        list_data=list_data,
                        list_lock=list_lock,
                        update_item=update_item,
                        log_filter_result=log_filter_result,
                    )
                if task_coro is None:
                    continue
                active_tasks.add(asyncio.create_task(task_coro))
                dispatched_any = True

            if active_tasks:
                done, pending_tasks = await asyncio.wait(
                    active_tasks,
                    timeout=0.5,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                active_tasks = set(pending_tasks)
                for completed_task in done:
                    try:
                        outcome = completed_task.result()
                    except Exception as exc:
                        warn(f"Worker de scheduler termino con error: {exc}")
                        continue
                    if not isinstance(outcome, TaskExecution):
                        continue
                    if outcome.stats:
                        for stat_key, stat_value in outcome.stats.items():
                            try:
                                image_stats[stat_key] = image_stats.get(stat_key, 0) + int(stat_value or 0)
                            except Exception:
                                continue
                    if outcome.finalized:
                        lead_states.pop(outcome.task.idx, None)
                        processed_in_run += 1
                        mark_list_dirty(finalized=True, item_idx=outcome.task.idx)
                        flush_list_state(force=False)
                        continue
                    if outcome.requeue:
                        retry_delay = max(1.0, float(outcome.requeue_delay_seconds or 0.0))
                        next_attempt_epoch = time.time() + retry_delay
                        next_attempt_count = int(outcome.task.attempts) + 1
                        task_max_retries = max(
                            1,
                            int(outcome.max_retries or _default_max_retries_for_task(outcome.task.task_type)),
                        )
                        retry_state = lead_states.get(outcome.task.idx)
                        if retry_state is not None and outcome.account:
                            retry_state.account = outcome.account
                        if outcome.task.task_type == TASK_IMAGE_DOWNLOAD and retry_state is not None:
                            retry_state.image_retry_count = max(retry_state.image_retry_count, next_attempt_count)
                            retry_state.image_next_attempt_at = _v2_epoch_to_iso(next_attempt_epoch)
                            lowered_reason = str(outcome.requeue_reason or "").lower()
                            if "soft_block" in lowered_reason:
                                retry_state.image_status = "soft_block"
                            elif "invalid_real" in lowered_reason:
                                retry_state.image_status = "invalid_real"
                            else:
                                retry_state.image_status = "rate_limited"
                            if not retry_state.image_reason:
                                retry_state.image_reason = "image_rate_limited"
                        should_retry = await _v2_mark_retry_pending(
                            list_data,
                            idx=outcome.task.idx,
                            account=outcome.account,
                            reason=outcome.requeue_reason,
                            next_attempt_epoch=next_attempt_epoch,
                            lock=list_lock,
                            task_type=outcome.task.task_type,
                            max_retries=task_max_retries,
                        )
                        if should_retry:
                            enqueue_task(
                                ScheduledTask(
                                    idx=outcome.task.idx,
                                    username=outcome.task.username,
                                    task_type=outcome.task.task_type,
                                    attempts=next_attempt_count,
                                    next_attempt_at=next_attempt_epoch,
                                ),
                                ready_at=next_attempt_epoch,
                            )
                            if retry_state is not None and outcome.task.task_type == TASK_IMAGE_DOWNLOAD:
                                retry_state.image_next_attempt_at = _v2_epoch_to_iso(next_attempt_epoch)
                            mark_list_dirty(finalized=False, item_idx=outcome.task.idx)
                            flush_list_state(force=False)
                        else:
                            if retry_state is not None:
                                retry_state.pending_evaluation = _build_retry_exhausted_evaluation(
                                    retry_state,
                                    task_type=outcome.task.task_type,
                                    retry_reason=outcome.requeue_reason,
                                )
                                retry_state.image_next_attempt_at = ""
                            await _v2_clear_retry_metadata(list_data, outcome.task.idx, list_lock)
                            enqueue_task(
                                ScheduledTask(
                                    idx=outcome.task.idx,
                                    username=outcome.task.username,
                                    task_type=TASK_FINALIZE,
                                ),
                                ready_at=time.time(),
                            )
                            mark_list_dirty(finalized=False, item_idx=outcome.task.idx)
                            flush_list_state(force=False)
                        continue
                    for next_task in outcome.next_tasks:
                        enqueue_task(next_task, ready_at=next_task.next_attempt_at or time.time())
            else:
                if not task_heap:
                    break
                next_ready = max(0.0, task_heap[0][0] - time.time())
                if next_ready >= 10.0 and time.monotonic() >= next_idle_notice_at:
                    warn(
                        (
                            "[LEADS][EN_ESPERA] "
                            f"sin_tareas_listas=si | proximo_intento_en={int(next_ready)}s"
                        )
                    )
                    next_idle_notice_at = time.monotonic() + idle_notice_every_seconds
                await _v2_sleep_with_stop(max(0.1, min(2.0, next_ready)), stop_event, should_stop)

            if dispatched_any or active_tasks:
                next_idle_notice_at = 0.0
            flush_list_state(force=False)

            if time.monotonic() >= next_progress_at:
                flush_list_state(force=True)
                pending_count = sum(
                    1 for item in (list_data.get("items") or []) if item.get("status") == "PENDING"
                )
                print(
                    (
                        f"progreso: procesadas={list_data.get('processed', 0)} | "
                        f"calificadas={list_data.get('qualified', 0)} | "
                        f"descartadas={list_data.get('discarded', 0)} | "
                        f"pendientes={pending_count} | "
                        f"cuentas={len(runtimes)}"
                    ),
                    flush=True,
                )
                print(
                    (
                        f"imagenes: procesadas={image_stats.get('processed_images', 0)} | "
                        f"rate_limited={image_stats.get('image_rate_limited', 0)} | "
                        f"omitidas={image_stats.get('image_skipped', 0)}"
                    ),
                    flush=True,
                )
                next_progress_at = time.monotonic() + progress_every_seconds

            if account_pool_unusable and not active_tasks:
                pending_local_tasks = any(
                    task.task_type not in ACCOUNT_RUNTIME_TASK_TYPES
                    for _ready_at, _priority, _seq, task in task_heap
                )
                if not pending_local_tasks:
                    stop_event.set()
                    break

            if not dispatched_any and not active_tasks and task_heap:
                await asyncio.sleep(0.05)

        if should_stop(stop_event):
            end_reason = "stop_requested"
    finally:
        for init_task in list(init_tasks):
            init_task.cancel()
        if init_tasks:
            await asyncio.gather(*init_tasks, return_exceptions=True)
        for active in list(active_tasks):
            active.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)
        shutdown_tasks = [asyncio.create_task(shutdown_account_runtime(runtime)) for runtime in runtimes]
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)

    refresh_list_stats(list_data)
    flush_list_state(force=True)
    if end_reason == "account_pool_unusable":
        stop_event.set()
    pending_count = sum(
        1 for item in (list_data.get("items") or []) if item.get("status") == "PENDING"
    )
    print(
        (
            f"fin de filtrado: motivo={end_reason} | "
            f"procesadas={list_data.get('processed', 0)} | "
            f"calificadas={list_data.get('qualified', 0)} | "
            f"descartadas={list_data.get('discarded', 0)} | "
            f"pendientes={pending_count} | "
            f"img_ok={image_stats.get('processed_images', 0)} | "
            f"img_rate_limited={image_stats.get('image_rate_limited', 0)} | "
            f"img_omitidas={image_stats.get('image_skipped', 0)}"
        ),
        flush=True,
    )
    return stop_event.is_set()


async def shutdown_account_runtime(runtime: AccountRuntime) -> None:
    await _safe_shutdown_runtime_handle(runtime)


async def init_account_runtime(
    account: Dict[str, Any],
    *,
    run_cfg: Any,
    per_account_concurrency: int,
    warn: Callable[[str], None],
) -> Optional[AccountRuntime]:
    env_delay_min = max(0.1, env_float("LEADS_DELAY_MIN_SECONDS", 20.0))
    env_delay_max = max(env_delay_min, env_float("LEADS_DELAY_MAX_SECONDS", 40.0))
    return await _v2_init_runtime(
        account,
        run_cfg=run_cfg,
        per_account_concurrency=per_account_concurrency,
        image_concurrency_per_account=clamp(env_int("LEADS_IMAGE_CONCURRENCY_PER_ACCOUNT", 1), 1, 4),
        profile_daily_budget=max(0, env_int("LEADS_MAX_PROFILES_PER_DAY", 0)),
        profile_delay_min_seconds=env_delay_min,
        profile_delay_max_seconds=env_delay_max,
        profile_retry_max=max(1, env_int("LEADS_PROFILE_MAX_RETRIES", DEFAULT_PROFILE_MAX_RETRIES)),
        image_retry_max=max(1, env_int("LEADS_IMAGE_MAX_RETRIES", DEFAULT_IMAGE_MAX_RETRIES)),
        rate_limit_retry_max=max(1, env_int("LEADS_RATE_LIMIT_MAX_RETRIES", DEFAULT_RATE_LIMIT_MAX_RETRIES)),
        profile_circuit_breaker_threshold=max(
            1,
            env_int("LEADS_PROFILE_429_BREAKER_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD),
        ),
        profile_circuit_breaker_seconds=max(
            1.0,
            env_float("LEADS_PROFILE_429_BREAKER_SECONDS", DEFAULT_CIRCUIT_BREAKER_SECONDS),
        ),
        image_circuit_breaker_threshold=max(
            1,
            env_int("LEADS_IMAGE_429_BREAKER_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD),
        ),
        image_circuit_breaker_seconds=max(
            1.0,
            env_float("LEADS_IMAGE_429_BREAKER_SECONDS", DEFAULT_CIRCUIT_BREAKER_SECONDS),
        ),
        warn=warn,
    )


async def mark_item_retry_pending(
    list_data: Dict[str, Any],
    *,
    idx: int,
    account: str,
    reason: str,
    next_attempt_epoch: float,
    lock: asyncio.Lock,
    task_type: str = TASK_PROFILE,
    max_retries: int = DEFAULT_PROFILE_MAX_RETRIES,
) -> bool:
    return await _v2_mark_retry_pending(
        list_data,
        idx=idx,
        account=account,
        reason=reason,
        next_attempt_epoch=next_attempt_epoch,
        lock=lock,
        task_type=task_type,
        max_retries=max_retries,
    )


async def phase1_extract_profile(
    runtime: AccountRuntime,
    username: str,
    *,
    warn: Callable[[str], None],
) -> Tuple[Optional[ProfileSnapshot], str]:
    normalized = (username or "").strip().lstrip("@")
    if not normalized:
        return None, "username_vacio"
    profile, status_code, reason = await fetch_profile_json_with_meta(runtime, normalized)
    if status_code == 429 and runtime.profile_limiter is not None:
        cooldown_seconds = await runtime.profile_limiter.apply_rate_limit(status=status_code, warn=warn)
        await _macro_register_rate_limit(runtime, warn=warn)
        raise LeadRetryLater(
            reason=f"http_429_cooldown_{int(cooldown_seconds)}s",
            delay_seconds=_deterministic_retry_delay_seconds(
                reason="http_429",
                attempts=1,
                cooldown_seconds=cooldown_seconds,
            ),
        )
    if runtime.profile_limiter is not None and status_code and status_code < 400:
        await runtime.profile_limiter.apply_success_tuning(warn=warn)
    if profile is None:
        return None, reason or "perfil_no_disponible"
    missing = missing_essential_fields(profile, require_image_url=False)
    if missing:
        return None, f"incomplete_data:{','.join(missing)}"
    return profile, ""

