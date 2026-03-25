from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class TextEngineThresholds:
    embeddings_threshold: float = 0.64
    hybrid_embeddings_weight: float = 0.75
    regex_floor_threshold: float = 0.35
    regex_ceiling_threshold: float = 0.85
    regex_coverage_base: float = 0.20
    regex_coverage_per_term: float = 0.10
    regex_coverage_max_terms: int = 4


def _clamp_float(value: Any, *, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(str(value).strip().replace(",", "."))
    except Exception:
        return float(default)
    return float(max(minimum, min(maximum, parsed)))


def _clamp_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        return int(default)
    return int(max(minimum, min(maximum, parsed)))


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    return _clamp_float(raw, default=default, minimum=0.0, maximum=1.0)


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    return _clamp_int(raw, default=default, minimum=minimum, maximum=maximum)


def default_text_engine_thresholds() -> TextEngineThresholds:
    return TextEngineThresholds(
        embeddings_threshold=_env_float("LEADS_TEXT_THRESHOLD", 0.64),
        hybrid_embeddings_weight=_env_float("LEADS_TEXT_HYBRID_EMBEDDINGS_WEIGHT", 0.75),
        regex_floor_threshold=_env_float("LEADS_TEXT_REGEX_FLOOR_THRESHOLD", 0.35),
        regex_ceiling_threshold=_env_float("LEADS_TEXT_REGEX_CEILING_THRESHOLD", 0.85),
        regex_coverage_base=_env_float("LEADS_TEXT_REGEX_COVERAGE_BASE", 0.20),
        regex_coverage_per_term=_env_float("LEADS_TEXT_REGEX_COVERAGE_PER_TERM", 0.10),
        regex_coverage_max_terms=_env_int(
            "LEADS_TEXT_REGEX_COVERAGE_MAX_TERMS",
            4,
            minimum=1,
            maximum=20,
        ),
    )


def text_engine_thresholds_to_dict(thresholds: TextEngineThresholds) -> dict[str, Any]:
    return {
        "embeddings_threshold": float(thresholds.embeddings_threshold),
        "hybrid_embeddings_weight": float(thresholds.hybrid_embeddings_weight),
        "regex_floor_threshold": float(thresholds.regex_floor_threshold),
        "regex_ceiling_threshold": float(thresholds.regex_ceiling_threshold),
        "regex_coverage_base": float(thresholds.regex_coverage_base),
        "regex_coverage_per_term": float(thresholds.regex_coverage_per_term),
        "regex_coverage_max_terms": int(thresholds.regex_coverage_max_terms),
    }


def sanitize_text_engine_thresholds_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    base = default_text_engine_thresholds()
    raw = dict(payload or {})
    thresholds = TextEngineThresholds(
        embeddings_threshold=_clamp_float(
            raw.get("embeddings_threshold"),
            default=base.embeddings_threshold,
            minimum=0.05,
            maximum=0.99,
        ),
        hybrid_embeddings_weight=_clamp_float(
            raw.get("hybrid_embeddings_weight"),
            default=base.hybrid_embeddings_weight,
            minimum=0.0,
            maximum=1.0,
        ),
        regex_floor_threshold=_clamp_float(
            raw.get("regex_floor_threshold"),
            default=base.regex_floor_threshold,
            minimum=0.0,
            maximum=0.98,
        ),
        regex_ceiling_threshold=_clamp_float(
            raw.get("regex_ceiling_threshold"),
            default=base.regex_ceiling_threshold,
            minimum=0.02,
            maximum=0.99,
        ),
        regex_coverage_base=_clamp_float(
            raw.get("regex_coverage_base"),
            default=base.regex_coverage_base,
            minimum=0.0,
            maximum=1.0,
        ),
        regex_coverage_per_term=_clamp_float(
            raw.get("regex_coverage_per_term"),
            default=base.regex_coverage_per_term,
            minimum=0.0,
            maximum=0.50,
        ),
        regex_coverage_max_terms=_clamp_int(
            raw.get("regex_coverage_max_terms"),
            default=base.regex_coverage_max_terms,
            minimum=1,
            maximum=20,
        ),
    )
    return text_engine_thresholds_to_dict(thresholds)


def build_text_engine_thresholds(
    payload: TextEngineThresholds | Mapping[str, Any] | None,
) -> TextEngineThresholds:
    if isinstance(payload, TextEngineThresholds):
        return payload
    cleaned = sanitize_text_engine_thresholds_payload(payload if isinstance(payload, Mapping) else None)
    return TextEngineThresholds(
        embeddings_threshold=float(cleaned["embeddings_threshold"]),
        hybrid_embeddings_weight=float(cleaned["hybrid_embeddings_weight"]),
        regex_floor_threshold=float(cleaned["regex_floor_threshold"]),
        regex_ceiling_threshold=float(cleaned["regex_ceiling_threshold"]),
        regex_coverage_base=float(cleaned["regex_coverage_base"]),
        regex_coverage_per_term=float(cleaned["regex_coverage_per_term"]),
        regex_coverage_max_terms=int(cleaned["regex_coverage_max_terms"]),
    )
