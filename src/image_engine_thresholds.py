from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class ImageEngineThresholds:
    gender_prob_threshold: float = 0.70
    beard_threshold: float = 0.60
    overweight_threshold: float = 0.56
    overweight_tolerance: float = 0.06
    overweight_male35_threshold: float = 0.14
    slim_threshold: float = 0.60
    age_min_tolerance_years: int = 1
    age_min_tolerance_over30_prob: float = 0.62
    sharpness_threshold: float = 0.30


def _clamp_float(value: Any, *, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(str(value).strip())
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


def default_image_engine_thresholds() -> ImageEngineThresholds:
    return ImageEngineThresholds(
        gender_prob_threshold=_env_float("LEADS_IMAGE_GENDER_PROB_THRESHOLD", 0.70),
        beard_threshold=_env_float("LEADS_IMAGE_BEARD_THRESHOLD", 0.60),
        overweight_threshold=_env_float("LEADS_IMAGE_OVERWEIGHT_THRESHOLD", 0.56),
        overweight_tolerance=_env_float("LEADS_IMAGE_OVERWEIGHT_TOLERANCE", 0.06),
        overweight_male35_threshold=_env_float("LEADS_IMAGE_OVERWEIGHT_MALE35_THRESHOLD", 0.14),
        slim_threshold=_env_float("LEADS_IMAGE_SLIM_THRESHOLD", 0.60),
        age_min_tolerance_years=_env_int(
            "LEADS_IMAGE_MIN_AGE_TOLERANCE_YEARS",
            1,
            minimum=0,
            maximum=8,
        ),
        age_min_tolerance_over30_prob=_env_float(
            "LEADS_IMAGE_MIN_AGE_TOLERANCE_OVER30_PROB",
            0.62,
        ),
        sharpness_threshold=_env_float("LEADS_IMAGE_SHARPNESS_THRESHOLD", 0.30),
    )


def image_engine_thresholds_to_dict(thresholds: ImageEngineThresholds) -> dict[str, Any]:
    return {
        "gender_prob_threshold": float(thresholds.gender_prob_threshold),
        "beard_threshold": float(thresholds.beard_threshold),
        "overweight_threshold": float(thresholds.overweight_threshold),
        "overweight_tolerance": float(thresholds.overweight_tolerance),
        "overweight_male35_threshold": float(thresholds.overweight_male35_threshold),
        "slim_threshold": float(thresholds.slim_threshold),
        "age_min_tolerance_years": int(thresholds.age_min_tolerance_years),
        "age_min_tolerance_over30_prob": float(thresholds.age_min_tolerance_over30_prob),
        "sharpness_threshold": float(thresholds.sharpness_threshold),
    }


def sanitize_image_engine_thresholds_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    base = default_image_engine_thresholds()
    raw = dict(payload or {})
    thresholds = ImageEngineThresholds(
        gender_prob_threshold=_clamp_float(
            raw.get("gender_prob_threshold"),
            default=base.gender_prob_threshold,
            minimum=0.50,
            maximum=0.99,
        ),
        beard_threshold=_clamp_float(
            raw.get("beard_threshold"),
            default=base.beard_threshold,
            minimum=0.05,
            maximum=0.99,
        ),
        overweight_threshold=_clamp_float(
            raw.get("overweight_threshold"),
            default=base.overweight_threshold,
            minimum=0.05,
            maximum=0.99,
        ),
        overweight_tolerance=_clamp_float(
            raw.get("overweight_tolerance"),
            default=base.overweight_tolerance,
            minimum=0.0,
            maximum=0.40,
        ),
        overweight_male35_threshold=_clamp_float(
            raw.get("overweight_male35_threshold"),
            default=base.overweight_male35_threshold,
            minimum=0.0,
            maximum=0.80,
        ),
        slim_threshold=_clamp_float(
            raw.get("slim_threshold"),
            default=base.slim_threshold,
            minimum=0.05,
            maximum=0.99,
        ),
        age_min_tolerance_years=_clamp_int(
            raw.get("age_min_tolerance_years"),
            default=base.age_min_tolerance_years,
            minimum=0,
            maximum=8,
        ),
        age_min_tolerance_over30_prob=_clamp_float(
            raw.get("age_min_tolerance_over30_prob"),
            default=base.age_min_tolerance_over30_prob,
            minimum=0.0,
            maximum=1.0,
        ),
        sharpness_threshold=_clamp_float(
            raw.get("sharpness_threshold"),
            default=base.sharpness_threshold,
            minimum=0.0,
            maximum=1.0,
        ),
    )
    return image_engine_thresholds_to_dict(thresholds)


def build_image_engine_thresholds(
    payload: ImageEngineThresholds | Mapping[str, Any] | None,
) -> ImageEngineThresholds:
    if isinstance(payload, ImageEngineThresholds):
        return payload
    cleaned = sanitize_image_engine_thresholds_payload(payload if isinstance(payload, Mapping) else None)
    return ImageEngineThresholds(
        gender_prob_threshold=float(cleaned["gender_prob_threshold"]),
        beard_threshold=float(cleaned["beard_threshold"]),
        overweight_threshold=float(cleaned["overweight_threshold"]),
        overweight_tolerance=float(cleaned["overweight_tolerance"]),
        overweight_male35_threshold=float(cleaned["overweight_male35_threshold"]),
        slim_threshold=float(cleaned["slim_threshold"]),
        age_min_tolerance_years=int(cleaned["age_min_tolerance_years"]),
        age_min_tolerance_over30_prob=float(cleaned["age_min_tolerance_over30_prob"]),
        sharpness_threshold=float(cleaned["sharpness_threshold"]),
    )

