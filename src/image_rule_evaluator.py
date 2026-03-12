from __future__ import annotations

from typing import Any, Mapping, Tuple

from src.image_attribute_filter import ImageAnalysisResult
from src.image_engine_thresholds import (
    ImageEngineThresholds,
    build_image_engine_thresholds,
    default_image_engine_thresholds,
)
from src.image_prompt_parser import ImageFilterRules

_DEFAULT_THRESHOLDS = default_image_engine_thresholds()
AGE_MIN_TOLERANCE_YEARS = int(_DEFAULT_THRESHOLDS.age_min_tolerance_years)
OVERWEIGHT_THRESHOLD = float(_DEFAULT_THRESHOLDS.overweight_threshold)
OVERWEIGHT_MALE35_THRESHOLD = float(_DEFAULT_THRESHOLDS.overweight_male35_threshold)
OVERWEIGHT_TOLERANCE = float(_DEFAULT_THRESHOLDS.overweight_tolerance)


def _passes_min_age_tolerance(
    *,
    age: int,
    min_age: int,
    analysis: ImageAnalysisResult,
    thresholds: ImageEngineThresholds,
) -> bool:
    if int(thresholds.age_min_tolerance_years) <= 0:
        return False
    if min_age < 30:
        return False
    if age + int(thresholds.age_min_tolerance_years) < min_age:
        return False
    probs = analysis.attribute_probs or {}
    age_over_30_prob = float(probs.get("age_over_30_prob", 0.0) or 0.0)
    return age_over_30_prob >= float(thresholds.age_min_tolerance_over30_prob)


def _effective_require_overweight_threshold(
    *,
    analysis: ImageAnalysisResult,
    rules: ImageFilterRules,
    thresholds: ImageEngineThresholds,
) -> float:
    base = max(0.0, float(thresholds.overweight_threshold) - float(thresholds.overweight_tolerance))
    if rules.gender != "male":
        return base
    if rules.min_age is None or int(rules.min_age) < 35:
        return base
    age = analysis.age
    if age is None:
        return base
    if int(age) < max(35, int(rules.min_age) - int(thresholds.age_min_tolerance_years)):
        return base
    probs = analysis.attribute_probs or {}
    age_over_30_prob = float(probs.get("age_over_30_prob", 0.0) or 0.0)
    if age_over_30_prob < 0.70:
        return base
    return min(base, float(thresholds.overweight_male35_threshold))


def _is_gender_match(
    *,
    analysis: ImageAnalysisResult,
    expected_gender: str,
    thresholds: ImageEngineThresholds,
) -> Tuple[bool, str]:
    probs = analysis.attribute_probs or {}
    raw_gender_prob = probs.get("gender_prob")
    if raw_gender_prob is not None:
        try:
            gender_prob = max(0.0, min(1.0, float(raw_gender_prob)))
            confidence_threshold = float(max(0.50, min(0.99, thresholds.gender_prob_threshold)))
            if expected_gender == "male":
                if gender_prob >= confidence_threshold:
                    return True, "ok"
                return False, "gender_mismatch"
            if expected_gender == "female":
                if (1.0 - gender_prob) >= confidence_threshold:
                    return True, "ok"
                return False, "gender_mismatch"
        except Exception:
            pass

    if not analysis.gender:
        return False, "gender_unknown"
    if analysis.gender != expected_gender:
        return False, "gender_mismatch"
    return True, "ok"


def evaluate_image_rules(
    analysis: ImageAnalysisResult,
    rules: ImageFilterRules,
    *,
    thresholds: ImageEngineThresholds | Mapping[str, Any] | None = None,
) -> Tuple[bool, str]:
    effective_thresholds = build_image_engine_thresholds(thresholds)

    if rules.has_constraints() and not bool(analysis.face_detected):
        return False, "no_face"

    if rules.gender:
        gender_ok, gender_reason = _is_gender_match(
            analysis=analysis,
            expected_gender=rules.gender,
            thresholds=effective_thresholds,
        )
        if not gender_ok:
            return False, gender_reason

    if rules.min_age is not None:
        if analysis.age is None:
            return False, "age_unknown"
        min_age = int(rules.min_age)
        age = int(analysis.age)
        if rules.min_age_strict:
            if age <= min_age:
                return False, "age_below_min"
        elif age < min_age:
            if not _passes_min_age_tolerance(
                age=age,
                min_age=min_age,
                analysis=analysis,
                thresholds=effective_thresholds,
            ):
                return False, "age_below_min"

    if rules.max_age is not None:
        if analysis.age is None:
            return False, "age_unknown"
        max_age = int(rules.max_age)
        age = int(analysis.age)
        if rules.max_age_strict:
            if age >= max_age:
                return False, "age_above_max"
        elif age > max_age:
            return False, "age_above_max"

    beard_prob = float(analysis.beard_prob or 0.0)
    if rules.require_beard and beard_prob < float(effective_thresholds.beard_threshold):
        return False, "no_beard"
    if rules.forbid_beard and beard_prob >= float(effective_thresholds.beard_threshold):
        return False, "beard_forbidden"

    require_overweight_threshold = _effective_require_overweight_threshold(
        analysis=analysis,
        rules=rules,
        thresholds=effective_thresholds,
    )
    forbid_overweight_threshold = min(
        1.0,
        float(effective_thresholds.overweight_threshold) + float(effective_thresholds.overweight_tolerance),
    )
    overweight_prob = float((analysis.attribute_probs or {}).get("overweight", 0.0))
    if rules.require_overweight and overweight_prob < require_overweight_threshold:
        return False, "not_overweight"
    if rules.forbid_overweight and overweight_prob >= forbid_overweight_threshold:
        return False, "overweight_forbidden"

    slim_prob = float((analysis.attribute_probs or {}).get("slim", 0.0))
    if rules.require_slim and slim_prob < float(effective_thresholds.slim_threshold):
        return False, "not_slim"
    if rules.forbid_slim and slim_prob >= float(effective_thresholds.slim_threshold):
        return False, "slim_forbidden"

    sharpness = float((analysis.attribute_probs or {}).get("sharpness", 0.0))
    if rules.require_sharp and sharpness < float(effective_thresholds.sharpness_threshold):
        return False, "not_sharp"
    if rules.forbid_sharp and sharpness >= float(effective_thresholds.sharpness_threshold):
        return False, "sharp_forbidden"

    return True, "image_match"
