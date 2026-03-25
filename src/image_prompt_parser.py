from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ImageFilterRules:
    gender: Optional[str] = None
    min_age: Optional[int] = None
    min_age_strict: bool = False
    max_age: Optional[int] = None
    max_age_strict: bool = False
    require_beard: bool = False
    forbid_beard: bool = False
    require_overweight: bool = False
    forbid_overweight: bool = False
    require_slim: bool = False
    forbid_slim: bool = False
    require_sharp: bool = False
    forbid_sharp: bool = False

    def has_constraints(self) -> bool:
        return any(
            (
                self.gender is not None,
                self.min_age is not None,
                self.max_age is not None,
                self.require_beard,
                self.forbid_beard,
                self.require_overweight,
                self.forbid_overweight,
                self.require_slim,
                self.forbid_slim,
                self.require_sharp,
                self.forbid_sharp,
            )
        )


_GENDER_KEYWORDS = {
    "male": ("hombre", "masculino", "male", "man", "varon"),
    "female": ("mujer", "femenino", "female", "woman"),
}

_BEARD_KEYWORDS = ("barba", "beard", "bigote", "mustache", "moustache")
_OVERWEIGHT_KEYWORDS = ("sobrepeso", "obeso", "obesidad", "overweight", "chubby")
_SLIM_KEYWORDS = ("delgado", "delgada", "flaco", "flaca", "slim", "lean")
_SHARP_KEYWORDS = (
    "nitido",
    "nitida",
    "enfocado",
    "enfocada",
    "claro",
    "clara",
    "alta calidad",
    "sharp",
    "in focus",
)
_BLUR_KEYWORDS = ("borroso", "borrosa", "desenfocado", "desenfocada", "blurry", "blur")
_NO_BEARD_KEYWORDS = ("afeitado", "lampino", "clean shaven", "clean-shaven")
_NO_OVERWEIGHT_KEYWORDS = ("peso normal", "normal weight")


def parse_image_prompt(prompt: str) -> ImageFilterRules:
    normalized = _normalize_text(prompt)
    if not normalized:
        return ImageFilterRules()

    gender = _detect_gender(normalized)
    min_age, min_age_strict = _extract_min_age(normalized)
    max_age, max_age_strict = _extract_max_age(normalized)
    require_beard = _has_affirmative_term(normalized, _BEARD_KEYWORDS)
    forbid_beard = _has_negated_term(normalized, _BEARD_KEYWORDS) or any(
        _contains_term(normalized, term) for term in _NO_BEARD_KEYWORDS
    )
    require_overweight = _has_affirmative_term(normalized, _OVERWEIGHT_KEYWORDS)
    forbid_overweight = _has_negated_term(normalized, _OVERWEIGHT_KEYWORDS) or any(
        _contains_term(normalized, term) for term in _NO_OVERWEIGHT_KEYWORDS
    )
    require_slim = _has_affirmative_term(normalized, _SLIM_KEYWORDS)
    forbid_slim = _has_negated_term(normalized, _SLIM_KEYWORDS)
    require_sharp = (
        _has_affirmative_term(normalized, _SHARP_KEYWORDS)
        or _has_negated_term(normalized, _BLUR_KEYWORDS)
    )
    forbid_sharp = _has_affirmative_term(normalized, _BLUR_KEYWORDS)

    # Keep the parser resilient to natural-language exclusion examples such as
    # "no califican menores de 35", which can accidentally produce contradictions.
    if min_age is not None and max_age is not None and int(max_age) <= int(min_age):
        max_age = None
        max_age_strict = False

    if require_beard and forbid_beard:
        forbid_beard = False
    if require_overweight and forbid_overweight:
        forbid_overweight = False
    if require_slim and forbid_slim:
        forbid_slim = False
    if require_sharp and forbid_sharp:
        forbid_sharp = False

    return ImageFilterRules(
        gender=gender,
        min_age=min_age,
        min_age_strict=min_age_strict,
        max_age=max_age,
        max_age_strict=max_age_strict,
        require_beard=require_beard,
        forbid_beard=forbid_beard,
        require_overweight=require_overweight,
        forbid_overweight=forbid_overweight,
        require_slim=require_slim,
        forbid_slim=forbid_slim,
        require_sharp=require_sharp,
        forbid_sharp=forbid_sharp,
    )


def _normalize_text(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9+\s_-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _contains_term(haystack: str, term: str) -> bool:
    token = _normalize_text(term)
    if not token:
        return False
    if " " in token:
        return token in haystack
    return bool(re.search(rf"\b{re.escape(token)}\b", haystack))


def _contains_negated_term(haystack: str, term: str) -> bool:
    token = _normalize_text(term)
    if not token:
        return False
    patterns = (
        rf"\bsin\s+{re.escape(token)}\b",
        rf"\bno\s+{re.escape(token)}\b",
        rf"\bwithout\s+{re.escape(token)}\b",
    )
    return any(bool(re.search(pattern, haystack)) for pattern in patterns)


def _has_affirmative_term(haystack: str, terms: tuple[str, ...]) -> bool:
    for term in terms:
        if _contains_term(haystack, term) and not _contains_negated_term(haystack, term):
            return True
    return False


def _has_negated_term(haystack: str, terms: tuple[str, ...]) -> bool:
    return any(_contains_negated_term(haystack, term) for term in terms)


def _detect_gender(normalized_prompt: str) -> Optional[str]:
    male_positive = _has_affirmative_term(normalized_prompt, _GENDER_KEYWORDS["male"])
    female_positive = _has_affirmative_term(normalized_prompt, _GENDER_KEYWORDS["female"])
    male_negative = _has_negated_term(normalized_prompt, _GENDER_KEYWORDS["male"])
    female_negative = _has_negated_term(normalized_prompt, _GENDER_KEYWORDS["female"])

    if male_positive and (female_negative or not female_positive):
        return "male"
    if female_positive and (male_negative or not male_positive):
        return "female"
    return None


def _extract_min_age(normalized_prompt: str) -> tuple[Optional[int], bool]:
    patterns = (
        (r"(?:mayor\s+de|mayor\s+a|mas\s+de)\s*(\d{1,2})\b", True),
        (r"(?:edad\s+minima\s+de)\s*(\d{1,2})\b", False),
        (r"\b(\d{1,2})\s*\+", False),
    )
    for pattern, strict in patterns:
        match = re.search(pattern, normalized_prompt)
        if match:
            age = _parse_age(match.group(1))
            if age is not None:
                return age, strict
    return None, False


def _extract_max_age(normalized_prompt: str) -> tuple[Optional[int], bool]:
    patterns = (
        (r"(?:menor\s+de|menor\s+a|menos\s+de)\s*(\d{1,2})\b", True),
        (r"(?:edad\s+maxima\s+de)\s*(\d{1,2})\b", False),
        (r"\b(?:hasta|max\s*)\s*(\d{1,2})\b", False),
    )
    for pattern, strict in patterns:
        match = re.search(pattern, normalized_prompt)
        if match:
            age = _parse_age(match.group(1))
            if age is not None:
                return age, strict
    return None, False


def _parse_age(raw: str) -> Optional[int]:
    try:
        age = int(str(raw).strip())
    except Exception:
        return None
    if age <= 0 or age > 100:
        return None
    return age
