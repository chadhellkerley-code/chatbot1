from __future__ import annotations

import threading
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple

import cv2  # type: ignore
import numpy as np  # type: ignore

from paths import runtime_root


FAIRFACE_GENDER_MODEL_URL = (
    "https://huggingface.co/onnx-community/fairface_gender_image_detection-ONNX/resolve/main/onnx/model.onnx"
)
FAIRFACE_AGE_MODEL_URL = (
    "https://huggingface.co/onnx-community/fairface_age_image_detection-ONNX/resolve/main/onnx/model.onnx"
)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_MODELS_DIR = runtime_root(_PROJECT_ROOT) / "models"
_FAIRFACE_GENDER_MODEL_PATH = _MODELS_DIR / "fairface_gender.onnx"
_FAIRFACE_AGE_MODEL_PATH = _MODELS_DIR / "fairface_age.onnx"
_DOWNLOAD_TIMEOUT_SECONDS = 240

_GENDER_SESSION: Any = None
_AGE_SESSION: Any = None
_SESSION_LOCK = threading.Lock()

_AGE_LABELS = (
    "0-2",
    "3-9",
    "10-19",
    "20-29",
    "30-39",
    "40-49",
    "50-59",
    "60-69",
    "more than 70",
)
_AGE_CENTERS = (1, 6, 15, 25, 35, 45, 55, 65, 75)


@dataclass(frozen=True)
class FairFacePrediction:
    gender_prob_male: float | None
    gender_label: str
    age_estimate: int | None
    age_bucket: str
    age_bucket_prob: float
    age_over_30_prob: float | None
    age_probs: Dict[str, float]


def _ensure_models_dir() -> Path:
    _MODELS_DIR.mkdir(parents=True, exist_ok=True)
    return _MODELS_DIR


def _download_file(url: str, target_path: Path) -> None:
    _ensure_models_dir()
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    with urllib.request.urlopen(url, timeout=_DOWNLOAD_TIMEOUT_SECONDS) as response:
        payload = response.read()
    if not payload:
        raise RuntimeError(f"Model download returned empty payload: {url}")
    temp_path.write_bytes(payload)
    temp_path.replace(target_path)


def ensure_fairface_model_files() -> Tuple[Path, Path]:
    _ensure_models_dir()
    if not _FAIRFACE_GENDER_MODEL_PATH.exists() or _FAIRFACE_GENDER_MODEL_PATH.stat().st_size <= 0:
        _download_file(FAIRFACE_GENDER_MODEL_URL, _FAIRFACE_GENDER_MODEL_PATH)
    if not _FAIRFACE_AGE_MODEL_PATH.exists() or _FAIRFACE_AGE_MODEL_PATH.stat().st_size <= 0:
        _download_file(FAIRFACE_AGE_MODEL_URL, _FAIRFACE_AGE_MODEL_PATH)
    if not _FAIRFACE_GENDER_MODEL_PATH.exists() or _FAIRFACE_GENDER_MODEL_PATH.stat().st_size <= 0:
        raise RuntimeError(f"FairFace gender model missing after download: {_FAIRFACE_GENDER_MODEL_PATH}")
    if not _FAIRFACE_AGE_MODEL_PATH.exists() or _FAIRFACE_AGE_MODEL_PATH.stat().st_size <= 0:
        raise RuntimeError(f"FairFace age model missing after download: {_FAIRFACE_AGE_MODEL_PATH}")
    return _FAIRFACE_GENDER_MODEL_PATH, _FAIRFACE_AGE_MODEL_PATH


def _get_sessions() -> Tuple[Any, Any]:
    global _GENDER_SESSION, _AGE_SESSION
    if _GENDER_SESSION is not None and _AGE_SESSION is not None:
        return _GENDER_SESSION, _AGE_SESSION

    with _SESSION_LOCK:
        if _GENDER_SESSION is not None and _AGE_SESSION is not None:
            return _GENDER_SESSION, _AGE_SESSION
        gender_path, age_path = ensure_fairface_model_files()
        try:
            import onnxruntime as ort  # type: ignore
        except Exception as exc:
            raise RuntimeError("onnxruntime is required for FairFace analysis.") from exc
        _GENDER_SESSION = ort.InferenceSession(str(gender_path), providers=["CPUExecutionProvider"])
        _AGE_SESSION = ort.InferenceSession(str(age_path), providers=["CPUExecutionProvider"])
        return _GENDER_SESSION, _AGE_SESSION


def warmup_fairface_models() -> bool:
    gender_session, age_session = _get_sessions()
    return gender_session is not None and age_session is not None


def is_fairface_loaded() -> bool:
    return _GENDER_SESSION is not None and _AGE_SESSION is not None


class FairFaceAnalyzer:
    def analyze(self, face_bgr: Any) -> FairFacePrediction:
        if face_bgr is None or not hasattr(face_bgr, "shape") or len(face_bgr.shape) < 2:
            return FairFacePrediction(
                gender_prob_male=None,
                gender_label="uncertain",
                age_estimate=None,
                age_bucket="unknown",
                age_bucket_prob=0.0,
                age_over_30_prob=None,
                age_probs={},
            )

        gender_session, age_session = _get_sessions()
        tensor = self._preprocess(face_bgr)

        gender_logits = np.asarray(
            gender_session.run(None, {gender_session.get_inputs()[0].name: tensor})[0]
        ).reshape(-1)
        age_logits = np.asarray(
            age_session.run(None, {age_session.get_inputs()[0].name: tensor})[0]
        ).reshape(-1)
        if gender_logits.size < 2 or age_logits.size < len(_AGE_LABELS):
            return FairFacePrediction(
                gender_prob_male=None,
                gender_label="uncertain",
                age_estimate=None,
                age_bucket="unknown",
                age_bucket_prob=0.0,
                age_over_30_prob=None,
                age_probs={},
            )

        gender_probs = self._softmax(gender_logits[:2])
        # FairFace gender model labels are [Female, Male].
        male_prob = float(gender_probs[1])
        gender_label = self._label_from_probability(male_prob)

        age_probs_vector = self._softmax(age_logits[: len(_AGE_LABELS)])
        age_map = {label: float(prob) for label, prob in zip(_AGE_LABELS, age_probs_vector)}
        age_idx = int(np.argmax(age_probs_vector))
        age_estimate = int(_AGE_CENTERS[age_idx])
        age_bucket = str(_AGE_LABELS[age_idx])
        age_bucket_prob = float(age_probs_vector[age_idx])
        age_over_30_prob = float(np.sum(age_probs_vector[4:]))

        return FairFacePrediction(
            gender_prob_male=male_prob,
            gender_label=gender_label,
            age_estimate=age_estimate,
            age_bucket=age_bucket,
            age_bucket_prob=age_bucket_prob,
            age_over_30_prob=age_over_30_prob,
            age_probs=age_map,
        )

    @staticmethod
    def _preprocess(face_bgr: np.ndarray) -> np.ndarray:
        resized = cv2.resize(face_bgr, (224, 224), interpolation=cv2.INTER_LINEAR)
        rgb = cv2.cvtColor(resized, cv2.COLOR_BGR2RGB).astype(np.float32) / 255.0
        normalized = (rgb - 0.5) / 0.5
        chw = np.transpose(normalized, (2, 0, 1))
        return np.expand_dims(chw, axis=0).astype(np.float32)

    @staticmethod
    def _softmax(values: np.ndarray) -> np.ndarray:
        shifted = values - float(np.max(values))
        exponent = np.exp(shifted)
        denominator = float(np.sum(exponent))
        if denominator <= 0.0:
            return np.ones_like(values, dtype=np.float32) / max(1, values.size)
        return exponent / denominator

    @staticmethod
    def _label_from_probability(probability: float) -> str:
        if probability > 0.70:
            return "male"
        if probability < 0.30:
            return "female"
        return "uncertain"
