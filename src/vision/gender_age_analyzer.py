from __future__ import annotations

import threading
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Tuple

import cv2  # type: ignore
import numpy as np  # type: ignore

from paths import runtime_root


GENDERAGE_MODEL_URL = "https://huggingface.co/LPDoctor/insightface/resolve/main/genderage.onnx"
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_MODELS_DIR = runtime_root(_PROJECT_ROOT) / "models"
_GENDERAGE_MODEL_PATH = _MODELS_DIR / "genderage.onnx"
_DOWNLOAD_TIMEOUT_SECONDS = 180

_GENDERAGE_SESSION: Any = None
_GENDERAGE_SESSION_LOCK = threading.Lock()


@dataclass(frozen=True)
class GenderAgePrediction:
    gender_prob: float | None
    age_estimate: int | None
    gender_label: str


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


def ensure_genderage_model_file() -> Path:
    _ensure_models_dir()
    if _GENDERAGE_MODEL_PATH.exists() and _GENDERAGE_MODEL_PATH.stat().st_size > 0:
        return _GENDERAGE_MODEL_PATH
    _download_file(GENDERAGE_MODEL_URL, _GENDERAGE_MODEL_PATH)
    if not _GENDERAGE_MODEL_PATH.exists() or _GENDERAGE_MODEL_PATH.stat().st_size <= 0:
        raise RuntimeError(f"GenderAge model file missing after download: {_GENDERAGE_MODEL_PATH}")
    return _GENDERAGE_MODEL_PATH


def get_genderage_session() -> Any:
    global _GENDERAGE_SESSION
    if _GENDERAGE_SESSION is not None:
        return _GENDERAGE_SESSION
    with _GENDERAGE_SESSION_LOCK:
        if _GENDERAGE_SESSION is not None:
            return _GENDERAGE_SESSION
        model_path = ensure_genderage_model_file()
        try:
            import onnxruntime as ort  # type: ignore
        except Exception as exc:
            raise RuntimeError("onnxruntime is required for gender/age image analysis.") from exc
        _GENDERAGE_SESSION = ort.InferenceSession(str(model_path), providers=["CPUExecutionProvider"])
        return _GENDERAGE_SESSION


def warmup_genderage_model() -> bool:
    return get_genderage_session() is not None


def is_genderage_loaded() -> bool:
    return _GENDERAGE_SESSION is not None


class GenderAgeAnalyzer:
    def analyze(self, face_bgr: Any) -> GenderAgePrediction:
        if face_bgr is None:
            return GenderAgePrediction(gender_prob=None, age_estimate=None, gender_label="uncertain")
        if not hasattr(face_bgr, "shape") or len(face_bgr.shape) < 2:
            return GenderAgePrediction(gender_prob=None, age_estimate=None, gender_label="uncertain")

        session = get_genderage_session()
        input_meta = session.get_inputs()[0]
        input_name = input_meta.name
        input_height, input_width = self._resolve_input_size(input_meta.shape)
        tensor = self._preprocess(face_bgr, input_height=input_height, input_width=input_width)

        outputs = session.run(None, {input_name: tensor})
        if not outputs:
            return GenderAgePrediction(gender_prob=None, age_estimate=None, gender_label="uncertain")

        vector = np.asarray(outputs[0]).reshape(-1).astype(np.float32)
        if vector.size < 3:
            return GenderAgePrediction(gender_prob=None, age_estimate=None, gender_label="uncertain")

        gender_logits = vector[:2]
        gender_probs = self._softmax(gender_logits)
        male_prob = float(gender_probs[0])
        age_estimate = int(round(max(0.0, min(100.0, float(vector[2]) * 100.0))))
        gender_label = self._label_from_probability(male_prob)
        return GenderAgePrediction(
            gender_prob=male_prob,
            age_estimate=age_estimate,
            gender_label=gender_label,
        )

    @staticmethod
    def _resolve_input_size(shape: Any) -> Tuple[int, int]:
        if isinstance(shape, (list, tuple)) and len(shape) >= 4:
            raw_h = shape[2]
            raw_w = shape[3]
            if isinstance(raw_h, int) and raw_h > 0 and isinstance(raw_w, int) and raw_w > 0:
                return int(raw_h), int(raw_w)
        return 112, 112

    @staticmethod
    def _preprocess(face_bgr: np.ndarray, *, input_height: int, input_width: int) -> np.ndarray:
        resized_112 = cv2.resize(face_bgr, (112, 112), interpolation=cv2.INTER_LINEAR)
        rgb = cv2.cvtColor(resized_112, cv2.COLOR_BGR2RGB).astype(np.float32)
        normalized = (rgb - 127.5) / 128.0
        nchw = np.transpose(normalized, (2, 0, 1))
        batch = np.expand_dims(nchw, axis=0).astype(np.float32)

        if input_height == 112 and input_width == 112:
            return batch

        # Some public genderage exports are fixed to 96x96. Keep the required
        # 112x112 preprocessing first, then adapt to the model's expected size.
        adapted = np.transpose(batch[0], (1, 2, 0))
        adapted = cv2.resize(adapted, (input_width, input_height), interpolation=cv2.INTER_LINEAR)
        adapted = np.transpose(adapted, (2, 0, 1))
        return np.expand_dims(adapted, axis=0).astype(np.float32)

    @staticmethod
    def _softmax(values: np.ndarray) -> np.ndarray:
        shifted = values - float(np.max(values))
        exponent = np.exp(shifted)
        denominator = float(np.sum(exponent))
        if denominator <= 0.0:
            return np.array([0.5, 0.5], dtype=np.float32)
        return exponent / denominator

    @staticmethod
    def _label_from_probability(probability: float) -> str:
        if probability > 0.70:
            return "male"
        if probability < 0.30:
            return "female"
        return "uncertain"
