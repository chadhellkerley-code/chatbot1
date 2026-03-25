from __future__ import annotations

from dataclasses import dataclass
import threading
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple

import cv2  # type: ignore
import numpy as np  # type: ignore

from paths import runtime_root


SCRFD_MODEL_URL = "https://huggingface.co/LPDoctor/insightface/resolve/main/scrfd_10g_bnkps.onnx"
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_MODELS_DIR = runtime_root(_PROJECT_ROOT) / "models"
_SCRFD_MODEL_PATH = _MODELS_DIR / "scrfd_10g_bnkps.onnx"
_DOWNLOAD_TIMEOUT_SECONDS = 180

_SCRFD_SESSION: Any = None
_SCRFD_SESSION_LOCK = threading.Lock()


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


def ensure_scrfd_model_file() -> Path:
    _ensure_models_dir()
    if _SCRFD_MODEL_PATH.exists() and _SCRFD_MODEL_PATH.stat().st_size > 0:
        return _SCRFD_MODEL_PATH
    _download_file(SCRFD_MODEL_URL, _SCRFD_MODEL_PATH)
    if not _SCRFD_MODEL_PATH.exists() or _SCRFD_MODEL_PATH.stat().st_size <= 0:
        raise RuntimeError(f"SCRFD model file missing after download: {_SCRFD_MODEL_PATH}")
    return _SCRFD_MODEL_PATH


def get_scrfd_session() -> Any:
    global _SCRFD_SESSION
    if _SCRFD_SESSION is not None:
        return _SCRFD_SESSION
    with _SCRFD_SESSION_LOCK:
        if _SCRFD_SESSION is not None:
            return _SCRFD_SESSION
        model_path = ensure_scrfd_model_file()
        try:
            import onnxruntime as ort  # type: ignore
        except Exception as exc:
            raise RuntimeError("onnxruntime is required for SCRFD image detection.") from exc
        _SCRFD_SESSION = ort.InferenceSession(str(model_path), providers=["CPUExecutionProvider"])
        return _SCRFD_SESSION


def warmup_scrfd_model() -> bool:
    return get_scrfd_session() is not None


def is_scrfd_loaded() -> bool:
    return _SCRFD_SESSION is not None


@dataclass(frozen=True)
class FaceDetection:
    left: int
    top: int
    right: int
    bottom: int
    score: float
    keypoints: Tuple[Tuple[float, float], ...] = ()


class SCRFDFaceDetector:
    def __init__(
        self,
        *,
        input_size: Tuple[int, int] = (640, 640),
        score_threshold: float = 0.50,
        nms_threshold: float = 0.40,
    ) -> None:
        self.input_size = (int(input_size[0]), int(input_size[1]))
        self.score_threshold = float(score_threshold)
        self.nms_threshold = float(nms_threshold)
        self._anchor_cache: Dict[Tuple[int, int, int, int], np.ndarray] = {}

    def detect(self, frame: Any) -> List[Tuple[int, int, int, int, float]]:
        detailed = self.detect_detailed(frame)
        return [
            (det.left, det.top, det.right, det.bottom, det.score)
            for det in detailed
        ]

    def detect_detailed(self, frame: Any) -> List[FaceDetection]:
        if frame is None:
            return []
        if not hasattr(frame, "shape") or len(frame.shape) < 2:
            return []

        original_height, original_width = int(frame.shape[0]), int(frame.shape[1])
        if original_height <= 0 or original_width <= 0:
            return []

        session = get_scrfd_session()
        input_name = session.get_inputs()[0].name
        input_tensor, resize_scale = self._prepare_input(frame)
        outputs = session.run(None, {input_name: input_tensor})
        detections = self._decode_outputs(
            outputs,
            input_width=self.input_size[0],
            input_height=self.input_size[1],
            scale=resize_scale,
            original_width=original_width,
            original_height=original_height,
        )
        if not detections:
            return []
        boxes = np.asarray(
            [[det.left, det.top, det.right, det.bottom] for det in detections],
            dtype=np.float32,
        )
        scores = np.asarray([det.score for det in detections], dtype=np.float32)
        keep = self._nms(boxes, scores, self.nms_threshold)
        final_rows = [detections[idx] for idx in keep]
        final_rows.sort(key=lambda item: item.score, reverse=True)
        return final_rows

    def _prepare_input(self, frame: np.ndarray) -> Tuple[np.ndarray, float]:
        input_width, input_height = self.input_size
        src_height, src_width = int(frame.shape[0]), int(frame.shape[1])
        scale = min(float(input_width) / float(src_width), float(input_height) / float(src_height))
        resized_width = max(1, int(round(src_width * scale)))
        resized_height = max(1, int(round(src_height * scale)))

        resized = cv2.resize(frame, (resized_width, resized_height), interpolation=cv2.INTER_LINEAR)
        canvas = np.zeros((input_height, input_width, 3), dtype=np.uint8)
        canvas[:resized_height, :resized_width] = resized

        rgb = cv2.cvtColor(canvas, cv2.COLOR_BGR2RGB).astype(np.float32)
        normalized = (rgb - 127.5) / 128.0
        nchw = np.transpose(normalized, (2, 0, 1))
        batch = np.expand_dims(nchw, axis=0).astype(np.float32)
        return batch, scale

    def _decode_outputs(
        self,
        outputs: List[np.ndarray],
        *,
        input_width: int,
        input_height: int,
        scale: float,
        original_width: int,
        original_height: int,
    ) -> List[FaceDetection]:
        if len(outputs) < 6:
            return []

        feature_levels = len(outputs) // 3
        if feature_levels <= 0:
            return []

        score_outputs = outputs[:feature_levels]
        bbox_outputs = outputs[feature_levels : feature_levels * 2]
        kps_outputs = outputs[feature_levels * 2 : feature_levels * 3] if len(outputs) >= feature_levels * 3 else []
        strides = self._resolve_strides(feature_levels)

        rows: List[FaceDetection] = []
        for level_idx, stride in enumerate(strides):
            scores = np.asarray(score_outputs[level_idx]).reshape(-1)
            bbox_preds = np.asarray(bbox_outputs[level_idx]).reshape(-1, 4)
            if scores.size <= 0 or bbox_preds.shape[0] <= 0:
                continue
            kps_preds = None
            if level_idx < len(kps_outputs):
                kps_preds = np.asarray(kps_outputs[level_idx]).reshape(-1, 10)

            feat_height = max(1, int(round(float(input_height) / float(stride))))
            feat_width = max(1, int(round(float(input_width) / float(stride))))
            locations = max(1, feat_height * feat_width)
            anchors_per_location = max(1, int(round(float(bbox_preds.shape[0]) / float(locations))))
            centers = self._anchor_centers(feat_height, feat_width, stride, anchors_per_location)
            if centers.shape[0] != bbox_preds.shape[0]:
                continue
            if kps_preds is not None and kps_preds.shape[0] != centers.shape[0]:
                kps_preds = None

            positive = np.where(scores >= self.score_threshold)[0]
            if positive.size <= 0:
                continue

            selected_centers = centers[positive]
            selected_bbox = bbox_preds[positive] * float(stride)
            decoded = self._distance2bbox(selected_centers, selected_bbox)
            decoded[:, [0, 2]] = decoded[:, [0, 2]] / max(1e-6, float(scale))
            decoded[:, [1, 3]] = decoded[:, [1, 3]] / max(1e-6, float(scale))
            decoded[:, 0] = np.clip(decoded[:, 0], 0.0, float(original_width - 1))
            decoded[:, 1] = np.clip(decoded[:, 1], 0.0, float(original_height - 1))
            decoded[:, 2] = np.clip(decoded[:, 2], 0.0, float(original_width - 1))
            decoded[:, 3] = np.clip(decoded[:, 3], 0.0, float(original_height - 1))
            decoded_kps = None
            if kps_preds is not None:
                selected_kps = kps_preds[positive] * float(stride)
                decoded_kps = self._distance2kps(selected_centers, selected_kps)
                decoded_kps[:, 0::2] = decoded_kps[:, 0::2] / max(1e-6, float(scale))
                decoded_kps[:, 1::2] = decoded_kps[:, 1::2] / max(1e-6, float(scale))
                decoded_kps[:, 0::2] = np.clip(decoded_kps[:, 0::2], 0.0, float(original_width - 1))
                decoded_kps[:, 1::2] = np.clip(decoded_kps[:, 1::2], 0.0, float(original_height - 1))

            for idx, (box, score) in enumerate(zip(decoded, scores[positive])):
                left = int(round(float(min(box[0], box[2]))))
                top = int(round(float(min(box[1], box[3]))))
                right = int(round(float(max(box[0], box[2]))))
                bottom = int(round(float(max(box[1], box[3]))))
                if right - left < 8 or bottom - top < 8:
                    continue
                keypoints: Tuple[Tuple[float, float], ...] = ()
                if decoded_kps is not None and idx < decoded_kps.shape[0]:
                    row = decoded_kps[idx].reshape(-1, 2)
                    keypoints = tuple((float(px), float(py)) for px, py in row.tolist())
                rows.append(
                    FaceDetection(
                        left=left,
                        top=top,
                        right=right,
                        bottom=bottom,
                        score=float(score),
                        keypoints=keypoints,
                    )
                )
        return rows

    def _anchor_centers(
        self,
        feat_height: int,
        feat_width: int,
        stride: int,
        num_anchors: int,
    ) -> np.ndarray:
        cache_key = (feat_height, feat_width, stride, num_anchors)
        cached = self._anchor_cache.get(cache_key)
        if cached is not None:
            return cached
        centers = np.stack(np.mgrid[:feat_height, :feat_width][::-1], axis=-1).astype(np.float32)
        centers = (centers * float(stride)).reshape((-1, 2))
        if num_anchors > 1:
            centers = np.repeat(centers, repeats=num_anchors, axis=0)
        self._anchor_cache[cache_key] = centers
        return centers

    @staticmethod
    def _distance2bbox(points: np.ndarray, distances: np.ndarray) -> np.ndarray:
        x1 = points[:, 0] - distances[:, 0]
        y1 = points[:, 1] - distances[:, 1]
        x2 = points[:, 0] + distances[:, 2]
        y2 = points[:, 1] + distances[:, 3]
        return np.stack([x1, y1, x2, y2], axis=1)

    @staticmethod
    def _distance2kps(points: np.ndarray, distances: np.ndarray) -> np.ndarray:
        result = np.zeros_like(distances, dtype=np.float32)
        result[:, 0::2] = points[:, 0:1] + distances[:, 0::2]
        result[:, 1::2] = points[:, 1:2] + distances[:, 1::2]
        return result

    @staticmethod
    def _nms(boxes: np.ndarray, scores: np.ndarray, threshold: float) -> List[int]:
        if boxes.size <= 0 or scores.size <= 0:
            return []
        x1 = boxes[:, 0]
        y1 = boxes[:, 1]
        x2 = boxes[:, 2]
        y2 = boxes[:, 3]
        areas = (x2 - x1 + 1.0) * (y2 - y1 + 1.0)
        order = scores.argsort()[::-1]
        keep: List[int] = []

        while order.size > 0:
            idx = int(order[0])
            keep.append(idx)
            if order.size == 1:
                break
            xx1 = np.maximum(x1[idx], x1[order[1:]])
            yy1 = np.maximum(y1[idx], y1[order[1:]])
            xx2 = np.minimum(x2[idx], x2[order[1:]])
            yy2 = np.minimum(y2[idx], y2[order[1:]])

            width = np.maximum(0.0, xx2 - xx1 + 1.0)
            height = np.maximum(0.0, yy2 - yy1 + 1.0)
            intersection = width * height
            union = areas[idx] + areas[order[1:]] - intersection
            overlap = np.zeros_like(intersection, dtype=np.float32)
            valid = union > 0.0
            overlap[valid] = intersection[valid] / union[valid]
            remaining = np.where(overlap <= float(threshold))[0]
            order = order[remaining + 1]

        return keep

    @staticmethod
    def _resolve_strides(level_count: int) -> List[int]:
        if level_count == 3:
            return [8, 16, 32]
        if level_count == 5:
            return [8, 16, 32, 64, 128]
        strides: List[int] = []
        value = 8
        for _ in range(level_count):
            strides.append(value)
            value *= 2
        return strides
