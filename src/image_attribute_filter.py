from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import Any, Dict, Optional, Tuple

import cv2  # type: ignore
import numpy as np  # type: ignore

from src.vision.face_detector_scrfd import FaceDetection, SCRFDFaceDetector, warmup_scrfd_model
from src.vision.fairface_analyzer import FairFaceAnalyzer, FairFacePrediction, warmup_fairface_models
from src.vision.gender_age_analyzer import GenderAgeAnalyzer, warmup_genderage_model

_WARMUP_LOCK = threading.Lock()
_WARMUP_CACHE: Optional[Dict[str, bool]] = None


@dataclass
class ImageAnalysisResult:
    face_detected: bool
    age: Optional[int]
    gender: Optional[str]
    beard_prob: float
    attribute_probs: Dict[str, float] = field(default_factory=dict)


class ImageAttributeFilter:
    def __init__(
        self,
        *,
        face_detector: Optional[SCRFDFaceDetector] = None,
        fairface_analyzer: Optional[FairFaceAnalyzer] = None,
        gender_age_analyzer: Optional[GenderAgeAnalyzer] = None,
    ) -> None:
        self._face_detector = face_detector or SCRFDFaceDetector()
        self._face_detector_low_conf = SCRFDFaceDetector(score_threshold=0.35, nms_threshold=0.45)
        self._fairface_analyzer = fairface_analyzer or FairFaceAnalyzer()
        self._gender_age_analyzer = gender_age_analyzer or GenderAgeAnalyzer()

    @staticmethod
    def warmup_models() -> Dict[str, bool]:
        global _WARMUP_CACHE
        with _WARMUP_LOCK:
            if isinstance(_WARMUP_CACHE, dict):
                return dict(_WARMUP_CACHE)
            try:
                scrfd_loaded = bool(warmup_scrfd_model())
            except Exception:
                scrfd_loaded = False
            try:
                fairface_loaded = bool(warmup_fairface_models())
            except Exception:
                fairface_loaded = False
            try:
                genderage_loaded = bool(warmup_genderage_model())
            except Exception:
                genderage_loaded = False
            _WARMUP_CACHE = {
                "scrfd_loaded": scrfd_loaded,
                "fairface_loaded": fairface_loaded,
                "genderage_loaded": genderage_loaded,
            }
            return dict(_WARMUP_CACHE)

    def analyze(self, image_bytes: bytes) -> ImageAnalysisResult:
        frame = self._decode_image(image_bytes)
        if frame is None:
            return ImageAnalysisResult(
                face_detected=False,
                age=None,
                gender=None,
                beard_prob=0.0,
                attribute_probs={},
            )

        detection_frame, detections = self._detect_faces_with_fallbacks(frame)
        if not detections:
            return ImageAnalysisResult(
                face_detected=False,
                age=None,
                gender=None,
                beard_prob=0.0,
                attribute_probs={},
            )

        best_detection = self._select_primary_detection(detections)
        face_crop = self._extract_face_patch(detection_frame, best_detection)
        if face_crop is None:
            return ImageAnalysisResult(
                face_detected=False,
                age=None,
                gender=None,
                beard_prob=0.0,
                attribute_probs={},
            )

        face_context_crop = self._crop_face_with_padding(
            detection_frame,
            (best_detection.left, best_detection.top, best_detection.right, best_detection.bottom),
            padding_ratio=0.38,
        )
        if face_context_crop is None:
            face_context_crop = face_crop

        try:
            fairface_prediction = self._fairface_analyzer.analyze(face_crop)
        except Exception:
            fairface_prediction = FairFacePrediction(
                gender_prob_male=None,
                gender_label="uncertain",
                age_estimate=None,
                age_bucket="unknown",
                age_bucket_prob=0.0,
                age_over_30_prob=None,
                age_probs={},
            )
        genderage_prediction = self._gender_age_analyzer.analyze(face_crop)
        beard_prob = self._estimate_beard_probability(face_crop)
        face_box_ratio = self._safe_face_box_ratio(best_detection)
        overweight_core = self._estimate_overweight_probability(face_crop, face_box_ratio=face_box_ratio)
        overweight_context = self._estimate_overweight_probability(face_context_crop, face_box_ratio=face_box_ratio)
        overweight_prob = float(max(overweight_core, overweight_context))
        sharpness = self._estimate_sharpness_score(face_context_crop)

        gender_prob = self._blend_gender_probability(
            fairface_prediction.gender_prob_male,
            genderage_prediction.gender_prob,
        )
        gender: Optional[str] = self._label_from_probability(gender_prob)
        age = self._blend_age_estimate(
            fairface_prediction.age_estimate,
            genderage_prediction.age_estimate,
        )

        attribute_probs: Dict[str, float] = {}
        if gender_prob is not None:
            attribute_probs["gender_prob"] = gender_prob
        attribute_probs["age_over_30_prob"] = float(fairface_prediction.age_over_30_prob or 0.0)
        attribute_probs["age_bucket_prob"] = float(fairface_prediction.age_bucket_prob or 0.0)
        attribute_probs["beard_prob"] = float(beard_prob)
        attribute_probs["overweight_core"] = float(overweight_core)
        attribute_probs["overweight_context"] = float(overweight_context)
        attribute_probs["overweight"] = float(overweight_prob)
        attribute_probs["slim"] = float(max(0.0, min(1.0, 1.0 - overweight_prob)))
        attribute_probs["sharpness"] = float(sharpness)

        return ImageAnalysisResult(
            face_detected=True,
            age=age,
            gender=gender,
            beard_prob=float(beard_prob),
            attribute_probs=attribute_probs,
        )

    def _detect_faces_with_fallbacks(self, frame: Any) -> Tuple[Any, list[FaceDetection]]:
        if frame is None or not hasattr(frame, "shape"):
            return frame, []

        for detector in (self._face_detector, self._face_detector_low_conf):
            detections = detector.detect_detailed(frame)
            if detections:
                return frame, detections

        enhanced = self._enhance_for_face_detection(frame)
        if enhanced is not None:
            for detector in (self._face_detector, self._face_detector_low_conf):
                detections = detector.detect_detailed(enhanced)
                if detections:
                    return enhanced, detections

        centered = self._center_crop(frame, ratio=0.86)
        if centered is not None:
            for detector in (self._face_detector, self._face_detector_low_conf):
                detections = detector.detect_detailed(centered)
                if detections:
                    return centered, detections
            centered_enhanced = self._enhance_for_face_detection(centered)
            if centered_enhanced is not None:
                for detector in (self._face_detector, self._face_detector_low_conf):
                    detections = detector.detect_detailed(centered_enhanced)
                    if detections:
                        return centered_enhanced, detections

        return frame, []

    @staticmethod
    def _enhance_for_face_detection(frame: Any) -> Any:
        if frame is None or not hasattr(frame, "shape") or len(frame.shape) < 2:
            return None
        height = int(frame.shape[0]) if len(frame.shape) > 0 else 0
        width = int(frame.shape[1]) if len(frame.shape) > 1 else 0
        if height <= 1 or width <= 1:
            return None

        target_min_side = 320.0
        min_side = float(max(1, min(height, width)))
        scale = min(2.6, max(1.0, target_min_side / min_side))
        if scale > 1.01:
            resized = cv2.resize(
                frame,
                (max(1, int(round(width * scale))), max(1, int(round(height * scale)))),
                interpolation=cv2.INTER_CUBIC,
            )
        else:
            resized = frame.copy()

        ycrcb = cv2.cvtColor(resized, cv2.COLOR_BGR2YCrCb)
        luma = ycrcb[:, :, 0]
        clahe = cv2.createCLAHE(clipLimit=2.2, tileGridSize=(8, 8))
        ycrcb[:, :, 0] = clahe.apply(luma)
        contrast = cv2.cvtColor(ycrcb, cv2.COLOR_YCrCb2BGR)
        blur = cv2.GaussianBlur(contrast, (0, 0), sigmaX=1.2, sigmaY=1.2)
        sharpened = cv2.addWeighted(contrast, 1.5, blur, -0.5, 0)
        return sharpened

    @staticmethod
    def _center_crop(frame: Any, *, ratio: float) -> Any:
        if frame is None or not hasattr(frame, "shape") or len(frame.shape) < 2:
            return None
        height = int(frame.shape[0]) if len(frame.shape) > 0 else 0
        width = int(frame.shape[1]) if len(frame.shape) > 1 else 0
        if height <= 1 or width <= 1:
            return None
        safe_ratio = float(max(0.35, min(1.0, ratio)))
        crop_h = max(2, int(round(height * safe_ratio)))
        crop_w = max(2, int(round(width * safe_ratio)))
        top = max(0, (height - crop_h) // 2)
        left = max(0, (width - crop_w) // 2)
        bottom = min(height, top + crop_h)
        right = min(width, left + crop_w)
        crop = frame[top:bottom, left:right]
        if crop is None or not hasattr(crop, "size") or int(crop.size) <= 0:
            return None
        return crop

    @staticmethod
    def _select_primary_detection(detections: list[FaceDetection]) -> FaceDetection:
        if not detections:
            raise ValueError("detections must not be empty")
        if len(detections) == 1:
            return detections[0]

        def _rank(det: FaceDetection) -> float:
            width = max(1.0, float(det.right - det.left))
            height = max(1.0, float(det.bottom - det.top))
            area = width * height
            score = max(0.0, float(det.score))
            return area * (0.40 + score)

        return max(detections, key=_rank)

    @staticmethod
    def _blend_gender_probability(primary: Optional[float], secondary: Optional[float]) -> Optional[float]:
        values = []
        weights = []
        if primary is not None:
            values.append(float(primary))
            weights.append(0.80)
        if secondary is not None:
            values.append(float(secondary))
            weights.append(0.20)
        if not values or not weights:
            return None
        weighted = sum(value * weight for value, weight in zip(values, weights))
        total_weight = sum(weights)
        if total_weight <= 0.0:
            return None
        return float(max(0.0, min(1.0, weighted / total_weight)))

    @staticmethod
    def _blend_age_estimate(primary: Optional[int], secondary: Optional[int]) -> Optional[int]:
        if primary is None and secondary is None:
            return None
        if primary is None:
            return int(max(0, min(100, int(secondary or 0))))
        if secondary is None:
            return int(max(0, min(100, int(primary or 0))))
        blended = (0.75 * float(primary)) + (0.25 * float(secondary))
        return int(max(0, min(100, round(blended))))

    @staticmethod
    def _label_from_probability(probability: Optional[float]) -> Optional[str]:
        if probability is None:
            return None
        prob = float(probability)
        if prob > 0.70:
            return "male"
        if prob < 0.30:
            return "female"
        return None

    @staticmethod
    def _decode_image(image_bytes: bytes) -> Any:
        payload = bytes(image_bytes or b"")
        if not payload:
            return None
        array = np.frombuffer(payload, dtype=np.uint8)
        if array.size <= 0:
            return None
        frame = cv2.imdecode(array, cv2.IMREAD_COLOR)
        if frame is None:
            return None
        return frame

    def _extract_face_patch(self, frame: Any, detection: FaceDetection) -> Any:
        aligned = None
        if detection.keypoints:
            aligned = self._align_face(frame, detection.keypoints, output_size=112)
        if aligned is not None:
            return aligned
        return self._crop_face_with_padding(
            frame,
            (detection.left, detection.top, detection.right, detection.bottom),
            padding_ratio=0.20,
        )

    @staticmethod
    def _crop_face_with_padding(
        frame: Any,
        box: Tuple[int, int, int, int],
        *,
        padding_ratio: float,
    ) -> Any:
        if frame is None or not hasattr(frame, "shape"):
            return None
        frame_height = int(frame.shape[0]) if len(frame.shape) > 0 else 0
        frame_width = int(frame.shape[1]) if len(frame.shape) > 1 else 0
        if frame_height <= 0 or frame_width <= 0:
            return None

        left, top, right, bottom = [int(value) for value in box]
        width = right - left
        height = bottom - top
        if width <= 1 or height <= 1:
            return None

        pad_x = int(round(width * float(padding_ratio)))
        pad_y = int(round(height * float(padding_ratio)))

        padded_left = max(0, left - pad_x)
        padded_top = max(0, top - pad_y)
        padded_right = min(frame_width, right + pad_x)
        padded_bottom = min(frame_height, bottom + pad_y)

        if padded_right - padded_left <= 1 or padded_bottom - padded_top <= 1:
            return None

        crop = frame[padded_top:padded_bottom, padded_left:padded_right]
        if crop is None or not hasattr(crop, "size") or int(crop.size) <= 0:
            return None
        return crop

    @staticmethod
    def _safe_face_box_ratio(detection: FaceDetection) -> float:
        width = max(1.0, float(detection.right - detection.left))
        height = max(1.0, float(detection.bottom - detection.top))
        return float(max(0.0, min(2.0, width / height)))

    @staticmethod
    def _align_face(
        frame: np.ndarray,
        keypoints: Tuple[Tuple[float, float], ...],
        *,
        output_size: int = 112,
    ) -> Optional[np.ndarray]:
        if frame is None or len(keypoints) < 5:
            return None
        src = np.asarray(keypoints[:5], dtype=np.float32)
        if src.shape != (5, 2):
            return None
        # ArcFace canonical 112x112 landmarks.
        dst = np.array(
            [
                [38.2946, 51.6963],
                [73.5318, 51.5014],
                [56.0252, 71.7366],
                [41.5493, 92.3655],
                [70.7299, 92.2041],
            ],
            dtype=np.float32,
        )
        if int(output_size) != 112:
            scale = float(output_size) / 112.0
            dst = dst * scale
        matrix, _inliers = cv2.estimateAffinePartial2D(src, dst, method=cv2.LMEDS)
        if matrix is None:
            return None
        aligned = cv2.warpAffine(
            frame,
            matrix,
            (int(output_size), int(output_size)),
            flags=cv2.INTER_LINEAR,
            borderMode=cv2.BORDER_REFLECT,
        )
        if aligned is None or int(aligned.size) <= 0:
            return None
        return aligned

    @staticmethod
    def _estimate_beard_probability(face_bgr: np.ndarray) -> float:
        if face_bgr is None or not hasattr(face_bgr, "shape"):
            return 0.0
        height = int(face_bgr.shape[0]) if len(face_bgr.shape) > 0 else 0
        width = int(face_bgr.shape[1]) if len(face_bgr.shape) > 1 else 0
        if height < 48 or width < 48:
            return 0.0

        gray = cv2.cvtColor(face_bgr, cv2.COLOR_BGR2GRAY).astype(np.float32)
        cheek = gray[int(height * 0.38) : int(height * 0.52), int(width * 0.12) : int(width * 0.88)]
        chin = gray[int(height * 0.76) : int(height * 0.96), int(width * 0.22) : int(width * 0.78)]
        if cheek.size <= 0 or chin.size <= 0:
            return 0.0

        chin_std = float(np.std(chin))
        cheek_std = float(np.std(cheek))
        texture_ratio = chin_std / max(1e-6, cheek_std)
        texture_component = max(0.0, min(1.0, (texture_ratio - 0.68) / 0.42))

        hsv = cv2.cvtColor(face_bgr, cv2.COLOR_BGR2HSV)
        chin_sat = hsv[int(height * 0.76) : int(height * 0.96), int(width * 0.22) : int(width * 0.78), 1].astype(
            np.float32
        )
        sat_score = float(np.mean(chin_sat)) / 255.0
        low_sat_component = max(0.0, min(1.0, (0.50 - sat_score) / 0.50))

        edges_chin = cv2.Canny(chin.astype(np.uint8), 35, 105)
        edges_cheek = cv2.Canny(cheek.astype(np.uint8), 35, 105)
        edge_ratio = float(np.mean(edges_chin > 0)) / max(1e-6, float(np.mean(edges_cheek > 0)))
        edge_component = max(0.0, min(1.0, (edge_ratio - 0.50) / 0.70))

        probability = 1.30 * (
            (0.50 * texture_component)
            + (0.35 * low_sat_component)
            + (0.15 * edge_component)
        )
        return float(max(0.0, min(1.0, probability)))

    @staticmethod
    def _estimate_overweight_probability(face_bgr: np.ndarray, *, face_box_ratio: float) -> float:
        if face_bgr is None or not hasattr(face_bgr, "shape"):
            return 0.0
        height = int(face_bgr.shape[0]) if len(face_bgr.shape) > 0 else 0
        width = int(face_bgr.shape[1]) if len(face_bgr.shape) > 1 else 0
        if height < 56 or width < 56:
            return 0.0

        resized = cv2.resize(face_bgr, (112, 112), interpolation=cv2.INTER_LINEAR)
        ycrcb = cv2.cvtColor(resized, cv2.COLOR_BGR2YCrCb)
        hsv = cv2.cvtColor(resized, cv2.COLOR_BGR2HSV)

        skin_ycrcb = cv2.inRange(
            ycrcb,
            np.array([0, 133, 77], dtype=np.uint8),
            np.array([255, 173, 127], dtype=np.uint8),
        )
        skin_hsv = cv2.inRange(
            hsv,
            np.array([0, 20, 20], dtype=np.uint8),
            np.array([25, 255, 255], dtype=np.uint8),
        )
        skin_mask = cv2.bitwise_or(skin_ycrcb, skin_hsv)
        kernel = np.ones((3, 3), dtype=np.uint8)
        skin_mask = cv2.morphologyEx(skin_mask, cv2.MORPH_OPEN, kernel, iterations=1)
        skin_mask = cv2.morphologyEx(skin_mask, cv2.MORPH_CLOSE, kernel, iterations=2)

        mask = np.zeros_like(skin_mask)
        contours, _hierarchy = cv2.findContours(skin_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if contours:
            largest = max(contours, key=cv2.contourArea)
            if cv2.contourArea(largest) > 300:
                cv2.drawContours(mask, [largest], -1, 255, thickness=-1)
            else:
                mask = skin_mask
        else:
            mask = skin_mask

        def _row_width(mask_img: np.ndarray, y: int) -> float:
            yy = max(0, min(mask_img.shape[0] - 1, int(y)))
            cols = np.where(mask_img[yy] > 0)[0]
            if cols.size <= 0:
                return 0.0
            return float(cols[-1] - cols[0] + 1)

        cheek_w = _row_width(mask, int(112 * 0.55))
        jaw_w = _row_width(mask, int(112 * 0.76))
        neck_w = _row_width(mask, int(112 * 0.90))

        if cheek_w <= 0.0:
            cheek_w = 1.0

        jaw_ratio = jaw_w / cheek_w
        neck_ratio = neck_w / cheek_w
        ratio_component = max(0.0, min(1.0, (float(face_box_ratio) - 0.72) / 0.22))
        jaw_component = max(0.0, min(1.0, (jaw_ratio - 0.72) / 0.30))
        neck_component = max(0.0, min(1.0, (neck_ratio - 0.55) / 0.40))

        gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY).astype(np.float32)
        cheek = gray[int(112 * 0.44) : int(112 * 0.56), int(112 * 0.22) : int(112 * 0.78)]
        jaw = gray[int(112 * 0.72) : int(112 * 0.90), int(112 * 0.24) : int(112 * 0.76)]
        if cheek.size > 0 and jaw.size > 0:
            brightness_delta = (float(np.mean(cheek)) - float(np.mean(jaw))) / 90.0
            brightness_component = max(0.0, min(1.0, brightness_delta))
        else:
            brightness_component = 0.0

        probability = (
            (0.45 * jaw_component)
            + (0.25 * neck_component)
            + (0.20 * ratio_component)
            + (0.10 * brightness_component)
        )
        return float(max(0.0, min(1.0, probability)))

    @staticmethod
    def _estimate_sharpness_score(face_bgr: np.ndarray) -> float:
        if face_bgr is None or not hasattr(face_bgr, "shape"):
            return 0.0
        height = int(face_bgr.shape[0]) if len(face_bgr.shape) > 0 else 0
        width = int(face_bgr.shape[1]) if len(face_bgr.shape) > 1 else 0
        if height < 24 or width < 24:
            return 0.0
        gray = cv2.cvtColor(face_bgr, cv2.COLOR_BGR2GRAY)
        laplacian_variance = float(cv2.Laplacian(gray, cv2.CV_64F).var())
        # Empirical normalization for profile images to a 0..1 confidence-like score.
        normalized = (laplacian_variance - 20.0) / 180.0
        return float(max(0.0, min(1.0, normalized)))
