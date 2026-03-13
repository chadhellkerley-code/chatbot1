import unittest

import cv2  # type: ignore
import numpy as np  # type: ignore

from src.vision.face_detector_scrfd import SCRFDFaceDetector, warmup_scrfd_model
from src.vision.gender_age_analyzer import GenderAgeAnalyzer, warmup_genderage_model


class ImageOnnxLocalEngineTests(unittest.TestCase):
    def test_detector_and_genderage_return_probability(self):
        self.assertTrue(warmup_scrfd_model())
        self.assertTrue(warmup_genderage_model())

        dummy = np.full((320, 320, 3), 255, dtype=np.uint8)
        cv2.circle(dummy, (160, 140), 72, (210, 190, 170), -1)
        cv2.circle(dummy, (135, 125), 9, (0, 0, 0), -1)
        cv2.circle(dummy, (185, 125), 9, (0, 0, 0), -1)
        cv2.ellipse(dummy, (160, 175), (32, 18), 0, 0, 180, (25, 25, 25), 3)

        detector = SCRFDFaceDetector()
        boxes = detector.detect(dummy)
        self.assertIsInstance(boxes, list)

        if boxes:
            left, top, right, bottom, _score = boxes[0]
            crop = dummy[top:bottom, left:right]
        else:
            crop = dummy[80:240, 80:240]

        analyzer = GenderAgeAnalyzer()
        prediction = analyzer.analyze(crop)

        self.assertIsNotNone(prediction.gender_prob)
        self.assertGreaterEqual(float(prediction.gender_prob), 0.0)
        self.assertLessEqual(float(prediction.gender_prob), 1.0)
        self.assertIsNotNone(prediction.age_estimate)


if __name__ == "__main__":
    unittest.main()

