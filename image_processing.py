import cv2
import numpy as np
from skimage.metrics import structural_similarity as ssim
from openai_operations import process_image
import logging
import base64

logger = logging.getLogger(__name__)

class ImageProcessor:
    def __init__(self):
        self.prev_frames = {}
        self.base_frames = {}
        self.change_accumulators = {}
        self.last_processed_images = {}
        self.ssim_threshold = 0.95  # Adjust this threshold as needed

    async def should_process_image(self, camera_id, img):
        if camera_id not in self.prev_frames:
            self.prev_frames[camera_id] = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            self.base_frames[camera_id] = img.copy().astype(float)
            self.change_accumulators[camera_id] = np.zeros(img.shape[:2], dtype=np.float32)
            self.last_processed_images[camera_id] = img
            return True

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        ssim_value = ssim(self.prev_frames[camera_id], gray)

        if ssim_value < self.ssim_threshold:
            cv2.accumulateWeighted(img, self.base_frames[camera_id], 0.1)

            frame_diff = cv2.absdiff(gray, self.prev_frames[camera_id])
            thresh = cv2.adaptiveThreshold(frame_diff, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                           cv2.THRESH_BINARY, 11, 2)
            self.change_accumulators[camera_id] += thresh.astype(np.float32) / 255.0

            self.prev_frames[camera_id] = gray
            self.last_processed_images[camera_id] = img
            return True

        self.prev_frames[camera_id] = gray
        return False

    def get_last_processed_image(self, camera_id):
        return self.last_processed_images.get(camera_id)

    async def process_image_if_changed(self, camera_id, img):
        if await self.should_process_image(camera_id, img):
            # Encode the image as PNG
            _, buffer = cv2.imencode('.png', img)
            base64_image = base64.b64encode(buffer).decode('utf-8')
            
            description, confidence = await process_image(base64_image)
            return description, confidence
        else:
            logger.info(f"Image for camera {camera_id} hasn't changed significantly. Skipping processing.")
            return None, None

    def normalize_change_accumulator(self, camera_id):
        max_change = np.max(self.change_accumulators[camera_id])
        if max_change > 0:
            self.change_accumulators[camera_id] /= max_change

    