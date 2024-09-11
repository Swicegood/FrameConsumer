import cv2
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import asyncio

class PersonDetector:
    def __init__(self, num_threads=44):
        self.hog = cv2.HOGDescriptor()
        self.hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
        self.executor = ThreadPoolExecutor(max_workers=num_threads)

    def detect_person(self, frame):
        # Resize the frame for faster detection
        frame = cv2.resize(frame, (640, 480))
        # Convert the frame to grayscale for faster detection
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        
        # Detect people in the image
        boxes, weights = self.hog.detectMultiScale(gray, winStride=(8,8))
        
        return len(boxes) > 0

    async def detect_person_async(self, frame):
        return await asyncio.get_event_loop().run_in_executor(self.executor, self.detect_person, frame)

    def detect_persons_batch(self, frames):
        return list(self.executor.map(self.detect_person, frames))

person_detector = PersonDetector(num_threads=44)