import asyncio
import aioredis
import asyncpg
import time
import json
import logging
import time
from datetime import datetime
import base64
import ast
from collections import defaultdict
from config import REDIS_HOST, REDIS_PORT, REDIS_QUEUE, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, REDIS_STATE_CHANNEL, PROCESS_STATE, camera_names
from db_operations import connect_database, store_results
from redis_operations import connect_redis, get_frame
from openai_operations import process_image
from state_processing import process_state
from websocket_operations import connect_websocket, send_to_django
from scheduled_checks import schedule_checks

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROCESSING_SET = "processing_frames"
PROCESSING_TIMEOUT = 180  # 3 minutes
MAX_QUEUE_SIZE = 20  # Limit the queue size for each camera


class FrameProcessor:
    def __init__(self):
        self.last_processed_time = {camera: 0 for camera in camera_names}
        self.frame_queue = defaultdict(lambda: asyncio.Queue(maxsize=MAX_QUEUE_SIZE))

    async def process_frame(self, frame_data, pool, websocket, redis):
        logger.info(f"Processing frame: {frame_data[:120]}...")
        try:
            data = ast.literal_eval(frame_data.decode('utf-8'))
            camera_id = data['camera_id']
            camera_index = data['camera_index']
            timestamp = datetime.fromisoformat(data['timestamp'])
            image_data = data['frame']
            base64_image = base64.b64encode(image_data).decode('utf-8')

            description, confidence = await process_image(base64_image)
            
            if description is not None:
                camera_name = camera_names.get(camera_id, 'Unknown')
                await store_results(pool, camera_id, camera_index, timestamp, description, confidence, image_data, camera_name)
                await send_to_django(websocket, f"{camera_name} {camera_index} {timestamp} {description}")
                
                self.last_processed_time[camera_id] = time.time()
                
                logger.info(f"Processed frame for camera {camera_id}")
                await redis.srem(PROCESSING_SET, frame_data)
                logger.info("Removed frame from processing set")
        except Exception as e:
            logger.error(f"Error processing frame: {str(e)}")
            await redis.srem(PROCESSING_SET, frame_data)
            logger.info("Removed errored frame from processing set")

    async def add_frame(self, frame_data):
        data = ast.literal_eval(frame_data.decode('utf-8'))
        camera_id = data['camera_id']
        try:
            await asyncio.wait_for(self.frame_queue[camera_id].put(frame_data), timeout=1.0)
        except asyncio.TimeoutError:
            logger.warning(f"Queue full for camera {camera_id}, dropping frame")

    async def get_next_frame(self):
        # Find the camera with the oldest processed frame
        oldest_camera = min(self.last_processed_time, key=self.last_processed_time.get)
        
        # If we have a frame for this camera, return it
        if not self.frame_queue[oldest_camera].empty():
            return await self.frame_queue[oldest_camera].get()
        # If not, find the next available frame
        for camera_id, queue in self.frame_queue.items():
            if not queue.empty():
                return await queue.get()
        
        return None

async def inspect_redis(redis):
    # Check the size of the processing set
    size = await redis.scard(PROCESSING_SET)
    print(f"Size of processing set: {size}")

    # Get all members of the processing set
    members = await redis.smembers(PROCESSING_SET)
    print(f"Members of processing set: {members}")

    # Check the size of the input queue
    queue_size = await redis.llen(REDIS_QUEUE)
    print(f"Size of input queue: {queue_size}")

    # Get the first few items in the input queue (without removing them)
    queue_items = await redis.lrange(REDIS_QUEUE, 0, 5)
    print(f"First few items in the input queue: {queue_items}")
            
async def get_work(redis):
    frame = await redis.rpop(REDIS_QUEUE)
    if frame: 
        logger.info(f"Got frame from queue: {frame[:120]}...")  # Log first 100 chars
        try:
            ast.literal_eval(frame.decode('utf-8'))
            await redis.sadd(PROCESSING_SET, frame)
            logger.info("Added frame to processing set")
            return frame
        except (ValueError, SyntaxError) as e:
            logger.error(f"Invalid data in Redis queue: {e}")
    return None

async def clean_processing_set(redis):
    while True:
        current_time = time.time()
        cursor = 0
        while True:
            cursor, keys = await redis.sscan(PROCESSING_SET, cursor, count=100)  # Process in smaller batches
            for frame in keys:
                try:
                    data = ast.literal_eval(frame.decode('utf-8'))
                    frame_time = datetime.fromisoformat(data['timestamp']).timestamp()
                    if current_time - frame_time > PROCESSING_TIMEOUT:
                        # If frame has been in processing for too long, move it back to the queue
                        await redis.srem(PROCESSING_SET, frame)
                        await redis.lpush(REDIS_QUEUE, frame)
                except (ValueError, SyntaxError, AttributeError) as e:
                    logger.error(f"Invalid data in processing set: {e}")
                    logger.error(f"Problematic data: {frame}")
                    # Remove invalid data from the processing set
                    await redis.srem(PROCESSING_SET, frame)
            if cursor == 0:
                break
        await asyncio.sleep(60)

async def main():
    redis_client = await connect_redis()
    redis = await aioredis.create_redis_pool(f'redis://{REDIS_HOST}:{REDIS_PORT}')
    db_conn = await connect_database()
    pool = await asyncpg.create_pool(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    websocket = await connect_websocket()

    # Ensure PROCESSING_SET is a set
    await redis.delete(PROCESSING_SET)
    await redis.sadd(PROCESSING_SET, "dummy")
    await redis.srem(PROCESSING_SET, "dummy")
    
    asyncio.create_task(clean_processing_set(redis))
    #asyncio.create_task(periodic_inspection(redis))
    
    frame_processor = FrameProcessor()
    
    camera_count = 0
    state_processing_interval = 60
    last_state_processing = 0

    await schedule_checks()
    try:
        while True:
            try:
                loop_start_time = time.time()
                frames_processed = 0
                    
                while time.time() - loop_start_time < 10:  # 10-second timeout
                    frame_data = await asyncio.wait_for(get_work(redis), timeout=1.0)
                    if frame_data is None:
                        logger.debug("No frames available, breaking inner loop")
                        break
                    
                    logger.info(f"Adding frame to processor: {frame_data[:50]}...")  # Log first 50 chars
                    await frame_processor.add_frame(frame_data)
                    frames_processed += 1
                    
                # Process the next frame
                next_frame = await frame_processor.get_next_frame()
                if next_frame:
                    await frame_processor.process_frame(next_frame, pool, websocket, redis)
                    camera_count += 1
                    if PROCESS_STATE:
                        if camera_count >= len(camera_names):
                            # All cameras processed, check if it's time to process state
                            current_time = time.time()
                            if current_time - last_state_processing >= state_processing_interval:
                                await process_state(db_conn, redis_client)
                                last_state_processing = current_time
                            camera_count = 0
                
                # Check for state processing request from Django
                state_request = await redis_client.blpop(REDIS_STATE_CHANNEL, timeout=1)
                if state_request:
                    await process_state(db_conn, redis_client)
                pass
            except asyncio.TimeoutError:
                logger.warning("Timeout while waiting for work, continuing...")
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                await asyncio.sleep(1)
    finally:
        # Close Redis connection
        redis.close()
        await redis.wait_closed()
            
async def periodic_inspection(redis):
    while True:
        await inspect_redis(redis)
        await asyncio.sleep(60)  # Inspect every 60 seconds

if __name__ == "__main__":
    asyncio.run(main())