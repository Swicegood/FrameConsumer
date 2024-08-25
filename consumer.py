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
from config import REDIS_HOST, REDIS_PORT, REDIS_QUEUE, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, REDIS_STATE_CHANNEL, PROCESS_STATE, camera_names, CAMERA_IDS, MODULUS, INSTANCE_INDEX, ADDITIONAL_INDEX
from db_operations import connect_database, store_results
from redis_operations import connect_redis, get_frame
from openai_operations import process_image
from state_processing import process_state
from websocket_operations import connect_websocket, send_to_django
from scheduled_checks import schedule_checks

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REDIS_FRAME_KEY = "camera_frames:{}"  # Will be formatted with camera_id

class FrameProcessor:
    def __init__(self):
        self.last_processed_time = {camera: 0 for camera in CAMERA_IDS}

    async def process_frame(self, frame_data, pool, websocket):
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
        except Exception as e:
            logger.error(f"Error processing frame: {str(e)}")

async def main():
    redis_client = await connect_redis()
    redis = await aioredis.create_redis_pool(f'redis://{REDIS_HOST}:{REDIS_PORT}')
    db_conn = await connect_database()
    pool = await asyncpg.create_pool(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    websocket = await connect_websocket()

    frame_processor = FrameProcessor()
    
    camera_index = INSTANCE_INDEX
    camera_count = 0
    state_processing_interval = 60
    last_state_processing = 0

    try:
        while True:
            try:
                camera_id = CAMERA_IDS[camera_index]
                frame_data = await redis.get(REDIS_FRAME_KEY.format(camera_id))
                
                if frame_data and (camera_index % MODULUS == INSTANCE_INDEX or (camera_index + ADDITIONAL_INDEX) % MODULUS == INSTANCE_INDEX):
                
                    await frame_processor.process_frame(frame_data, pool, websocket)
                
                camera_index = (camera_index + 1) % len(CAMERA_IDS)
                
                camera_count += 1
                if PROCESS_STATE:
                    if camera_count >= len(camera_names):
                        # All cameras processed, check if it's time to process state
                        current_time = time.time()
                        if current_time - last_state_processing >= state_processing_interval:
                            await process_state(db_conn, redis_client)
                            last_state_processing = current_time
                        camera_count = 0
                state_request = await redis.blpop(REDIS_STATE_CHANNEL, timeout=1)
                if state_request:
                    await process_state(pool, redis)
                
                await asyncio.sleep(0.1)  # Prevent CPU overuse
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                await asyncio.sleep(1)
    finally:
        redis.close()
        await redis.wait_closed()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())