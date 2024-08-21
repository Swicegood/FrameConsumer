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
from config import REDIS_HOST, REDIS_PORT, REDIS_QUEUE, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, REDIS_STATE_CHANNEL, camera_names
from db_operations import connect_database, store_results
from redis_operations import connect_redis, get_frame
from openai_operations import process_image
from state_processing import process_state
from websocket_operations import connect_websocket, send_to_django
from scheduled_checks import schedule_checks
import asyncpg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROCESSING_SET = "processing_frames"
PROCESSING_TIMEOUT = 300  # 5 minutes

async def process_frame(frame_data, pool, websocket, redis):
    try:
        data = ast.literal_eval(frame_data.decode('utf-8'))
    except (SyntaxError, ValueError) as e:
        logger.error(f"Failed to parse frame data: {e}")
        return None

    if not all(key in data for key in ['camera_id', 'camera_index', 'timestamp', 'frame']):
        logger.error("Frame data is incomplete.")
        return None
    try:
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
            
            # Remove the frame from the processing set
            await redis.srem(PROCESSING_SET, frame_data)
    except Exception as e:
        logger.error(f"Error processing frame: {str(e)}")
        # In case of error, remove from processing set to allow reprocessing
        await redis.srem(PROCESSING_SET, frame_data)


async def get_work(redis):
    while True:
        # Atomic operation: move an item from the queue to the processing set
        frame = await redis.rpoplpush(REDIS_QUEUE, PROCESSING_SET)
        if frame:
            return frame
        await asyncio.sleep(0.1)

async def clean_processing_set(redis):
    while True:
        current_time = time.time()
        async for frame in redis.iscan(match=PROCESSING_SET):
            try:
                data = json.loads(frame)
                if current_time - data['enqueue_time'] > PROCESSING_TIMEOUT:
                    # If frame has been in processing for too long, move it back to the queue
                    await redis.smove(PROCESSING_SET, REDIS_QUEUE, frame)
            except json.JSONDecodeError:
                # If frame data is invalid, remove it from the processing set
                await redis.srem(PROCESSING_SET, frame)
        await asyncio.sleep(60)  # Run this check every minute

async def main():
    redis_client = await connect_redis()
    redis = await aioredis.create_redis_pool(f'redis://{REDIS_HOST}:{REDIS_PORT}')
    db_conn = await connect_database()
    pool = await asyncpg.create_pool(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    websocket = await connect_websocket()

    await redis.delete(PROCESSING_SET)  # Clear any existing data
    await redis.sadd(PROCESSING_SET, "dummy")  # Ensure it's created as a set
    await redis.srem(PROCESSING_SET, "dummy")  # Remove the dummy value
    # Start the cleaning task
    asyncio.create_task(clean_processing_set(redis))
    
    camera_count = 0
    state_processing_interval = 60  # Process state every 60 seconds
    last_state_processing = 0

    # Schedule the checks
    await schedule_checks()

    while True:
        try:
            frame_data = await get_work(redis)
            if frame_data:
                await process_frame(frame_data, pool, websocket, redis)
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

            await asyncio.sleep(0.1)  # Short sleep to prevent CPU overuse
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())