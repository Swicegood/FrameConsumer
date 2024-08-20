import asyncio
import logging
import time
from datetime import datetime
import base64
import ast
from config import camera_names, camera_indexes, REDIS_STATE_CHANNEL, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
from db_operations import connect_database, store_results
from redis_operations import connect_redis, get_frame
from openai_operations import process_image
from state_processing import process_state
from websocket_operations import connect_websocket, send_to_django
from scheduled_checks import schedule_checks
import asyncpg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def process_frame(frame_data):
    try:
        data = ast.literal_eval(frame_data.decode('utf-8'))
    except (SyntaxError, ValueError) as e:
        logger.error(f"Failed to parse frame data: {e}")
        return None

    if not all(key in data for key in ['camera_id', 'camera_index', 'timestamp', 'frame']):
        logger.error("Frame data is incomplete.")
        return None
    
    camera_id = data['camera_id']
    camera_index = data['camera_index']
    timestamp = datetime.fromisoformat(data['timestamp'])
    image_data = data['frame']
    base64_image = base64.b64encode(image_data).decode('utf-8')

    description, confidence = await process_image(base64_image)
    
    if description is None:
        return None

    return camera_id, camera_index, timestamp, description, confidence, image_data

async def main():
    redis_client = await connect_redis()
    db_conn = await connect_database()
    pool = await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    websocket = await connect_websocket()

    camera_count = 0
    state_processing_interval = 60  # Process state every 60 seconds
    last_state_processing = 0

    # Schedule the checks
    await schedule_checks()

    while True:
        try:
            # Check for frame in the queue
            frame_data = await get_frame(redis_client)
            
            if frame_data:
                result = await process_frame(frame_data[1])
                if result:
                    camera_id, camera_index, timestamp, description, confidence, image_data = result
                    camera_name = camera_names.get(camera_id, 'Unknown')
                    await store_results(pool, camera_id, camera_index, timestamp, description, confidence, image_data, camera_name)
                    await send_to_django(websocket, f"{camera_name} {camera_index} {timestamp} {description}")
                    camera_count += 1
                    if camera_count >= len(camera_names) or camera_count == 1:
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
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())