import asyncio
import aioredis
import logging
from config import REDIS_HOST, REDIS_PORT, REDIS_QUEUE, REDIS_STATE_RESULT_CHANNEL

logger = logging.getLogger(__name__)

async def connect_redis():
    while True:
        try:
            redis_client = await aioredis.create_redis_pool(f'redis://{REDIS_HOST}:{REDIS_PORT}')
            logger.info("Connected to Redis")
            return redis_client
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            await asyncio.sleep(5)

async def get_frame(redis_client):
    return await redis_client.blpop(REDIS_QUEUE, timeout=1)

async def publish_state_result(redis_client, state_result):
    await redis_client.publish(REDIS_STATE_RESULT_CHANNEL, state_result)

async def get_latest_frame(redis_client, camera_id):
    # Assuming we're storing the latest frame with a key like 'latest_frame:{camera_id}'
    frame_data = await redis_client.get(f'latest_frame:{camera_id}')
    return frame_data