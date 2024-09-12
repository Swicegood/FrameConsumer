import asyncio
import aiocron
from datetime import datetime, time
import pytz
import json
import base64
from openai_operations import process_image, process_descriptions_for_presence
from db_operations import fetch_descriptions_for_timerange, connect_database
from redis_operations import connect_redis, get_latest_frame_wrapper
from collections import Counter
import logging

ALERT_QUEUE = 'alert_queue'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def check_curtains(redis_client, db_conn, camera_id, check_time, start_time, end_time):
    try:
        frame = await get_latest_frame_wrapper(db_conn, camera_id)
        if frame is None:
            logger.warning(f"No frame available for camera {camera_id} at {check_time}")
            return

        # Convert frame to base64, handling different types
        if isinstance(frame, memoryview):
            frame_bytes = frame.tobytes()
        elif isinstance(frame, bytes):
            frame_bytes = frame
        elif isinstance(frame, str):
            frame_bytes = frame.encode('utf-8')
        else:
            logger.error(f"Unexpected frame type for camera {camera_id}: {type(frame)}")
            return

        frame_base64 = base64.b64encode(frame_bytes).decode('utf-8')
        logger.info(f"Frame data type: {type(frame)}, base64 length: {len(frame_base64)}")

        descriptions = await fetch_descriptions_for_timerange(db_conn, camera_id, start_time, end_time)
        if not descriptions:
            logger.warning(f"No descriptions available for camera {camera_id} between {start_time} and {end_time}")
            return
        
        if not any(word in descriptions.lower() for word in ["deities", "statues", "deity", "figures"]):
            alert_data = {
                'camera_id': camera_id,
                'check_time': check_time,
                'message': f"Curtains are closed for camera {camera_id} at {check_time}",
                'frame': frame_base64
            }
            await redis_client.rpush(ALERT_QUEUE, json.dumps(alert_data))
            logger.info(f"Alert pushed to queue for camera {camera_id}: Curtains closed")
        else:
            logger.info(f"Curtains are open for camera {camera_id} at {check_time}")
    except Exception as e:
        logger.error(f"Error in check_curtains for camera {camera_id}: {str(e)}")


async def schedule_checks():
    redis_client = await connect_redis()
    db_conn = await connect_database()
    
    # Set timezone
    tz = pytz.timezone('America/New_York')

    # AXIS_ID checks
    aiocron.crontab('33-38 12 * * *', func=check_curtains, args=(redis_client, db_conn, "AXIS_ID", "12:33pm", time(12,33), time(12,38)), start=True, tz=tz)
    aiocron.crontab('18-22 16 * * *', func=check_curtains, args=(redis_client, db_conn, "AXIS_ID", "4:18pm", time(16,18), time(16,22)), start=True, tz=tz)
    aiocron.crontab('3-8 19 * * *', func=check_curtains, args=(redis_client, db_conn, "AXIS_ID", "7:03pm", time(19,3), time(19,8)), start=True, tz=tz)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(schedule_checks())
    asyncio.get_event_loop().run_forever()