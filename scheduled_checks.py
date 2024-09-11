import asyncio
import aiocron
from datetime import datetime, time
import pytz
import json
import base64
from openai_operations import process_image, process_descriptions_for_presence
from db_operations import fetch_descriptions_for_timerange, connect_database
from redis_operations import connect_redis, get_latest_frame_wrapper, get_rapid_frame
from collections import Counter
import logging
from person_detector import person_detector

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

async def check_presence(redis_client, db_conn, camera_id, start_time, end_time):
    try:
        frame = await get_latest_frame_wrapper(db_conn, camera_id)
        if frame is None:
            logger.warning(f"No frame available for camera {camera_id} at {start_time} up to {end_time}")
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

        results = []
        for _ in range(20):
            result = await process_descriptions_for_presence(descriptions)
            logger.info(f"Result for presence check: {result}")
            results.append(result)

        most_common = Counter(results).most_common(1)[0][0]
        
        if "no" in most_common:
            alert_data = {
                'camera_id': camera_id,
                'check_time': f"{start_time}-{end_time}",
                'message': f"No person detected for camera {camera_id} between {start_time} and {end_time}",
                'frame': frame_base64
            }
            await redis_client.rpush(ALERT_QUEUE, json.dumps(alert_data))
            logger.info(f"Alert pushed to queue for camera {camera_id}: No person detected")
    except Exception as e:
        logger.error(f"Error in check_presence for camera {camera_id}: {str(e)}")
        
async def check_person_presence(redis_client, camera_id, start_time, end_time):
    frame_buffer = []
    person_detected = False
    
    async def capture_and_buffer(buffer_size=10):
        nonlocal frame_buffer
        while len(frame_buffer) < buffer_size:
            frame = await get_rapid_frame(redis_client, camera_id)
            if frame:
                frame_buffer.append(frame)
            await asyncio.sleep(0.1)  # Aim for 10 fps

    async def process_buffer():
        nonlocal person_detected, frame_buffer
        if frame_buffer:
            results = person_detector.detect_persons_batch(frame_buffer)
            person_detected = any(results)
            frame_buffer = []  # Clear the buffer after processing

    while datetime.now().time() >= start_time and datetime.now().time() <= end_time:
        await capture_and_buffer()
        await process_buffer()
        
        if person_detected:
            break
        
        current_time = datetime.now().time()
        if (current_time.minute % 1 == 0 and current_time.second == 0):  # Every minute
            frame = frame_buffer[-1] if frame_buffer else await get_rapid_frame(redis_client, camera_id)
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
            if not person_detected:
                alert_data = {
                    'camera_id': camera_id,
                    'check_time': f"{start_time}-{current_time}",
                    'message': f"No person detected for camera {camera_id} between {start_time} and {current_time}",
                    'frame': frame_base64
                }
                await redis_client.rpush(ALERT_QUEUE, json.dumps(alert_data))

    if not person_detected:
        frame = frame_buffer[-1] if frame_buffer else await get_rapid_frame(redis_client, camera_id)
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
        
        alert_data = {
            'camera_id': camera_id,
            'check_time': f"{start_time}-{end_time}",
            'message': f"No person detected for camera {camera_id} between {start_time} and {end_time}",
            'frame': frame_base64
        }
        await redis_client.rpush(ALERT_QUEUE, json.dumps(alert_data))

async def schedule_checks():
    redis_client = await connect_redis()
    db_conn = await connect_database()
    
    # Set timezone
    tz = pytz.timezone('America/New_York')

    # AXIS_ID checks
    aiocron.crontab('33-38 12 * * *', func=check_curtains, args=(redis_client, db_conn, "AXIS_ID", "12:33pm", time(12,33), time(12,38)), start=True, tz=tz)
    aiocron.crontab('18-22 16 * * *', func=check_curtains, args=(redis_client, db_conn, "AXIS_ID", "4:18pm", time(16,18), time(16,22)), start=True, tz=tz)
    aiocron.crontab('3-8 19 * * *', func=check_curtains, args=(redis_client, db_conn, "AXIS_ID", "7:03pm", time(19,3), time(19,8)), start=True, tz=tz)

    # sHlS7ewuGDEd2ef4 checks
    aiocron.crontab('58-59 11 * * *', func=check_person_presence, args=(redis_client, db_conn, "sHlS7ewuGDEd2ef4", time(11,55), time(12,0)), start=True, tz=tz)
    aiocron.crontab('55 15 * * 1-6', func=check_person_presence, args=(redis_client, db_conn, "sHlS7ewuGDEd2ef4", time(15,45), time(15,55)), start=True, tz=tz)
    aiocron.crontab('45 18 * * 1-6', func=check_person_presence, args=(redis_client, db_conn, "sHlS7ewuGDEd2ef4", time(18,40), time(18,45)), start=True, tz=tz)
    aiocron.crontab('45 17 * * 1-6', func=check_person_presence, args=(redis_client, db_conn, "sHlS7ewuGDEd2ef4", time(17,40), time(17,45)), start=True, tz=tz)

    # g8rHNVCflWO1ptKN checks
    aiocron.crontab('45-50 3 * * *', func=check_presence, args=(redis_client, db_conn, "g8rHNVCflWO1ptKN", time(3,30), time(4,0)), start=True, tz=tz)
    aiocron.crontab('0-10 11 * * *', func=check_presence, args=(redis_client, db_conn, "g8rHNVCflWO1ptKN", time(10,30), time(11,10)), start=True, tz=tz)
    aiocron.crontab('30-45 15 * * *', func=check_presence, args=(redis_client, db_conn, "g8rHNVCflWO1ptKN", time(16,30), time(16,45)), start=True, tz=tz)
    aiocron.crontab('0-15 18 * * *', func=check_presence, args=(redis_client, db_conn, "g8rHNVCflWO1ptKN", time(18,0), time(18,25)), start=True, tz=tz)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(schedule_checks())
    asyncio.get_event_loop().run_forever()