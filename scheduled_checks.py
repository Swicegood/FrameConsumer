import asyncio
import aiocron
from datetime import datetime, time
import pytz
import json
from openai_operations import process_image_for_curtains, process_descriptions_for_presence
from db_operations import fetch_descriptions_for_timerange
from redis_operations import connect_redis, get_latest_frame
from collections import Counter

ALERT_QUEUE = 'alert_queue'

async def check_curtains(redis_client, camera_id, check_time):
    frame = await get_latest_frame(redis_client, camera_id)
    if frame is None:
        print(f"No frame available for camera {camera_id} at {check_time}")
        return

    results = []
    for _ in range(20):
        result = await process_image_for_curtains(frame)
        results.append(result)

    most_common = Counter(results).most_common(1)[0][0]
    
    if most_common == "yes":
        alert_data = {
            'camera_id': camera_id,
            'check_time': check_time,
            'message': f"Curtains are closed for camera {camera_id} at {check_time}",
            'frame': frame.decode('utf-8') if frame else None
        }
        await redis_client.rpush(ALERT_QUEUE, json.dumps(alert_data))

async def check_presence(redis_client, camera_id, start_time, end_time):
    descriptions = await fetch_descriptions_for_timerange(camera_id, start_time, end_time)
    if not descriptions:
        print(f"No descriptions available for camera {camera_id} between {start_time} and {end_time}")
        return

    results = []
    for _ in range(20):
        result = await process_descriptions_for_presence(descriptions)
        results.append(result)

    most_common = Counter(results).most_common(1)[0][0]
    
    if most_common == "no":
        frame = await get_latest_frame(redis_client, camera_id)
        alert_data = {
            'camera_id': camera_id,
            'check_time': f"{start_time}-{end_time}",
            'message': f"No person detected for camera {camera_id} between {start_time} and {end_time}",
            'frame': frame.decode('utf-8') if frame else None
        }
        await redis_client.rpush(ALERT_QUEUE, json.dumps(alert_data))

async def schedule_checks():
    redis_client = await connect_redis()
    
    # Set timezone
    tz = pytz.timezone('America/New_York')

    # AXIS_ID checks
    aiocron.crontab('33 12 * * *', func=check_curtains, args=(redis_client, "AXIS_ID", "12:33pm"), start=True, tz=tz)
    aiocron.crontab('18 16 * * *', func=check_curtains, args=(redis_client, "AXIS_ID", "4:18pm"), start=True, tz=tz)
    aiocron.crontab('3 19 * * *', func=check_curtains, args=(redis_client, "AXIS_ID", "7:03pm"), start=True, tz=tz)

    # sHlS7ewuGDEd2ef4 checks
    aiocron.crontab('55 11 * * *', func=check_presence, args=(redis_client, "sHlS7ewuGDEd2ef4", time(11,55), time(12,0)), start=True, tz=tz)
    aiocron.crontab('45 15 * * *', func=check_presence, args=(redis_client, "sHlS7ewuGDEd2ef4", time(15,45), time(15,55)), start=True, tz=tz)
    aiocron.crontab('40 18 * * *', func=check_presence, args=(redis_client, "sHlS7ewuGDEd2ef4", time(18,40), time(18,45)), start=True, tz=tz)

    # g8rHNVCflWO1ptKN checks
    aiocron.crontab('30 3 * * *', func=check_presence, args=(redis_client, "g8rHNVCflWO1ptKN", time(3,30), time(4,0)), start=True, tz=tz)
    aiocron.crontab('30 10 * * *', func=check_presence, args=(redis_client, "g8rHNVCflWO1ptKN", time(10,30), time(11,10)), start=True, tz=tz)
    aiocron.crontab('30 16 * * *', func=check_presence, args=(redis_client, "g8rHNVCflWO1ptKN", time(16,30), time(16,45)), start=True, tz=tz)
    aiocron.crontab('0 18 * * *', func=check_presence, args=(redis_client, "g8rHNVCflWO1ptKN", time(18,0), time(18,25)), start=True, tz=tz)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(schedule_checks())
    asyncio.get_event_loop().run_forever()