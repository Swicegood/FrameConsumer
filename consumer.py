import asyncio
import aioredis
import logging
import time
from datetime import datetime
import psycopg2
from psycopg2 import sql
import base64
import os
import websockets
import json
from openai import AsyncOpenAI
import ast

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurations from environment variables or default values
REDIS_HOST = os.getenv('REDIS_HOST', '192.168.0.71')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_QUEUE = os.getenv('REDIS_QUEUE', 'frame_queue')

DB_HOST = os.getenv('DB_HOST', '192.168.0.71')
DB_NAME = os.getenv('DB_NAME', 'visionmon')
DB_USER = os.getenv('DB_USER', 'pguser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'pgpass')

OPENAI_BASE_URL = os.getenv('OPENAI_BASE_URL', 'http://192.168.0.199:1337/v1')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', 'lm-studio')

DJANGO_WEBSOCKET_URL = os.getenv('DJANGO_WEBSOCKET_URL', 'ws://192.168.0.71:8020/ws/llm_output/')

# Initialize OpenAI client
client = AsyncOpenAI(base_url=OPENAI_BASE_URL, api_key=OPENAI_API_KEY)

# Global variables for connections
redis_client = None
db_conn = None
websocket = None

camera_names = {
    "I6Dvhhu1azyV9rCu": "Audio_Visual", "oaQllpjP0sk94nCV": "Bhoga_Shed", "PxnDZaXu2awYbMmS": "Back_Driveway",
    "mKlJgNx7tXwalch1": "Deck_Stairs", "rHWz9GRDFxrOZF7b": "Down_Pujari", "LRqgKMMjjJbNEeyE": "Field",
    "94uZsJ2yIouIXp2x": "Greenhouse", "5SJZivf8PPsLWw2n": "Hall", "g8rHNVCflWO1ptKN": "Kitchen",
    "t3ZIWTl9jZU1JGEI": "Pavillion", "iY9STaEt7K9vS8yJ": "Prabhupada", "jlNNdFFvhQ2o2kmn": "Stage",
    "IOKAu7MMacLh79zn": "Temple", "sHlS7ewuGDEd2ef4": "Up_Pujari", "OSF13XTCKhpIkyXc": "Walk-in",
    "jLUEC60zHGo7BXfj": "Walkway"
}

camera_descriptions = {
    "Audio_Visual": "A server room/AV/alter prep area typically used for getting alter itmes and adjusting volume or equipment.",
    "Bhoga_Shed": "Food pantry for the deities, will rarely be occupied by anyone other than pujaris or kitchen staff.",
    "Back_Driveway": "The driveway behind the temple, typically used for deliveries or parking.",
    "Deck_Stairs": "The stairs leading to the deck, typically used by pujaris or visitors to access the temple.",
    "Down_Pujari": "The area downstairs where pujaris prepare dresses for the deiteis, typically only occupied by pujaris.",
    "Field": "The field behind the temple, typically used for outdoor events or gatherings. May be occupied by visitors or devotees.",
    "Greenhouse": "Soley for growing sacred Tulasi plants, typically only occupied by pujaris or gardeners.",
    "Hall": "Dining and event hall, typically used for prasadam distribution, classes, or gatherings. May be occupied by visitors or devotees.",
    "Kitchen": "The temple kitchen, typically used for cooking prasadam or preparing food. May be occupied by kitchen staff or pujaris.",
    "Pavillion": "The pavillion area, typically used for outdoor events or gatherings. May be occupied by visitors or devotees.",
    "Prabhupada": "The main hall of the temple, typically used for kirtans, classes, or gatherings. May be occupied by visitors or devotees. There is a statue of Srila Prabhupada in this area.",
    "Stage": "The stage area, typically used for performances, kirtans, or classes, and prasadam distribution. May be occupied by visitors or devotees.",
    "Temple": "The main hall of the temple, typically used for kirtans, classes, or gatherings. May be occupied by visitors or devotees.",
    "Up_Pujari": "[Placeholder: Describe the Up_Pujari area, its purpose, and what's normally visible]",
    "Walk-in": "Cold storage area on backside of the temple for perishable items, typically only occupied by kitchen staff or pujaris.",
    "Walkway": "The walkway leading to the temple, typically used by visitors or devotees."
}

async def connect_redis():
    global redis_client
    while True:
        try:
            redis_client = await aioredis.create_redis_pool(f'redis://{REDIS_HOST}:{REDIS_PORT}')
            logging.info("Connected to Redis")
            return
        except Exception as e:
            logging.error(f"Failed to connect to Redis: {str(e)}")
            await asyncio.sleep(5)

async def connect_database():
    global db_conn
    while True:
        try:
            db_conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            cur = db_conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS frame_analysis (
                    id SERIAL PRIMARY KEY,
                    camera_id VARCHAR(255),
                    camera_index INTEGER,
                    timestamp TIMESTAMP,
                    llm_description TEXT,
                    confidence FLOAT
                )
            """)
            db_conn.commit()
            logging.info("Connected to PostgreSQL database")
            return
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to PostgreSQL: {str(e)}")
            await asyncio.sleep(5)

async def connect_websocket():
    global websocket
    while True:
        try:
            websocket = await websockets.connect(DJANGO_WEBSOCKET_URL)
            logging.info("Connected to Django WebSocket")
            return
        except Exception as e:
            logging.error(f"Failed to connect to WebSocket: {str(e)}")
            await asyncio.sleep(5)

async def send_to_django(message):
    global websocket
    while True:
        try:
            if not websocket:
                await connect_websocket()
            await websocket.send(json.dumps({'message': message}))
            return
        except websockets.exceptions.ConnectionClosed:
            logging.error("WebSocket connection closed. Attempting to reconnect...")
            websocket = None
        except Exception as e:
            logging.error(f"Error sending message to Django: {str(e)}")
            websocket = None
        await asyncio.sleep(1)


async def process_frame(frame_data):
    """Process a single frame through the LLM."""
    try:
        # Use ast.literal_eval() instead of json.loads()
        data = ast.literal_eval(frame_data.decode('utf-8'))
    except (SyntaxError, ValueError) as e:
        logging.error(f"Failed to parse frame data: {e}")
        return None

    if not all(key in data for key in ['camera_id', 'camera_index', 'timestamp', 'frame']):
        logging.error("Frame data is incomplete.")
        return None
    
    camera_id = data['camera_id']
    camera_index = data['camera_index']
    timestamp = datetime.fromisoformat(data['timestamp'])
    base64_image = base64.b64encode(data['frame']).decode('utf-8')

    camera_name = camera_names.get(camera_id, "Unknown")
    camera_description = camera_descriptions.get(camera_name, "No specific description available.")

    messages = [
        {
            "role": "system",
            "content": f"You are analyzing real-time footage from a security camera installed at {camera_name}. {camera_description} Your task is to vigilantly monitor for any unusual or potentially hazardous activities that could signify a security threat, safety issue, or require immediate attention.",
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": 
                    """Instructions: Examine the provided image carefully. Describe any unusual activities, focusing on details such as:

    • Unidentified individuals or unauthorized access, especially during off-hours.
    • Signs of forced entry such as broken windows or doors.
    • Vehicles that are in restricted areas or moving in an unusual pattern.
    • Suspicious behavior such as loitering near sensitive areas, unusual gatherings, or individuals carrying suspicious items.
    • Safety hazards including signs of fire (smoke, flames), flooding, or significant obstructions that could pose risks to safety.

Your response should prioritize immediate actionable insights that can assist security personnel in assessing and responding to the situation promptly. Provide specific descriptions, including the location within the scene (e.g., 'near the north exit'), the appearance of individuals or objects, and any actions they are taking that are noteworthy.

Response Format: Deliver your analysis as a concise report, suitable for immediate review by security personnel, formatted as follows:

    • Alert Type: [Type of alert, e.g., Unauthorized Access, Safety Hazard]
    • Details: [Detailed description of the situation]
    • Location Specifics: [Exact location or noticeable landmarks in the image]
    • Recommended Action: [Suggest a specific action based on the observation, e.g., 'Notify security to approach the north exit', 'Review further to confirm identity', etc.]"""
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{base64_image}"
                    },
                },
            ],
        },
    ]

    try:
        completion = await client.chat.completions.create(
            model="not used",
            messages=messages,
            max_tokens=1000,
            stream=True
        )

        description = ""
        async for chunk in completion:
            if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                description += chunk.choices[0].delta.content
                print(chunk.choices[0].delta.content, end="", flush=True)

        confidence = 0.0  # Placeholder for future confidence extraction
        return camera_id, camera_index, timestamp, description, confidence

    except Exception as e:
        logging.error(f"LLM completion error: {str(e)}")
        return None

async def store_results(camera_id, camera_index, timestamp, description, confidence):
    """Store the results in the database."""
    global db_conn
    while True:
        try:
            if not db_conn or db_conn.closed:
                await connect_database()
            cur = db_conn.cursor()
            cur.execute(
                sql.SQL("INSERT INTO frame_analysis (camera_id, camera_index, timestamp, llm_description, confidence) VALUES (%s, %s, %s, %s, %s)"),
                (camera_id, camera_index, timestamp, description, confidence)
            )
            db_conn.commit()
            logging.info(f"Stored results for camera {camera_index}")
            return
        except psycopg2.Error as e:
            logging.error(f"Database error: {str(e)}")
            db_conn = None
            await asyncio.sleep(5)

async def main():
    while True:
        try:
            if not redis_client:
                await connect_redis()
            if not db_conn or db_conn.closed:
                await connect_database()
            if not websocket:
                await connect_websocket()

            frame_data = await redis_client.blpop(REDIS_QUEUE, timeout=1)
            
            if frame_data:
                result = await process_frame(frame_data[1])
                if result:
                    camera_id, camera_index, timestamp, description, confidence = result
                    await store_results(camera_id, camera_index, timestamp, description, confidence)
                    camera_name = camera_names.get(camera_id, 'Unknown')
                    await send_to_django(f"{camera_name} {camera_index} {timestamp} {description}")
            else:
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())