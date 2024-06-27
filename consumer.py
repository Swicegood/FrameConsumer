import redis
import cv2
import numpy as np
import logging
import time
from datetime import datetime
import psycopg2
from psycopg2 import sql
import base64
import os
import asyncio
import websockets
import json

try:
    from openai import OpenAI
except ImportError:
    logging.error("OpenAI library could not be found. Please install it using 'pip install openai'.")

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

DJANGO_WEBSOCKET_URL = os.getenv('DJANGO_WEBSOCKET_URL', 'ws://127.0.0.1:8000/ws/llm_output/')

# Initialize OpenAI client
client = OpenAI(base_url=OPENAI_BASE_URL, api_key=OPENAI_API_KEY)

# Global variables for connections
redis_client = None
db_conn = None
websocket = None

async def connect_redis():
    global redis_client
    while True:
        try:
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
            await redis_client.ping()
            logging.info("Connected to Redis")
            return
        except redis.ConnectionError as e:
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
        data = eval(frame_data.decode('utf-8'))
    except SyntaxError as e:
        logging.error(f"Failed to parse frame data: {e}")
        return None

    if 'camera_id' not in data or 'camera_index' not in data or 'timestamp' not in data or 'frame' not in data:
        logging.error("Frame data is incomplete.")
        return None
    
    camera_id = data['camera_id']
    camera_index = data['camera_index']
    timestamp = datetime.fromisoformat(data['timestamp'])
    base64_image = base64.b64encode(data['frame']).decode('utf-8')

    messages = [
        {
            "role": "system",
            "content": "This is a chat between a user and an assistant. The assistant is helping the user to describe an image.",
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "What's in this image?"},
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
        completion = client.chat.completions.create(
            model="not used",
            messages=messages,
            max_tokens=1000,
            stream=True
        )

        description = ""
        for chunk in completion:
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

camera_names = {
    "I6Dvhhu1azyV9rCu": "Audio Visual", "oaQllpjP0sk94nCV": "Bhoga Shed", "PxnDZaXu2awYbMmS": "Back Driveway",
    "mKlJgNx7tXwalch1": "Deck Stairs", "rHWz9GRDFxrOZF7b": "Down Pujari", "LRqgKMMjjJbNEeyE": "Field",
    "94uZsJ2yIouIXp2x": "Greenhouse", "5SJZivf8PPsLWw2n": "Hall", "g8rHNVCflWO1ptKN": "Kitchen",
    "t3ZIWTl9jZU1JGEI": "Pavillion", "iY9STaEt7K9vS8yJ": "Prabhupada", "jlNNdFFvhQ2o2kmn": "Stage",
    "IOKAu7MMacLh79zn": "Temple", "sHlS7ewuGDEd2ef4": "Up Pujari", "OSF13XTCKhpIkyXc": "Walk-in",
    "jLUEC60zHGo7BXfj": "Walkway"
}

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
                    await send_to_django(f"{camera_names.get(camera_id, 'Unknown')} {camera_index} {timestamp} {description}")
            else:
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())