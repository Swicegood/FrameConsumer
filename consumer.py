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

# Initialize Redis client
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# WebSocket connection
websocket = None

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
    try:
        if not websocket:
            await connect_websocket()
        await websocket.send(message)
    except Exception as e:
        logging.error(f"Error sending message to Django: {str(e)}")
        websocket = None  # Reset the connection
        await asyncio.sleep(1)  # Wait before retrying

def initialize_database():
    """Initialize the database connection and create table if not exists."""
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()
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
    conn.commit()
    return conn

async def process_frame(frame_data):
    """Process a single frame through the LLM."""
    try:
        data = eval(frame_data.decode('utf-8'))
    except SyntaxError as e:
        logging.error(f"Failed to parse frame data: {e}")
        return None

    # Ensure all necessary data is available
    if 'camera_id' not in data or 'camera_index' not in data or 'timestamp' not in data or 'frame' not in data:
        logging.error("Frame data is incomplete.")
        return None
    camera_id = data['camera_id']
    camera_index = data['camera_index']
    timestamp = datetime.fromisoformat(data['timestamp'])
    # Assume data['frame'] is a base64-encoded PNG image string ready for the LLM
    base64_image = base64.b64encode(data['frame']).decode('utf-8')

    # Prepare the message payload for the LLM
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

    # Call the LLM with error handling
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


def store_results(conn, camera_id, camera_index, timestamp, description, confidence):
    """Store the results in the database."""
    cur = conn.cursor()
    cur.execute(
        sql.SQL("INSERT INTO frame_analysis (camera_id, camera_index, timestamp, llm_description, confidence) VALUES (%s, %s, %s, %s, %s)"),
        (camera_id, camera_index, timestamp, description, confidence)
    )
    conn.commit()

async def main():
    conn = initialize_database()
    await connect_websocket()
    
    while True:
        try:
            # Try to get a frame from the Redis queue
            frame_data = redis_client.blpop(REDIS_QUEUE, timeout=1)
            
            if frame_data:
                # Process the frame
                camera_id, camera_index, timestamp, description, confidence = await process_frame(frame_data[1])
                # Store the results
                store_results(conn, camera_id, camera_index, timestamp, description, confidence)
                logging.info(f"Processed and stored frame from camera {camera_index}")
                # Send the results to Django via WebSocket
                await send_to_django(f"{camera_id} {camera_index} {timestamp} {description}")
            else:
                # No frame available, wait a bit before trying again
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            await asyncio.sleep(5) # Wait a bit before retrying after an error


if __name__ == "__main__":
    asyncio.run(main())