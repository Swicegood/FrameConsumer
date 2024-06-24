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
DB_PASSWORD = os.getenv('DB_PASSWORD', 'phare7462g')

OPENAI_BASE_URL = os.getenv('OPENAI_BASE_URL', 'http://192.168.0.199:1337/v1')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', 'lm-studio')

# Initialize OpenAI client
client = OpenAI(base_url=OPENAI_BASE_URL, api_key=OPENAI_API_KEY)

# Initialize Redis client
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

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

def process_frame(frame_data):
    """Process a single frame through the LLM."""
    # Parse the frame data
    data = eval(frame_data.decode('utf-8'))  # Decode bytes to string before eval
    camera_id = data['camera_id']
    camera_index = data['camera_index']
    timestamp = datetime.fromisoformat(data['timestamp'])
    
    # Assume data['frame'] is a base64-encoded PNG image string ready for the LLM
    base64_image = data['frame']
    
    # Run inference
    completion = client.chat.completions.create(
        model="not used",
        messages=[
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
                            "url": f"data:image/jpeg;base64,{base64_image}"
                        },
                    },
                ],
            }
        ],
        max_tokens=1000,
        stream=True
    )

    description = ""
    for chunk in completion:
        if chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)
    
    # Placeholder for future confidence extraction
    confidence = 0.0
    
    return camera_id, camera_index, timestamp, description, confidence
   

def store_results(conn, camera_id, camera_index, timestamp, description, confidence):
    """Store the results in the database."""
    cur = conn.cursor()
    cur.execute(
        sql.SQL("INSERT INTO frame_analysis (camera_id, camera_index, timestamp, llm_description, confidence) VALUES (%s, %s, %s, %s, %s)"),
        (camera_id, camera_index, timestamp, description, confidence)
    )
    conn.commit()

def main():
    conn = initialize_database()
    
    while True:
        try:
            # Try to get a frame from the Redis queue
            frame_data = redis_client.blpop(REDIS_QUEUE, timeout=1)
            
            if frame_data:
                # Process the frame
                camera_id, camera_index, timestamp, description, confidence = process_frame(frame_data[1])
                
                # Store the results
                store_results(conn, camera_id, camera_index, timestamp, description, confidence)
                
                logging.info(f"Processed and stored frame from camera {camera_index}")
            else:
                # No frame available, wait a bit before trying again
                time.sleep(1)
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            time.sleep(5)  # Wait a bit before retrying after an error

if __name__ == "__main__":
    main()