import redis
import json
import cv2
import numpy as np
import logging
import time
from datetime import datetime
import psycopg2
from psycopg2 import sql
# Adapted from OpenAI's Vision example 
from openai import OpenAI
import base64
import requests

# Point to the local server
client = OpenAI(base_url="http://192.168.0.199:1337/v1", api_key="lm-studio")


# Read the image and encode it to base64:
base64_image = ""
try:
  image = open(path.replace("'", ""), "rb").read()
  base64_image = base64.b64encode(image).decode("utf-8")
except:
  print("Couldn't read the image. Make sure the path is correct and the file exists.")
  exit()

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
        {"type": "text", "text": "What’s in this image?"},
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

for chunk in completion:
  if chunk.choices[0].delta.content:
    print(chunk.choices[0].delta.content, end="", flush=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Redis configuration
REDIS_HOST = '192.168.0.71'
REDIS_PORT = 6379
REDIS_QUEUE = 'frame_queue'

# PostgreSQL configuration
DB_HOST = '192.168.0.71'
DB_NAME = 'visionmon'
DB_USER = 'pguser'
DB_PASSWORD = 'phare7462g'

# LLM configuration

# Initialize Redis client
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# Initialize LLM


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
    data = eval(frame_data)  # Be cautious with eval, ensure input is trusted
    camera_id = data['camera_id']
    camera_index = data['camera_index']
    timestamp = datetime.fromisoformat(data['timestamp'])
    
    # Decode the frame
    nparr = np.frombuffer(data['frame'], np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    # Prepare the image for the LLM
    # Read the image and encode it to base64:
base64_image = ""
try:
  
  base64_image = base64.b64encode(frame).decode("utf-8")
except:
  print("Couldn't read the image.")
  exit()
    
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
        {"type": "text", "text": "What’s in this image?"},
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

for chunk in completion:
  if chunk.choices[0].delta.content:
    description = chunk.choices[0].delta.content
    print(chunk.choices[0].delta.content, end="", flush=True)
    
    # To be added later, igonre for now: Get the predicted class and confidence
    
    
    # To be added later, igonre for now. Get the class label (adjust based on your model's output)

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
        # Try to get a frame from the Redis queue
        frame_data = redis_client.blpop(REDIS_QUEUE, timeout=1)
        
        if frame_data:
            try:
                # Process the frame
                camera_id, camera_index, timestamp, description, confidence = process_frame(frame_data[1])
                
                # Store the results
                store_results(conn, camera_id, camera_index, timestamp, description, confidence)
                
                logging.info(f"Processed and stored frame from camera {camera_index}")
            except Exception as e:
                logging.error(f"Error processing frame: {str(e)}")
        else:
            # No frame available, wait a bit before trying again
            time.sleep(1)

if __name__ == "__main__":
    main()