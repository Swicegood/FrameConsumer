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

# ... (keep existing imports and configurations)

# Add new Redis channels for signaling
REDIS_CAMERA_CHANNEL = 'camera_processing'
REDIS_STATE_CHANNEL = 'state_processing'
REDIS_STATE_RESULT_CHANNEL = 'state_result'

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

DJANGO_WEBSOCKET_URL = os.getenv('DJANGO_WEBSOCKET_URL', 'ws://localhost:8001/ws/llm_output/')

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

camera_indexes = {'I6Dvhhu1azyV9rCu': 1, 'oaQllpjP0sk94nCV': 3, 'PxnDZaXu2awYbMmS': 2, 'mKlJgNx7tXwalch1': 4, 'rHWz9GRDFxrOZF7b': 5, 'LRqgKMMjjJbNEeyE': 6, '94uZsJ2yIouIXp2x': 7, '5SJZivf8PPsLWw2n': 8, 'g8rHNVCflWO1ptKN': 9, 't3ZIWTl9jZU1JGEI': 10, 'iY9STaEt7K9vS8yJ': 11, 'jlNNdFFvhQ2o2kmn': 12, 'IOKAu7MMacLh79zn': 13, 'sHlS7ewuGDEd2ef4': 14, 'OSF13XTCKhpIkyXc': 15, 'jLUEC60zHGo7BXfj': 16}

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
    "Up_Pujari": "The upstairs area where pujaris prepare dresses for the deities, typically only occupied by pujaris.",
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
            
            # Create binary_data table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS visionmon_binary_data (
                    id SERIAL PRIMARY KEY,
                    data BYTEA NOT NULL
                )
            """)
            
            # Create metadata table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS visionmon_metadata (
                    id SERIAL PRIMARY KEY,
                    data_id INTEGER NOT NULL,
                    camera_id VARCHAR(255),
                    camera_index INTEGER,
                    timestamp TIMESTAMP,
                    description TEXT,
                    confidence FLOAT
                )
            """)
            
            # Add foreign key constraint if it doesn't exist
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints 
                        WHERE constraint_name = 'fk_binary_data' AND table_name = 'visionmon_metadata'
                    ) THEN
                        ALTER TABLE visionmon_metadata
                        ADD CONSTRAINT fk_binary_data
                        FOREIGN KEY (data_id)
                        REFERENCES visionmon_binary_data (id)
                        ON DELETE CASCADE;
                    END IF;
                END $$;
            """)
            
            # Create index on data_id if it doesn't exist
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_metadata_data_id ON visionmon_metadata(data_id);
            """)
            
            # Commit the changes
            db_conn.commit()
            
            logging.info("Connected to PostgreSQL database and ensured schema is up to date")
            return
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to PostgreSQL or set up schema: {str(e)}")
            if db_conn:
                db_conn.close()
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
    image_data = data['frame']  # This should be the binary image data
    base64_image = base64.b64encode(image_data).decode('utf-8')

    camera_name = camera_names.get(camera_id, "Unknown")
    camera_description = camera_descriptions.get(camera_name, "No specific description available.")

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
            max_tokens=500,
            stream=True
        )

        description = ""
        async for chunk in completion:
            if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                description += chunk.choices[0].delta.content
                print(chunk.choices[0].delta.content, end="", flush=True)

        confidence = 0.0  # Placeholder for future confidence extraction
        return camera_id, camera_index, timestamp, description, confidence, image_data

    except Exception as e:
        logging.error(f"LLM completion error: {str(e)}")
        return None

async def store_results(camera_id, camera_index, timestamp, description, confidence, image_data):
    """Store the results and image data in the database."""
    global db_conn
    while True:
        try:
            if not db_conn or db_conn.closed:
                await connect_database()
            
            cur = db_conn.cursor()
            
            # Start transaction
            cur.execute("BEGIN;")
            
            # Insert binary data
            cur.execute(
                "INSERT INTO visionmon_binary_data (data) VALUES (%s) RETURNING id;",
                (psycopg2.Binary(image_data),)
            )
            binary_data_id = cur.fetchone()[0]
            
            # Insert metadata
            cur.execute(
                """INSERT INTO visionmon_metadata 
                   (data_id, camera_id, camera_index, timestamp, description, confidence) 
                   VALUES (%s, %s, %s, %s, %s, %s);""",
                (binary_data_id, camera_id, camera_index, timestamp, description, confidence)
            )
            
            # Commit transaction
            cur.execute("COMMIT;")
            
            logging.info(f"Stored results and image for camera {camera_index}")
            return
        except psycopg2.Error as e:
            logging.error(f"Database error: {str(e)}")
            cur.execute("ROLLBACK;")
            db_conn = None
            await asyncio.sleep(5)
import json
import logging
from openai import AsyncStream

async def process_state():
    """Process the overall state and individual camera states."""
    global db_conn
    try:
        if not db_conn or db_conn.closed:
            await connect_database()
        
        cur = db_conn.cursor()
        
        # Fetch the latest descriptions for all cameras (for facility state)
        cur.execute("""
            SELECT camera_id, description
            FROM visionmon_metadata
            WHERE (camera_id, timestamp) IN (
                SELECT camera_id, MAX(timestamp)
                FROM visionmon_metadata
                GROUP BY camera_id
            )
        """)
        latest_descriptions = dict(cur.fetchall())
        
        # Fetch aggregated descriptions from last hour for each camera (for camera states)
        cur.execute("""
            SELECT camera_id, STRING_AGG(description, ' ') as descriptions
            FROM visionmon_metadata
            WHERE timestamp >= NOW() - INTERVAL '1 hour'
            GROUP BY camera_id
        """)
        hourly_aggregated_descriptions = dict(cur.fetchall())
        
        # Process overall facility state
        all_recent_descriptions = " ".join(latest_descriptions.values())
        facility_state_stream = await process_facility_state(all_recent_descriptions)
        facility_state = await extract_content(facility_state_stream)
        
        # Process individual camera states
        camera_states_stream = await process_camera_states(hourly_aggregated_descriptions)
        camera_states = await extract_content(camera_states_stream)
        
        # Send results to Redis for Django to pick up
        state_result = json.dumps({
            'facility_state': facility_state,
            'camera_states': camera_states
        })
        await redis_client.publish(REDIS_STATE_RESULT_CHANNEL, state_result)
        print(f"Facility State: {facility_state}")
        print(f"Camera States: {camera_states}")
        
    except Exception as e:
        logging.error(f"Error processing state: {str(e)}")

async def extract_content(stream):
    """Extract content from AsyncStream object."""
    if isinstance(stream, AsyncStream):
        content = ""
        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                content += chunk.choices[0].delta.content
        return content
    return stream  # If it's not an AsyncStream, return as is
        
async def process_facility_state(all_recent_descriptions):
    prompt = f"""Please analyze the following most recent descriptions from all cameras in the facility and determine the overall current state of the facility. 
    Output one or more of the following states: "busy", "off-hours", "festival happening", "night-time", "quiet" or "meal time". 
    Please output only those words.

Most Recent Descriptions from all cameras: {all_recent_descriptions}"""

    try:
        completion = await client.chat.completions.create(
            model="not used",
            messages=[
                {"role": "system", "content": "You are an AI tasked with determining the overall current state of a facility based on the most recent security camera descriptions from all areas."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=20,
        )
                
        return completion.choices[0].message.content
    
    except Exception as e:
        return f"Error processing facility state: {str(e)}"
    
async def process_camera_states(hourly_aggregated_descriptions):
    camera_states = {}
    for camera_id, aggregated_description in hourly_aggregated_descriptions.items():
        prompt = f"""Please analyze the following aggregated descriptions from the last hour for a single camera and determine the state of this specific area of the facility. 
        Output one or more of the following states: "busy", "off-hours", "festival happening", "night-time", "quiet" or "meal time". 
        Please output only those words and no more.

Aggregated Descriptions from the last hour for camera {camera_id}: {aggregated_description}"""

        try:
            completion = await client.chat.completions.create(
                model="not used",
                messages=[
                    {"role": "system", "content": "You are an AI tasked with determining the state of a specific area in a facility based on aggregated security camera descriptions from the last hour."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=20,
            )

            camera_states[camera_names[camera_id]+' '+str(camera_indexes[camera_id])] = completion.choices[0].message.content
        except Exception as e:
            camera_states[camera_names[camera_id]+' '+str(camera_indexes[camera_id])] = f"Error processing camera state: {str(e)}"
    
    return camera_states

async def main():
    global redis_client
    camera_count = 0
    state_processing_interval = 60  # Process state every 60 seconds
    last_state_processing = 0

    while True:
        try:
            if not redis_client:
                await connect_redis()
            if not db_conn or db_conn.closed:
                await connect_database()
            if not websocket:
                await connect_websocket()

            # Check for frame in the queue
            frame_data = await redis_client.blpop(REDIS_QUEUE, timeout=1)
            
            if frame_data:
                result = await process_frame(frame_data[1])
                if result:
                    camera_id, camera_index, timestamp, description, confidence, image_data = result
                    await store_results(camera_id, camera_index, timestamp, description, confidence, image_data)
                    camera_name = camera_names.get(camera_id, 'Unknown')
                    await send_to_django(f"{camera_name} {camera_index} {timestamp} {description}")
                    
                    camera_count += 1
                    if camera_count >= len(camera_names) or camera_count == 1:
                        # All cameras processed, check if it's time to process state
                        current_time = time.time()
                        if current_time - last_state_processing >= state_processing_interval:
                            await process_state()
                            last_state_processing = current_time
                        camera_count = 0

            # Check for state processing request from Django
            state_request = await redis_client.blpop(REDIS_STATE_CHANNEL, timeout=1)
            if state_request:
                await process_state()

            await asyncio.sleep(0.1)  # Short sleep to prevent CPU overuse
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())