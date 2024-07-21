import asyncio
import logging
import psycopg2
from psycopg2 import sql
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

async def connect_database():
    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            cur = conn.cursor()
            
            # Create tables and indexes
            cur.execute("""
                CREATE TABLE IF NOT EXISTS visionmon_binary_data (
                    id SERIAL PRIMARY KEY,
                    data BYTEA NOT NULL
                )
            """)
            
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
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_metadata_data_id ON visionmon_metadata(data_id);
            """)
            
            conn.commit()
            
            logger.info("Connected to PostgreSQL database and ensured schema is up to date")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL or set up schema: {str(e)}")
            await asyncio.sleep(5)

async def store_results(conn, camera_id, camera_index, timestamp, description, confidence, image_data):
    while True:
        try:
            cur = conn.cursor()
            
            cur.execute("BEGIN;")
            
            cur.execute(
                "INSERT INTO visionmon_binary_data (data) VALUES (%s) RETURNING id;",
                (psycopg2.Binary(image_data),)
            )
            binary_data_id = cur.fetchone()[0]
            
            cur.execute(
                """INSERT INTO visionmon_metadata 
                   (data_id, camera_id, camera_index, timestamp, description, confidence) 
                   VALUES (%s, %s, %s, %s, %s, %s);""",
                (binary_data_id, camera_id, camera_index, timestamp, description, confidence)
            )
            
            cur.execute("COMMIT;")
            
            logger.info(f"Stored results and image for camera {camera_index}")
            return
        except psycopg2.Error as e:
            logger.error(f"Database error: {str(e)}")
            cur.execute("ROLLBACK;")
            await asyncio.sleep(5)

async def fetch_latest_descriptions(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT camera_id, description
        FROM visionmon_metadata
        WHERE (camera_id, timestamp) IN (
            SELECT camera_id, MAX(timestamp)
            FROM visionmon_metadata
            GROUP BY camera_id
        )
    """)
    return dict(cur.fetchall())

async def fetch_hourly_aggregated_descriptions(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT camera_id, STRING_AGG(description, ' ') as descriptions
        FROM visionmon_metadata
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
        GROUP BY camera_id
    """)
    return dict(cur.fetchall())