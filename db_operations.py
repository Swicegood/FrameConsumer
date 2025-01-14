import asyncio
import logging
import psycopg2
from psycopg2 import sql
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
from datetime import datetime, time

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
                    confidence FLOAT,
                    camera_name VARCHAR(255)
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

import asyncpg

async def store_results(pool, camera_id, camera_index, timestamp, description, confidence, image_data, camera_name):
    async with pool.acquire() as conn:
        async with conn.transaction():
            binary_data_id = await conn.fetchval(
                "INSERT INTO visionmon_binary_data (data) VALUES ($1) RETURNING id",
                image_data
            )
            
            await conn.execute("""
                INSERT INTO visionmon_metadata 
                (data_id, camera_id, camera_index, timestamp, description, confidence, camera_name) 
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, binary_data_id, camera_id, camera_index, timestamp, description, confidence, camera_name)
    
    logger.info(f"Stored results and image for camera {camera_index}")

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

async def fetch_aggregated_descriptions(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT camera_id, STRING_AGG(description, ' ') as descriptions
        FROM visionmon_metadata
        WHERE (camera_id, timestamp) IN (
            SELECT camera_id, MAX(timestamp)
            FROM visionmon_metadata
            GROUP BY camera_id
        )
        GROUP BY camera_id
    """)
    return dict(cur.fetchall())

async def fetch_descriptions_for_timerange(conn, camera_id, start_time, end_time):
    cur = conn.cursor()
    
    # Convert time objects to strings in HH:MM format
    start_time_str = start_time.strftime('%H:%M')
    end_time_str = end_time.strftime('%H:%M')
    
    cur.execute("""
        SELECT STRING_AGG(description, ' ') as descriptions
        FROM visionmon_metadata
        WHERE camera_id = %s
        AND (CAST(timestamp AS TIME) BETWEEN %s AND %s)
        AND timestamp::date = CURRENT_DATE
    """, (camera_id, start_time_str, end_time_str))
    
    result = cur.fetchone()
    return result[0] if result else None

# You may want to add a function to get the latest frame for a specific camera
async def get_latest_frame(conn, camera_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT vb.data
        FROM visionmon_binary_data vb
        JOIN visionmon_metadata vm ON vb.id = vm.data_id
        WHERE vm.camera_id = %s
        ORDER BY vm.timestamp DESC
        LIMIT 1
    """, (camera_id,))
    result = cur.fetchone()
    return result[0] if result else None

async def update_timestamp(pool, camera_id, timestamp):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE visionmon_metadata
            SET timestamp = $2
            WHERE camera_id = $1 AND timestamp = (
                SELECT MAX(timestamp)
                FROM visionmon_metadata
                WHERE camera_id = $1
            )
        """, camera_id, timestamp)