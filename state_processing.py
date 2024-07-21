import json
import logging
from openai_operations import process_facility_state, process_camera_states
from db_operations import fetch_latest_descriptions, fetch_hourly_aggregated_descriptions
from redis_operations import publish_state_result

logger = logging.getLogger(__name__)

async def process_state(db_conn, redis_client):
    try:
        # Fetch the latest descriptions for all cameras (for facility state)
        latest_descriptions = await fetch_latest_descriptions(db_conn)
        
        # Fetch aggregated descriptions from last hour for each camera (for camera states)
        hourly_aggregated_descriptions = await fetch_hourly_aggregated_descriptions(db_conn)
        
        # Process overall facility state
        all_recent_descriptions = " ".join(latest_descriptions.values())
        facility_state = await process_facility_state(all_recent_descriptions)
        
        # Process individual camera states
        camera_states = await process_camera_states(hourly_aggregated_descriptions)
        
        # Send results to Redis for Django to pick up
        state_result = json.dumps({
            'facility_state': facility_state,
            'camera_states': camera_states
        })
        await publish_state_result(redis_client, state_result)
        
        print(f"Facility State: {facility_state}")
        print(f"Camera States: {camera_states}")
        
    except Exception as e:
        logger.error(f"Error processing state: {str(e)}")