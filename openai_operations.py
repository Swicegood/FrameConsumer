import logging
from openai import AsyncOpenAI
from config import OPENAI_BASE_URL, OPENAI_API_KEY, camera_names, camera_indexes
from state_processing import is_night_time, time_zone_str

logger = logging.getLogger(__name__)

client = AsyncOpenAI(base_url=OPENAI_BASE_URL, api_key=OPENAI_API_KEY)

async def process_image(base64_image):
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
        }
    ]

    try:
        completion = await client.chat.completions.create(
            model="not used",
            messages=messages,
            max_tokens=500,
        )

        return completion.choices[0].message.content, 0.0
    except Exception as e:
        logger.error(f"LLM completion error: {str(e)}")
        return None, None

async def process_facility_state(all_recent_descriptions):
    prompt = f"""Please analyze the following most recent descriptions from all cameras in the facility and determine the overall current state of the facility. Note "bustling" means a lot of activity right now, "festival happening" means special pageantry taking place, "crowd gathering" means people are gathering, "over capacity" means the building can not accomodate so many people,   "quiet" means not much activity, "person present" means an individual is there, and "people eating" means people are consuming food. Output only one of the following states: "bustling", "festival happening", "crowd gathering", "over capacity", "quiet", "person present" or "people eating". Please output only those words and nothing else.

Most Recent Descriptions from all cameras: {all_recent_descriptions}"""

    try:
        completion = await client.chat.completions.create(
            model="not used",
            messages=[
                {"role": "system", "content": "You are an AI tasked with determining the overall current state of a facility based on the most recent security camera descriptions from all areas."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100,
        )
        
        return completion.choices[0].message.content
    except Exception as e:
        return f"Error processing facility state: {str(e)}"

async def process_camera_state(camera_id, aggregated_description):
    additional_state = ""
    additional_definition = ""
    if camera_id in ["oaQllpjP0sk94nCV", "OSF13XTCKhpIkyXc"]:
        additional_state = ", door open"
        additional_definition = "'door open' means the door is open which it shoudn't be,"
    prompt = f"""Please analyze the following aggregated descriptions from the last hour for a single camera and determine the state of this specific area of the facility. Note "bustling" means a lot of activity right now, "festival happening" means special pageantry taking place, "crowd gathering" means people are gathering, {additional_definition} "quiet" means not much activity, "person present" means an individual is there, and "people eating" means people are consuming food.
    Output one or more of the following states: "bustling", "festival happening", "crowd gathering", "quiet", "person present"{additional_state} or "people eating". Please output only those words and nothing else.

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

        return completion.choices[0].message.content
    except Exception as e:
        return f"Error processing camera state: {str(e)}"

async def process_camera_states(hourly_aggregated_descriptions):
    camera_states = {}
    for camera_id, aggregated_description in hourly_aggregated_descriptions.items():
        state = await process_camera_state(camera_id, aggregated_description)
        if is_night_time(time_zone_str):
            state += ", night-time"
        camera_states[camera_names[camera_id]+' '+str(camera_indexes[camera_id])] = state
    return camera_states

async def process_image_for_curtains(base64_image):
    messages = [
        {
            "role": "system",
            "content": "You are an AI assistant tasked with analyzing images.",
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "In this image do you see any closed curtains? answer only yes or no. Only output yes or no, no other words"},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{base64_image}"
                    },
                },
            ],
        }
    ]

    try:
        completion = await client.chat.completions.create(
            model="not used",
            messages=messages,
            max_tokens=10,
        )

        return completion.choices[0].message.content.strip().lower()
    except Exception as e:
        logger.error(f"LLM completion error: {str(e)}")
        return None

async def process_descriptions_for_presence(descriptions):
    prompt = f"""Based on these descriptions is there a person or people present? answer only yes or no. Only output yes or no, no other words.

Descriptions: {descriptions}"""

    try:
        completion = await client.chat.completions.create(
            model="not used",
            messages=[
                {"role": "system", "content": "You are an AI tasked with determining if people are present based on security camera descriptions."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=10,
        )

        return completion.choices[0].message.content.strip().lower()
    except Exception as e:
        logger.error(f"LLM completion error: {str(e)}")
        return None