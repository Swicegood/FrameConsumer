import logging
from openai import AsyncOpenAI
from config import OPENAI_BASE_URL, OPENAI_API_KEY, OPENAI_VISION_URL, camera_names, camera_indexes


logger = logging.getLogger(__name__)

client = AsyncOpenAI(base_url=OPENAI_BASE_URL, api_key=OPENAI_API_KEY)
vision_client = AsyncOpenAI(base_url=OPENAI_VISION_URL, api_key=OPENAI_API_KEY)

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
        completion = await vision_client.chat.completions.create(
            model="llava",
            messages=messages,
            max_tokens=200,
        )

        return completion.choices[0].message.content, 0.0
    except Exception as e:
        logger.error(f"LLM completion error: {str(e)}")
        return None, None

async def process_facility_state(all_recent_descriptions):
    prompt = f"""Please analyze the following most recent descriptions from all cameras in the facility and determine the overall current state of the facility. Note "bustling" means a lot of activity right now, "big religious festival" means special pageantry taking place, "religious or spiritual gathering" means people are gathering, "over capacity" means the building can not accomodate so many people,   "nothing" means not significant activity, "single person present" means an individual is there, and "people eating" means people are consuming food. Output only one of the following states: "bustling", "big religious festival", "religious or spiritual gathering", "over capacity", "nothing", "single person present" or "people eating". Please output only those words and nothing else.

Most Recent Descriptions from all cameras: {all_recent_descriptions}"""

    try:
        completion = await client.chat.completions.create(
            model="llava",
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
    prompt = f"""Please analyze the following aggregated descriptions of a scene and determine the average state of the scene. Note "bustling" means a lot of activity right now, "big religious festival" means special pageantry taking place, "religious or spiritual gathering" means people are gathering, {additional_definition} "nothing" means no significant activity, "single person present" means an individual is there, and "people eating" means people are consuming food.
    Output one or more of the following states: "bustling", "big religious festival", "religious or spiritual gathering", "nothing", "single person present"{additional_state} or "people eating". Please output only those words and nothing else. If you cant't determine the state, output "nothing". Do not output any other words, besides the states I listed.

Aggregated Descriptions from the scene {camera_id}: {aggregated_description}. 
Now, like you were instructed before seeing all the descriptions, give the most common state of the scene. Note "bustling" means a lot of activity right now, "big religious festival" means special pageantry taking place, "religious or spiritual gathering" means people are gathering, {additional_definition} "nothing" means no significant activity, "single person present" means an individual is there, and "people eating" means people are consuming food.
    Output one or more of the following states: "bustling", "big religious festival", "religious or spiritual gathering", "nothing", "single person present"{additional_state} or "people eating". Please output only those words and nothing else. If you cant't determine the state, output "nothing". Do not output any other words, besides the states I listed."""

    try:
        completion = await client.chat.completions.create(
            model="llava",
            messages=[
                {"role": "system", "content": "You are an AI tasked with determining the state of a specific area in a facility based on aggregated security descriptions of a scene over 1 hour.  Look for patterns in the instant descriptions over the whole time period to determine the state."},
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
        
        from state_processing import is_night_time, time_zone_str
        
        if is_night_time(time_zone_str):
            state += ", night-time"
        camera_states[camera_names[camera_id]+' '+str(camera_indexes[camera_id])] = state
    return camera_states

async def process_descriptions_for_presence(descriptions):
    prompt = f"""Based on these descriptions is there a single person or people present? Answer only yes or no. Only output yes or no, no other words.

Descriptions: {descriptions} Based on these descriptions is there a single person or people present or religious or spiritual gathering? Answer only yes or no. Only output yes or no, no other words."""

    try:
        completion = await client.chat.completions.create(
            model="llava",
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