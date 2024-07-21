import logging
from openai import AsyncOpenAI
from config import OPENAI_BASE_URL, OPENAI_API_KEY, camera_names, camera_indexes

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
            stream=True
        )

        description = ""
        async for chunk in completion:
            if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                description += chunk.choices[0].delta.content
                print(chunk.choices[0].delta.content, end="", flush=True)

        return description, 0.0  # Placeholder confidence
    except Exception as e:
        logger.error(f"LLM completion error: {str(e)}")
        return None, None

async def process_facility_state(all_recent_descriptions):
    prompt = f"""Please analyze the following most recent descriptions from all cameras in the facility and determine the overall current state of the facility. Note "busy" means a lot of activity right now, "festival happening" means special pageantry taking place, "crowd gathering" means people are gathering, "night-time" means it is dark, "quiet" means not much activity, "person present" means an individual is there, and "people eating" means people are consuming food. Output only one of the following states: "busy", "festival happening", "crowd gathering", "night-time", "quiet", "person present" or "people eating". Please output only those words and nothing else.

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
    prompt = f"""Please analyze the following aggregated descriptions from the last hour for a single camera and determine the state of this specific area of the facility. Note "busy" means a lot of activity right now, "festival happening" means special pageantry taking place, "crowd gathering" means people are gathering, "night-time" means it is dark, "quiet" means not much activity, "person present" means an individual is there, and "people eating" means people are consuming food.
    Output one or more of the following states: "busy", "festival happening", "crowd gathering", "night-time", "quiet", "person present" or "people eating". Please output only those words and nothing else.

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
        camera_states[camera_names[camera_id]+' '+str(camera_indexes[camera_id])] = state
    return camera_states