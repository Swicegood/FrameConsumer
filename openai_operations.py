import logging
from openai import AsyncOpenAI
from config import OPENAI_BASE_URL, OPENAI_API_KEY, OPENAI_VISION_URL, camera_names, camera_indexes
from datetime import datetime
import pytz


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
    from datetime import datetime
    import pytz
    from state_processing import time_zone_str

    # First check if this is the AXIS camera and if we're in specific time windows
    if camera_id == "AXIS_ID":
        # Get current time in the specified timezone
        tz = pytz.timezone(time_zone_str)
        current_time = datetime.now(tz)
        hour = current_time.hour
        minute = current_time.minute
        weekday = current_time.weekday()  # 0-6 (Monday-Sunday)
        
        # Convert current time to minutes since midnight for easier comparison
        current_minutes = hour * 60 + minute
        
        # Define time windows (in minutes since midnight)
        time_windows = [
            (4*60 + 30, 5*60),      # 4:30 AM - 5:00 AM
            (7*60 + 15, 12*60),     # 7:15 AM - 12:00 PM
            (12*60 + 30, 13*60),    # 12:30 PM - 1:00 PM
            (19*60, 20*60),         # 7:00 PM - 8:00 PM
        ]
        
        # Add the weekday-specific windows
        if weekday > 5:  # Saturday-Sunday
            time_windows.append((13*60, 16*60))  # 1:00 PM - 4:00 PM
        if weekday < 6:  # Monday-Saturday
            time_windows.append((16*60 + 15, 17*60 + 45))  # 4:15 PM - 5:45 PM
        else:  # Sunday
            time_windows.append((16*60, 16*60 + 30))       # 4:00 PM - 4:30 PM
            
        # Check if current time falls within any of the windows
        for start, end in time_windows:
            if start <= current_minutes <= end:
                return "nothing"

    # Continue with the existing logic for other cases
    additional_state = ""
    additional_definition = ""
    if camera_id in ["oaQllpjP0sk94nCV", "OSF13XTCKhpIkyXc"]:
        additional_state = ", door open"
        additional_definition = "'door open' means the door is open which it shoudn't be,"
    
    prompt = f"""Please analyze the following description of a scene and determine the average state of activity based strictly on the number of people explicitly present. Use the following guidelines:
•	“Bustling”: There is a lot of activity right now (many people and high energy).
•	“Big religious festival”: A special event or pageantry with clear evidence of festival-like activities.
•	“Religious or spiritual gathering”: Multiple people are explicitly gathering for a spiritual or religious purpose.
•	“Nothing”: There is no significant activity, and no people are observed.
•	“Single person present”: Exactly one person is present in the scene.
•	“People eating”: Explicit mention of people consuming food.
{additional_state}: {additional_definition}
<data start>

 {camera_id}: {aggregated_description}. 
<data end>
Rules:
•	Base your determination strictly on the description. Do not infer or assume activity beyond what is stated.
•	If the description mentions exactly one person, output “single person present”, regardless of the context.
•	If the description does not clearly describe multiple people or activity, output “nothing”.
•	Only output one or more of the predefined states: “bustling”, “big religious festival”, “religious or spiritual gathering”, “nothing”, “single person present”, “people eating”.

Output: one or more of the predefined states only. If you cannot determine a state, output “nothing”. Do not output any other text."""

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