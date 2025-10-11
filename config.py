import os

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', '192.168.0.71')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_QUEUE = os.getenv('REDIS_QUEUE', 'frame_queue')
REDIS_CAMERA_CHANNEL = 'camera_processing'
REDIS_STATE_CHANNEL = 'state_processing'
REDIS_STATE_RESULT_CHANNEL = 'state_result'

# Database configuration
DB_HOST = os.getenv('DB_HOST', '192.168.0.71')
DB_NAME = os.getenv('DB_NAME', 'visionmon')
DB_USER = os.getenv('DB_USER', 'pguser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'pgpass')

# OpenAI configuration
OPENAI_BASE_URL = os.getenv('OPENAI_BASE_URL', 'http://192.168.0.55:1234/v1')
OPENAI_VISION_URL = os.getenv('OPENAI_VISION_URL', OPENAI_BASE_URL)
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', 'lm-studio')

# Django WebSocket URL
DJANGO_WEBSOCKET_URL = os.getenv('DJANGO_WEBSOCKET_URL', 'ws://localhost:8001/ws/llm_output/')

PROCESS_STATE = os.getenv('PROCESS_STATE', False)

MODULUS = int(os.getenv('MODULUS', "1"))
INSTANCE_INDEX = int(os.getenv('INSTANCE_INDEX', "0"))
ADDITIONAL_INDEX = int(os.getenv('ADDITIONAL_INDEX', "2"))

# Camera information - reduced to 8 cameras for faster processing
camera_names = {
    "rHWz9GRDFxrOZF7b": "Down_Pujari",
    "5SJZivf8PPsLWw2n": "Hall",
    "g8rHNVCflWO1ptKN": "Kitchen",
    "iY9STaEt7K9vS8yJ": "Prabhupada",
    "IOKAu7MMacLh79zn": "Temple",
    "sHlS7ewuGDEd2ef4": "Up_Pujari",
    "AXIS_ID": "Axis",
    "prXH5H6e9GxOij1Z": "Front_Driveway"
}

camera_indexes = {
    'rHWz9GRDFxrOZF7b': 1,  # Down_Pujari
    '5SJZivf8PPsLWw2n': 2,  # Hall
    'g8rHNVCflWO1ptKN': 3,  # Kitchen
    'iY9STaEt7K9vS8yJ': 4,  # Prabhupada
    'IOKAu7MMacLh79zn': 5,  # Temple
    'sHlS7ewuGDEd2ef4': 6,  # Up_Pujari
    'AXIS_ID': 7,           # Axis
    'prXH5H6e9GxOij1Z': 8   # Front_Driveway
}

CAMERA_IDS = list(camera_indexes.keys())

camera_descriptions = {
    "Down_Pujari": "The area downstairs where pujaris prepare dresses for the deiteis, typically only occupied by pujaris.",
    "Hall": "Dining and event hall, typically used for prasadam distribution, classes, or gatherings. May be occupied by visitors or devotees.",
    "Kitchen": "The temple kitchen, typically used for cooking prasadam or preparing food. May be occupied by kitchen staff or pujaris.",
    "Prabhupada": "The main hall of the temple, typically used for kirtans, classes, or gatherings. May be occupied by visitors or devotees. There is a statue of Srila Prabhupada in this area.",
    "Temple": "The main hall of the temple, typically used for kirtans, classes, or gatherings. May be occupied by visitors or devotees.",
    "Up_Pujari": "The upstairs area where pujaris prepare dresses for the deities, typically only occupied by pujaris.",
    "Axis": "Camera monitoring specific area with deity viewing.",
    "Front_Driveway": "The driveway in front of the temple, typically used for arrivals, departures, or parking."
}