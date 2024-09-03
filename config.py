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
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', 'lm-studio')

# Django WebSocket URL
DJANGO_WEBSOCKET_URL = os.getenv('DJANGO_WEBSOCKET_URL', 'ws://localhost:8001/ws/llm_output/')

PROCESS_STATE = os.getenv('PROCESS_STATE', False)

MODULUS = int(os.getenv('MODULUS', "1"))
INSTANCE_INDEX = int(os.getenv('INSTANCE_INDEX', "0"))
ADDITIONAL_INDEX = int(os.getenv('ADDITIONAL_INDEX', "2"))

# Camera information
camera_names = {
    "I6Dvhhu1azyV9rCu": "Audio_Visual", "oaQllpjP0sk94nCV": "Bhoga_Shed", "PxnDZaXu2awYbMmS": "Back_Driveway",
    "mKlJgNx7tXwalch1": "Deck_Stairs", "rHWz9GRDFxrOZF7b": "Down_Pujari", "LRqgKMMjjJbNEeyE": "Field",
    "94uZsJ2yIouIXp2x": "Greenhouse", "5SJZivf8PPsLWw2n": "Hall", "g8rHNVCflWO1ptKN": "Kitchen",
    "t3ZIWTl9jZU1JGEI": "Pavillion", "iY9STaEt7K9vS8yJ": "Prabhupada", "jlNNdFFvhQ2o2kmn": "Stage",
    "IOKAu7MMacLh79zn": "Temple", "sHlS7ewuGDEd2ef4": "Up_Pujari", "OSF13XTCKhpIkyXc": "Walk-in",
    "jLUEC60zHGo7BXfj": "Walkway", "AXIS_ID": "Axis"
}

camera_indexes = {'I6Dvhhu1azyV9rCu': 1, 'oaQllpjP0sk94nCV': 3, 'PxnDZaXu2awYbMmS': 2, 'mKlJgNx7tXwalch1': 4, 'rHWz9GRDFxrOZF7b': 5, 'LRqgKMMjjJbNEeyE': 6, '94uZsJ2yIouIXp2x': 7, '5SJZivf8PPsLWw2n': 8, 'g8rHNVCflWO1ptKN': 9, 't3ZIWTl9jZU1JGEI': 10, 'iY9STaEt7K9vS8yJ': 11, 'jlNNdFFvhQ2o2kmn': 12, 'IOKAu7MMacLh79zn': 13, 'sHlS7ewuGDEd2ef4': 14, 'OSF13XTCKhpIkyXc': 15, 'jLUEC60zHGo7BXfj': 16, 'AXIS_ID': 17}

CAMERA_IDS = list(camera_indexes.keys())

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