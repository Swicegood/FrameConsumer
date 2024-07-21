import asyncio
import websockets
import json
import logging
from config import DJANGO_WEBSOCKET_URL

logger = logging.getLogger(__name__)

async def connect_websocket():
    while True:
        try:
            websocket = await websockets.connect(DJANGO_WEBSOCKET_URL)
            logger.info("Connected to Django WebSocket")
            return websocket
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {str(e)}")
            await asyncio.sleep(5)

async def send_to_django(websocket, message):
    while True:
        try:
            if not websocket:
                websocket = await connect_websocket()
            await websocket.send(json.dumps({'message': message}))
            return
        except websockets.exceptions.ConnectionClosed:
            logger.error("WebSocket connection closed. Attempting to reconnect...")
            websocket = None
        except Exception as e:
            logger.error(f"Error sending message to Django: {str(e)}")
            websocket = None
        await asyncio.sleep(1)