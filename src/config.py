# src/config.py
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv('BOT_TOKEN')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
API_TOKEN = os.getenv('API_TOKEN')
GOOGLE_FILE = os.getenv('GOOGLE_FILE').split(',')[0]

ADMIN_IDS = [int(id.strip()) for id in os.getenv('ADMIN_IDS', '').split(',') if id.strip()]
PACKET_INTERVAL = 5

DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}"f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS
