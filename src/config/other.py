# src/config2.py
import os
from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
API_TOKEN = os.getenv('APP_GOOGLE_TOKEN')