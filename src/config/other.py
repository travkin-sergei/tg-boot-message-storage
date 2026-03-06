#src/config.py
import os
from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
GOOGLE_FILE = os.getenv('GOOGLE_FILE')