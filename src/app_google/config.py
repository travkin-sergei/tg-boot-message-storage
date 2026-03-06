# src/app_google/config.py
"""
Список констант приложения APP_GOOGLE
"""
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

APP_GOOGLE_FILE = os.getenv('APP_GOOGLE_FILE')
"""Наверное стоить заменить имя листа, но я не уверен."""
SHEET_NAME: str = "02.03.2026"  # datetime.now().strftime("%d.%m.%Y")
DB_SCHEMA: str = "test"
COLUMN_MAPPING: dict[str, str] = {
    "№ п/п": "number",
    "дата комментария": "date_comment",
    "Ссылка": "link_post",
    "Краткое описание": "short_description",
    "Кол-во подписчиков": "subscribers",
    "Текст комментария": "comment",
    "Исправления": "corrections",
    "Ответственный за публикацию": "responsible",
    "Статус опубликования": "status",
}
