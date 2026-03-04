# src/app_google/check_db.py
import asyncio

from sqlalchemy import text
from src.app_google.database import engine
from src.app_log import logger


async def check():
    logger.info("Проверка базы данных...")
    async with engine.connect() as conn:
        # 1. Какая БД и пользователь
        result = await conn.execute(text("SELECT current_database(), current_user"))
        db, user = result.fetchone()
        logger.info(f"БД: {db}, пользователь: {user}")

        # 2. Таблицы в схеме public
        result = await conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' ORDER BY table_name
        """))
        tables = [r[0] for r in result.fetchall()]
        logger.info(f"📋 Таблицы в public: {tables if tables else '(пусто)'}")

        # 3. Если task_list есть — покажем структуру
        if 'task_list' in tables:
            result = await conn.execute(text("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = 'task_list'
                ORDER BY ordinal_position
            """))
            logger.info("Структура task_list:")
            for col in result.fetchall():
                logger.info(f"   • {col[0]}: {col[1]} {'(NULL)' if col[2] == 'YES' else '(NOT NULL)'}")

            # Проверка уникального индекса
            result = await conn.execute(text("""
                SELECT indexname, indexdef FROM pg_indexes 
                WHERE schemaname = 'public' AND tablename = 'task_list'
            """))
            indexes = result.fetchall()
            logger.info(f"Индексы: {indexes if indexes else '(нет)'}")
        else:
            logger.info("Таблица task_list НЕ найдена в схеме public")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(check())
