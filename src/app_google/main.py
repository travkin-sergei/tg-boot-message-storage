# src/main.py
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from src.app_google.models import Base

from src.config import DATABASE_URL
from src.app_log import logger
from src.app_google.pipline.line1 import sync_google_sheet_to_db
from src import config


GOOGLE_CONFIG = {
    "list_name": "Telegram",
    "file_code": config.GOOGLE_FILE,
    "output_parquet": "temp_data/telegram.parquet",
    "column_mapping": {
        "№ п/п": "key",
        "дата": "date",
        "Ссылка": "link",
        "Краткое описание": "short_description",
        "Кто нашел ссылку и написал диалог": "autor",
        "Текст комментария": "text",
        "Исправления": "corrections",
        "Ответственный за публикацию": "responsible",
        "Статус опубликования": "status"
    }
}


async def init_db(engine):
    """
    Создаёт все таблицы в БД на основе моделей SQLAlchemy.
    Безопасно: если таблица уже есть — ничего не делает.
    """
    async with engine.begin() as conn:
        # create_all проверяет существование таблиц перед созданием
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Проверка/создание таблиц завершено")


async def main():
    """Точка входа в приложение"""
    logger.info("Запуск синхронизации Google Sheets → DB")

    # 1. Создаем асинхронный движок
    engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    try:
        # 2. Инициализация БД: создание таблиц
        await init_db(engine)

        # 3. Открываем сессию и запускаем пайплайн
        async with async_session() as session:
            stats = await sync_google_sheet_to_db(
                session=session,
                list_name=GOOGLE_CONFIG["list_name"],
                file_code=GOOGLE_CONFIG["file_code"],
                output_parquet=GOOGLE_CONFIG["output_parquet"],
                column_mapping=GOOGLE_CONFIG["column_mapping"]
            )

            # 4. Вывод результатов
            logger.info(f"Итоги синхронизации:")
            logger.info(f"Успешно: {stats.get('success', 0)}")
            logger.info(f"Ошибок: {stats.get('errors', 0)}")
            logger.info(f"Всего: {stats.get('total', 0)}")

    except Exception as error:
        logger.error(f"Критическая ошибка при запуске: {error}", exc_info=True)
    finally:
        # 5. Закрываем соединения
        await engine.dispose()
        logger.info("🏁 Завершение работы")


if __name__ == "__main__":
    asyncio.run(main())
