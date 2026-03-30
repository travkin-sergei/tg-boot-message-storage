# src/app_groups/main.py
"""
Точка входа для микросервиса app_groups.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# === Ядро (строгое переиспользование) ===
from src.config.logger import config_logging, logger
from src.config.database import DBManager, AsyncDBConnection

# === Локальные модули ===
from src.app_groups.api import router
from src.app_groups.config import config, DB_SCHEMA
from src.app_groups.models import Base


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    # === Startup ===
    logger.info(f"🚀 Запуск {config.APP_NAME} v{config.VERSION}...")

    # 1. Инициализация логирования
    config_logging(
        level=config.LOG_LEVEL,
        log_file=config.LOG_FILE,
        mask_sensitive_data=True,
        log_base_path="src"
    )

    # 2. Получение асинхронного подключения через DBManager
    db_conn: AsyncDBConnection = DBManager.get_async_connection(config.DB_CONNECTION_NAME)

    if not await db_conn.create_pool(min_size=2, max_size=10):
        logger.error("❌ Не удалось создать asyncpg пул подключений")
        raise RuntimeError("AsyncDB Pool Initialization Failed")

    logger.info(f"✅ Async-пул создан для '{config.DB_CONNECTION_NAME}'")

    # 3. Создание схемы и таблиц через asyncpg + SQLAlchemy (async)
    try:
        # Получаем строку подключения для SQLAlchemy engine
        # Используем тот же env-ключ, что и в AsyncDBConnection
        import os
        from dotenv import load_dotenv
        load_dotenv()

        raw_conn_str = os.getenv('APP_GOOGLE_DB')
        if not raw_conn_str:
            raise ValueError("APP_GOOGLE_DB не найден в .env")

        # SQLAlchemy async engine с asyncpg
        engine = create_async_engine(
            raw_conn_str,  # asyncpg понимает postgresql:// нативно
            echo=False,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10
        )

        # Создаём схему через asyncpg (быстрее и надёжнее)
        async with db_conn.get_cursor() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
            logger.info(f"✅ Схема '{DB_SCHEMA}' проверена/создана")

        # Создаём таблицы через SQLAlchemy async
        async with engine.begin() as conn:
            # Фильтруем таблицы только для нашей схемы
            tables = [
                t for t in Base.metadata.tables.values()
                if t.schema == DB_SCHEMA
            ]
            if tables:
                await conn.run_sync(
                    lambda sync_conn: Base.metadata.create_all(
                        sync_conn,
                        tables=tables,
                        checkfirst=True
                    )
                )
                logger.info(f"✅ Создано {len(tables)} таблиц в схеме '{DB_SCHEMA}'")
            else:
                logger.warning(f"⚠️ Нет таблиц для создания в схеме '{DB_SCHEMA}'")

        await engine.dispose()  # Закрываем временный engine

    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}", exc_info=True)
        raise

    yield

    # === Shutdown ===
    logger.info(f"🛑 Остановка {config.APP_NAME}...")
    await DBManager.close_all_async()
    DBManager.close_all()  # На всякий случай закрываем и sync


# === Создание приложения ===
app = FastAPI(
    title="App Groups Service",
    description="Микросервис управления сайтами и группами",
    version=config.VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# === Middleware ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Роутеры ===
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(
        "src.app_groups.main:app",
        host=config.HOST,
        port=config.PORT,
        reload=config.RELOAD,
        log_level="info"
    )