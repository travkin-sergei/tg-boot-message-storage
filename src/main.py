# src/main.py
import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.app_google.models import init_db_schema
from src.app_google.api import router as app_google_router
from src.config.logger import logger, config_logging
from src.config.database import DBManager

config_logging(level=logging.INFO, log_file='logs/app.log')


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Инициализация/очистка ресурсов при старте/остановке."""

    # 1. Инициализация подключений к БД
    DBManager.initialize_all(['app_google_target'], async_mode=True)
    logger.info("Подключения к БД инициализированы")

    # 2. Инициализация схемы и таблиц app_google
    db_ok = await init_db_schema("test")
    if not db_ok:
        logger.error("Ошибка инициализации БД для app_google")
    else:
        logger.info("app_google: схема и таблица готовы")

    yield

    # 3. Очистка при завершении
    await DBManager.close_all_async()
    logger.info("Подключения к БД закрыты")


app = FastAPI(
    title="App Gateway API",
    description="Шлюз для изолированных приложений",
    version="1.0.0",
    lifespan=lifespan,
)

# Подключаем роутеры
app.include_router(app_google_router)


@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "App Gateway",
        "apps": {"app-google": "/app-google"}
    }


if __name__ == "__main__":
    import uvicorn
    from src.config.logger import config_logging
    from pathlib import Path

    config_logging(level=logging.INFO, log_file='logs/app.log')

    # === Конфигурация для uvicorn ===
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "colored": {
                "()": "src.config.logger.RelativePathFormatter",
                "fmt": "%(log_color)s%(asctime)s | %(levelname)-8s | %(lineno)4d | %(short_path)s | %(classname)s | %(funcName)s | %(message)s%(reset)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "base_path": str(Path(__file__).resolve().parent.parent),
                "use_colors": True,
                "log_colors": {
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'bold_red',
                },
                "reset": True,
                "style": "%",
            },
        },
        "handlers": {
            "console": {
                "formatter": "colored",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            # Все uvicorn-логгеры используют цветной форматтер
            "uvicorn": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["console"], "level": "INFO", "propagate": False},
        },
        "root": {
            "handlers": ["console"],
            "level": "INFO",
        },
    }

    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_config=LOGGING_CONFIG,
        log_level="info"
    )
