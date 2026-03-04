# src/main.py
import asyncio
import logging

from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import NullPool

from src import config
from src.app_log import logger
from src.app_google.models import Base
from src.app_google.pipline.line1 import sync_google_sheet_to_db
from src.config import DATABASE_URL, API_TOKEN

# КРИТИЧНО: file_code берётся сразу из config, а не внутри __main__
# Иначе при запуске через uvicorn значение останется None
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

# Логирование конфига при загрузке модуля
logger.info(f"Конфигурация загружена: GOOGLE_FILE={config.GOOGLE_FILE}")

_engine = None
_async_session = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager: инициализация и очистка ресурсов"""
    global _engine, _async_session

    # Создаём движок
    _engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        poolclass=NullPool  # Важно для стабильности с FastAPI
    )
    _async_session = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)

    # Создаём таблицы, если их нет
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("БД инициализирована")

    yield  # Приложение работает

    # Закрытие соединений при выключении
    if _engine:
        await _engine.dispose()
    logger.info("Соединения с БД закрыты")


app = FastAPI(
    title="Google Sheets Sync API",
    description="API для синхронизации Google Sheets → PostgreSQL",
    version="1.0.0",
    lifespan=lifespan
)

# Безопасность: Bearer token
security = HTTPBearer()


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> bool:
    """Проверка API-ключа из config.API_TOKEN"""
    if not API_TOKEN or credentials.credentials != API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return True


async def _run_sync_task():
    """Внутренняя функция для выполнения синхронизации"""
    try:
        logger.info("Запуск фоновой синхронизации...")

        # Проверка: задан ли file_code
        if not GOOGLE_CONFIG["file_code"]:
            logger.error("GOOGLE_CONFIG['file_code'] is None. Проверьте .env")
            return

        async with _async_session() as session:
            stats = await sync_google_sheet_to_db(
                session=session,
                list_name=GOOGLE_CONFIG["list_name"],
                file_code=GOOGLE_CONFIG["file_code"],
                output_parquet=GOOGLE_CONFIG["output_parquet"],
                column_mapping=GOOGLE_CONFIG["column_mapping"]
            )
        logger.info(f"Синхронизация завершена: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Ошибка в фоновой задаче: {e}", exc_info=True)
        raise


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "ok",
        "service": "Google Sheets Sync API",
        "version": "1.0.0"
    }


@app.post("/sync/start", dependencies=[Depends(verify_token)])
async def start_sync(background_tasks: BackgroundTasks):
    """
    Запускает синхронизацию Google Sheets → DB в фоновом режиме.
    Требуется заголовок: Authorization: Bearer <API_TOKEN>
    """
    background_tasks.add_task(_run_sync_task)
    logger.info("📡 Запрос на синхронизацию принят")
    return {
        "status": "started",
        "message": "Синхронизация запущена в фоновом режиме",
        "timestamp": asyncio.get_event_loop().time()
    }


@app.get("/sync/status")
async def sync_status():
    """Статус последнего запуска (заглушка)"""
    return {
        "status": "unknown",
        "message": "Статус отслеживается в логах",
        "note": "Проверьте логи для деталей выполнения"
    }


@app.post("/sync/run-now", dependencies=[Depends(verify_token)])
async def run_sync_now():
    """
    Запускает синхронизацию синхронно и ждёт завершения.
    ⚠️ Использовать только для тестов!
    """
    try:
        if not GOOGLE_CONFIG["file_code"]:
            raise HTTPException(status_code=400, detail="GOOGLE_FILE not configured")

        async with _async_session() as session:
            stats = await sync_google_sheet_to_db(
                session=session,
                list_name=GOOGLE_CONFIG["list_name"],
                file_code=GOOGLE_CONFIG["file_code"],
                output_parquet=GOOGLE_CONFIG["output_parquet"],
                column_mapping=GOOGLE_CONFIG["column_mapping"]
            )
        return {
            "status": "completed",
            "stats": stats,
            "message": "Синхронизация завершена"
        }
    except Exception as e:
        logger.error(f"Ошибка синхронного запуска: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# src/main.py (в блоке if __name__ == "__main__")

if __name__ == "__main__":
    import uvicorn
    from src.app_log import config_logging

    # Сначала настраиваем root-логгер
    config_logging(level=logging.INFO, log_file='logs/app.log')

    # Конфигурация логгеров для uvicorn
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "src.app_log.RelativePathFormatter",  # Ваш форматтер!
                "fmt": "%(asctime)s | %(levelname)-8s | %(short_path)s | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["default"], "level": "INFO", "propagate": False},
        },
    }

    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_config=LOGGING_CONFIG  # 🔥 Применяем нашу конфигурацию к uvicorn
    )
