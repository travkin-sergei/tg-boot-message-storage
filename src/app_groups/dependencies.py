# src/app_groups/dependencies.py
"""
Зависимости FastAPI для app_groups.
Интеграция с централизованными модулями:
- src.config.database (async_session)
- src.config.logger
"""
from fastapi import Depends, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from src.app_groups.crud import AppGroupsDB
from src.config.database import async_session
from src.config.logger import logger


async def get_db_session() -> AsyncSession:
    """
    Зависимость для получения сессии БД.
        - Не логирует ошибки — это делает CRUD-слой.
        - Не делает автоматический commit — это делает CRUD-слой.
        - Только управляет жизненным циклом сессии.
    """
    session: AsyncSession = async_session()
    try:
        yield session
    finally:
        await session.close()  # Закрываем сессию в любом случае


async def get_db_service(session: AsyncSession = Depends(get_db_session)) -> AppGroupsDB:
    """
    Зависимость для получения CRUD-сервиса.

    Returns:
        AppGroupsDB: Экземпляр сервиса с активной сессией.
    """
    return AppGroupsDB(session_factory=lambda: session)


async def log_request(request: Request, call_next):
    """Middleware для логирования входящих запросов."""
    logger.info(f"🔹 {request.method} {request.url.path}")
    try:
        response = await call_next(request)
        # Логируем только ошибки 4xx/5xx, не 2xx/3xx
        if response.status_code >= 400:
            logger.warning(f"🔸 {request.method} {request.url.path} → {response.status_code}")
        else:
            logger.debug(f"🔸 {request.method} {request.url.path} → {response.status_code}")
        return response
    except HTTPException as e:
        # 🔹 Ожидаемые ошибки (409, 404) — логируем как INFO/WARNING
        if e.status_code == 409:
            logger.info(f"⚠️ Конфликт данных: {request.method} {request.url.path} → 409")
        elif e.status_code == 404:
            logger.debug(f"ℹ️ Не найдено: {request.method} {request.url.path} → 404")
        else:
            logger.warning(f"⚠️ HTTP {e.status_code}: {request.method} {request.url.path}")
        raise
    except Exception as e:
        # 🔹 Неожиданные ошибки — логируем как ERROR
        logger.error(f"❌ Ошибка обработки запроса: {e}", exc_info=True)
        raise
