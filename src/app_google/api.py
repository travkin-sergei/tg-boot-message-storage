"""
FastAPI Router для app_google.
Изолирован: импортирует только из src.config.* и своих модулей.
"""
from datetime import date
from typing import Optional, List, Annotated
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.config.database import engine, async_session
from src.config.other import API_TOKEN
from src.app_google.config import APP_GOOGLE_FILE
from src.app_google.schemas import SyncResponse, SyncRequest, StatsResponse, TaskResponse, TaskFilter
from src.app_google.models import TaskList
from src.app_google.main import main as run_pipeline

# === Настройки роутера ===
router = APIRouter(prefix="/app_google", tags=["app_google"])
security = HTTPBearer()


# === Вспомогательные функции ===
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> bool:
    """Проверка токена (изолирована внутри приложения)."""
    if not API_TOKEN or credentials.credentials != API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return True


async def get_db_session() -> AsyncSession:
    """Зависимость для сессии БД."""
    if engine is None:
        raise HTTPException(status_code=500, detail="Database not initialized")
    async with async_session() as session:
        yield session


# === ЭНДПОИНТЫ ===

@router.post("/sync", response_model=SyncResponse, dependencies=[Depends(verify_token)])
async def trigger_sync(request: SyncRequest, background_tasks: BackgroundTasks) -> SyncResponse:
    """
    Запускает синхронизацию Google Sheets → DB.

    Требуется заголовок: `Authorization: Bearer <API_TOKEN>`
    """
    target_file = APP_GOOGLE_FILE
    if not target_file:
        raise HTTPException(status_code=400, detail="APP_GOOGLE_FILE not configured")

    # Запуск в фоне (не блокирует ответ)
    background_tasks.add_task(
        run_pipeline,
        file_code=APP_GOOGLE_FILE,
        sheet_name=request.sheet_name
    )
    return SyncResponse(status="started", message="Синхронизация запущена")


@router.get("/tasks", response_model=List[TaskResponse])
async def get_tasks(
        filters: Annotated[TaskFilter, Depends()],
        db: AsyncSession = Depends(get_db_session)
) -> List[TaskResponse]:
    """Получение задач с фильтрацией по дате."""
    query = select(TaskList).where(TaskList.is_active == True)

    if filters.date_from:
        query = query.where(TaskList.date_comment >= filters.date_from)
    if filters.date_to:
        query = query.where(TaskList.date_comment <= filters.date_to)
    if filters.status:
        query = query.where(TaskList.status == filters.status)
    if filters.responsible:
        query = query.where(TaskList.responsible == filters.responsible)

    query = query.order_by(TaskList.date_comment.desc()).limit(filters.limit).offset(filters.offset)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/tasks/{link_post}", response_model=TaskResponse)
async def get_task(link_post: str, db: AsyncSession = Depends(get_db_session)) -> TaskResponse:
    """Получение задачи по первичному ключу."""
    query = select(TaskList).where(TaskList.link_post == link_post, TaskList.is_active == True)
    result = await db.execute(query)
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.get("/stats", response_model=StatsResponse)
async def get_stats(date_from: Optional[date] = Query(None),
                    date_to: Optional[date] = Query(None),
                    db: AsyncSession = Depends(get_db_session)) -> StatsResponse:
    """Статистика по задачам."""
    base = select(TaskList).where(TaskList.is_active == True)
    if date_from:
        base = base.where(TaskList.date_comment >= date_from)
    if date_to:
        base = base.where(TaskList.date_comment <= date_to)

    total = await db.execute(select(func.count(TaskList.link_post)).select_from(base.subquery()))

    by_status = await db.execute(
        select(TaskList.status, func.count(TaskList.link_post))
        .select_from(base.subquery()).group_by(TaskList.status)
    )

    by_resp = await db.execute(
        select(TaskList.responsible, func.count(TaskList.link_post))
        .select_from(base.subquery()).group_by(TaskList.responsible)
    )

    dates = await db.execute(
        select(func.min(TaskList.date_comment), func.max(TaskList.date_comment), func.max(TaskList.updated_at))
        .select_from(base.subquery())
    )
    row = dates.first()

    return StatsResponse(
        total_tasks=total.scalar() or 0,
        by_status={r[0] or 'unknown': r[1] for r in by_status.fetchall()},
        by_responsible={r[0] or 'unassigned': r[1] for r in by_resp.fetchall()},
        date_range={'min': row[0], 'max': row[1]} if row else {},
        last_updated=row[2] if row else None
    )


@router.get("/health")
async def health():
    """Health check для app_google."""
    return {"status": "ok", "service": "app_google"}
