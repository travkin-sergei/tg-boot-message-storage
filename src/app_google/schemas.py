from datetime import date, datetime
from pydantic import BaseModel, Field
from typing import Optional, List, Annotated


class SyncRequest(BaseModel):
    """Запрос на запуск синхронизации."""
    file_code: Optional[str] = None
    sheet_name: Optional[str] = None


class SyncResponse(BaseModel):
    """Ответ после запуска синхронизации."""
    status: str
    message: str
    stats: Optional[dict] = None
    timestamp: datetime = Field(default_factory=datetime.now)


class TaskFilter(BaseModel):
    """Фильтры для получения задач."""
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    status: Optional[str] = None
    responsible: Optional[str] = None
    limit: int = Field(100, ge=1, le=1000)
    offset: int = Field(0, ge=0)


class TaskResponse(BaseModel):
    """Ответ с данными задачи."""

    link_post: str
    number: Optional[str] = None
    date_comment: Optional[date] = None
    short_description: Optional[str] = None
    autor: Optional[str] = None
    subscribers: Optional[int] = None
    comment: Optional[str] = None
    corrections: Optional[str] = None
    responsible: Optional[str] = None
    status: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    is_active: bool

    class Config:
        from_attributes = True


class StatsResponse(BaseModel):
    """Статистика по данным."""
    total_tasks: int
    by_status: dict[str, int]
    by_responsible: dict[str, int]
    date_range: dict[str, Optional[date]]
    last_updated: Optional[datetime]
