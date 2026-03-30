# src/app_groups/schemas.py
"""
Pydantic схемы для валидации запросов/ответов API.
Изолированы от ORM-моделей.

"""
import re
from pydantic import BaseModel, Field, HttpUrl, ConfigDict, field_validator
from typing import Optional, List
from datetime import datetime, date


class BaseSchema(BaseModel):
    """Базовая схема с настройками."""
    model_config = ConfigDict(from_attributes=True, extra='ignore')


# =============================================================================
# === Sites ===
# =============================================================================
class SiteCreate(BaseSchema):
    """Запрос на создание/обновление сайта."""
    title: Optional[str] = Field(None, max_length=255, description="Название сайта")
    url: HttpUrl = Field(..., description="Полный URL сайта")
    regexp: Optional[str] = Field(None, max_length=255, description="Regexp для валидации ссылок")
    description: Optional[str] = Field(None, description="Описание сайта")

    @field_validator('regexp')
    @classmethod
    def validate_regexp(cls, v: Optional[str]) -> Optional[str]:
        if v:
            try:
                re.compile(v)
            except re.error as e:
                raise ValueError(f"Невалидный regexp: {e}")
        return v


class SitePublic(BaseSchema):
    """
    🔹 ПУБЛИЧНЫЙ ОТВЕТ: только нужные поля.
    Исключены: is_active, created_at, updated_at
    """
    id: int
    title: Optional[str]
    url: HttpUrl
    regexp: Optional[str]
    description: Optional[str]


class SiteResponse(SiteCreate):
    """Внутренний ответ (для админки): все поля БД."""
    id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


class SiteGroupCreate(BaseSchema):
    """🔹 Запрос: ТОЛЬКО group_link, сайт определится автоматически."""
    group_link: HttpUrl = Field(..., description="Ссылка на группу")
    group_id: Optional[str] = Field(None, max_length=128)
    group_name: Optional[str] = Field(None, max_length=255)
    is_verified: bool = False


class SiteGroupPublic(BaseSchema):
    """Публичный ответ: возвращаем site_url вместо site_id."""
    id: int
    site_url: Optional[HttpUrl] = Field(None,
                                        description="URL сайта, к которому привязана группа")  # ← было site_id: int
    group_link: HttpUrl
    group_id: Optional[str]
    group_name: Optional[str]
    is_verified: bool


# === Users ===
class UserCreate(BaseSchema):
    # 🔹 site_url вместо site_id
    site_url: Optional[HttpUrl] = Field(None, description="URL сайта (определяет site_id автоматически)")
    # 🔹 user_id — это ID пользователя на площадке (строка), НЕ внутренний ID БД
    user_id: Optional[str] = Field(None, max_length=255,
                                   description="ID пользователя на площадке (например, @username или 123456)")
    user_log: Optional[str] = Field(None, max_length=255)
    user_name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None


class UserPublic(BaseSchema):
    """Публичный ответ пользователя."""
    id: int  # ← внутренний ID БД (только для отладки, можно скрыть)
    site_url: HttpUrl  # ← вместо site_id
    user_id: Optional[str]  # ID на площадке
    user_log: Optional[str]
    user_name: Optional[str]
    description: Optional[str]


class UserResponse(UserCreate):
    """Внутренний ответ: все поля БД."""
    id: int
    is_active: bool
    created_at: datetime


class SiteGroupResponse(BaseSchema):
    id: int
    site_url: Optional[HttpUrl] = None  # ← вместо site_id
    group_link: HttpUrl
    group_id: Optional[str]
    group_name: Optional[str]
    is_verified: bool
    is_active: bool
    created_at: datetime
    updated_at: datetime


# =============================================================================
# === Статистика ===
# =============================================================================
class GroupStatsResponse(BaseSchema):
    group_id: int
    stat_date: date
    count_user: int


# =============================================================================
# === Общие ответы ===
# =============================================================================
class MessageResponse(BaseModel):
    message: str
    detail: Optional[str] = None


class PaginatedResponse(BaseModel):
    items: List
    total: int
    page: int
    limit: int
