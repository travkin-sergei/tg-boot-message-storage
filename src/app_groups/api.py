# src/app_groups/api.py
"""API роутеры для app_groups."""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional
from src.app_groups.crud import AppGroupsDB, normalize_site_url
from src.app_groups.dependencies import get_db_service
from src.app_groups.schemas import (
    SiteCreate, UserResponse, UserCreate,
    SiteGroupResponse, SiteGroupCreate, MessageResponse, SitePublic, SiteGroupPublic, UserPublic
)
from src.config.logger import logger

router = APIRouter(prefix="/api/v1/groups", tags=["App Groups"])


@router.get("/health", response_model=MessageResponse, status_code=status.HTTP_200_OK)
async def health_check() -> MessageResponse:
    """Базовая проверка — процесс запущен."""
    return MessageResponse(message="app_groups service is running", detail=None)


@router.get("/sites", response_model=List[SitePublic])  # ← изменили на SitePublic
async def list_sites(is_active: Optional[bool] = None,
                     limit: int = Query(100, le=1000),
                     offset: int = Query(0, ge=0),
                     db: AppGroupsDB = Depends(get_db_service)):
    return await db.get_sites(is_active=is_active, limit=limit, offset=offset)


@router.post("/sites", response_model=SitePublic, status_code=status.HTTP_201_CREATED)  # ← SitePublic
async def create_site(site: SiteCreate,
                      db: AppGroupsDB = Depends(get_db_service)):
    result = await db.save_site(
        title=site.title,
        url=str(site.url),
        regexp=site.regexp,
        description=site.description
    )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Сайт с таким URL уже существует"
        )
    return result



@router.post("/groups", response_model=SiteGroupResponse, status_code=status.HTTP_201_CREATED)
async def create_group(group: SiteGroupCreate,
                       db: AppGroupsDB = Depends(get_db_service)):
    """
    Создать группу.
    🔹 Клиент отправляет ТОЛЬКО group_link.
    🔹 Сайт определяется автоматически.
    """
    # Авто-определение сайта по group_link
    site = await db.find_site_by_group_link(str(group.group_link))
    if not site:
        raise HTTPException(
            status_code=404,
            detail=f"Сайт не найден для: {group.group_link}. "
                   f"Добавьте сайт с подходящим URL или regexp."
        )

    # Сохраняем группу с найденным site_id
    result = await db.save_group(
        site_id=site.id,
        group_link=str(group.group_link),
        group_id=group.group_id,
        group_name=group.group_name,
        is_verified=group.is_verified
    )

    if not result:
        raise HTTPException(409, detail="Группа уже существует")

    return result  # Pydantic вернёт ответ по схеме SiteGroupResponse


@router.get("/groups", response_model=List[SiteGroupPublic])
async def list_groups(
        site_url: Optional[str] = Query(None, description="Фильтр по URL сайта"),
        is_verified: Optional[bool] = Query(None),
        limit: int = Query(100, le=1000),
        offset: int = Query(0, ge=0),
        db: AppGroupsDB = Depends(get_db_service)
):
    # Преобразуем site_url → site_id для фильтрации
    site_id = None
    if site_url:
        site_id = await db.resolve_site_id_by_url(site_url)
        if not site_id:
            return []  # Сайт не найден — пустой список

    groups = await db.get_groups(
        site_id=site_id,
        is_verified=is_verified,
        limit=limit,
        offset=offset
    )

    # 🔹 Трансформируем ORM-объекты в ответ с site_url
    result = []
    for g in groups:
        # Получаем URL сайта через relationship (ленивая загрузка)
        site_url = None
        if g.site_id:
            # Быстрый запрос к кэшу сессии или отдельный запрос
            async with db._get_session() as session:
                from sqlalchemy import select
                from src.app_groups.models import Sites
                res = await session.execute(
                    select(Sites.url).where(Sites.id == g.site_id)
                )
                site_url = res.scalar_one_or_none()

        result.append({
            "id": g.id,
            "site_url": site_url,  # ← преобразуем site_id → site_url
            "group_link": g.group_link,
            "group_id": g.group_id,
            "group_name": g.group_name,
            "is_verified": g.is_verified
        })

    return result


# === Users: POST ===
@router.post("/users", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
async def create_user(user: UserCreate,
                      db: AppGroupsDB = Depends(get_db_service)):
    # Определяем site_id по site_url или по user_log (если есть логика)
    site_id = None
    if user.site_url:
        site_id = await db.resolve_site_id_by_url(str(user.site_url))
    if not site_id:
        raise HTTPException(404, detail=f"Сайт не найден: {user.site_url}")

    result = await db.save_user(
        site_id=site_id,
        user_id=user.user_id,  # ← это ID на площадке, не меняем
        user_log=user.user_log,
        user_name=user.user_name,
        description=user.description
    )
    if not result:
        raise HTTPException(409, detail="Пользователь уже существует")

    return {
        "id": result.id,
        "site_url": str(user.site_url),
        "user_id": result.user_id,
        "user_log": result.user_log,
        "user_name": result.user_name,
        "description": result.description
    }


# === Users: GET ===
@router.get("/users", response_model=List[UserPublic])
async def list_users(
        site_url: Optional[str] = Query(None, description="Фильтр по URL сайта"),
        limit: int = Query(100, le=1000),
        offset: int = Query(0, ge=0),
        db: AppGroupsDB = Depends(get_db_service)
):
    site_id = await db.resolve_site_id_by_url(site_url) if site_url else None
    return await db.get_users(site_id=site_id, limit=limit, offset=offset)
