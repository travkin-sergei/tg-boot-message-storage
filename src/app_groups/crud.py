"""
CRUD операции для app_groups.
Зависимости:
- src.app_groups.models (ваши ORM модели — строго переиспользуем)
- src.config.database (engine, async_session — централизованно)
- src.config.logger (логирование)
"""
import re

from datetime import date
from typing import Optional, Sequence
from urllib.parse import urlparse

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.app_groups.models import Base, Sites, Users, SiteGroup, StatGroupUser
from src.config.database import async_session, engine
from src.config.logger import logger


def normalize_site_url(url: str) -> str:
    """Нормализует URL к базовому виду: https://domain/"""
    if not url:
        return url
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url.lstrip('www.')
    parsed = urlparse(url)
    netloc = parsed.netloc.lstrip('www.')
    return f"https://{netloc}/"


# === Внутри класса AppGroupsDB ===

async def resolve_site_id_by_url(self, site_url: str) -> Optional[int]:
    """Находит site_id по нормализованному URL сайта."""
    if not site_url or engine is None:
        return None
    normalized = normalize_site_url(site_url)
    try:
        async with self._get_session() as session:
            result = await session.execute(
                select(Sites.id).where(Sites.url == normalized)
            )
            return result.scalar_one_or_none()
    except Exception as e:
        logger.error(f"❌ Ошибка поиска site_id: {e}", exc_info=True)
        return None


async def find_site_by_group_link(self, group_link: str) -> Optional[Sites]:
    """
    Находит сайт по group_link через:
    1. Точное совпадение нормализованного URL
    2. regexp из таблицы site
    3. Префиксное совпадение (group_link начинается с url сайта)
    """
    if engine is None:
        return None
    try:
        async with self._get_session() as session:
            normalized = normalize_site_url(group_link)

            # 1. Точное совпадение
            result = await session.execute(
                select(Sites).where(Sites.url == normalized)
            )
            site = result.scalar_one_or_none()
            if site:
                return site

            # 2. Поиск по regexp
            result = await session.execute(
                select(Sites).where(Sites.regexp.isnot(None))
            )
            for s in result.scalars().all():
                if s.regexp:
                    try:
                        if re.match(s.regexp, group_link):
                            return s
                    except re.error:
                        logger.warning(f"⚠️ Некорректный regexp у сайта #{s.id}")

            # 3. Префиксное совпадение
            result = await session.execute(
                select(Sites).where(Sites.url.isnot(None))
            )
            for s in result.scalars().all():
                if s.url and normalized.startswith(s.url.rstrip('/')):
                    return s

            return None
    except Exception as e:
        logger.error(f"❌ Ошибка поиска сайта: {e}", exc_info=True)
        return None


# =============================================================================
# === Класс AppGroupsDB ===
# =============================================================================
class AppGroupsDB:
    """Работа с БД через SQLAlchemy ORM для app_groups."""

    DEFAULT_SCHEMA: str = 'app_groups'

    def __init__(self, schema: Optional[str] = None, session_factory=None):
        self.schema = schema or self.DEFAULT_SCHEMA
        self._session_factory = session_factory or (lambda: async_session())

    def _get_session(self) -> AsyncSession:
        """Возвращает новый экземпляр сессии."""
        return self._session_factory()

    async def resolve_site_id_by_url(self, site_url: str) -> Optional[int]:
        """Находит site_id по нормализованному URL сайта."""
        if not site_url or engine is None:
            return None
        normalized = normalize_site_url(site_url)
        try:
            async with self._get_session() as session:
                result = await session.execute(
                    select(Sites.id).where(Sites.url == normalized)
                )
                return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"❌ Ошибка поиска site_id: {e}", exc_info=True)
            return None

    async def find_site_by_group_link(self, group_link: str) -> Optional[Sites]:
        """
        Находит сайт по group_link через:
        1. Точное совпадение нормализованного URL
        2. regexp из таблицы site
        3. Префиксное совпадение (group_link начинается с url сайта)
        """
        if engine is None:
            return None
        try:
            async with self._get_session() as session:
                normalized = normalize_site_url(group_link)

                # 1. Точное совпадение
                result = await session.execute(
                    select(Sites).where(Sites.url == normalized)
                )
                site = result.scalar_one_or_none()
                if site:
                    return site

                # 2. Поиск по regexp
                result = await session.execute(
                    select(Sites).where(Sites.regexp.isnot(None))
                )
                for s in result.scalars().all():
                    if s.regexp:
                        try:
                            if re.match(s.regexp, group_link):
                                return s
                        except re.error:
                            logger.warning(f"⚠️ Некорректный regexp у сайта #{s.id}")

                # 3. Префиксное совпадение
                result = await session.execute(
                    select(Sites).where(Sites.url.isnot(None))
                )
                for s in result.scalars().all():
                    if s.url and normalized.startswith(s.url.rstrip('/')):
                        return s

                return None
        except Exception as e:
            logger.error(f"❌ Ошибка поиска сайта: {e}", exc_info=True)
            return None

    async def find_site_by_url(self, group_link: str) -> Optional[Sites]:
        """
        Находит сайт по group_link через нормализацию/regexp/префикс.
        Используется для авто-определения сайта при создании группы.
        """
        if engine is None:
            return None
        try:
            async with self._get_session() as session:
                # 1. Точное совпадение нормализованного URL
                normalized = normalize_site_url(group_link)
                result = await session.execute(
                    select(Sites).where(Sites.url == normalized)
                )
                site = result.scalar_one_or_none()
                if site:
                    return site

                # 2. Поиск по regexp
                result = await session.execute(
                    select(Sites).where(Sites.regexp.isnot(None))
                )
                for s in result.scalars().all():
                    if s.regexp:
                        try:
                            if re.match(s.regexp, group_link):
                                return s
                        except re.error:
                            logger.warning(f"⚠️ Некорректный regexp у сайта #{s.id}")

                # 3. Префиксное совпадение
                result = await session.execute(
                    select(Sites).where(Sites.url.isnot(None))
                )
                for s in result.scalars().all():
                    if s.url and normalized.startswith(s.url.rstrip('/')):
                        return s

                return None
        except Exception as e:
            logger.error(f"❌ Ошибка поиска сайта для {group_link}: {e}", exc_info=True)
            return None

    # -------------------------------------------------------------------------
    # === CRUD: Sites ===
    # -------------------------------------------------------------------------
    async def save_site(self,
                        title: Optional[str] = None,
                        url: Optional[str] = None,
                        regexp: Optional[str] = None,
                        description: Optional[str] = None,
                        commit: bool = True) -> Optional[Sites]:
        """Сохранить сайт. URL автоматически нормализуется."""
        if engine is None:
            logger.error("engine не инициализирован")
            return None

        # Нормализуем URL перед сохранением
        if url:
            url = normalize_site_url(url)

        try:
            async with self._get_session() as session:
                record = Sites(
                    title=title, url=url, regexp=regexp, description=description
                )
                session.add(record)
                if commit:
                    await session.commit()
                    await session.refresh(record)
                logger.info(f"💾 ORM: Сохранён сайт ID={record.id}, url={url}")
                return record
        except IntegrityError as e:
            logger.warning(f"⚠️ Дубликат сайта (url={url}): {e.orig}")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сайта: {e}", exc_info=True)
            return None

    async def get_sites(self,
                        is_active: Optional[bool] = None,
                        limit: int = 100,
                        offset: int = 0) -> Sequence[Sites]:
        if engine is None:
            return []
        try:
            async with self._get_session() as session:
                query = select(Sites).order_by(Sites.created_at.desc())
                if is_active is not None:
                    query = query.where(Sites.is_active == is_active)
                query = query.limit(limit).offset(offset)
                result = await session.execute(query)
                return result.scalars().all()
        except Exception as e:
            logger.error(f"❌ Ошибка чтения сайтов: {e}", exc_info=True)
            return []

    async def update_site(self, site_id: int, **kwargs) -> bool:
        if engine is None or not kwargs:
            return False
        try:
            async with self._get_session() as session:
                result = await session.execute(
                    update(Sites).where(Sites.id == site_id).values(**kwargs)
                )
                await session.commit()
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"❌ Ошибка обновления сайта: {e}", exc_info=True)
            return False

    # -------------------------------------------------------------------------
    # === CRUD: Users ===
    # -------------------------------------------------------------------------
    async def save_user(self,
                        site_id: int,
                        user_id: Optional[str] = None,
                        user_log: Optional[str] = None,
                        user_pas: Optional[str] = None,
                        user_name: Optional[str] = None,
                        description: Optional[str] = None,
                        commit: bool = True) -> Optional[Users]:
        if engine is None:
            logger.error("engine не инициализирован")
            return None
        try:
            async with self._get_session() as session:
                record = Users(
                    site_id=site_id, user_id=user_id, user_log=user_log,
                    user_pas=user_pas, user_name=user_name, description=description
                )
                session.add(record)
                if commit:
                    await session.commit()
                    await session.refresh(record)
                logger.info(f"💾 ORM: Сохранён пользователь ID={record.id}")
                return record
        except IntegrityError as e:
            logger.warning(f"⚠️ Дубликат пользователя: {e.orig}")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения пользователя: {e}", exc_info=True)
            return None

    async def get_users(self,
                        site_id: Optional[int] = None,
                        is_active: Optional[bool] = None,
                        limit: int = 100,
                        offset: int = 0) -> Sequence[Users]:
        if engine is None:
            return []
        try:
            async with self._get_session() as session:
                query = select(Users).order_by(Users.created_at.desc())
                if site_id is not None:
                    query = query.where(Users.site_id == site_id)
                if is_active is not None:
                    query = query.where(Users.is_active == is_active)
                query = query.limit(limit).offset(offset)
                result = await session.execute(query)
                return result.scalars().all()
        except Exception as e:
            logger.error(f"❌ Ошибка чтения пользователей: {e}", exc_info=True)
            return []

    # -------------------------------------------------------------------------
    # === CRUD: SiteGroup ===
    # -------------------------------------------------------------------------
    async def save_group(self,
                         site_id: int,
                         group_link: str,
                         group_id: Optional[str] = None,
                         group_name: Optional[str] = None,
                         is_verified: bool = False,
                         commit: bool = True) -> Optional[SiteGroup]:
        if engine is None:
            logger.error("engine не инициализирован")
            return None
        try:
            async with self._get_session() as session:
                record = SiteGroup(
                    site_id=site_id, group_link=group_link,
                    group_id=group_id, group_name=group_name,
                    is_verified=is_verified
                )
                session.add(record)
                if commit:
                    await session.commit()
                    await session.refresh(record)
                logger.info(f"💾 ORM: Сохранена группа ID={record.id}")
                return record
        except IntegrityError as e:
            logger.warning(f"⚠️ Дубликат группы: {e.orig}")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения группы: {e}", exc_info=True)
            return None

    async def get_groups(self,
                         site_id: Optional[int] = None,
                         is_verified: Optional[bool] = None,
                         is_active: Optional[bool] = None,
                         limit: int = 100,
                         offset: int = 0) -> Sequence[SiteGroup]:
        if engine is None:
            return []
        try:
            async with self._get_session() as session:
                query = select(SiteGroup).order_by(SiteGroup.created_at.desc())
                if site_id is not None:
                    query = query.where(SiteGroup.site_id == site_id)
                if is_verified is not None:
                    query = query.where(SiteGroup.is_verified == is_verified)
                if is_active is not None:
                    query = query.where(SiteGroup.is_active == is_active)
                query = query.limit(limit).offset(offset)
                result = await session.execute(query)
                return result.scalars().all()
        except Exception as e:
            logger.error(f"❌ Ошибка чтения групп: {e}", exc_info=True)
            return []

    # -------------------------------------------------------------------------
    # === CRUD: StatGroupUser ===
    # -------------------------------------------------------------------------
    async def save_stat(self,
                        group_id: int,
                        stat_date: date,
                        count_user: int,
                        commit: bool = True) -> Optional[StatGroupUser]:
        if engine is None:
            return None
        try:
            async with self._get_session() as session:
                record = StatGroupUser(
                    group_id=group_id, stat_date=stat_date, count_user=count_user
                )
                session.add(record)
                if commit:
                    await session.commit()
                    await session.refresh(record)
                return record
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения статистики: {e}", exc_info=True)
            return None

    async def get_stats(self,
                        group_id: Optional[int] = None,
                        date_from: Optional[date] = None,
                        date_to: Optional[date] = None,
                        limit: int = 100,
                        offset: int = 0) -> Sequence[StatGroupUser]:
        if engine is None:
            return []
        try:
            async with self._get_session() as session:
                query = select(StatGroupUser).order_by(StatGroupUser.stat_date.desc())
                if group_id is not None:
                    query = query.where(StatGroupUser.group_id == group_id)
                if date_from:
                    query = query.where(StatGroupUser.stat_date >= date_from)
                if date_to:
                    query = query.where(StatGroupUser.stat_date <= date_to)
                query = query.limit(limit).offset(offset)
                result = await session.execute(query)
                return result.scalars().all()
        except Exception as e:
            logger.error(f"❌ Ошибка чтения статистики: {e}", exc_info=True)
            return []
