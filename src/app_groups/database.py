"""
Работа с БД через SQLAlchemy ORM для app_groups.
Зависимости:
- src.config.database (engine, async_session)
- src.config.logger
- src.app_groups.models (Sites, Users, SiteGroup, StatGroupUser, Base)
"""
from datetime import date
from typing import Optional, Sequence, TypeVar, Any

from sqlalchemy import select, func, update, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.app_groups.models import Base, Sites, Users, SiteGroup, StatGroupUser
from src.config.database import engine, async_session
from src.config.logger import logger

T = TypeVar('T', bound=Base)


class AppGroupsDB:
    """Работа с БД через SQLAlchemy ORM для app_groups."""

    # Схема по умолчанию (можно переопределить при инициализации)
    DEFAULT_SCHEMA: str = 'app_groups'

    def __init__(self, schema: Optional[str] = None, session_factory=None):
        """
        Args:
            schema: Имя схемы БД (по умолчанию из модели или DEFAULT_SCHEMA).
            session_factory: Фабрика сессий (по умолчанию из config.database).
        """
        self.schema = schema or self.DEFAULT_SCHEMA
        self._session_factory = session_factory or async_session

    # -------------------------------------------------------------------------
    # === Вспомогательные методы ===
    # -------------------------------------------------------------------------

    async def _get_session(self) -> AsyncSession:
        """Получить активную сессию."""
        if engine is None:
            raise RuntimeError("engine не инициализирован")
        return self._session_factory()

    @staticmethod
    async def ensure_schema_exists(schema: Optional[str] = None) -> bool:
        """Создать схему, если не существует (требует прав CREATE SCHEMA)."""
        if engine is None:
            logger.error("engine не инициализирован")
            return False

        target_schema = schema or AppGroupsDB.DEFAULT_SCHEMA
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
                )
                logger.info(f"Схема '{target_schema}' проверена/создана")
                return True
        except Exception as e:
            logger.error(f"Ошибка создания схемы: {e}", exc_info=True)
            return False

    # -------------------------------------------------------------------------
    # === CRUD: Sites ===
    # -------------------------------------------------------------------------

    async def save_site(self,
                        title: Optional[str] = None,
                        url: Optional[str] = None,
                        regexp: Optional[str] = None,
                        description: Optional[str] = None,
                        commit: bool = True) -> Optional[Sites]:
        """
        Сохранить сайт через ORM.

        Returns:
            Sites | None: Сохранённый объект или None при ошибке.
        """
        if engine is None:
            logger.error("engine не инициализирован")
            return None

        try:
            async with self._get_session() as session:
                record = Sites(
                    title=title,
                    url=url,
                    regexp=regexp,
                    description=description,
                )
                session.add(record)
                if commit:
                    await session.commit()
                    await session.refresh(record)
                logger.info(f"💾 ORM: Сохранён сайт ID={record.id}")
                return record
        except Exception as e:
            logger.error(f"Ошибка сохранения сайта: {e}", exc_info=True)
            return None

    async def save_sites_batch(self,
                               sites: list[dict[str, Any]],
                               commit: bool = True) -> tuple[int, int]:
        """
        Пакетное сохранение сайтов (bulk insert).

        Args:
            sites: Список словарей с данными сайтов.

        Returns:
            tuple: (успешно сохранено, ошибок)
        """
        if engine is None or not sites:
            return 0, 0

        success_count = 0
        error_count = 0

        try:
            async with self._get_session() as session:
                for site_data in sites:
                    try:
                        record = Sites(**{k: v for k, v in site_data.items() if hasattr(Sites, k)})
                        session.add(record)
                        success_count += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка добавления сайта: {e}")
                        error_count += 1

                if commit and success_count > 0:
                    await session.commit()
                    logger.info(f"✅ Пакетно сохранено {success_count} сайтов")

        except Exception as e:
            logger.error(f"Ошибка пакетного сохранения: {e}", exc_info=True)
            error_count += len(sites) - success_count

        return success_count, error_count

    async def get_sites(self,
                        is_active: Optional[bool] = None,
                        limit: int = 100,
                        offset: int = 0) -> Sequence[Sites]:
        """
        Получить сайты с фильтрацией.
        """
        if engine is None:
            return []

        try:
            async with self._get_session() as session:
                query = select(Sites).order_by(Sites.created_at.desc())

                if is_active is not None:
                    query = query.where(Sites.is_active == is_active)

                query = query.limit(limit).offset(offset)

                result = await session.execute(query)
                sites = result.scalars().all()

                logger.debug(f"📜 ORM: Получено {len(sites)} сайтов")
                return sites
        except Exception as e:
            logger.error(f"Ошибка чтения сайтов: {e}", exc_info=True)
            return []

    async def update_site(self,
                          site_id: int,
                          title: Optional[str] = None,
                          url: Optional[str] = None,
                          regexp: Optional[str] = None,
                          description: Optional[str] = None,
                          is_active: Optional[bool] = None) -> bool:
        """
        Обновить сайт по ID.
        """
        if engine is None:
            return False

        try:
            async with self._get_session() as session:
                update_data = {}
                if title is not None:
                    update_data['title'] = title
                if url is not None:
                    update_data['url'] = url
                if regexp is not None:
                    update_data['regexp'] = regexp
                if description is not None:
                    update_data['description'] = description
                if is_active is not None:
                    update_data['is_active'] = is_active

                if not update_data:
                    return True  # Нечего обновлять

                query = (
                    update(Sites)
                    .where(Sites.id == site_id)
                    .values(**update_data)
                )

                result = await session.execute(query)
                await session.commit()

                if result.rowcount > 0:
                    logger.info(f"✅ Сайт #{site_id} обновлён")
                    return True
                logger.warning(f"⚠️ Сайт #{site_id} не найден для обновления")
                return False
        except Exception as e:
            logger.error(f"Ошибка обновления сайта: {e}", exc_info=True)
            return False

    async def delete_site(self, site_id: int) -> bool:
        """
        Удалить сайт по ID (мягкое удаление через is_active).
        """
        return await self.update_site(site_id, is_active=False)

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
        """
        Сохранить пользователя через ORM.
        """
        if engine is None:
            logger.error("engine не инициализирован")
            return None

        try:
            async with self._get_session() as session:
                record = Users(
                    site_id=site_id,
                    user_id=user_id,
                    user_log=user_log,
                    user_pas=user_pas,
                    user_name=user_name,
                    description=description,
                )
                session.add(record)
                if commit:
                    await session.commit()
                    await session.refresh(record)
                logger.info(f"💾 ORM: Сохранён пользователь ID={record.id}")
                return record
        except Exception as e:
            logger.error(f"Ошибка сохранения пользователя: {e}", exc_info=True)
            return None

    async def save_users_batch(self,
                               users: list[dict[str, Any]],
                               commit: bool = True) -> tuple[int, int]:
        """Пакетное сохранение пользователей."""
        if engine is None or not users:
            return 0, 0

        success_count = 0
        error_count = 0

        try:
            async with self._get_session() as session:
                for user_data in users:
                    try:
                        record = Users(**{k: v for k, v in user_data.items() if hasattr(Users, k)})
                        session.add(record)
                        success_count += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка добавления пользователя: {e}")
                        error_count += 1

                if commit and success_count > 0:
                    await session.commit()
                    logger.info(f"✅ Пакетно сохранено {success_count} пользователей")

        except Exception as e:
            logger.error(f"Ошибка пакетного сохранения: {e}", exc_info=True)
            error_count += len(users) - success_count

        return success_count, error_count

    async def get_users(self,
                        site_id: Optional[int] = None,
                        is_active: Optional[bool] = None,
                        limit: int = 100,
                        offset: int = 0) -> Sequence[Users]:
        """Получить пользователей с фильтрацией."""
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
                users = result.scalars().all()

                logger.debug(f"📜 ORM: Получено {len(users)} пользователей")
                return users
        except Exception as e:
            logger.error(f"Ошибка чтения пользователей: {e}", exc_info=True)
            return []

    async def update_user(self,
                          user_id: int,
                          user_log: Optional[str] = None,
                          user_name: Optional[str] = None,
                          description: Optional[str] = None,
                          is_active: Optional[bool] = None) -> bool:
        """Обновить пользователя по ID."""
        if engine is None:
            return False

        try:
            async with self._get_session() as session:
                update_data = {}
                if user_log is not None:
                    update_data['user_log'] = user_log
                if user_name is not None:
                    update_data['user_name'] = user_name
                if description is not None:
                    update_data['description'] = description
                if is_active is not None:
                    update_data['is_active'] = is_active

                if not update_data:
                    return True

                query = (
                    update(Users)
                    .where(Users.id == user_id)
                    .values(**update_data)
                )

                result = await session.execute(query)
                await session.commit()

                if result.rowcount > 0:
                    logger.info(f"✅ Пользователь #{user_id} обновлён")
                    return True
                logger.warning(f"⚠️ Пользователь #{user_id} не найден")
                return False
        except Exception as e:
            logger.error(f"Ошибка обновления пользователя: {e}", exc_info=True)
            return False

    async def delete_user(self, user_id: int) -> bool:
        """Удалить пользователя (мягкое удаление)."""
        return await self.update_user(user_id, is_active=False)

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
        """Сохранить группу через ORM."""
        if engine is None:
            logger.error("engine не инициализирован")
            return None

        try:
            async with self._get_session() as session:
                record = SiteGroup(
                    site_id=site_id,
                    group_link=group_link,
                    group_id=group_id,
                    group_name=group_name,
                    is_verified=is_verified,
                )
                session.add(record)
                if commit:
                    await session.commit()
                    await session.refresh(record)
                logger.info(f"💾 ORM: Сохранена группа ID={record.id}")
                return record
        except Exception as e:
            logger.error(f"Ошибка сохранения группы: {e}", exc_info=True)
            return None

    async def save_groups_batch(self,
                                groups: list[dict[str, Any]],
                                commit: bool = True) -> tuple[int, int]:
        """Пакетное сохранение групп."""
        if engine is None or not groups:
            return 0, 0

        success_count = 0
        error_count = 0

        try:
            async with self._get_session() as session:
                for group_data in groups:
                    try:
                        record = SiteGroup(**{k: v for k, v in group_data.items() if hasattr(SiteGroup, k)})
                        session.add(record)
                        success_count += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка добавления группы: {e}")
                        error_count += 1

                if commit and success_count > 0:
                    await session.commit()
                    logger.info(f"✅ Пакетно сохранено {success_count} групп")

        except Exception as e:
            logger.error(f"Ошибка пакетного сохранения: {e}", exc_info=True)
            error_count += len(groups) - success_count

        return success_count, error_count

    async def get_groups(self,
                         site_id: Optional[int] = None,
                         is_verified: Optional[bool] = None,
                         is_active: Optional[bool] = None,
                         limit: int = 100,
                         offset: int = 0) -> Sequence[SiteGroup]:
        """Получить группы с фильтрацией."""
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
                groups = result.scalars().all()

                logger.debug(f"📜 ORM: Получено {len(groups)} групп")
                return groups
        except Exception as e:
            logger.error(f"Ошибка чтения групп: {e}", exc_info=True)
            return []

    async def update_group(self,
                           group_id: int,
                           group_name: Optional[str] = None,
                           is_verified: Optional[bool] = None,
                           is_active: Optional[bool] = None) -> bool:
        """Обновить группу по ID."""
        if engine is None:
            return False

        try:
            async with self._get_session() as session:
                update_data = {}
                if group_name is not None:
                    update_data['group_name'] = group_name
                if is_verified is not None:
                    update_data['is_verified'] = is_verified
                if is_active is not None:
                    update_data['is_active'] = is_active

                if not update_data:
                    return True

                query = (
                    update(SiteGroup)
                    .where(SiteGroup.id == group_id)
                    .values(**update_data)
                )

                result = await session.execute(query)
                await session.commit()

                if result.rowcount > 0:
                    logger.info(f"✅ Группа #{group_id} обновлена")
                    return True
                logger.warning(f"⚠️ Группа #{group_id} не найдена")
                return False
        except Exception as e:
            logger.error(f"Ошибка обновления группы: {e}", exc_info=True)
            return False

    async def delete_group(self, group_id: int) -> bool:
        """Удалить группу (мягкое удаление)."""
        return await self.update_group(group_id, is_active=False)

    # -------------------------------------------------------------------------
    # === CRUD: StatGroupUser ===
    # -------------------------------------------------------------------------

    async def save_stat(self,
                        group_id: int,
                        stat_date: date,
                        count_user: int,
                        commit: bool = True) -> Optional[StatGroupUser]:
        """Сохранить статистику через ORM."""
        if engine is None:
            logger.error("engine не инициализирован")
            return None

        try:
            async with self._get_session() as session:
                record = StatGroupUser(
                    group_id=group_id,
                    stat_date=stat_date,
                    count_user=count_user,
                )
                session.add(record)
                if commit:
                    await session.commit()
                    await session.refresh(record)
                logger.info(f"💾 ORM: Сохранена статистика ID={record.id}")
                return record
        except Exception as e:
            logger.error(f"Ошибка сохранения статистики: {e}", exc_info=True)
            return None

    async def get_stats(self,
                        group_id: Optional[int] = None,
                        date_from: Optional[date] = None,
                        date_to: Optional[date] = None,
                        limit: int = 100,
                        offset: int = 0) -> Sequence[StatGroupUser]:
        """Получить статистику с фильтрацией."""
        if engine is None:
            return []

        try:
            async with self._get_session() as session:
                query = select(StatGroupUser).order_by(StatGroupUser.stat_date.desc())

                if group_id is not None:
                    query = query.where(StatGroupUser.group_id == group_id)
                if date_from is not None:
                    query = query.where(StatGroupUser.stat_date >= date_from)
                if date_to is not None:
                    query = query.where(StatGroupUser.stat_date <= date_to)

                query = query.limit(limit).offset(offset)

                result = await session.execute(query)
                stats = result.scalars().all()

                logger.debug(f"📜 ORM: Получено {len(stats)} записей статистики")
                return stats
        except Exception as e:
            logger.error(f"Ошибка чтения статистики: {e}", exc_info=True)
            return []

    # -------------------------------------------------------------------------
    # === Статистика (аналог get_stats в примере) ===
    # -------------------------------------------------------------------------

    async def get_summary_stats(
            self,
            group_by: Optional[str] = None  # 'site', 'group', 'date'
    ) -> list[dict[str, Any]]:
        """
        Получить сводную статистику по данным.
        """
        if engine is None:
            return []

        try:
            async with self._get_session() as session:
                if group_by == 'site':
                    query = select(
                        Sites.id,
                        Sites.title,
                        func.count(Users.id).label('users_count'),
                        func.count(SiteGroup.id).label('groups_count')
                    ).join(Users, Users.site_id == Sites.id, isouter=True)\
                     .join(SiteGroup, SiteGroup.site_id == Sites.id, isouter=True)\
                     .group_by(Sites.id, Sites.title)
                elif group_by == 'group':
                    query = select(
                        SiteGroup.id,
                        SiteGroup.group_name,
                        func.count(StatGroupUser.id).label('stats_count'),
                        func.sum(StatGroupUser.count_user).label('total_members')
                    ).join(StatGroupUser, StatGroupUser.group_id == SiteGroup.id, isouter=True)\
                     .group_by(SiteGroup.id, SiteGroup.group_name)
                else:
                    query = select(
                        func.count(Sites.id).label('total_sites'),
                        func.count(Users.id).label('total_users'),
                        func.count(SiteGroup.id).label('total_groups'),
                        func.sum(StatGroupUser.count_user).label('total_members'),
                        func.min(StatGroupUser.stat_date).label('first_stat_date'),
                        func.max(StatGroupUser.stat_date).label('last_stat_date')
                    )

                result = await session.execute(query)
                rows = result.fetchall()

                return [dict(row._mapping) for row in rows]

        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}", exc_info=True)
            return []