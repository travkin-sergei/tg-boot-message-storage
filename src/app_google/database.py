"""
Работа с БД через SQLAlchemy ORM для app_google.
Зависимости:
- src.config.database (engine, async_session)
- src.config.logger
- src.app_google.models (TaskList, Base)
"""
from datetime import datetime
from typing import Optional, Sequence, TypeVar

from sqlalchemy import select, func, update, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.app_google.models import Base, TaskList
from src.config.database import engine, async_session
from src.config.logger import logger

T = TypeVar('T', bound=Base)


class AppGoogleDB:
    """Работа с БД через SQLAlchemy ORM для app_google."""

    # Схема по умолчанию (можно переопределить при инициализации)
    DEFAULT_SCHEMA: str = 'test'

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
            raise RuntimeError(" engine не инициализирован")
        return self._session_factory()

    @staticmethod
    async def ensure_schema_exists(schema: Optional[str] = None) -> bool:
        """Создать схему, если не существует (требует прав CREATE SCHEMA)."""
        if engine is None:
            logger.error("engine не инициализирован")
            return False

        target_schema = schema or AppGoogleDB.DEFAULT_SCHEMA
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
    # === CRUD операции для TaskList ===
    # -------------------------------------------------------------------------

    async def save_task(self,
                        number: Optional[str] = None,
                        date_comment: Optional[datetime] = None,
                        link_post: Optional[str] = None,
                        short_description: Optional[str] = None,
                        autor: Optional[str] = None,
                        subscribers: Optional[int] = None,
                        comment: Optional[str] = None,
                        corrections: Optional[str] = None,
                        responsible: Optional[str] = None,
                        status: Optional[str] = None,
                        commit: bool = True) -> Optional[TaskList]:
        """
        Сохранить задачу через ORM.

        Returns:
            TaskList | None: Сохранённый объект или None при ошибке.
        """
        if engine is None:
            logger.error("engine не инициализирован")
            return None

        try:
            async with self._get_session() as session:
                record = TaskList(
                    number=number,
                    date_comment=date_comment,
                    link_post=link_post,
                    short_description=short_description,
                    autor=autor,
                    subscribers=subscribers,
                    comment=comment,
                    corrections=corrections,
                    responsible=responsible,
                    status=status,
                )
                session.add(record)
                if commit:
                    await session.commit()
                    await session.refresh(record)
                logger.info(f"💾 ORM: Сохранена задача ID={record.id}")
                return record
        except Exception as e:
            logger.error(f"Ошибка сохранения задачи: {e}", exc_info=True)
            return None

    async def save_tasks_batch(self,
                               tasks: list[dict[str, any]],
                               commit: bool = True) -> tuple[int, int]:
        """
        Пакетное сохранение задач (bulk insert).

        Args:
            tasks: Список словарей с данными задач.

        Returns:
            tuple: (успешно сохранено, ошибок)
        """
        if engine is None or not tasks:
            return 0, 0

        success_count = 0
        error_count = 0

        try:
            async with self._get_session() as session:
                for task_data in tasks:
                    try:
                        record = TaskList(**{k: v for k, v in task_data.items() if hasattr(TaskList, k)})
                        session.add(record)
                        success_count += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка добавления задачи: {e}")
                        error_count += 1

                if commit and success_count > 0:
                    await session.commit()
                    logger.info(f"✅ Пакетно сохранено {success_count} задач")

        except Exception as e:
            logger.error(f" Ошибка пакетного сохранения: {e}", exc_info=True)
            error_count += len(tasks) - success_count

        return success_count, error_count

    async def get_tasks(self,
                        status_filter: Optional[str] = None,
                        date_from: Optional[datetime] = None,
                        date_to: Optional[datetime] = None,
                        limit: int = 100,
                        offset: int = 0) -> Sequence[TaskList]:
        """
        Получить задачи с фильтрацией.
        """
        if engine is None:
            return []

        try:
            async with self._get_session() as session:
                query = select(TaskList).order_by(TaskList.date_comment.desc())

                if status_filter:
                    query = query.where(TaskList.status == status_filter)
                if date_from:
                    query = query.where(TaskList.date_comment >= date_from)
                if date_to:
                    query = query.where(TaskList.date_comment <= date_to)

                query = query.limit(limit).offset(offset)

                result = await session.execute(query)
                tasks = result.scalars().all()

                logger.debug(f"📜 ORM: Получено {len(tasks)} задач")
                return tasks
        except Exception as e:
            logger.error(f" Ошибка чтения задач: {e}", exc_info=True)
            return []

    async def update_task_status(self,
                                 task_id: int,
                                 new_status: str,
                                 corrections: Optional[str] = None) -> bool:
        """
        Обновить статус задачи.
        """
        if engine is None:
            return False

        try:
            async with self._get_session() as session:
                update_data = {'status': new_status}
                if corrections is not None:
                    update_data['corrections'] = corrections

                query = (
                    update(TaskList)
                    .where(TaskList.id == task_id)
                    .values(**update_data)
                )

                result = await session.execute(query)
                await session.commit()

                if result.rowcount > 0:
                    logger.info(f"✅ Задача #{task_id}: статус обновлён на '{new_status}'")
                    return True
                logger.warning(f"⚠️ Задача #{task_id} не найдена для обновления")
                return False
        except Exception as e:
            logger.error(f" Ошибка обновления задачи: {e}", exc_info=True)
            return False

    async def delete_task(self, task_id: int) -> bool:
        """
        Удалить задачу по ID (мягкое удаление через is_active).
        """
        return await self.update_task_status(task_id, 'deleted')  # или реальное удаление

    async def get_stats(
            self,
            group_by: Optional[str] = None  # 'status', 'responsible', 'date_comment'
    ) -> list[dict[str, any]]:
        """
        Получить статистику по задачам.
        """
        if engine is None:
            return []

        try:
            async with self._get_session() as session:
                if group_by == 'status':
                    query = select(
                        TaskList.status,
                        func.count(TaskList.id).label('count'),
                        func.sum(TaskList.subscribers).label('total_subscribers')
                    ).group_by(TaskList.status)
                elif group_by == 'responsible':
                    query = select(
                        TaskList.responsible,
                        func.count(TaskList.id).label('count')
                    ).group_by(TaskList.responsible)
                else:
                    query = select(
                        func.count(TaskList.id).label('total'),
                        func.sum(TaskList.subscribers).label('total_subscribers'),
                        func.min(TaskList.date_comment).label('first_date'),
                        func.max(TaskList.date_comment).label('last_date')
                    )

                result = await session.execute(query)
                rows = result.fetchall()

                # Конвертируем Row в dict
                return [dict(row._mapping) for row in rows]

        except Exception as e:
            logger.error(f" Ошибка получения статистики: {e}", exc_info=True)
            return []
