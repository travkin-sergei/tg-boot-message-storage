# src/app_google/function.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from typing import Optional
from .models import TaskList
from ..app_log import logger


async def upsert_task_list_entry(session: AsyncSession,
                                 key: Optional[str] = None,
                                 date: Optional[datetime] = None,
                                 link: Optional[str] = None,
                                 short_description: Optional[str] = None,
                                 autor: Optional[str] = None,
                                 text: Optional[str] = None,
                                 corrections: Optional[str] = None,
                                 responsible: Optional[str] = None,
                                 status: Optional[str] = None,
                                 is_active: bool = True) -> Optional[TaskList]:
    """
    UPSERT: вставляет запись или обновляет при конфликте (key + date).

    Логика:
    - Если записи с таким key+date нет → INSERT
    - Если запись с таким key+date есть → UPDATE всех полей

    Args:
        session: Асинхронная сессия SQLAlchemy.
        key: Уникальный ключ (вместе с date).
        date: Дата комментария.
        link: Ссылка ТГ.
        short_description: Краткое описание.
        autor: Кто нашел ссылку.
        text: Текст комментария.
        corrections: Исправления.
        responsible: Ответственный.
        status: Статус.
        is_active: Активность записи.

    Returns:
        TaskList: Созданная или обновлённая запись.
        None: При ошибке.
    """
    try:
        # Данные для вставки/обновления
        values = {
            'key': key,
            'date': date,
            'link': link,
            'short_description': short_description,
            'autor': autor,
            'text': text,
            'corrections': corrections,
            'responsible': responsible,
            'status': status,
            'is_active': is_active,
        }

        # PostgreSQL INSERT ... ON CONFLICT DO UPDATE
        stmt = pg_insert(TaskList).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=['key', 'date'],  # 🔥 Поля из UniqueConstraint
            set_={
                'link': stmt.excluded.link,
                'short_description': stmt.excluded.short_description,
                'autor': stmt.excluded.autor,
                'text': stmt.excluded.text,
                'corrections': stmt.excluded.corrections,
                'responsible': stmt.excluded.responsible,
                'status': stmt.excluded.status,
                'is_active': stmt.excluded.is_active,
                'updated_at': func.now(),  # 🔥 Авто-обновление timestamp
            }
        )

        await session.execute(stmt)
        await session.commit()

        # Получаем актуальную запись из БД
        result = await session.execute(
            select(TaskList).where(
                TaskList.key == key,
                TaskList.date == date
            )
        )
        entry = result.scalar_one_or_none()

        if entry:
            logger.info(f"UPSERT: key={key}, date={date}, id={entry.id}")
        return entry

    except SQLAlchemyError as error:
        await session.rollback()
        logger.error(f"Ошибка UPSERT: {error}", exc_info=True)
        return None
    except Exception as error:
        await session.rollback()
        logger.error(f"Неожиданная ошибка: {error}", exc_info=True)
        return None


async def bulk_upsert_task_list(session: AsyncSession,
                                records: list[dict]) -> dict:
    """
    Массовый UPSERT записей в таблицу task_list.

    Args:
        session: Асинхронная сессия SQLAlchemy.
        records: Список словарей с данными для вставки/обновления.
                 Каждый словарь должен содержать 'key' и 'date'.

    Returns:
        dict: Статистика {'total': N, 'success': N, 'errors': N}
    """
    stats = {'total': len(records), 'success': 0, 'errors': 0}

    if not records:
        logger.warning("⚠️ Пустой список записей для UPSERT")
        return stats

    try:
        # Фильтруем записи без key или date (они не пройдут UniqueConstraint)
        valid_records = [
            r for r in records
            if r.get('key') is not None and r.get('date') is not None
        ]

        if not valid_records:
            logger.warning("⚠️ Нет валидных записей (требуется key и date)")
            return stats

        # PostgreSQL INSERT ... ON CONFLICT
        stmt = pg_insert(TaskList).values(valid_records)
        stmt = stmt.on_conflict_do_update(
            index_elements=['key', 'date'],
            set_={
                'link': stmt.excluded.link,
                'short_description': stmt.excluded.short_description,
                'autor': stmt.excluded.autor,
                'text': stmt.excluded.text,
                'corrections': stmt.excluded.corrections,
                'responsible': stmt.excluded.responsible,
                'status': stmt.excluded.status,
                'is_active': stmt.excluded.is_active,
                'updated_at': func.now(),
            }
        )

        await session.execute(stmt)
        await session.commit()

        stats['success'] = len(valid_records)
        logger.info(f"✅ Массовый UPSERT: {stats['success']} из {stats['total']} записей")
        return stats

    except SQLAlchemyError as error:
        await session.rollback()
        logger.error(f"❌ Ошибка массового UPSERT: {error}", exc_info=True)
        stats['errors'] = stats['total']
        return stats
    except Exception as error:
        await session.rollback()
        logger.error(f"❌ Неожиданная ошибка: {error}", exc_info=True)
        stats['errors'] = stats['total']
        return stats
