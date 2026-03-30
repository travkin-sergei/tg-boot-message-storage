# src/app_google/models.py
from datetime import datetime
from typing_extensions import Annotated
from sqlalchemy import (
    func, DateTime, Date, Boolean, Text, String, sql
)
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, registry
from sqlalchemy.ext.asyncio import AsyncAttrs

from src.config.logger import logger

str_64 = Annotated[str, 64]
int_pk = Annotated[int, mapped_column(primary_key=True)]

created_at = Annotated[
    datetime, mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
]
updated_at_annotation = Annotated[
    datetime, mapped_column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        comment='{"name":"Запись обновлена"}'
    )
]
is_active = Annotated[
    bool, mapped_column(
        Boolean, server_default=sql.true(), nullable=False, default=True, comment='{"name":"Запись активна"}'
    )
]


class Base(AsyncAttrs, DeclarativeBase):
    __abstract__ = True
    registry = registry(
        type_annotation_map={str_64: String(64)}
    )


class TaskList(Base):
    __tablename__ = 'task_list'
    __table_args__ = (
        {
            'schema': 'test',
            'comment': '{"name": "Список задач", "npa": ""}',
        }
    )

    link_post: Mapped[str] = mapped_column(Text, primary_key=True, comment='{"name":"Ссылка на пост"}')

    # Стандартные поля аудита
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at_annotation]
    is_active: Mapped[is_active]

    # Пользовательские данные
    number: Mapped[int | None] = mapped_column(Text, comment='{"name":"№ п/п"}')
    date_comment: Mapped[datetime | None] = mapped_column(Date, comment='{"name":"дата комментария"}')
    short_description: Mapped[str | None] = mapped_column(Text, comment='{"name":"Краткое описание"}')
    autor: Mapped[str | None] = mapped_column(Text, comment='{"name":"Кто нашел ссылку и написал диалог"}')
    subscribers: Mapped[int | None] = mapped_column(comment='{"name":"Кол-во подписчиков"}')
    comment: Mapped[str | None] = mapped_column(Text, comment='{"name":"Текст комментария"}')
    corrections: Mapped[str | None] = mapped_column(Text, comment='{"name":"Исправления"}')
    responsible: Mapped[str | None] = mapped_column(Text, comment='{"name":"Ответственный за публикацию"}')
    status: Mapped[str | None] = mapped_column(Text, comment='{"name":"Статус"}')



async def init_db_schema(shema):
    """Создаёт все таблицы из моделей, если их нет."""
    from src.config.database import engine
    from sqlalchemy import text

    if engine is None:
        logger.error("engine не инициализирован")
        return False

    try:
        # Сначала гарантируем, что схема существует
        async with engine.begin() as conn:
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {shema}"))
            # Создаём таблицы только в этой схеме
            await conn.run_sync(Base.metadata.create_all)
        logger.info(f"Таблицы инициализированы в схеме '{shema}'")
        return True
    except Exception as e:
        logger.error(f"Ошибка инициализации таблиц: {e}", exc_info=True)
        return False
