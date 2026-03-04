# src/app_google/models.py
from datetime import datetime
from typing_extensions import Annotated
from sqlalchemy import (
    func, DateTime, Date, Boolean, Text, String, sql,
    UniqueConstraint  # 🔥 Импорт ограничения
)
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, registry
from sqlalchemy.ext.asyncio import AsyncAttrs

str_64 = Annotated[str, 64]
int_pk = Annotated[int, mapped_column(primary_key=True)]

created_at = Annotated[
    datetime, mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
]
updated_at_annotation = Annotated[
    datetime, mapped_column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),  # Авто-обновление
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
    # schema задаётся в каждой таблице явно
    registry = registry(
        type_annotation_map={str_64: String(64)}
    )


class TaskList(Base):
    __tablename__ = 'task_list'
    __table_args__ = (
        UniqueConstraint('key', 'date', name='uq_task_list_key_date'),
        {
            'schema': 'test',
            'comment': '{"name": "Список задач", "npa": ""}',
        }
    )

    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at_annotation]
    is_active: Mapped[is_active]

    # Пользовательские данные
    key: Mapped[str | None] = mapped_column(Text, comment='{"name":"№ п/п"}')
    date: Mapped[datetime | None] = mapped_column(Date, comment='{"name":"дата комментария"}')
    link: Mapped[str | None] = mapped_column(Text, comment='{"name":"Ссылка ТГ"}')
    short_description: Mapped[str | None] = mapped_column(Text, comment='{"name":"Краткое описание"}')
    autor: Mapped[str | None] = mapped_column(Text, comment='{"name":"Кто нашел ссылку и написал диалог"}')
    text: Mapped[str | None] = mapped_column(Text, comment='{"name":"Текст комментария"}')
    corrections: Mapped[str | None] = mapped_column(Text, comment='{"name":"Исправления"}')
    responsible: Mapped[str | None] = mapped_column(Text, comment='{"name":"Ответственный за публикацию"}')
    status: Mapped[str | None] = mapped_column(Text, comment='{"name":"Статус"}')
