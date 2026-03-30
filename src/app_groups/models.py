# src/app_groups/models.py
"""
Модуль модели данных для приложения 'app_groups'.
Хранит информацию о сайтах и их социальных группах.
"""

from datetime import datetime, date
from typing_extensions import Annotated
from sqlalchemy import (
    text, DateTime, Date, Boolean, Text, String, BigInteger, Integer,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
from sqlalchemy.ext.asyncio import AsyncAttrs

from src.app_groups.config import DB_SCHEMA
from src.config.database import engine
from src.config.logger import logger

# === Типовые аннотации ===
str_64 = Annotated[str, mapped_column(String(64))]
str_128 = Annotated[str, mapped_column(String(128))]
str_255 = Annotated[str, mapped_column(String(255))]
str_url = Annotated[str, mapped_column(String(2048))]
int_pk = Annotated[int, mapped_column(Integer, primary_key=True)]

# === Поля аудита ===
created_at_type = Annotated[
    datetime,
    mapped_column(
        DateTime(timezone=False),
        server_default=text('now()'),
        comment='{"name":"Запись создана"}'
    )
]
updated_at_type = Annotated[
    datetime,
    mapped_column(
        DateTime(timezone=False),
        server_default=text('now()'),
        onupdate=text('now()'),
        comment='{"name":"Запись обновлена"}'
    )
]
is_active_type = Annotated[
    bool,
    mapped_column(
        Boolean,
        server_default='true',
        nullable=False,
        default=True,
        comment='{"name":"Запись активна"}'
    )
]


class AuditMixin:
    """Миксин для добавления стандартных полей аудита."""
    is_active: Mapped[is_active_type]
    created_at: Mapped[created_at_type]
    updated_at: Mapped[updated_at_type]


class Base(AsyncAttrs, AuditMixin, DeclarativeBase):
    """Базовый класс для app_groups."""
    __abstract__ = True


class Sites(Base):
    """
    Справочник сайтов.
    Уникальность обеспечивается полем url.
    """
    __tablename__ = 'site'
    __table_args__ = (
        UniqueConstraint('url', name='uq_site_url'),
        Index('ix_site_is_active', 'is_active'),
        {
            'schema': DB_SCHEMA,
            'comment': '{"name": "Справочник сайтов", "module": "app_groups"}',
        }
    )
    id: Mapped[int_pk]

    title: Mapped[str | None] = mapped_column(
        String(255), nullable=True,
        comment='{"name":"Название сайта"}'
    )
    url: Mapped[str | None] = mapped_column(
        String(2048), nullable=True, unique=False,
        comment='{"name":"Полный URL сайта (с протоколом)"}'
    )
    regexp: Mapped[str | None] = mapped_column(
        String(255), nullable=True,
        comment='{"name":"Регулярное выражение для валидации ссылок групп"}'
    )
    description: Mapped[str | None] = mapped_column(
        Text, nullable=True,
        comment='{"name":"Описание сайта"}'
    )

    # Relationships для удобства
    users: Mapped[list["Users"]] = relationship("Users", back_populates="site")
    groups: Mapped[list["SiteGroup"]] = relationship("SiteGroup", back_populates="site")

    def __repr__(self):
        return f"<Sites(id={self.id}, url={self.url})>"


class Users(Base):
    """Список аккаунтов на каждом сайте."""
    __tablename__ = 'user'
    __table_args__ = (
        {
            'schema': DB_SCHEMA,
            'comment': '{"name": "Справочник пользователей."}',
        }
    )
    id: Mapped[int_pk]
    site_id: Mapped[int] = mapped_column(
        ForeignKey(f'{DB_SCHEMA}.site.id'),
        nullable=False, index=True,
        comment='{"name":"ID сайта (внешний ключ)"}'
    )
    user_id: Mapped[str | None] = mapped_column(String(255), nullable=True, comment='{"name":"id пользователя"}')
    user_log: Mapped[str | None] = mapped_column(String(255), nullable=True, comment='{"name":"login"}')
    user_pas: Mapped[str | None] = mapped_column(String(255), nullable=True, comment='{"name":"password"}')
    user_name: Mapped[str | None] = mapped_column(String(255), nullable=True, comment='{"name":"Пользователь"}')
    description: Mapped[str | None] = mapped_column(Text, nullable=True, comment='{"name":"Описание пользователя"}')

    # Relationships для удобства
    site: Mapped["Sites"] = relationship("Sites", back_populates="users")
    user_groups: Mapped[list["UserGroups"]] = relationship("UserGroups", back_populates="user")
    blocked_groups: Mapped[list["BlockUserGroup"]] = relationship("BlockUserGroup", back_populates="user")
    blocked_sites: Mapped[list["BlockUserSite"]] = relationship("BlockUserSite", back_populates="user")


class SiteGroup(Base):
    """
    Социальные группы, связанные с сайтом.
    Уникальность: связка (сайт, платформа, ссылка).
    """
    __tablename__ = 'site_group'
    __table_args__ = (
        UniqueConstraint('site_id', 'group_link', name='uq_site_link'),
        Index('ix_social_groups_group_link', 'group_link'),
        {
            'schema': DB_SCHEMA,
            'comment': '{"name": "Социальные группы сайтов", "module": "app_groups"}',
        }
    )
    id: Mapped[int_pk]
    site_id: Mapped[int] = mapped_column(
        ForeignKey(f'{DB_SCHEMA}.site.id'),
        nullable=False, index=True,
        comment='{"name":"ID сайта (внешний ключ)"}'
    )
    group_link: Mapped[str] = mapped_column(
        String(2048),
        nullable=False,
        comment='{"name":"Ссылка на группу"}'
    )
    group_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment='{"name":"Внешний ID группы"}'
    )
    group_name: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment='{"name":"Название группы"}'
    )
    is_verified: Mapped[bool] = mapped_column(
        Boolean,
        server_default='false',
        default=False,
        nullable=False,
        comment='{"name":"Требуется верификация"}'
    )
    # Relationships для удобства
    site: Mapped["Sites"] = relationship("Sites", back_populates="groups")
    stats: Mapped[list["StatGroupUser"]] = relationship("StatGroupUser", back_populates="group")
    members: Mapped[list["UserGroups"]] = relationship("UserGroups", back_populates="group")
    blocked_users: Mapped[list["BlockUserGroup"]] = relationship("BlockUserGroup", back_populates="group")

    def __repr__(self):
        return f"<SocialGroup(id={self.id}, group_link={self.group_link})>"


class StatGroupUser(Base):
    """
    Групповая статистика.
    """
    __tablename__ = 'stat_group_user'
    __table_args__ = (
        UniqueConstraint('group_id', 'stat_date', name='uq_group_stat_date'),
        {'schema': DB_SCHEMA, 'comment': '{"name": "Статистика социальных групп"}'}
    )
    id: Mapped[int_pk]
    group_id: Mapped[int] = mapped_column(
        ForeignKey(f'{DB_SCHEMA}.site_group.id'), nullable=False, index=True
    )
    stat_date: Mapped[date] = mapped_column(Date, nullable=False)
    count_user: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # Relationships для удобства
    group: Mapped["SiteGroup"] = relationship("SiteGroup", back_populates="stats")


class UserGroups(Base):
    """Связь пользователей и групп на сайте."""
    __tablename__ = 'user_group'
    __table_args__ = (
        # Уникальная связка: пользователь + группа + сайт
        UniqueConstraint('user_id', 'group_id', name='uq_usergroups_user_group'),
        Index('ix_user_group_user', 'user_id'),
        Index('ix_user_group_group', 'group_id'),
        {'schema': DB_SCHEMA, 'comment': '{"name": "Связь пользователей и групп на сайте"}'}
    )
    id: Mapped[int_pk]

    user_id: Mapped[int] = mapped_column(
        ForeignKey(f'{DB_SCHEMA}.user.id'),
        nullable=False, index=True,
        comment='{"name":"ID пользователя"}'
    )
    group_id: Mapped[int] = mapped_column(
        ForeignKey(f'{DB_SCHEMA}.site_group.id'),
        nullable=False, index=True,
        comment='{"name":"ID группы"}'
    )

    # Relationships для удобства
    user: Mapped["Users"] = relationship("Users", back_populates="user_groups")
    group: Mapped["SiteGroup"] = relationship("SiteGroup", back_populates="members")


class BlockUserGroup(Base):
    """Связь пользователей и групп на сайте."""
    __tablename__ = 'block_user_group'  # ← новое уникальное имя
    __table_args__ = (
        # Уникальная связка: пользователь + группа + сайт
        UniqueConstraint('user_id', 'group_id', name='uq_blockusergroup_user_group'),
        Index('ix_block_user_group_user', 'user_id'),
        Index('ix_block_user_group_group', 'group_id'),
        {'schema': DB_SCHEMA, 'comment': '{"name": "Блокировки пользователей в группах на сайте"}'}
    )
    id: Mapped[int_pk]

    user_id: Mapped[int] = mapped_column(
        ForeignKey(f'{DB_SCHEMA}.user.id'),
        nullable=False, index=True,
        comment='{"name":"ID пользователя"}'
    )
    group_id: Mapped[int] = mapped_column(
        ForeignKey(f'{DB_SCHEMA}.site_group.id'),
        nullable=False, index=True,
        comment='{"name":"ID группы"}'
    )
    user: Mapped["Users"] = relationship("Users", back_populates="blocked_groups")
    group: Mapped["SiteGroup"] = relationship("SiteGroup", back_populates="blocked_users")


class BlockUserSite(Base):
    """Связь пользователей и групп на сайте."""
    __tablename__ = 'block_user_site'
    __table_args__ = (
        # Уникальная связка: пользователь + группа + сайт
        UniqueConstraint('user_id', 'site_id', name='uq_blockusersite_user_site'),
        Index('ix_block_user_site_user', 'user_id'),
        Index('ix_block_user_site_site', 'site_id'),
        {'schema': DB_SCHEMA, 'comment': '{"name": "Блокировки пользователей на сайте"}'}
    )
    id: Mapped[int_pk]

    user_id: Mapped[int] = mapped_column(
        ForeignKey(f'{DB_SCHEMA}.user.id'),
        nullable=False, index=True,
        comment='{"name":"ID пользователя"}'
    )
    site_id: Mapped[int] = mapped_column(
        ForeignKey(f'{DB_SCHEMA}.site.id'),
        nullable=False, index=True,
        comment='{"name":"ID сайта (внешний ключ)"}'
    )
    user: Mapped["Users"] = relationship("Users", back_populates="blocked_sites")
    site: Mapped["Sites"] = relationship("Sites")


async def init_db_schema(schema_name: str = DB_SCHEMA) -> bool:
    """
    Создаёт схему и таблицы только для моделей app_groups.

    Args:
        schema_name: Имя схемы в БД (по умолчанию берётся из DB_SCHEMA)

    Returns:
        bool: True при успехе, False при ошибке
    """

    if engine is None:
        logger.error("engine не инициализирован")
        return False

    try:
        # 1. Создаём схему, если не существует
        async with engine.begin() as conn:
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            await conn.commit()  # Явный коммит для создания схемы

        # 2. Создаём таблицы только для текущей схемы
        # Фильтруем метаданные, чтобы не создавать таблицы из других модулей
        tables_to_create = [
            table for table in Base.metadata.tables.values()
            if table.schema == schema_name
        ]

        if not tables_to_create:
            logger.warning(f"Не найдено таблиц для создания в схеме '{schema_name}'")
            return True

        # Для DDL-операций с async engine можно использовать run_sync
        # или создать временный sync engine (более надёжно для некоторых драйверов)
        async with engine.begin() as conn:
            await conn.run_sync(
                lambda sync_conn: Base.metadata.create_all(
                    sync_conn,
                    tables=tables_to_create,
                    checkfirst=True
                )
            )

        logger.info(f"Создано {len(tables_to_create)} таблиц в схеме '{schema_name}'")
        return True

    except Exception as e:
        logger.error(f"Ошибка инициализации схемы '{schema_name}': {e}", exc_info=True)
        return False
