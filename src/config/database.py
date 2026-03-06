# src/config/database.py
"""
Универсальный класс подключения к базам данных
"""
import os
import re
import logging
import asyncpg
import psycopg2

from dotenv import load_dotenv
from typing import Optional, Dict
from urllib.parse import urlparse, parse_qs
from contextlib import contextmanager, asynccontextmanager
from psycopg2 import pool as psycopg2_pool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.config.logger import logger

load_dotenv()


class SecureString(str):
    """Безопасная строка, скрывает содержимое при выводе."""

    def __str__(self):
        return "(********)"

    def __repr__(self):
        return "(********)"

    def __format__(self, format_spec):
        return "(********)"

    def __getattribute__(self, name):
        if name in ['__reduce__', '__reduce_ex__', '__getnewargs__', '__getstate__']:
            raise AttributeError(f"Доступ к методу '{name}' запрещён")
        return super().__getattribute__(name)

    def get_raw(self) -> str:
        """Получение исходного значения (только для внутреннего использования)."""
        return super().__str__()


class DBConnection:
    """Безопасное подключение к базе данных."""

    PASSWORD_PATTERN = re.compile(r'password=([^@\s]+)@')

    def __init__(self, db_name: Optional[str] = None):
        self.__db_name = db_name
        self.__connection_string: Optional[SecureString] = None
        self.__connection_pool = None
        self.__initialized = False
        self._initialize_connection(db_name)

    def _initialize_connection(self, db_name: Optional[str]) -> None:
        if db_name is None:
            logger.warning("Имя базы данных не указано")
            return
        connection_string = self._get_connection_string(db_name)
        if connection_string:
            self.__connection_string = SecureString(connection_string)
            self.__initialized = True
            logger.info(f"Подключение к БД '{db_name}' инициализировано")
        else:
            logger.error(f"Не найдена строка подключения для БД '{db_name}'")

    def _parse_postgres_url(self, url: str) -> str:
        """Конвертирует postgresql:// URL в DSN формат для psycopg2."""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        dsn = f"host={parsed.hostname} port={parsed.port or 5432} dbname={parsed.path.lstrip('/')}"
        if parsed.username:
            dsn += f" user={parsed.username}"
        if parsed.password:
            dsn += f" password={parsed.password}"
        for k, v in params.items():
            dsn += f" {k}={v[0]}"
        return dsn

    def _get_connection_string(self, db_name: str) -> Optional[str]:
        """Получение строки подключения по имени базы данных."""
        connection_map = {
            'base_01': os.getenv('DB_LOCAL_01'),
            'app_google_target': os.getenv('APP_GOOGLE_DB'),
        }
        conn_str = connection_map.get(db_name)

        # Конвертируем URL-формат в DSN, если нужно
        if conn_str and conn_str.startswith('postgresql://'):
            return self._parse_postgres_url(conn_str)
        return conn_str

    def get_connection(self) -> Optional[psycopg2.extensions.connection]:
        if not self.__connection_string:
            logger.warning("Строка подключения не инициализирована")
            return None
        try:
            conn = psycopg2.connect(self.__connection_string.get_raw())
            logger.debug(f"Успешное подключение к БД '{self.__db_name}'")
            return conn
        except psycopg2.Error as error:
            logger.error(f"Ошибка подключения к БД: {type(error).__name__}: {error}")
            return None
        except Exception as error:
            logger.error(f"Неожиданная ошибка подключения: {type(error).__name__}: {error}")
            return None

    @contextmanager
    def get_cursor(self, commit: bool = False):
        conn = self.get_connection()
        if conn is None:
            raise ConnectionError("Не удалось подключиться к базе данных")
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as error:
            conn.rollback()
            logger.error(f"Ошибка транзакции: {type(error).__name__}")
            raise
        finally:
            cursor.close()
            conn.close()

    def create_pool(self, minconn: int = 1, maxconn: int = 10) -> bool:
        if not self.__connection_string:
            logger.warning("Невозможно создать пул: строка подключения не инициализирована")
            return False
        try:
            self.__connection_pool = psycopg2_pool.SimpleConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                dsn=self.__connection_string.get_raw()
            )
            self.__initialized = True
            logger.info(f"Пул соединений создан для БД '{self.__db_name}'")
            return True
        except Exception as error:
            logger.error(f"Ошибка создания пула: {error}")
            return False

    def get_pooled_connection(self) -> Optional[psycopg2.extensions.connection]:
        if self.__connection_pool is None:
            logger.warning("Пул соединений не инициализирован")
            return None
        try:
            return self.__connection_pool.getconn()
        except Exception as error:
            logger.error(f"Ошибка получения соединения из пула: {error}")
            return None

    def return_pooled_connection(self, conn: psycopg2.extensions.connection) -> None:
        if self.__connection_pool and conn:
            try:
                self.__connection_pool.putconn(conn)
            except Exception as error:
                logger.error(f"Ошибка возврата соединения в пул: {error}")

    @contextmanager
    def get_pooled_cursor(self, commit: bool = False):
        conn = self.get_pooled_connection()
        if conn is None:
            raise ConnectionError("Не удалось получить соединение из пула")
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as error:
            conn.rollback()
            logger.error(f"Ошибка транзакции: {type(error).__name__}")
            raise
        finally:
            cursor.close()
            self.return_pooled_connection(conn)

    def close_pool(self) -> None:
        if self.__connection_pool:
            try:
                self.__connection_pool.closeall()
                logger.info(f"Пул соединений закрыт для БД '{self.__db_name}'")
            except Exception as error:
                logger.error(f"Ошибка закрытия пула: {error}")
            finally:
                self.__connection_pool = None

    @property
    def is_initialized(self) -> bool:
        return self.__initialized

    @property
    def db_name(self) -> Optional[str]:
        return self.__db_name

    def __str__(self) -> str:
        return f"DBConnection(db='{self.__db_name or 'None'}')"

    def __repr__(self) -> str:
        return f"DBConnection(db='{self.__db_name or 'None'}')"

    def __del__(self):
        self.close_pool()

    def _sanitize_connection_string(self, conn_string: str) -> str:
        if not conn_string:
            return "(none)"
        return self.PASSWORD_PATTERN.sub('password=****@', conn_string)

    def log_connection_info(self, level: int = logging.INFO) -> None:
        if self.__connection_string:
            sanitized = self._sanitize_connection_string(str(self.__connection_string))
            logger.log(level, f"Подключение к БД '{self.__db_name}': {sanitized}")
        else:
            logger.log(level, f"Подключение к БД '{self.__db_name}' не инициализировано")


class AsyncDBConnection:
    def __init__(self, db_name: str = None):
        self.__db_name = db_name
        self.__connection_string: Optional[SecureString] = None
        self.__pool = None
        self.__initialized = False
        self._initialize_connection(db_name)

    def _initialize_connection(self, db_name: str):
        conn_str = self._get_connection_string(db_name)
        if conn_str:
            self.__connection_string = SecureString(conn_str)
            self.__initialized = True
            logger.info(f"Async-подключение '{db_name}' инициализировано")

    def _get_connection_string(self, db_name: str) -> Optional[str]:
        connection_map = {'app_google_target': os.getenv('APP_GOOGLE_DB')}
        conn_str = connection_map.get(db_name)
        # asyncpg понимает postgresql:// нативно
        return conn_str

    async def create_pool(self, min_size=1, max_size=10):
        if not self.__connection_string:
            return False
        self.__pool = await asyncpg.create_pool(
            self.__connection_string.get_raw(),
            min_size=min_size, max_size=max_size
        )
        return True

    @asynccontextmanager
    async def get_cursor(self):
        if not self.__pool:
            await self.create_pool()
        async with self.__pool.acquire() as conn:
            try:
                yield conn
            finally:
                pass  # asyncpg сам вернёт соединение

    async def close_pool(self):
        if self.__pool:
            await self.__pool.close()
            self.__pool = None

    @property
    def is_initialized(self) -> bool:
        return self.__initialized

    @property
    def db_name(self) -> Optional[str]:
        return self.__db_name

    def __str__(self) -> str:
        return f"AsyncDBConnection(db='{self.__db_name}')"


class DBManager:
    """
    Менеджер подключений к нескольким базам данных.
    Singleton паттерн для управления подключениями (sync + async).
    """

    _instance = None
    _connections: Dict[str, DBConnection] = {}
    _async_connections: Dict[str, AsyncDBConnection] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_connection(cls, db_name: str) -> DBConnection:
        """Получение синхронного подключения (синглтон)."""
        if db_name not in cls._connections:
            cls._connections[db_name] = DBConnection(db_name)
        return cls._connections[db_name]

    @classmethod
    def get_async_connection(cls, db_name: str) -> AsyncDBConnection:
        """Получение асинхронного подключения (синглтон)."""
        if db_name not in cls._async_connections:
            cls._async_connections[db_name] = AsyncDBConnection(db_name)
        return cls._async_connections[db_name]

    @classmethod
    def initialize_all(cls, db_names: list, async_mode: bool = False) -> None:
        """Инициализация подключений (синхронных или асинхронных)."""
        if async_mode:
            for db_name in db_names:
                cls.get_async_connection(db_name)
            logger.info(f"Инициализировано async-подключений: {len(cls._async_connections)}")
        else:
            for db_name in db_names:
                cls.get_connection(db_name)
            logger.info(f"Инициализировано sync-подключений: {len(cls._connections)}")

    @classmethod
    async def close_all_async(cls) -> None:
        """Закрытие всех асинхронных пулов соединений."""
        for db_name, connection in cls._async_connections.items():
            await connection.close_pool()
        cls._async_connections.clear()
        logger.info("Все async-подключения закрыты")

    @classmethod
    def close_all(cls) -> None:
        """Закрытие всех синхронных пулов соединений."""
        for db_name, connection in cls._connections.items():
            connection.close_pool()
        cls._connections.clear()
        logger.info("Все sync-подключения закрыты")

    @classmethod
    def get_all_connections(cls) -> Dict[str, DBConnection]:
        """Копия всех синхронных подключений."""
        return cls._connections.copy()

    @classmethod
    def get_all_async_connections(cls) -> Dict[str, AsyncDBConnection]:
        """Копия всех асинхронных подключений."""
        return cls._async_connections.copy()

    @classmethod
    def reset(cls) -> None:
        """Сброс всех подключений (для тестов)."""
        cls.close_all()
        cls._async_connections.clear()
        cls._connections.clear()
        logger.info("DBManager сброшен")


__all__ = ['DBConnection', 'DBManager', 'SecureString', 'AsyncDBConnection']

DATABASE_URL = os.getenv('APP_GOOGLE_DB')

if not DATABASE_URL:
    logger.error("APP_GOOGLE_DB не задан в .env")
    engine = None
    async_session = None
else:
    original_url = DATABASE_URL.strip()

    # Исправляем опечатки в протоколе
    if original_url.startswith('ostgresql://'):
        logger.warning("Исправлена опечатка: 'ostgresql://' → 'postgresql://'")
        original_url = 'postgresql' + original_url[11:]  # добавляем 'p'

    # Добавляем драйвер asyncpg, если не указан
    if original_url.startswith('postgresql://') and 'postgresql+asyncpg://' not in original_url:
        logger.info("Добавлен драйвер asyncpg к URL")
        DATABASE_URL = original_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
    elif original_url.startswith('postgresql+asyncpg://'):
        DATABASE_URL = original_url
    else:
        logger.error(f"Неподдерживаемый формат URL: {original_url}")
        DATABASE_URL = None

    if DATABASE_URL:
        try:
            engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
            async_session = async_sessionmaker(engine, expire_on_commit=False)
            logger.info("SQLAlchemy engine инициализирован")
        except Exception as e:
            logger.error(f"Ошибка создания engine: {e}", exc_info=True)
            engine = None
            async_session = None
    else:
        engine = None
        async_session = None
