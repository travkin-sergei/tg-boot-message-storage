# src/config/database.py
"""
Универсальный класс подключения к базам данных.
БЕЗОПАСНОСТЬ:
- Все строки подключения инкапсулированы в SecureString
- Логи автоматически маскируют секреты через SensitiveDataFilter
- Ошибки не раскрывают детали подключения
- Разделение форматов URL для asyncpg и SQLAlchemy
"""
import os
import re
import logging
import asyncpg
import psycopg2

from dotenv import load_dotenv
from typing import Optional, Dict, Union
from urllib.parse import urlparse, parse_qs, unquote
from contextlib import contextmanager, asynccontextmanager
from psycopg2 import pool as psycopg2_pool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from src.config.logger import logger, SensitiveDataFilter

load_dotenv()


# =============================================================================
# === SecureString: инкапсуляция чувствительных данных ===
# =============================================================================

class SecureString(str):
    """
    Безопасная строка для хранения секретов.
    При любом выводе/логировании показывает (********).
    """

    def __str__(self):
        return "(********)"

    def __repr__(self):
        return "SecureString(****)"

    def __format__(self, format_spec):
        return "(********)"

    def __getattribute__(self, name):
        # Запрещаем сериализацию и доступ к внутренним методам
        if name in ['__reduce__', '__reduce_ex__', '__getnewargs__',
                    '__getstate__', '__setstate__', '__dict__']:
            raise AttributeError(f"Доступ к '{name}' запрещён для безопасности")
        return super().__getattribute__(name)

    def get_raw(self) -> str:
        """Получение исходного значения — ТОЛЬКО для внутреннего использования."""
        # Добавляем проверку стека вызовов для дополнительной защиты
        import inspect
        frame = inspect.currentframe().f_back
        caller_module = frame.f_globals.get('__name__', '') if frame else ''
        # Разрешаем доступ только из доверенных модулей
        if caller_module and 'config.database' in caller_module:
            return super().__str__()
        logger.warning(f"Попытка доступа к SecureString.get_raw() из {caller_module}")
        return "(********)"  # Возвращаем заглушку при несанкционированном доступе


# =============================================================================
# === Утилиты для безопасной работы с подключениями ===
# =============================================================================

def _normalize_for_asyncpg(conn_str: str) -> str:
    """
    Конвертирует строку подключения в формат для asyncpg.
    asyncpg понимает ТОЛЬКО: postgresql:// или postgres://
    """
    if not conn_str:
        return conn_str

    # Убираем драйвер SQLAlchemy, если есть
    if conn_str.startswith('postgresql+asyncpg://'):
        return conn_str.replace('postgresql+asyncpg://', 'postgresql://', 1)
    if conn_str.startswith('postgresql+pg8000://'):
        return conn_str.replace('postgresql+pg8000://', 'postgresql://', 1)

    return conn_str


def _normalize_for_sqlalchemy(conn_str: str) -> str:
    """
    Конвертирует строку подключения в формат для SQLAlchemy + asyncpg.
    SQLAlchemy требует: postgresql+asyncpg://
    """
    if not conn_str:
        return conn_str

    # Добавляем драйвер, если не указан
    if conn_str.startswith('postgresql://') and 'postgresql+asyncpg://' not in conn_str:
        return conn_str.replace('postgresql://', 'postgresql+asyncpg://', 1)

    return conn_str


def _sanitize_for_log(conn_str: Optional[str]) -> str:
    """
    Безопасное представление строки подключения для логов.
    Никогда не раскрывает реальные учётные данные.
    """
    if not conn_str:
        return "(not set)"

    try:
        parsed = urlparse(conn_str)
        # Показываем только хост и базу, пароль маскируем
        user = parsed.username or "(unknown)"
        host = parsed.hostname or "(unknown)"
        port = parsed.port or 5432
        dbname = parsed.path.lstrip('/') or "(unknown)"
        return f"postgresql://{user}:****@{host}:{port}/{dbname}"
    except Exception:
        return "(invalid connection string)"


def _validate_connection_string(conn_str: str) -> tuple[bool, str]:
    """
    Валидация строки подключения.
    Returns: (is_valid, error_message)
    """
    if not conn_str:
        return False, "Пустая строка подключения"

    try:
        parsed = urlparse(conn_str)

        # Проверка схемы
        if parsed.scheme not in ['postgresql', 'postgresql+asyncpg', 'postgresql+pg8000']:
            return False, f"Неподдерживаемая схема: {parsed.scheme}"

        # Проверка обязательных компонентов
        if not parsed.hostname:
            return False, "Отсутствует хост в строке подключения"
        if not parsed.path or parsed.path == '/':
            return False, "Отсутствует имя базы данных"

        # Предупреждение о небезопасном подключении
        if parsed.scheme == 'postgresql' and not parsed.hostname.startswith(('localhost', '127.0.0.1', '::1')):
            logger.warning(
                f"⚠️ Подключение к '{parsed.hostname}' без SSL. "
                "Рекомендуется добавить ?sslmode=require"
            )

        return True, ""

    except Exception as e:
        return False, f"Ошибка парсинга URL: {type(e).__name__}"


# =============================================================================
# === DBConnection: синхронное подключение (psycopg2) ===
# =============================================================================

class DBConnection:
    """Безопасное синхронное подключение к базе данных."""

    PASSWORD_PATTERN = re.compile(r'password=([^@\s]+)@', re.IGNORECASE)

    def __init__(self, db_name: Optional[str] = None):
        self.__db_name: Optional[str] = db_name
        self.__connection_string: Optional[SecureString] = None
        self.__connection_pool = None
        self.__initialized: bool = False
        self._initialize_connection(db_name)

    def _initialize_connection(self, db_name: Optional[str]) -> None:
        if db_name is None:
            logger.warning("Имя базы данных не указано")
            return

        conn_str = self._get_connection_string(db_name)
        if not conn_str:
            logger.error(f"Не найдена строка подключения для БД '{db_name}'")
            return

        # Валидация перед использованием
        is_valid, error_msg = _validate_connection_string(conn_str)
        if not is_valid:
            logger.error(f"Невалидная строка подключения для '{db_name}': {error_msg}")
            return

        # Инкапсулируем в SecureString
        self.__connection_string = SecureString(conn_str)
        self.__initialized = True
        logger.info(f"Подключение к БД '{db_name}' инициализировано: {_sanitize_for_log(conn_str)}")

    def _parse_postgres_url(self, url: str) -> str:
        """Конвертирует postgresql:// URL в DSN формат для psycopg2."""
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)

            dsn_parts = [
                f"host={parsed.hostname}",
                f"port={parsed.port or 5432}",
                f"dbname={parsed.path.lstrip('/') or 'postgres'}"
            ]
            if parsed.username:
                dsn_parts.append(f"user={unquote(parsed.username)}")
            if parsed.password:
                # Пароль уже будет замаскирован в логах через SensitiveDataFilter
                dsn_parts.append(f"password={unquote(parsed.password)}")

            for k, v in params.items():
                if v and v[0]:  # Только непустые параметры
                    dsn_parts.append(f"{k}={unquote(v[0])}")

            return " ".join(dsn_parts)
        except Exception as e:
            logger.error(f"Ошибка парсинга URL: {e}")
            return url  # Возвращаем как есть, пусть psycopg2 попробует

    def _get_connection_string(self, db_name: str) -> Optional[str]:
        """Получение строки подключения по имени базы данных."""
        connection_map = {
            'base_01': os.getenv('DB_LOCAL_01'),
            'app_google_target': os.getenv('APP_GOOGLE_DB'),
            'app_groups_target': os.getenv('APP_GROUPS_DB'),
        }
        conn_str = connection_map.get(db_name)

        if not conn_str:
            return None

        # Для psycopg2 нужен DSN или postgresql:// URL
        if conn_str.startswith('postgresql://'):
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
        except psycopg2.OperationalError as e:
            # Не раскрываем детали ошибки подключения
            logger.error(f"Ошибка подключения к БД '{self.__db_name}': операция не выполнена")
            return None
        except psycopg2.Error as e:
            logger.error(f"Ошибка БД '{self.__db_name}': {type(e).__name__}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка подключения к БД '{self.__db_name}'")
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
            logger.error(f"Ошибка транзакции в БД '{self.__db_name}'")
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
        except psycopg2.Error:
            logger.error(f"Ошибка создания пула для БД '{self.__db_name}'")
            return False
        except Exception:
            logger.error(f"Неожиданная ошибка при создании пула для БД '{self.__db_name}'")
            return False

    def get_pooled_connection(self) -> Optional[psycopg2.extensions.connection]:
        if self.__connection_pool is None:
            logger.warning("Пул соединений не инициализирован")
            return None
        try:
            return self.__connection_pool.getconn()
        except Exception:
            logger.error(f"Ошибка получения соединения из пула БД '{self.__db_name}'")
            return None

    def return_pooled_connection(self, conn: psycopg2.extensions.connection) -> None:
        if self.__connection_pool and conn:
            try:
                self.__connection_pool.putconn(conn)
            except Exception:
                logger.error(f"Ошибка возврата соединения в пул БД '{self.__db_name}'")

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
            logger.error(f"Ошибка транзакции в пуле БД '{self.__db_name}'")
            raise
        finally:
            cursor.close()
            self.return_pooled_connection(conn)

    def close_pool(self) -> None:
        if self.__connection_pool:
            try:
                self.__connection_pool.closeall()
                logger.info(f"Пул соединений закрыт для БД '{self.__db_name}'")
            except Exception:
                logger.error(f"Ошибка закрытия пула для БД '{self.__db_name}'")
            finally:
                self.__connection_pool = None

    @property
    def is_initialized(self) -> bool:
        return self.__initialized

    @property
    def db_name(self) -> Optional[str]:
        return self.__db_name

    def __str__(self) -> str:
        return f"DBConnection(db='{self.__db_name or 'None'}', initialized={self.__initialized})"

    def __repr__(self) -> str:
        return self.__str__()

    def __del__(self):
        try:
            self.close_pool()
        except Exception:
            pass  # Игнорируем ошибки при удалении объекта

    def _sanitize_connection_string(self, conn_string: str) -> str:
        """Устаревший метод — используйте _sanitize_for_log()"""
        if not conn_string:
            return "(none)"
        return self.PASSWORD_PATTERN.sub('password=****@', conn_string)

    def log_connection_info(self, level: int = logging.INFO) -> None:
        """Безопасное логирование информации о подключении."""
        if self.__connection_string:
            # Используем безопасную функцию для форматирования
            sanitized = _sanitize_for_log(self.__connection_string.get_raw())
            logger.log(level, f"Подключение к БД '{self.__db_name}': {sanitized}")
        else:
            logger.log(level, f"Подключение к БД '{self.__db_name}' не инициализировано")


# =============================================================================
# === AsyncDBConnection: асинхронное подключение (asyncpg) ===
# =============================================================================

class AsyncDBConnection:
    """Безопасное асинхронное подключение к базе данных через asyncpg."""

    def __init__(self, db_name: Optional[str] = None):
        self.__db_name: Optional[str] = db_name
        self.__connection_string: Optional[SecureString] = None
        self.__pool = None
        self.__initialized: bool = False
        self._initialize_connection(db_name)

    def _initialize_connection(self, db_name: Optional[str]) -> None:
        if db_name is None:
            logger.warning("Имя базы данных не указано для async-подключения")
            return

        conn_str = self._get_connection_string(db_name)
        if not conn_str:
            logger.error(f"Не найдена строка подключения для async БД '{db_name}'")
            return

        # Валидация
        is_valid, error_msg = _validate_connection_string(conn_str)
        if not is_valid:
            logger.error(f"Невалидная строка подключения для async '{db_name}': {error_msg}")
            return

        # Конвертируем в формат для asyncpg (убираем драйвер SQLAlchemy)
        conn_str_asyncpg = _normalize_for_asyncpg(conn_str)

        self.__connection_string = SecureString(conn_str_asyncpg)
        self.__initialized = True
        logger.info(f"Async-подключение '{db_name}' инициализировано: {_sanitize_for_log(conn_str_asyncpg)}")

    def _get_connection_string(self, db_name: str) -> Optional[str]:
        """Получение строки подключения — в формате для asyncpg."""
        connection_map = {
            'base_01': os.getenv('DB_LOCAL_01'),
            'app_google_target': os.getenv('APP_GOOGLE_DB'),
            'app_groups_target': os.getenv('APP_GROUPS_DB'),
        }
        conn_str = connection_map.get(db_name)

        if not conn_str:
            return None

        # Возвращаем как есть — asyncpg понимает postgresql://
        # Конвертация в _normalize_for_asyncpg делается при инициализации
        return conn_str

    async def create_pool(self, min_size: int = 1, max_size: int = 10) -> bool:
        if not self.__connection_string:
            logger.warning("Невозможно создать async-пул: строка подключения не инициализирована")
            return False

        try:
            conn_str = self.__connection_string.get_raw()

            # Дополнительная проверка формата для asyncpg
            if not conn_str.startswith(('postgresql://', 'postgres://')):
                logger.error(f"asyncpg требует схему postgresql://, получено: {conn_str[:20]}...")
                return False

            self.__pool = await asyncpg.create_pool(
                conn_str,
                min_size=min_size,
                max_size=max_size,
                command_timeout=60  # Таймаут для защиты от зависших запросов
            )
            logger.info(f"Async-пул создан для БД '{self.__db_name}' (min={min_size}, max={max_size})")
            return True

        except asyncpg.InvalidConfigurationError:
            logger.error(f"Ошибка конфигурации async-пула для БД '{self.__db_name}'")
            return False
        except asyncpg.PostgresError:
            logger.error(f"Ошибка PostgreSQL при создании async-пула для БД '{self.__db_name}'")
            return False
        except Exception:
            logger.error(f"Неожиданная ошибка при создании async-пула для БД '{self.__db_name}'")
            return False

    @asynccontextmanager
    async def get_cursor(self):
        """Асинхронный контекстный менеджер для получения соединения."""
        if not self.__pool:
            if not await self.create_pool():
                raise ConnectionError(f"Не удалось создать пул для БД '{self.__db_name}'")

        async with self.__pool.acquire() as conn:
            try:
                yield conn
            finally:
                # asyncpg сам вернёт соединение в пул
                pass

    async def close_pool(self) -> None:
        if self.__pool:
            try:
                await self.__pool.close(timeout=10)  # Таймаут для закрытия
                logger.info(f"Async-пул закрыт для БД '{self.__db_name}'")
            except Exception:
                logger.error(f"Ошибка закрытия async-пула для БД '{self.__db_name}'")
            finally:
                self.__pool = None

    @property
    def is_initialized(self) -> bool:
        return self.__initialized

    @property
    def db_name(self) -> Optional[str]:
        return self.__db_name

    def __str__(self) -> str:
        return f"AsyncDBConnection(db='{self.__db_name or 'None'}', initialized={self.__initialized})"

    def __repr__(self) -> str:
        return self.__str__()


# =============================================================================
# === Глобальные переменные для SQLAlchemy (с безопасной инициализацией) ===
# =============================================================================

# Глобальные переменные — инициализируются ниже
engine = None
async_session: Optional[async_sessionmaker] = None


def _init_sqlalchemy_engine(db_name: str = 'app_google_target') -> bool:
    """
    Безопасная инициализация SQLAlchemy engine.

    Args:
        db_name: Ключ из connection_map для получения строки подключения.

    Returns:
        bool: True при успехе, False при ошибке.
    """
    global engine, async_session

    # Если уже инициализировано — не переинициализируем
    if engine is not None:
        return True

    # Получаем строку подключения из env
    connection_map = {
        'base_01': os.getenv('DB_LOCAL_01'),
        'app_google_target': os.getenv('APP_GOOGLE_DB'),
        'app_groups_target': os.getenv('APP_GROUPS_DB'),
    }
    conn_str = connection_map.get(db_name)

    if not conn_str:
        logger.error(f"Не найдена строка подключения для SQLAlchemy: {db_name}")
        return False

    # Валидация
    is_valid, error_msg = _validate_connection_string(conn_str)
    if not is_valid:
        logger.error(f"Невалидная строка подключения для SQLAlchemy '{db_name}': {error_msg}")
        return False

    # Конвертируем в формат для SQLAlchemy + asyncpg
    conn_str_sqlalchemy = _normalize_for_sqlalchemy(conn_str)

    try:
        # Создаём engine с безопасными настройками
        engine = create_async_engine(
            conn_str_sqlalchemy,
            echo=False,  # Не логировать SQL-запросы с параметрами
            pool_pre_ping=True,  # Проверка соединения перед использованием
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,  # Таймаут получения соединения из пула
            pool_recycle=3600,  # Пересоздавать соединения каждые 1 час
        )

        async_session = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,  # Избегаем проблем с асинхронностью
            autocommit=False,
            autoflush=False,
        )

        logger.info(f"SQLAlchemy engine инициализирован для '{db_name}': {_sanitize_for_log(conn_str_sqlalchemy)}")
        return True

    except Exception as e:
        # Не раскрываем детали ошибки инициализации
        logger.error(f"Ошибка инициализации SQLAlchemy engine для '{db_name}'")
        engine = None
        async_session = None
        return False


# Инициализируем глобальные переменные при импорте модуля
# Используем дефолтное имя БД, но можно переопределить через параметры
_init_sqlalchemy_engine('app_google_target')


# =============================================================================
# === DBManager: синглтон для управления подключениями ===
# =============================================================================

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
        for db_name, connection in list(cls._async_connections.items()):
            await connection.close_pool()
        cls._async_connections.clear()
        logger.info("Все async-подключения закрыты")

    @classmethod
    def close_all(cls) -> None:
        """Закрытие всех синхронных пулов соединений."""
        for db_name, connection in list(cls._connections.items()):
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
        # Для async нужно дождаться закрытия
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # Если цикл запущен — создаём задачу
                asyncio.create_task(cls.close_all_async())
            else:
                asyncio.run(cls.close_all_async())
        except RuntimeError:
            # Нет запущенного цикла — создаём новый
            asyncio.run(cls.close_all_async())
        cls._async_connections.clear()
        cls._connections.clear()
        logger.info("DBManager сброшен")


__all__ = [
    'SecureString',
    'DBConnection',
    'AsyncDBConnection',
    'DBManager',
    'engine',
    'async_session',
    '_init_sqlalchemy_engine',
    '_sanitize_for_log',
    '_normalize_for_asyncpg',
    '_normalize_for_sqlalchemy',
]