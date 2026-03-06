# src/config/database.py
"""
Универсальный класс подключения к базам данных
"""
import os
import re
import logging
from typing import Optional, Dict
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)


class SecureString(str):
    """
    Безопасная строка, которая скрывает содержимое при выводе.
    Защищает от утечки чувствительных данных в логах и отладке.
    """

    def __str__(self):
        return "(********)"

    def __repr__(self):
        return "(********)"

    def __format__(self, format_spec):
        return "(********)"

    def __getattribute__(self, name):
        # Блокируем методы, которые могут раскрыть содержимое
        if name in ['__reduce__', '__reduce_ex__', '__getnewargs__', '__getstate__']:
            raise AttributeError(f"Доступ к методу '{name}' запрещён")
        return super().__getattribute__(name)


class DBConnection:
    """
    Безопасное подключение к базе данных.
    Поддерживает множественные базы данных с защитой чувствительных данных.
    """

    # Маска для скрытия паролей в строке подключения
    PASSWORD_PATTERN = re.compile(r'password=([^@\s]+)@')

    def __init__(self, db_name: Optional[str] = None):
        """
        Инициализация подключения к базе данных.

        Args:
            db_name: Имя базы данных ('base_1', 'base_2', и т.д.)
        """
        self.__db_name = db_name
        self.__connection_string: Optional[SecureString] = None
        self.__connection_pool = None
        self.__initialized = False

        self._initialize_connection(db_name)

    def _initialize_connection(self, db_name: Optional[str]) -> None:
        """Инициализация строки подключения"""
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

    def _get_connection_string(self, db_name: str) -> Optional[str]:
        """
        Получение строки подключения по имени базы данных.
        Если требуется добавить подключения, то надо дописать строку именно здесь.

        Args:
            db_name: Имя базы данных

        Returns:
            Строка подключения или None
        """
        connection_map = {
            'base_01': os.getenv('DB_LOCAL_01'),
        }

        return connection_map.get(db_name)

    def get_connection(self) -> Optional[psycopg2.extensions.connection]:
        """
        Возвращает объект подключения к базе данных.

        Returns:
            Объект подключения или None при ошибке
        """
        if not self.__connection_string:
            logger.warning("Строка подключения не инициализирована")
            return None

        try:
            conn = psycopg2.connect(str(self.__connection_string))
            logger.debug(f"Успешное подключение к БД '{self.__db_name}'")
            return conn
        except psycopg2.Error as error:
            logger.error(f"Ошибка подключения к БД: {type(error).__name__}")
            return None
        except Exception as error:
            logger.error(f"Неожиданная ошибка подключения: {type(error).__name__}")
            return None

    @contextmanager
    def get_cursor(self, commit: bool = False):
        """
        Контекстный менеджер для работы с курсором.
        Автоматически закрывает курсор и соединение.

        Args:
            commit: Автоматически коммитить транзакцию

        Yields:
            Курсор базы данных
        """
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
        """
        Создание пула соединений.

        Args:
            minconn: Минимальное количество соединений
            maxconn: Максимальное количество соединений

        Returns:
            True если пул создан успешно
        """
        if not self.__connection_string:
            logger.warning("Невозможно создать пул: строка подключения не инициализирована")
            return False

        try:
            self.__connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                dsn=str(self.__connection_string)
            )
            self.__initialized = True
            logger.info(f"Пул соединений создан для БД '{self.__db_name}'")
            return True
        except Exception as error:
            logger.error(f"Ошибка создания пула: {error}")
            return False

    def get_pooled_connection(self) -> Optional[psycopg2.extensions.connection]:
        """
        Получение соединения из пула.

        Returns:
            Соединение из пула или None
        """
        if self.__connection_pool is None:
            logger.warning("Пул соединений не инициализирован")
            return None

        try:
            conn = self.__connection_pool.getconn()
            return conn
        except Exception as error:
            logger.error(f"Ошибка получения соединения из пула: {error}")
            return None

    def return_pooled_connection(self, conn: psycopg2.extensions.connection) -> None:
        """
        Возврат соединения в пул.

        Args:
            conn: Соединение для возврата
        """
        if self.__connection_pool and conn:
            try:
                self.__connection_pool.putconn(conn)
            except Exception as error:
                logger.error(f"Ошибка возврата соединения в пул: {error}")

    @contextmanager
    def get_pooled_cursor(self, commit: bool = False):
        """
        Контекстный менеджер для работы с курсором из пула.

        Args:
            commit: Автоматически коммитить транзакцию

        Yields:
            Курсор базы данных
        """
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
        """Закрытие пула соединений"""
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
        """Проверка инициализации подключения"""
        return self.__initialized

    @property
    def db_name(self) -> Optional[str]:
        """Получение имени базы данных"""
        return self.__db_name

    def __str__(self) -> str:
        return f"DBConnection(db='{self.__db_name or 'None'}')"

    def __repr__(self) -> str:
        return f"DBConnection(db='{self.__db_name or 'None'}')"

    def __del__(self):
        """Очистка ресурсов при уничтожении объекта"""
        self.close_pool()

    def _sanitize_connection_string(self, conn_string: str) -> str:
        """
        Очистка строки подключения от чувствительных данных для логирования.

        Args:
            conn_string: Исходная строка подключения

        Returns:
            Очищенная строка подключения
        """
        if not conn_string:
            return "(none)"

        # Замена пароля на маску
        sanitized = self.PASSWORD_PATTERN.sub('password=****@', conn_string)
        return sanitized

    def log_connection_info(self, level: int = logging.INFO) -> None:
        """
        Логирование информации о подключении (без чувствительных данных).

        Args:
            level: Уровень логирования
        """
        if self.__connection_string:
            sanitized = self._sanitize_connection_string(str(self.__connection_string))
            logger.log(level, f"Подключение к БД '{self.__db_name}': {sanitized}")
        else:
            logger.log(level, f"Подключение к БД '{self.__db_name}' не инициализировано")


# Глобальный менеджер подключений
class DBManager:
    """
    Менеджер подключений к нескольким базам данных.
    Singleton паттерн для управления всеми подключениями.
    """

    _instance = None
    _connections: Dict[str, DBConnection] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_connection(cls, db_name: str) -> DBConnection:
        """
        Получение подключения к указанной базе данных.

        Args:
            db_name: Имя базы данных

        Returns:
            Объект подключения
        """
        if db_name not in cls._connections:
            cls._connections[db_name] = DBConnection(db_name)
        return cls._connections[db_name]

    @classmethod
    def initialize_all(cls, db_names: list) -> None:
        """
        Инициализация всех указанных подключений.

        Args:
            db_names: Список имён баз данных
        """
        for db_name in db_names:
            cls.get_connection(db_name)
        logger.info(f"Инициализировано подключений: {len(cls._connections)}")

    @classmethod
    def close_all(cls) -> None:
        """Закрытие всех подключений"""
        for db_name, connection in cls._connections.items():
            connection.close_pool()
        cls._connections.clear()
        logger.info("Все подключения закрыты")

    @classmethod
    def get_all_connections(cls) -> Dict[str, DBConnection]:
        """Получение всех подключений"""
        return cls._connections.copy()


# Экспорты
__all__ = ['DBConnection', 'DBManager', 'SecureString']
