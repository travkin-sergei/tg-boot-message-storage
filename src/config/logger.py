# src/config/logger.py
"""
Настройка логирования с цветным выводом, выравниванием, фильтрацией секретов и относительными путями.
"""
import logging
import os
import re
import sys
import colorlog

from pathlib import Path
from logging.handlers import RotatingFileHandler


# === Утилита для ручной маскировки ===
def mask_sensitive(value: str | None, visible_chars: int = 4) -> str:
    """Маскирует чувствительные строки, оставляя видимыми только крайние символы."""
    if not value:
        return "None"
    if len(value) <= visible_chars * 2:
        return "*" * len(value)
    half = visible_chars // 2
    return value[:half] + "*" * (len(value) - visible_chars) + value[-half:]


# === Фильтр для автоматической маскировки секретов ===
class SensitiveDataFilter(logging.Filter):
    """
    Умный фильтр: маскирует секреты и ВСЕ значения из .env.

    Работает в два этапа:
    1. При инициализации загружает все env-переменные в _env_values
    2. При фильтрации маскирует любые вхождения этих значений в логи
    """

    # Явные имена чувствительных переменных
    SENSITIVE_EXACT_NAMES = {
        'api_key', 'apikey', 'secret_key', 'password', 'token',
        'auth_token', 'access_token', 'private_key', 'client_secret'
    }

    # Паттерны для поиска секретов в тексте
    SENSITIVE_PATTERNS = [
        r'((?:api_?key|token|password|secret|auth)\s*[:=]\s*)[\'"]?([a-zA-Z0-9_\-]{16,})[\'"]?',
        r'(Bearer\s+)([a-zA-Z0-9_\-\.]{20,})',
        r'([?&](?:api_?key|token|secret)=)([a-zA-Z0-9_\-]{16,})',
    ]

    # Хранилище значений из .env для маскировки
    _env_values: set[str] = set()
    _env_values_loaded: bool = False

    def __init__(self, strict_mode: bool = True, load_env_values: bool = True):
        """
        Args:
            strict_mode: Если True — маскировать даже короткие значения.
            load_env_values: Если True — загрузить значения из os.environ для маскировки.
        """
        super().__init__()
        self.strict_mode = strict_mode

        # 🔥 Загружаем значения env-переменных при первом создании фильтра
        if load_env_values and not self._env_values_loaded:
            self._load_env_values()
            self._env_values_loaded = True

    @classmethod
    def _load_env_values(cls, min_length: int = 8):
        """
        Загружает значения всех переменных окружения для последующей маскировки.

        Args:
            min_length: Не маскировать значения короче этого (чтобы не засорять логи).
        """
        import os

        # Очищаем старое
        cls._env_values.clear()

        for key, value in os.environ.items():
            # Пропускаем системные переменные и слишком короткие значения
            if (not value or
                    len(value) < min_length or
                    key.startswith('_') or
                    key in ('PATH', 'PWD', 'HOME', 'USER', 'SHELL', 'TERM', 'LANG')):
                continue

            # Добавляем само значение
            cls._env_values.add(value)

            # Добавляем URL-кодированную версию (на случай логирования запросов)
            from urllib.parse import quote
            encoded = quote(value, safe='')
            if encoded != value:
                cls._env_values.add(encoded)

            # Добавляем hash для очень длинных значений (опционально)
            if len(value) > 50:
                import hashlib
                cls._env_values.add(hashlib.sha256(value.encode()).hexdigest()[:16])

        # Логируем (безопасно!) сколько значений загружено
        from src.config.logger import logger
        logger.debug(f"🔐 Loaded {len(cls._env_values)} env values for log masking")

    def filter(self, record: logging.LogRecord) -> bool:
        """Маскирует чувствительные данные в записи лога."""
        # Маскируем сообщение
        if hasattr(record, 'msg') and isinstance(record.msg, str):
            record.msg = self._mask_message(record.msg)

        # Маскируем аргументы форматирования
        if record.args:
            if isinstance(record.args, (list, tuple)):
                record.args = tuple(self._safe_mask_arg(arg) for arg in record.args)
            elif isinstance(record.args, dict):
                record.args = {k: self._safe_mask_arg(v) for k, v in record.args.items()}

        # Маскируем exception, если есть
        if record.exc_info and record.exc_info[1]:
            try:
                exc_str = str(record.exc_info[1])
                if any(env_val in exc_str for env_val in self._env_values):
                    record.exc_info = (
                        record.exc_info[0],
                        type(record.exc_info[1])("Exception details masked"),
                        record.exc_info[2]
                    )
            except Exception:
                pass  # Не ломаем логирование, если не смогли замаскировать

        return True

    def _mask_message(self, message: str) -> str:
        """Маскирует чувствительные данные в сообщении."""
        result = message

        # 1. Маскируем по паттернам (токены, ключи и т.д.)
        for pattern in self.SENSITIVE_PATTERNS:
            def selective_mask(match: re.Match) -> str:
                prefix = match.group(1)
                value = match.group(2)
                if self._is_safe_value(value):
                    return match.group(0)
                return f"{prefix}***"

            result = re.sub(pattern, selective_mask, result, flags=re.IGNORECASE)

        # 2. Маскируем по явным именам переменных
        for var_name in self.SENSITIVE_EXACT_NAMES:
            pattern = re.compile(
                rf'\b{re.escape(var_name)}\s*[:=]\s*[\'"]?([^\s\'",\}}\]]+)[\'"]?',
                re.IGNORECASE
            )
            result = pattern.sub(
                lambda m: m.group(0).split('=')[0] + '=***' if '=' in m.group(0)
                else m.group(0).split(':')[0] + ': ***',
                result
            )

        # 3. 🔥 Маскируем ВСЕ значения из .env
        for env_value in self._env_values:
            if env_value and len(env_value) >= 4 and env_value in result:
                # Заменяем только если значение не является частью безопасного контекста
                if not self._is_safe_value(env_value):
                    result = result.replace(env_value, '***')

        return result

    def _is_safe_value(self, value: str) -> bool:
        """
        Определяет, можно ли не маскировать значение.

        Безопасные значения:
        - Короткие (< 16 символов по умолчанию)
        - URL/пути
        - Числа, даты, домены
        - Стандартные форматы
        """
        if not value or len(value) < (16 if self.strict_mode else 8):
            return True
        if value.startswith(('http://', 'https://', 'ftp://', '/', '\\', 'file://')):
            return True
        if '.' in value and len(value.split('.')[-1]) <= 4:  # домен или расширение
            return True
        if value.isdigit() or value.replace('.', '').replace('-', '').isdigit():
            return True
        if re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', value, re.I):  # UUID
            return True
        if re.match(r'^\d{4}-\d{2}-\d{2}', value):  # дата
            return True
        return False

    def _safe_mask_arg(self, arg) -> str:
        """Безопасно маскирует аргумент лога."""
        if not isinstance(arg, str):
            return arg

        # Проверяем, не является ли аргумент значением из .env
        if arg in self._env_values and not self._is_safe_value(arg):
            return '***'

        # Для длинных строк — маскируем, если они похожи на секреты
        if len(arg) >= 20 and not self._is_safe_value(arg):
            return mask_sensitive(arg, visible_chars=4)

        return arg


class RelativePathFormatter(colorlog.ColoredFormatter):
    """Форматтер с относительными путями (от папки src/)."""

    CONSOLE_LOG_COLORS = {
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }

    def __init__(
            self,
            *args,
            base_path: str | Path | None = None,
            use_colors: bool = True,
            **kwargs
    ):

        if use_colors:
            kwargs['log_colors'] = {
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        else:
            kwargs['log_colors'] = {}

        # Убираем %(log_color)s/%(reset)s из формата, если цвета отключены
        if not use_colors and 'fmt' in kwargs:
            fmt = kwargs['fmt']
            fmt = fmt.replace('%(log_color)s', '').replace('%(reset)s', '')
            kwargs['fmt'] = fmt

        super().__init__(*args, **kwargs)
        self.base_path = Path(base_path).resolve() if base_path else None
        self.use_colors = use_colors

    def format(self, record: logging.LogRecord) -> str:
        # Добавляем кастомные поля в record перед форматированием
        record.short_path = self._get_short_path(record.pathname)
        record.classname = self._get_class_name(record)
        return super().format(record)

    @staticmethod
    def _get_class_name(record: logging.LogRecord) -> str:
        """Извлекает имя класса из контекста вызова."""
        if not (record.funcName and record.pathname):
            return '-'
        try:
            import inspect
            frame = inspect.currentframe()
            while frame:
                if (frame.f_code.co_name == record.funcName and
                        os.path.normpath(frame.f_code.co_filename) == os.path.normpath(record.pathname)):
                    if 'self' in frame.f_locals:
                        return frame.f_locals['self'].__class__.__name__
                    return '-'
                frame = frame.f_back
            return '-'
        except Exception:
            return '-'

    def _get_short_path(self, pathname: str) -> str:
        """Возвращает путь относительно папки 'src'."""
        if not pathname:
            return '-'
        path = Path(pathname)

        # Если задан base_path — считаем от него
        if self.base_path:
            try:
                return str(path.relative_to(self.base_path))
            except ValueError:
                return path.name

        # Авто-поиск: ищем 'src' в пути
        parts = path.parts
        try:
            src_index = parts.index('src')
            return str(Path(*parts[src_index:]))
        except ValueError:
            return path.name


def config_logging(level=logging.INFO,
                   log_file: str | Path | None = 'logs/app.log',
                   mask_sensitive_data: bool = True,
                   max_bytes: int = 10 * 1024 * 1024,
                   backup_count: int = 5,
                   rotation_mode: str = 'size',
                   log_base_path: str | Path | None = None) -> None:
    """Настройка логирования."""

    # %(short_path)s — наше кастомное поле
    log_format = '%(log_color)s%(asctime)s | %(levelname)-8s | %(lineno)4d | %(short_path)s | %(classname)s | %(funcName)s | %(message)s%(reset)s'

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if root_logger.handlers:
        root_logger.handlers.clear()

    console_formatter = RelativePathFormatter(
        fmt=f'%(log_color)s{log_format}%(reset)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        base_path=log_base_path,
        use_colors=True,
        style='%',
        reset=True
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(level)

    if mask_sensitive_data:
        console_handler.addFilter(SensitiveDataFilter())

    root_logger.addHandler(console_handler)

    # === Файловый обработчик (без цветов, но с относительными путями) ===
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Для файла — тот же формат, но без цветов
        file_format = '%(asctime)s | %(levelname)-8s | %(lineno)4d | %(short_path)s | %(classname)s | %(funcName)s | %(message)s'

        # Используем тот же класс форматтера, но без log_colors (он сам отключит цвета)
        file_formatter = RelativePathFormatter(
            fmt=file_format,
            datefmt='%Y-%m-%d %H:%M:%S',
            base_path=log_base_path,
            style='%'
        )

        if rotation_mode == 'time':
            from logging.handlers import TimedRotatingFileHandler
            file_handler = TimedRotatingFileHandler(
                filename=log_file,
                when='midnight',
                interval=1,
                backupCount=backup_count,
                encoding='utf-8',
                utc=True
            )
        else:
            file_handler = RotatingFileHandler(
                filename=log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8',
                delay=True
            )

        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(level)

        if mask_sensitive_data:
            file_handler.addFilter(SensitiveDataFilter())

        root_logger.addHandler(file_handler)


# Именованный логгер для модуля
logger = logging.getLogger(__name__)
