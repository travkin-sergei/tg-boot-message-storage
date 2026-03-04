# src/app_log.py
"""
Настройка логирования с цветным выводом, выравниванием, фильтрацией секретов и относительными путями.
"""
import logging
import sys
import os
import re
from pathlib import Path
from logging.handlers import RotatingFileHandler

import colorlog


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
    """Умный фильтр: маскирует только явные секреты."""

    SENSITIVE_EXACT_NAMES = {
        'api_key', 'apikey', 'secret_key', 'password', 'token',
        'auth_token', 'access_token', 'private_key', 'client_secret'
    }

    SENSITIVE_PATTERNS = [
        r'((?:api_?key|token|password|secret|auth)\s*[:=]\s*)[\'"]?([a-zA-Z0-9_\-]{16,})[\'"]?',
        r'(Bearer\s+)([a-zA-Z0-9_\-\.]{20,})',
        r'([?&](?:api_?key|token|secret)=)([a-zA-Z0-9_\-]{16,})',
    ]

    def __init__(self, strict_mode: bool = True):
        super().__init__()
        self.strict_mode = strict_mode

    def filter(self, record: logging.LogRecord) -> bool:
        if hasattr(record, 'msg') and isinstance(record.msg, str):
            record.msg = self._mask_message(record.msg)
        if record.args:
            if isinstance(record.args, (list, tuple)):
                record.args = tuple(self._safe_mask_arg(arg) for arg in record.args)
            elif isinstance(record.args, dict):
                record.args = {k: self._safe_mask_arg(v) for k, v in record.args.items()}
        return True

    def _mask_message(self, message: str) -> str:
        result = message
        for pattern in self.SENSITIVE_PATTERNS:
            def selective_mask(match: re.Match) -> str:
                prefix = match.group(1)
                value = match.group(2)
                if self._is_safe_value(value):
                    return match.group(0)
                return f"{prefix}***"

            result = re.sub(pattern, selective_mask, result, flags=re.IGNORECASE)
        for var_name in self.SENSITIVE_EXACT_NAMES:
            pattern = re.compile(
                rf'\b{re.escape(var_name)}\s*[:=]\s*[\'"]?([^\s\'",\}}\]]+)[\'"]?',
                re.IGNORECASE
            )
            result = pattern.sub(
                lambda m: m.group(0).split('=')[0] + '***' if '=' in m.group(0) else m.group(0).split(':')[0] + ': ***',
                result)
        return result

    def _is_safe_value(self, value: str) -> bool:
        if not value or len(value) < 16:
            return True
        if value.startswith(('http://', 'https://', 'ftp://')):
            return True
        if '.' in value and len(value.split('.')[-1]) <= 4:
            return True
        if value.isdigit() or value.replace('.', '').isdigit():
            return True
        if '/' in value or '\\' in value:
            return True
        return False

    def _safe_mask_arg(self, arg) -> str:
        if not isinstance(arg, str):
            return arg
        if len(arg) >= 20 and not self._is_safe_value(arg):
            return mask_sensitive(arg, visible_chars=4)
        return arg


class RelativePathFormatter(colorlog.ColoredFormatter):
    """Форматтер с относительными путями (от папки src/)."""

    def __init__(self, *args, base_path: str | Path | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_path = Path(base_path).resolve() if base_path else None

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

        # 🔍 Авто-поиск: ищем 'src' в пути
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

    # === Консольный обработчик (цветной) ===
    console_formatter = RelativePathFormatter(
        fmt=log_format,
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        },
        reset=True,
        style='%',
        base_path=log_base_path
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
