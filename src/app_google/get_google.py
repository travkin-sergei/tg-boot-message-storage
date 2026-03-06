#src/app_google/get_google.py
"""
Сервис для обработки Google Sheets.
Зависимость: только src.config.logger
"""
import asyncio
import aiohttp
from io import BytesIO
from typing import Any
from openpyxl import load_workbook

from src.app_google.config import APP_GOOGLE_FILE
from src.config.logger import logger


class GoogleSheetProcessor:
    """
    Изолированный класс для работы с Google Таблицами.

    Файл скачивается один раз и кэшируется внутри класса.
    Методы используют кэш — не требуют передачи content/file_code.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self._base_url = f'https://docs.google.com/spreadsheets/d/{APP_GOOGLE_FILE}/export?format=xlsx'
        self._cached_content: bytes | None = None
        self._cached_file_code: str | None = None

    async def download_file(self, file_code: str) -> bool:
        """
        Скачать Google Sheet в память и закэшировать внутри класса.

        При повторном вызове с тем же file_code — скачивание пропускается.

        Args:
            file_code: Идентификатор файла из URL.

        Returns:
            bool: True если файл загружен успешно, False при ошибке.
        """
        # Если файл уже загружен и code совпадает — используем кэш
        if self._cached_content is not None and self._cached_file_code == file_code:
            logger.debug(f"Используется кэш для файла: {file_code}")
            return True

        link = self._base_url.format(file_code=file_code.strip())

        try:
            logger.info(f"Загрузка файла: {file_code}")

            async with aiohttp.ClientSession() as session:
                async with session.get(link, timeout=aiohttp.ClientTimeout(total=self.timeout)) as response:
                    if response.status == 404:
                        logger.error("404: Файл не найден")
                        return False
                    response.raise_for_status()
                    content = await response.read()

            # Кэшируем
            self._cached_content = content
            self._cached_file_code = file_code.strip()

            logger.debug(f"✅ Файл загружен и закэширован: {len(content)} байт")
            return True

        except aiohttp.ClientError as e:
            logger.error(f"HTTP: {e}")
            return False
        except Exception as e:
            logger.error(f"download: {e}", exc_info=True)
            return False

    def _parse_sheet_names_sync(self, content: bytes) -> list[str]:
        """Синхронный парсинг имён листов (для выполнения в потоке)."""
        workbook = load_workbook(filename=BytesIO(content), read_only=True, data_only=True)
        return workbook.sheetnames

    async def get_sheet_names(self) -> list[str] | None:
        """
        Получить список всех листов из закэшированного файла.

        Returns:
            list[str] | None: Список имён листов или None если файл не загружен/ошибка.
        """
        if self._cached_content is None:
            logger.error("❌ Файл не загружен. Сначала вызовите download_file(file_code)")
            return None

        try:
            sheet_names = await asyncio.to_thread(self._parse_sheet_names_sync, self._cached_content)
            logger.info(f"📋 Найдено листов ({len(sheet_names)}): {sheet_names}")
            return sheet_names
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга имён листов: {e}", exc_info=True)
            return None

    def _parse_sheet_data_sync(self, content: bytes, list_name: str) -> list[dict[str, Any]] | None:
        """Синхронный парсинг данных листа (для выполнения в потоке)."""
        workbook = load_workbook(filename=BytesIO(content), read_only=True, data_only=True)

        if list_name not in workbook.sheetnames:
            logger.error(f"Лист '{list_name}' не найден. Доступные: {workbook.sheetnames}")
            return None

        worksheet = workbook[list_name]
        rows = list(worksheet.iter_rows(values_only=True))

        if not rows:
            logger.warning("Лист пустой")
            return []

        # Заголовки
        headers = [
            str(h).strip() if h is not None else f"column_{idx}"
            for idx, h in enumerate(rows[0])
        ]

        # Данные
        data = []
        for row in rows[1:]:
            if row and any(cell is not None for cell in row):
                record = {}
                for idx, header in enumerate(headers):
                    value = row[idx] if idx < len(row) else None
                    if isinstance(value, str):
                        value = value.strip()
                    record[header] = value
                data.append(record)

        return data

    async def get_sheet_data(self, list_name: str) -> list[dict[str, Any]] | None:
        """
        Получить данные конкретного листа из закэшированного файла.

        Args:
            list_name: Имя листа для извлечения.

        Returns:
            list[dict] | None: Данные листа как список словарей или None при ошибке.
        """
        if self._cached_content is None:
            logger.error("❌ Файл не загружен. Сначала вызовите download_file(file_code)")
            return None

        try:
            data = await asyncio.to_thread(self._parse_sheet_data_sync, self._cached_content, list_name)
            if data is not None:
                logger.info(f"✅ Получено строк из листа '{list_name}': {len(data)}")
            return data
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга листа '{list_name}': {e}", exc_info=True)
            return None

    async def get_sheet_columns(self, list_name: str) -> list[str] | None:
        """
        Получить только имена столбцов из листа (без загрузки всех данных).

        Args:
            list_name: Имя листа.

        Returns:
            list[str] | None: Список имён столбцов или None при ошибке.
        """
        if self._cached_content is None:
            logger.error("❌ Файл не загружен. Сначала вызовите download_file(file_code)")
            return None

        def _extract_columns_sync(content: bytes, list_name: str) -> list[str] | None:
            try:
                workbook = load_workbook(filename=BytesIO(content), read_only=True, data_only=True)

                if list_name not in workbook.sheetnames:
                    logger.error(f"Лист '{list_name}' не найден. Доступные: {workbook.sheetnames}")
                    return None

                worksheet = workbook[list_name]
                header_row = next(worksheet.iter_rows(values_only=True, max_row=1), None)
                if not header_row:
                    logger.warning("Лист пустой, нет заголовков")
                    return []

                return [
                    str(h).strip() if h is not None else f"column_{idx}"
                    for idx, h in enumerate(header_row)
                ]
            except Exception as e:
                logger.error(f"❌ Ошибка чтения заголовков: {e}", exc_info=True)
                return None

        try:
            columns = await asyncio.to_thread(_extract_columns_sync, self._cached_content, list_name)
            if columns is not None:
                logger.info(f"📊 Столбцы листа '{list_name}': {columns}")
            return columns
        except Exception as e:
            logger.error(f"❌ Ошибка получения столбцов: {e}", exc_info=True)
            return None

    def clear_cache(self) -> None:
        """Очистить кэш файла (освободить память)."""
        self._cached_content = None
        self._cached_file_code = None
        logger.debug("🗑️ Кэш очищен")

    @property
    def is_file_loaded(self) -> bool:
        """Проверить, загружен ли файл в кэш."""
        return self._cached_content is not None

    @property
    def cached_file_code(self) -> str | None:
        """Вернуть идентификатор закэшированного файла."""
        return self._cached_file_code
