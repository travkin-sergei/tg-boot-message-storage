import asyncio
from pathlib import Path
from typing import Optional, Dict
import pyarrow.parquet as pq
import logging

from src.app_google.external.api_client import google_to_parquet

logger = logging.getLogger(__name__)


def _rename_columns_sync(input_file: Path,
                         column_mapping: Dict[str, str],
                         output_file: Path) -> str:
    """
    Синхронная внутренняя функция для переименования столбцов.
    Выполняется в отдельном потоке.
    """
    # Читаем Parquet
    table = pq.read_table(input_file)
    original_columns = table.column_names

    # Формируем новые имена столбцов
    new_columns = []
    renamed_count = 0

    for col in original_columns:
        if col in column_mapping:
            new_name = column_mapping[col]
            new_columns.append(new_name)
            renamed_count += 1
            logger.info(f"🔄 Переименовано: '{col}' → '{new_name}'")
        else:
            new_columns.append(col)

    # Переименовываем столбцы
    new_table = table.rename_columns(new_columns)

    # Сохраняем результат
    pq.write_table(new_table, output_file, compression='snappy')

    logger.info(f"✅ Переименовано {renamed_count} столбцов. Сохранено в {output_file}")
    return str(output_file.resolve())


async def rename_parquet_columns(input_path: str,
                                 column_mapping: Dict[str, str],
                                 output_path: Optional[str] = None) -> Optional[str]:
    """
    Асинхронно переименовывает столбцы в Parquet-файле согласно маппингу.

    Args:
        input_path: Путь к исходному .parquet файлу.
        column_mapping: Словарь {старое_имя: новое_имя}.
        output_path: Путь для сохранения результата.
                     Если None — перезаписывает исходный файл.

    Returns:
        str: Путь к файлу с переименованными столбцами при успехе.
        None: При ошибке.
    """
    try:
        input_file = Path(input_path)

        if not input_file.exists():
            logger.error(f"❌ Файл не найден: {input_file}")
            return None

        # Определяем путь для сохранения
        if output_path is None:
            output_file = input_file  # Перезапись
        else:
            output_file = Path(output_path)
            # Создаём директорию синхронно (быстро, не блокирует)
            await asyncio.to_thread(output_file.parent.mkdir, parents=True, exist_ok=True)

        # Выполняем блокирующие I/O операции в отдельном потоке
        result = await asyncio.to_thread(
            _rename_columns_sync,
            input_file,
            column_mapping,
            output_file
        )

        return result

    except Exception as error:
        logger.error(f"Ошибка при переименовании столбцов: {error}", exc_info=True)
        return None


async def download_and_rename_parquet(list_name: str,
                                      file_code: str,
                                      output_path: str,
                                      column_mapping: Optional[Dict[str, str]] = None) -> Optional[str]:
    """
    Скачивает Google Sheet, сохраняет в Parquet и переименовывает столбцы.

    Args:
        list_name: Название листа в Google таблице.
        file_code: Идентификатор файла (из URL Google Sheets).
        output_path: Путь для сохранения .parquet файла.
        column_mapping: Словарь {старое_имя: новое_имя} для переименования.
                       Если None — переименование не выполняется.

    Returns:
        str: Путь к итоговому файлу при успехе.
        None: При любой ошибке.
    """
    # 1. Скачиваем и сохраняем в Parquet
    parquet_file = await google_to_parquet(
        list_name=list_name,
        file_code=file_code,
        output_path=output_path
    )

    if not parquet_file:
        logger.error("❌ Не удалось скачать или сохранить таблицу")
        return None

    # 2. Если есть маппинг — переименовываем столбцы
    if column_mapping:
        renamed_file = await rename_parquet_columns(
            input_path=parquet_file,
            column_mapping=column_mapping,
            output_path=output_path  # можно тот же путь (перезапись) или новый
        )
        if not renamed_file:
            logger.error("Не удалось переименовать столбцы")
            return None
        return renamed_file

    # 3. Если маппинга нет — возвращаем путь к исходному файлу
    return parquet_file