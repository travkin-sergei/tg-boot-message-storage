# src/app_google/pipline/line1.py
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, date
from sqlalchemy.ext.asyncio import AsyncSession

from src.app_log import logger
from src.app_google.external.processor import download_and_rename_parquet
from src.app_google.function import bulk_upsert_task_list


def _prepare_records(records: list[dict]) -> list[dict]:
    """
    Подготовка записей: приведение типов, очистка, валидация.
    """
    prepared = []

    if records:
        logger.debug(f"🔍 Пример записи до обработки: {records[0]}")

    # Поля, которые должны быть строками в БД
    text_fields = [
        'key', 'link', 'short_description', 'autor', 'text',
        'corrections', 'responsible', 'status'
    ]

    for idx, row in enumerate(records):
        # Очистка ключей от пробелов
        cleaned = {k.strip() if isinstance(k, str) else k: v for k, v in row.items()}

        # 1. Конвертация key в строку (критично!)
        raw_key = cleaned.get('key')
        if raw_key is not None:
            if isinstance(raw_key, (int, float)):
                cleaned['key'] = str(int(raw_key)) if float(raw_key).is_integer() else str(raw_key)
            else:
                cleaned['key'] = str(raw_key).strip() if isinstance(raw_key, str) else None
        else:
            cleaned['key'] = None

        # 2. Парсинг даты → date object
        raw_date = cleaned.get('date')
        if isinstance(raw_date, datetime):
            cleaned['date'] = raw_date.date()
        elif isinstance(raw_date, date):
            cleaned['date'] = raw_date
        elif isinstance(raw_date, str) and raw_date.strip():
            for fmt in ["%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S"]:
                try:
                    cleaned['date'] = datetime.strptime(raw_date.strip(), fmt).date()
                    break
                except ValueError:
                    continue
            else:
                cleaned['date'] = None
        else:
            cleaned['date'] = None

        # 3. Конвертация текстовых полей в str
        for field in text_fields:
            if field in cleaned and cleaned[field] is not None:
                val = cleaned[field]
                if isinstance(val, (int, float)):
                    cleaned[field] = str(int(val)) if isinstance(val, float) and val.is_integer() else str(val)
                elif isinstance(val, str):
                    cleaned[field] = val.strip()

        # 4. is_active → bool
        if 'is_active' in cleaned:
            val = cleaned['is_active']
            if isinstance(val, str):
                cleaned['is_active'] = val.lower() in ('true', '1', 'yes', 'да')
            elif isinstance(val, (int, float)):
                cleaned['is_active'] = bool(val)

        # 5. Валидация: key и date обязательны
        if cleaned.get('key') and cleaned.get('date'):
            prepared.append(cleaned)
        else:
            logger.debug(f"⚠️ Пропущена строка {idx}: key={cleaned.get('key')!r}, date={cleaned.get('date')!r}")

    logger.info(f"✅ Валидных записей: {len(prepared)} из {len(records)}")
    if prepared:
        logger.debug(f"🔍 Пример валидной записи: {prepared[0]}")

    return prepared


async def sync_google_sheet_to_db(
        session: AsyncSession,
        list_name: str,
        file_code: str,
        output_parquet: str,
        column_mapping: dict[str, str],
        delete_after_upload: bool = True  # 🔥 Новый параметр
) -> dict:
    """
    Полный пайплайн: Google Sheet → Parquet → Database (UPSERT).

    Args:
        delete_after_upload: Если True — удаляет .parquet файл после успешной загрузки.
    """

    # 1. Скачать и переименовать
    parquet_file = await download_and_rename_parquet(
        list_name=list_name,
        file_code=file_code,
        output_path=output_parquet,
        column_mapping=column_mapping
    )
    if not parquet_file:
        logger.error("❌ Не удалось скачать таблицу")
        return {'total': 0, 'success': 0, 'errors': 0}

    # 2. Читаем Parquet через pyarrow
    try:
        table = pq.read_table(parquet_file)
        records = table.to_pylist()
        if not records:
            logger.warning("Parquet файл пустой")
            return {'total': 0, 'success': 0, 'errors': 0}
        logger.info(f"Загружено {len(records)} записей из Parquet")
    except Exception as e:
        logger.error(f"Ошибка чтения Parquet: {e}", exc_info=True)
        return {'total': 0, 'success': 0, 'errors': 1}

    # 3. Подготовка данных: конвертация типов
    prepared_records = _prepare_records(records)

    if not prepared_records:
        logger.error("Нет валидных записей после подготовки")
        return {'total': len(records), 'success': 0, 'errors': len(records)}

    # 4. Массовый UPSERT в БД
    stats = await bulk_upsert_task_list(session=session, records=prepared_records)

    # 5. Удаление временного файла после успешной загрузки
    if delete_after_upload and stats.get('success', 0) > 0:
        try:
            parquet_path = Path(parquet_file)
            if parquet_path.exists():
                parquet_path.unlink()
                logger.info(f"Временный файл удалён: {parquet_file}")
        except Exception as e:
            logger.warning(f"Не удалось удалить файл {parquet_file}: {e}")

    return stats