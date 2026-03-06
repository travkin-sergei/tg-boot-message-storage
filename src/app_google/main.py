"""
Приложение app_google: пайплайн обработки Google Sheets.
"""
import duckdb
import pyarrow as pa

from datetime import datetime, date
from typing import Optional

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from src.app_google.config import APP_GOOGLE_FILE, COLUMN_MAPPING, SHEET_NAME
from src.app_google.model import TaskList
from src.config.logger import logger
from src.config.database import engine, async_session
from src.app_google.get_google import GoogleSheetProcessor

# === Константы ===

REQUIRED_COLUMNS: set[str] = set(COLUMN_MAPPING.keys())


# =========================================================================
# === Хелперы для конвертации типов ===
# =========================================================================

def safe_str(value) -> str | None:
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        return str(int(value)) if isinstance(value, float) and value == int(value) else str(value)
    return str(value).strip() or None


def safe_int(value) -> int | None:
    if value is None or value == '':
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value == int(value) else None
    try:
        cleaned = str(value).strip().replace(' ', '').replace('\xa0', '')
        return int(float(cleaned))
    except (ValueError, TypeError):
        return None


def safe_date(value) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    for fmt in ('%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(str(value).strip(), fmt).date()
        except ValueError:
            continue
    logger.warning(f"⚠️ Не удалось распарсить дату: {value!r}")
    return None


# =========================================================================
# === Запись в БД с UPSERT ===
# =========================================================================

async def save_tasks_to_db(records: list[dict[str, any]]) -> tuple[int, int, int]:
    """Сохраняет записи в БД через ORM с upsert по link_post."""
    if engine is None or not records:
        return 0, 0, 0

    inserted_count = updated_count = error_count = 0

    try:
        async with async_session() as session:
            for row in records:
                try:
                    task_data = {
                        'link_post': safe_str(row.get('link_post')),
                        'number': safe_str(row.get('number')),
                        'date_comment': safe_date(row.get('date_comment')),
                        'short_description': safe_str(row.get('short_description')),
                        'autor': safe_str(row.get('autor')),
                        'subscribers': safe_int(row.get('subscribers')),
                        'comment': safe_str(row.get('comment')),
                        'corrections': safe_str(row.get('corrections')),
                        'responsible': safe_str(row.get('responsible')),
                        'status': safe_str(row.get('status', 'new')),
                    }

                    stmt = pg_insert(TaskList).values(**task_data)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['link_post'],
                        set_={
                            'number': stmt.excluded.number,
                            'date_comment': stmt.excluded.date_comment,
                            'short_description': stmt.excluded.short_description,
                            'autor': stmt.excluded.autor,
                            'subscribers': stmt.excluded.subscribers,
                            'comment': stmt.excluded.comment,
                            'corrections': stmt.excluded.corrections,
                            'responsible': stmt.excluded.responsible,
                            'status': stmt.excluded.status,
                            'updated_at': func.now(),
                            'is_active': True,
                        }
                    )
                    result = await session.execute(stmt)
                    if result.rowcount == 1:
                        inserted_count += 1
                    elif result.rowcount == 2:
                        updated_count += 1
                    else:
                        inserted_count += 1
                except IntegrityError:
                    logger.debug(f"⚠️ Дубликат link_post: {row.get('link_post')}")
                    error_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка записи: {e}")
                    error_count += 1
            await session.commit()
            logger.info(f"✅ UPSERT: {inserted_count} вставлено, {updated_count} обновлено")
    except Exception as e:
        logger.error(f"❌ Ошибка транзакции: {e}", exc_info=True)
        error_count += len(records) - inserted_count - updated_count
    return inserted_count, updated_count, error_count


async def main(file_code: Optional[str] = None,
               sheet_name: Optional[str] = None) -> dict[str, any]:
    target_file = file_code or APP_GOOGLE_FILE
    target_sheet = sheet_name or SHEET_NAME  # SHEET_NAME = "02.03.2026"

    # === ВАЛИДАЦИЯ: отвергаем заглушки ===
    INVALID = {"string", "None", "", "your_token_here", "placeholder"}

    if not target_file or target_file in INVALID:
        logger.error("❌ GOOGLE_FILE не задан или некорректен")
        return {'success': False, 'message': 'GOOGLE_FILE not configured', 'stats': {}}

    # ✅ Добавь эту проверку для sheet_name:
    if target_sheet in INVALID:
        logger.warning(f"⚠️ Некорректный sheet_name '{target_sheet}', используем дефолт: {SHEET_NAME}")
        target_sheet = SHEET_NAME  # "02.03.2026"

    logger.info(f"🚀 Запуск пайплайна: {target_file} / {target_sheet}")

    processor = GoogleSheetProcessor(timeout=30)

    try:
        # 1. Скачать файл
        if not await processor.download_file(target_file):
            return {'success': False, 'message': 'Failed to download file', 'stats': {}}

        # 2. Получить список листов
        sheets = await processor.get_sheet_names()
        if sheets is None:
            return {'success': False, 'message': 'Failed to get sheets', 'stats': {}}

        # 3. Получить список столбцов
        columns = await processor.get_sheet_columns(target_sheet)
        if columns is None:
            return {'success': False, 'message': 'Failed to get columns', 'stats': {}}

        # 4. Получить данные листа
        data = await processor.get_sheet_data(target_sheet)
        if not data:
            return {'success': False, 'message': 'No data to process', 'stats': {}}

        logger.info(f"✅ Получено строк: {len(data)}")

        # Проверка обязательных столбцов
        available = set(columns) if columns else set()
        missing = [col for col in REQUIRED_COLUMNS if col not in available]
        if missing:
            logger.error(f"❌ Отсутствуют столбцы: {missing}")
            return {'success': False, 'message': f'Missing columns: {missing}', 'stats': {}}

        # === Обработка через DuckDB ===
        conn = duckdb.connect()
        try:
            arrow_data = {col: [row.get(col) for row in data] for col in COLUMN_MAPPING.keys()}
            table = pa.Table.from_pydict(arrow_data)
            conn.register('sheet_data', table)

            select_parts = [f'"{src}" AS {tgt}' for src, tgt in COLUMN_MAPPING.items()]
            query = f"SELECT {', '.join(select_parts)} FROM sheet_data WHERE \"Ссылка\" IS NOT NULL"

            filtered = conn.execute(query).fetchall()
            logger.info(f"✅ После фильтрации: {len(filtered)} строк")

            # Конвертация в list[dict]
            target_cols = list(COLUMN_MAPPING.values())
            records = [dict(zip(target_cols, row)) for row in filtered]

            # === Запись в БД ===
            if records:
                inserted, updated, errors = await save_tasks_to_db(records)
                total = inserted + updated
                return {
                    'success': errors == 0 or total > 0,
                    'message': f'Processed {total} records ({inserted} new, {updated} updated, {errors} errors)',
                    'stats': {'total': total, 'inserted': inserted, 'updated': updated, 'errors': errors,
                              'source_rows': len(records)}
                }
            return {'success': True, 'message': 'No records to save', 'stats': {}}
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка пайплайна: {e}", exc_info=True)
        return {'success': False, 'message': f'Pipeline error: {str(e)}', 'stats': {}}
    finally:
        processor.clear_cache()
        logger.debug("🧹 Кэш очищен")
