# src/app_google/pipline.py
"""
Приложение app_google:
1) Скачивает файл;
2) Фильтрует пустые строки;
3) Переименовывает столбцы;
4) Проверяет набор полей;
5) Сохраняет данные напрямую в БД (схема: test, таблица: task_list).

Режим записи: UPSERT по полю link_post (первичный ключ).
"""
import asyncio
import logging
import duckdb
import pyarrow as pa

from datetime import datetime, date

from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from src.app_google.model import init_db_schema, TaskList
from src.config.logger import logger, config_logging
from src.config.other import GOOGLE_FILE
from src.config.database import engine, async_session
from src.app_google._get_google import GoogleSheetProcessor

# config_logging(level=logging.INFO)


SHEET_NAME: str = "02.03.2026"
DB_SCHEMA: str = "test"

COLUMN_MAPPING: dict[str, str] = {
    "№ п/п": "number",
    "дата комментария": "date_comment",
    "Ссылка": "link_post",
    "Краткое описание": "short_description",
    "Кол-во подписчиков": "subscribers",
    "Текст комментария": "comment",
    "Исправления": "corrections",
    "Ответственный за публикацию": "responsible",
    "Статус опубликования": "status",
}
REQUIRED_COLUMNS: set[str] = set(COLUMN_MAPPING.keys())


def safe_str(value) -> str | None:
    """Конвертирует значение в str, обрабатывая числа и пустые значения."""
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value == int(value):
            return str(int(value))
        return str(value)
    return str(value).strip() or None


def safe_int(value) -> int | None:
    """Конвертирует значение в int, возвращая None при ошибке."""
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
    """Конвертирует значение в date для PostgreSQL."""
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


# src/app_google/pipline.py

async def ensure_schema_exists() -> bool:
    """Создаёт схему БД, если не существует."""
    if engine is None:
        logger.error("engine не инициализирован")
        return False
    try:
        from sqlalchemy import text
        async with engine.begin() as conn:
            # Создаём схему 'test' (или из конфига)
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}"))
            logger.info(f"✅ Схема '{DB_SCHEMA}' проверена/создана")
            return True
    except Exception as e:
        logger.error(f"Ошибка создания схемы: {e}", exc_info=True)
        return False


async def save_tasks_to_db(records: list[dict[str, any]]) -> tuple[int, int, int]:
    """
    Сохраняет записи в БД через ORM с upsert по link_post.

    Returns:
        tuple: (вставлено, обновлено, ошибок)
    """
    if engine is None or not records:
        return 0, 0, 0

    inserted_count = 0
    updated_count = 0
    error_count = 0

    try:
        async with async_session() as session:
            for row in records:
                try:
                    # Подготовка данных с конвертацией типов
                    task_data = {
                        'link_post': safe_str(row.get('link_post')),  # PK, обязательное
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

                    # === UPSERT: INSERT ... ON CONFLICT DO UPDATE ===
                    stmt = pg_insert(TaskList).values(**task_data)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['link_post'],  # Конфликт по первичному ключу
                        set_={
                            # Поля для обновления при конфликте
                            'number': stmt.excluded.number,
                            'date_comment': stmt.excluded.date_comment,
                            'short_description': stmt.excluded.short_description,
                            'autor': stmt.excluded.autor,
                            'subscribers': stmt.excluded.subscribers,
                            'comment': stmt.excluded.comment,
                            'corrections': stmt.excluded.corrections,
                            'responsible': stmt.excluded.responsible,
                            'status': stmt.excluded.status,
                            'updated_at': func.now(),  # Авто-обновление метки времени
                            'is_active': True,  # Восстанавливаем, если было мягкое удаление
                        }
                    )

                    result = await session.execute(stmt)

                    # В asyncpg: rowcount=1 → вставлено, rowcount=2 → обновлено
                    if result.rowcount == 1:
                        inserted_count += 1
                    elif result.rowcount == 2:
                        updated_count += 1
                    else:
                        inserted_count += 1  # fallback

                except IntegrityError as e:
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


# =========================================================================
# === Основная логика ===
# =========================================================================

# В конце src/app_google/pipline.py, замените функцию main():
async def main(file_code: str | None = None,
               sheet_name: str | None = None,
               upsert: bool = True) -> dict[str, any]:
    """Запускает пайплайн и возвращает результат."""
    from src.config.other import GOOGLE_FILE

    target_file = file_code or GOOGLE_FILE
    target_sheet = sheet_name or SHEET_NAME

    if not target_file:
        logger.error("GOOGLE_FILE не задан")
        return {'success': False, 'message': 'GOOGLE_FILE not configured', 'stats': {}}

    logger.info(f"Запуск пайплайна: {target_file} / {target_sheet}")

    # === КРИТИЧНО: инициализация схемы и таблиц ===
    if engine is None:
        logger.error("SQLAlchemy engine не инициализирован")
        return {'success': False, 'message': 'DB engine not initialized', 'stats': {}}

    # 1. Создаём схему, если нет
    await ensure_schema_exists()

    # 2. Создаём таблицы по моделям, если нет
    await init_db_schema()

    processor = GoogleSheetProcessor(timeout=30)

    try:
        # Инициализация БД
        if engine is None:
            logger.error("❌ SQLAlchemy engine не инициализирован")
            return {'success': False, 'message': 'DB engine not initialized', 'stats': {}}

        await ensure_schema_exists()
        await init_db_schema()

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
        available_columns = set(columns) if columns else set()
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in available_columns]
        if missing_columns:
            logger.error(f"❌ Отсутствуют столбцы: {missing_columns}")
            return {'success': False, 'message': f'Missing columns: {missing_columns}', 'stats': {}}

        # === Обработка через DuckDB ===
        conn = duckdb.connect()
        try:
            arrow_data = {col: [row.get(col) for row in data] for col in COLUMN_MAPPING.keys()}
            table = pa.Table.from_pydict(arrow_data)
            conn.register('sheet_data', table)

            select_parts = [f'"{src}" AS {tgt}' for src, tgt in COLUMN_MAPPING.items()]
            query = f"SELECT {', '.join(select_parts)} FROM sheet_data WHERE \"Ссылка\" IS NOT NULL"

            filtered_result = conn.execute(query).fetchall()
            logger.info(f"✅ После фильтрации: {len(filtered_result)} строк")

            # Конвертация в list[dict]
            target_columns = list(COLUMN_MAPPING.values())
            records = [dict(zip(target_columns, row)) for row in filtered_result]

            # === Запись в БД ===
            if records:
                inserted, updated, errors = await save_tasks_to_db(records)
                total = inserted + updated
                return {
                    'success': errors == 0 or total > 0,
                    'message': f'Processed {total} records ({inserted} new, {updated} updated, {errors} errors)',
                    'stats': {
                        'total': total,
                        'inserted': inserted,
                        'updated': updated,
                        'errors': errors,
                        'source_rows': len(records),
                    }
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

# if __name__ == '__main__':
#     asyncio.run(main())
