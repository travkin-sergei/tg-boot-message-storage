# src/app_google/utils.py
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Type


async def upsert_batch(session: AsyncSession,
                       table: Table,
                       data: List[Dict],
                       constraint_columns: List[str],
                       update_columns: List[str]):
    """UPSERT батчами через SQLAlchemy"""
    if not data:
        return

    stmt = insert(table).values(data)
    update_dict = {col: getattr(stmt.excluded, col) for col in update_columns}
    stmt = stmt.on_conflict_do_update(
        index_elements=constraint_columns,
        set_=update_dict
    )
    await session.execute(stmt)
    await session.commit()
