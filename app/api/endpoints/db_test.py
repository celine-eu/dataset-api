"""Postgres tables check"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.db.postgres import get_db_session

router = APIRouter(tags=["DB_test"])

@router.get("/db_test")
async def test_db(session: AsyncSession = Depends(get_db_session)):
    try:
        # get table list
        q = text("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
            ORDER BY table_name;
        """)
        results = await session.execute(q, {"schema": settings.POSTGRES_SCHEMA})

        return {
            "connected": True,
            "schema": settings.POSTGRES_SCHEMA,
            "tables_&_views": [r.table_name for r in results]
        }
    except Exception as e:
        print("DB query failed:", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database query failed"
        )