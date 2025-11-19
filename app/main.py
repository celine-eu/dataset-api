from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.db.postgres import get_session, init_db, shutdown_db

app = FastAPI(title="CELINE API Prototype")

@app.get("/")
async def root():
    return {"message": "Hello from CELINE FastAPI container!"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/db-test")
async def test_db(session: AsyncSession = Depends(get_session)):
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
            "tables+views": [r.table_name for r in results]
        }
    except Exception as e:
        print("DB query failed:", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database query failed"
        )

@app.on_event("startup")
async def startup_event():
    await init_db()

@app.on_event("shutdown")
async def shutdown_event():
    await shutdown_db()
