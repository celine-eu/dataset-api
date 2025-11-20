"""Health check endpoint"""
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.postgres import get_db_session

router = APIRouter(tags=["Health"])

@router.get("/health")
async def health_check(session: AsyncSession = Depends(get_db_session)):
    """Application health check"""
    try:
        # Check Postgres
        await session.execute(text("SELECT 1"))
        postgres_status = "healthy"
    except Exception as e:
        print(f"Postgres health check failed: {e}")
        postgres_status = "unhealthy"
    
    return {
        "status": "healthy" if postgres_status == "healthy" else "degraded",
        "services": {
            "postgres": postgres_status
        }
    }
