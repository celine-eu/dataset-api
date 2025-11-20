"""API Router configuration"""
from fastapi import APIRouter
from app.api.endpoints import health, db_test

api_router = APIRouter()

# Health check (no prefix for simplicity)
api_router.include_router(health.router)

# Data endpoints
api_router.include_router(db_test.router, prefix="/api")
