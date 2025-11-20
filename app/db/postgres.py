"""PostgreSQL database configuration"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.config import settings

# Create async engine
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,  # Set True for SQL logging
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

async def init_db() -> None:
    """Initialize database - create tables if needed"""
    try:
        async with engine.begin() as conn:
            # Ping the db
            await conn.execute(text("SELECT 1"))

        print(f"Database initialized successfully with schema: {settings.POSTGRES_SCHEMA}")
    except Exception as e:
        print(f"Database initialization failed: {e}")
        raise

async def shutdown_db() -> None:
    """Close database connections"""
    await engine.dispose()
    print("Database connections closed")

async def get_db_session() -> AsyncSession:
    """Dependency to get database session"""
    async with AsyncSessionLocal() as session:
        yield session
