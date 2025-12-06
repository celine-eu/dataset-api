from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Project
    PROJECT_NAME: str = "Celine Digital Twin APIs"
    VERSION: str = "1.0.0-SNAPSHOT"
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    
    # PostgreSQL
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str = "datasets"
    POSTGRES_HOST: str = "172.17.0.1"
    POSTGRES_PORT: int
    POSTGRES_SCHEMA: str

    # Pydantic automatically loads this .env
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True
    )
    
    @property
    def DATABASE_URL(self) -> str:
        """Build the database URL"""
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

settings = Settings()
