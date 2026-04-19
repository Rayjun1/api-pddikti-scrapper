"""
PDDikti Dosen Explorer — Backend Configuration
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    DB_HOST: str = "localhost"
    DB_PORT: int = 3306
    DB_USER: str = "pddikti_user"
    DB_PASSWORD: str = "password"
    DB_NAME: str = "pddikti_explorer"

    # Local dev: set USE_SQLITE=true to use SQLite instead of MySQL
    USE_SQLITE: bool = False

    # JWT
    JWT_SECRET: str = "change-this-to-a-very-long-random-string"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 480

    # Auto-logout inactivity timeout (seconds)
    INACTIVITY_TIMEOUT: int = 1800  # 30 minutes

    # Admin defaults
    ADMIN_USERNAME: str = "admin"
    ADMIN_PASSWORD: str = "admin123"

    # CORS — comma-separated allowed origins (e.g. https://your-app.vercel.app)
    CORS_ORIGINS: str = "*"

    @property
    def DATABASE_URL(self) -> str:
        if self.USE_SQLITE:
            return "sqlite+aiosqlite:///./pddikti_local.db"
        return (
            f"mysql+aiomysql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
