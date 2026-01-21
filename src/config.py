import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


def get_env_file() -> str:
    """Determine which .env file to load based on APP_ENV."""
    app_env = os.getenv("APP_ENV", "").lower()
    if app_env == "local":
        return ".env.local"
    elif app_env == "prod":
        return ".env.prod"
    return ".env"


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=get_env_file())

    # Database mode: "supabase" or "postgres"
    db_mode: str = "supabase"

    # Supabase config (used when db_mode="supabase")
    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None

    # PostgreSQL config (used when db_mode="postgres")
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "polymarket"
    postgres_password: str = "polymarket"
    postgres_db: str = "polymarket"

    log_level: str = "INFO"

    # Kalshi API credentials (RSA key-based auth)
    kalshi_api_key: Optional[str] = None
    kalshi_private_key_path: Optional[str] = None  # Path to PEM file
    kalshi_private_key: Optional[str] = None  # Or direct PEM content
    kalshi_base_url: str = "https://api.elections.kalshi.com/trade-api/v2"
    kalshi_ws_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"

    @property
    def postgres_dsn(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def async_postgres_dsn(self) -> str:
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
