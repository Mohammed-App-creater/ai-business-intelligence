from pydantic_settings import BaseSettings
from pydantic import AnyUrl

class Settings(BaseSettings):
    ENV: str = "development"
    APP_NAME: str = "ai-business-intel"
    DATABASE_URL: str
    VECTOR_TABLE_NAME: str = "embeddings"
    OPENAI_API_KEY: str | None = None
    SAAS_API_BASE: AnyUrl

    class Config:
        env_file = ".env"

settings = Settings()