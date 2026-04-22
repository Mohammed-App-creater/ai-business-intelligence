from pydantic_settings import BaseSettings
from pydantic import AnyUrl, ConfigDict

class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", extra="ignore")

    ENV: str = "development"
    APP_NAME: str = "ai-business-intel"
    VECTOR_TABLE_NAME: str = "embeddings"
    OPENAI_API_KEY: str | None = None
    SAAS_API_BASE: AnyUrl
    ANALYTICS_BACKEND_URL: str
    ANALYTICS_BACKEND_API_KEY: str = ""        # X-API-Key header (architecture doc v1.2)
    ANALYTICS_BACKEND_BEARER_TOKEN: str = ""   # Authorization: Bearer (UAT curl style) ← NEW

settings = Settings()