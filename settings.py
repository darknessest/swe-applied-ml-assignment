from pydantic_settings import BaseSettings, SettingsConfigDict


# Load environment variables from .env file:
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    OPENAI_API_KEY: str
