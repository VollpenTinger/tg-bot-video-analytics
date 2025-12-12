from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    BOT_TOKEN: str 
    ADMIN_ID: List[int] = []
    DB_HOST: str
    DB_PORT: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    YANDEX_API_KEY: str
    YANDEX_FOLDER_ID: str

    REDIS_HOST: str 
    REDIS_PORT: int
    REDIS_DB: int
    REDIS_PASSWORD: str
    REDIS_CACHE_TTL: int
    
    # Настройки кеширования
    ENABLE_CACHE: bool
    MIN_CACHE_LENGTH: int # Минимальное кол-во запросов для кеширования

    @property
    def database_url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

        """Получить URL для подключения к Redis"""
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
settings = Settings()