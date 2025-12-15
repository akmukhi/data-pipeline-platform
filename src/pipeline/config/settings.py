"""Configuration settings for the data pipeline platform."""

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    source_db_url: str = Field(..., alias="SOURCE_DB_URL")
    destination_db_url: str = Field(..., alias="DESTINATION_DB_URL")
    pool_size: int = Field(default=10, alias="DB_POOL_SIZE")
    max_overflow: int = Field(default=20, alias="DB_MAX_OVERFLOW")
    pool_recycle: int = Field(default=3600, alias="DB_POOL_RECYCLE")
    pool_timeout: int = Field(default=30, alias="DB_POOL_TIMEOUT")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class RedisSettings(BaseSettings):
    """Redis connection settings."""

    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_db: int = Field(default=0, alias="REDIS_DB")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class PipelineSettings(BaseSettings):
    """Pipeline configuration settings."""

    default_batch_size: int = Field(default=10000, alias="DEFAULT_BATCH_SIZE")
    max_batch_size: int = Field(default=100000, alias="MAX_BATCH_SIZE")
    pipeline_log_level: str = Field(default="INFO", alias="PIPELINE_LOG_LEVEL")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class Settings(BaseSettings):
    """Main settings class combining all configuration."""

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


# Global settings instance
settings = Settings()

