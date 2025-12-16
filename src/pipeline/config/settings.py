"""Configuration management using environment variables."""

import os
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""

    # Database Configuration
    SOURCE_DB_URL: str = os.getenv(
        "SOURCE_DB_URL", "postgresql://user:password@localhost:5432/source_db"
    )
    DEST_DB_URL: str = os.getenv(
        "DEST_DB_URL", "postgresql://user:password@localhost:5432/dest_db"
    )

    # Redis Configuration
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))

    # Celery Configuration
    CELERY_BROKER_URL: str = os.getenv(
        "CELERY_BROKER_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )
    CELERY_RESULT_BACKEND: str = os.getenv(
        "CELERY_RESULT_BACKEND", os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )
    CELERY_TASK_SERIALIZER: str = os.getenv("CELERY_TASK_SERIALIZER", "json")
    CELERY_RESULT_SERIALIZER: str = os.getenv("CELERY_RESULT_SERIALIZER", "json")
    CELERY_ACCEPT_CONTENT_STR: str = os.getenv("CELERY_ACCEPT_CONTENT", '["json"]')
    CELERY_TIMEZONE: str = os.getenv("CELERY_TIMEZONE", "UTC")
    CELERY_ENABLE_UTC: bool = os.getenv("CELERY_ENABLE_UTC", "true").lower() == "true"

    # Pipeline Configuration
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "10000"))
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))
    DEFAULT_CHUNK_SIZE: int = int(os.getenv("DEFAULT_CHUNK_SIZE", "10000"))

    # Retry Configuration
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_BACKOFF_BASE: float = float(os.getenv("RETRY_BACKOFF_BASE", "2.0"))
    RETRY_DELAY: float = float(os.getenv("RETRY_DELAY", "1.0"))

    # Idempotency Configuration
    IDEMPOTENCY_KEY_TTL_DAYS: int = int(os.getenv("IDEMPOTENCY_KEY_TTL_DAYS", "7"))
    IDEMPOTENCY_ENABLED: bool = (
        os.getenv("IDEMPOTENCY_ENABLED", "true").lower() == "true"
    )

    # Schema Registry Configuration
    SCHEMA_REGISTRY_TABLE: str = os.getenv("SCHEMA_REGISTRY_TABLE", "schema_versions")

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = os.getenv("LOG_FORMAT", "json")

    # Connection Pool Configuration
    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "5"))
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    DB_POOL_PRE_PING: bool = (
        os.getenv("DB_POOL_PRE_PING", "true").lower() == "true"
    )

    # Worker Configuration
    WORKER_CONCURRENCY: int = int(os.getenv("WORKER_CONCURRENCY", "4"))
    WORKER_MAX_TASKS_PER_CHILD: int = int(
        os.getenv("WORKER_MAX_TASKS_PER_CHILD", "1000")
    )

    @classmethod
    def get_celery_accept_content(cls) -> list:
        """Parse CELERY_ACCEPT_CONTENT into a list."""
        try:
            import json

            content = cls.CELERY_ACCEPT_CONTENT_STR.replace("'", '"')
            return json.loads(content)
        except Exception:
            return ["json"]


# Global settings instance
settings = Settings()

