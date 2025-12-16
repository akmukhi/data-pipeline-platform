"""Configuration management using environment variables with validation."""

import os
from typing import Dict, Optional

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

    @classmethod
    def validate_log_level(cls, value: str) -> str:
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        upper_value = value.upper()
        if upper_value not in valid_levels:
            raise ValueError(f"Invalid LOG_LEVEL: {value}. Must be one of {valid_levels}")
        return upper_value

    @classmethod
    def validate_log_format(cls, value: str) -> str:
        """Validate log format."""
        valid_formats = ["json", "text", "console"]
        lower_value = value.lower()
        if lower_value not in valid_formats:
            raise ValueError(f"Invalid LOG_FORMAT: {value}. Must be one of {valid_formats}")
        return lower_value

    def get_database_config(self) -> Dict[str, str]:
        """Get database configuration as dictionary."""
        return {
            "source_db_url": self.SOURCE_DB_URL,
            "dest_db_url": self.DEST_DB_URL,
        }

    def get_redis_config(self) -> Dict[str, any]:
        """Get Redis configuration as dictionary."""
        return {
            "url": self.REDIS_URL,
            "host": self.REDIS_HOST,
            "port": self.REDIS_PORT,
            "db": self.REDIS_DB,
        }

    def get_celery_config(self) -> Dict[str, any]:
        """Get Celery configuration as dictionary."""
        return {
            "broker_url": self.CELERY_BROKER_URL,
            "result_backend": self.CELERY_RESULT_BACKEND,
            "task_serializer": self.CELERY_TASK_SERIALIZER,
            "result_serializer": self.CELERY_RESULT_SERIALIZER,
            "accept_content": self.get_celery_accept_content(),
            "timezone": self.CELERY_TIMEZONE,
            "enable_utc": self.CELERY_ENABLE_UTC,
        }

    def get_pipeline_config(self) -> Dict[str, any]:
        """Get pipeline configuration as dictionary."""
        return {
            "batch_size": self.BATCH_SIZE,
            "max_workers": self.MAX_WORKERS,
            "default_chunk_size": self.DEFAULT_CHUNK_SIZE,
        }

    def get_retry_config(self) -> Dict[str, any]:
        """Get retry configuration as dictionary."""
        return {
            "max_retries": self.MAX_RETRIES,
            "backoff_base": self.RETRY_BACKOFF_BASE,
            "delay": self.RETRY_DELAY,
        }

    def get_logging_config(self) -> Dict[str, any]:
        """Get logging configuration as dictionary."""
        return {
            "level": self.validate_log_level(self.LOG_LEVEL),
            "format": self.validate_log_format(self.LOG_FORMAT),
        }

    def to_dict(self) -> Dict[str, any]:
        """Convert all settings to dictionary."""
        return {
            "database": self.get_database_config(),
            "redis": self.get_redis_config(),
            "celery": self.get_celery_config(),
            "pipeline": self.get_pipeline_config(),
            "retry": self.get_retry_config(),
            "logging": self.get_logging_config(),
            "idempotency": {
                "ttl_days": self.IDEMPOTENCY_KEY_TTL_DAYS,
                "enabled": self.IDEMPOTENCY_ENABLED,
            },
            "connection_pool": {
                "pool_size": self.DB_POOL_SIZE,
                "max_overflow": self.DB_MAX_OVERFLOW,
                "pool_pre_ping": self.DB_POOL_PRE_PING,
            },
            "worker": {
                "concurrency": self.WORKER_CONCURRENCY,
                "max_tasks_per_child": self.WORKER_MAX_TASKS_PER_CHILD,
            },
        }


# Global settings instance
settings = Settings()

