"""Unit tests for settings."""

import os
import pytest

from pipeline.config.settings import Settings


@pytest.mark.unit
class TestSettings:
    """Test Settings."""

    def test_default_values(self):
        """Test default settings values."""
        settings = Settings()

        assert settings.BATCH_SIZE == 10000
        assert settings.MAX_RETRIES == 3
        assert settings.LOG_LEVEL == "INFO"

    def test_validate_log_level(self):
        """Test log level validation."""
        assert Settings.validate_log_level("INFO") == "INFO"
        assert Settings.validate_log_level("debug") == "DEBUG"
        assert Settings.validate_log_level("ERROR") == "ERROR"

        with pytest.raises(ValueError):
            Settings.validate_log_level("INVALID")

    def test_validate_log_format(self):
        """Test log format validation."""
        assert Settings.validate_log_format("json") == "json"
        assert Settings.validate_log_format("TEXT") == "text"
        assert Settings.validate_log_format("console") == "console"

        with pytest.raises(ValueError):
            Settings.validate_log_format("INVALID")

    def test_get_celery_accept_content(self):
        """Test parsing Celery accept content."""
        content = Settings.get_celery_accept_content()
        assert isinstance(content, list)
        assert "json" in content

    def test_get_database_config(self):
        """Test getting database config."""
        settings = Settings()
        config = settings.get_database_config()

        assert "source_db_url" in config
        assert "dest_db_url" in config

    def test_get_redis_config(self):
        """Test getting Redis config."""
        settings = Settings()
        config = settings.get_redis_config()

        assert "url" in config
        assert "host" in config
        assert "port" in config

    def test_get_celery_config(self):
        """Test getting Celery config."""
        settings = Settings()
        config = settings.get_celery_config()

        assert "broker_url" in config
        assert "result_backend" in config

    def test_to_dict(self):
        """Test converting to dictionary."""
        settings = Settings()
        config_dict = settings.to_dict()

        assert "database" in config_dict
        assert "redis" in config_dict
        assert "celery" in config_dict
        assert "pipeline" in config_dict
        assert "retry" in config_dict
        assert "logging" in config_dict

