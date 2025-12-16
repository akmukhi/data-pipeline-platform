"""Pytest configuration and shared fixtures."""

import os
import sys
import tempfile
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Set test environment variables before importing pipeline modules
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["LOG_FORMAT"] = "text"


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Get test data directory."""
    return Path(__file__).parent / "fixtures" / "data"


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
                "david@example.com",
                "eve@example.com",
            ],
            "age": [25, 30, 35, 40, 45],
            "created_at": pd.date_range("2024-01-01", periods=5, freq="D"),
        }
    )


@pytest.fixture
def sample_dataframe_large() -> pd.DataFrame:
    """Create a larger sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": range(1, 1001),
            "name": [f"User_{i}" for i in range(1, 1001)],
            "value": [i * 10 for i in range(1, 1001)],
        }
    )


@pytest.fixture
def in_memory_db() -> Generator[Engine, None, None]:
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)

    # Create a test table
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE test_users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT,
                    age INTEGER,
                    created_at TIMESTAMP
                )
            """
            )
        )
        conn.commit()

        # Insert test data
        conn.execute(
            text(
                """
                INSERT INTO test_users (id, name, email, age, created_at)
                VALUES
                    (1, 'Alice', 'alice@example.com', 25, '2024-01-01'),
                    (2, 'Bob', 'bob@example.com', 30, '2024-01-02'),
                    (3, 'Charlie', 'charlie@example.com', 35, '2024-01-03')
            """
            )
        )
        conn.commit()

    yield engine

    engine.dispose()


@pytest.fixture
def postgres_db_url() -> str:
    """Get PostgreSQL database URL for testing."""
    return os.getenv(
        "TEST_POSTGRES_URL",
        "postgresql://pipeline:pipeline_password@localhost:5432/test_pipeline_db",
    )


@pytest.fixture
def redis_url() -> str:
    """Get Redis URL for testing."""
    return os.getenv("TEST_REDIS_URL", "redis://localhost:6379/1")


@pytest.fixture
def mock_redis(monkeypatch):
    """Mock Redis for testing."""
    import redis
    from unittest.mock import MagicMock

    mock_client = MagicMock(spec=redis.Redis)
    mock_client.ping.return_value = True
    mock_client.exists.return_value = False
    mock_client.setex.return_value = True
    mock_client.get.return_value = None
    mock_client.delete.return_value = 1
    mock_client.set.return_value = True

    def mock_from_url(url):
        return mock_client

    monkeypatch.setattr(redis, "from_url", mock_from_url)
    return mock_client


@pytest.fixture
def temp_file(tmp_path) -> Path:
    """Create a temporary file."""
    return tmp_path / "test_file.txt"


@pytest.fixture
def temp_config_file(tmp_path) -> Path:
    """Create a temporary config file."""
    config_file = tmp_path / "test_config.json"
    config_file.write_text(
        '{"select": ["id", "name"], "rename": {"name": "full_name"}}'
    )
    return config_file


@pytest.fixture
def temp_yaml_config_file(tmp_path) -> Path:
    """Create a temporary YAML config file."""
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(
        """
select:
  - id
  - name
rename:
  name: full_name
filter:
  age: 30
"""
    )
    return config_file


@pytest.fixture
def celery_app():
    """Get Celery app for testing."""
    from pipeline.workers.celery_app import celery_app

    # Use in-memory broker for testing
    celery_app.conf.update(
        task_always_eager=True,  # Execute tasks synchronously
        task_eager_propagates=True,  # Propagate exceptions
        broker_url="memory://",
        result_backend="cache+memory://",
    )
    return celery_app


@pytest.fixture
def mock_settings(monkeypatch):
    """Mock settings for testing."""
    test_settings = {
        "SOURCE_DB_URL": "postgresql://test:test@localhost:5432/test_db",
        "DEST_DB_URL": "postgresql://test:test@localhost:5432/test_db",
        "REDIS_URL": "redis://localhost:6379/0",
        "CELERY_BROKER_URL": "redis://localhost:6379/0",
        "CELERY_RESULT_BACKEND": "redis://localhost:6379/0",
        "BATCH_SIZE": 1000,
        "MAX_RETRIES": 3,
        "RETRY_BACKOFF_BASE": 2.0,
        "RETRY_DELAY": 1.0,
        "LOG_LEVEL": "DEBUG",
        "LOG_FORMAT": "text",
    }

    for key, value in test_settings.items():
        monkeypatch.setenv(key, str(value))

    return test_settings


@pytest.fixture
def correlation_id():
    """Generate a test correlation ID."""
    import uuid

    return str(uuid.uuid4())


@pytest.fixture
def pipeline_id():
    """Generate a test pipeline ID."""
    return "test_pipeline_001"


@pytest.fixture
def task_id():
    """Generate a test task ID."""
    import uuid

    return str(uuid.uuid4())

