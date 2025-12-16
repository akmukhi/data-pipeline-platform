# Test Suite

This directory contains unit and integration tests for the data pipeline platform.

## Structure

```
tests/
├── conftest.py              # Shared fixtures and configuration
├── fixtures/                # Test fixtures and data
│   └── data/               # Test data files
├── unit/                    # Unit tests
│   ├── test_connection_manager.py
│   ├── test_batch_ingestor.py
│   ├── test_sql_transformer.py
│   ├── test_code_transformer.py
│   ├── test_config_transformer.py
│   ├── test_schema_validator.py
│   ├── test_batch_writer.py
│   ├── test_retry.py
│   ├── test_idempotency.py
│   ├── test_logging.py
│   ├── test_settings.py
│   └── test_workers_tasks.py
└── integration/             # Integration tests
    ├── test_pipeline_flow.py
    ├── test_api.py
    └── test_workers.py
```

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Unit Tests Only

```bash
pytest tests/unit/
```

### Run Integration Tests Only

```bash
pytest tests/integration/
```

### Run Specific Test File

```bash
pytest tests/unit/test_batch_ingestor.py
```

### Run Specific Test

```bash
pytest tests/unit/test_batch_ingestor.py::TestBatchIngestor::test_ingest_from_query
```

### Run with Coverage

```bash
pytest --cov=pipeline --cov-report=html
```

### Run with Markers

```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"

# Run tests that require database
pytest -m requires_db
```

## Test Markers

Tests are marked with the following markers:

- `@pytest.mark.unit` - Unit tests
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.slow` - Slow running tests
- `@pytest.mark.requires_db` - Tests requiring database
- `@pytest.mark.requires_redis` - Tests requiring Redis
- `@pytest.mark.requires_celery` - Tests requiring Celery workers

## Fixtures

### Common Fixtures (in conftest.py)

- `sample_dataframe` - Sample DataFrame for testing
- `sample_dataframe_large` - Larger DataFrame (1000 rows)
- `in_memory_db` - In-memory SQLite database
- `mock_redis` - Mock Redis client
- `celery_app` - Celery app configured for testing
- `mock_settings` - Mock settings
- `correlation_id` - Test correlation ID
- `pipeline_id` - Test pipeline ID
- `task_id` - Test task ID

## Environment Variables

For integration tests, set:

```bash
export TEST_POSTGRES_URL="postgresql://user:pass@localhost:5432/test_db"
export TEST_REDIS_URL="redis://localhost:6379/1"
```

## Test Coverage

Current test coverage includes:

### Unit Tests
- ✅ Connection Manager
- ✅ Batch Ingestor
- ✅ SQL Transformer
- ✅ Code Transformer
- ✅ Config Transformer
- ✅ Schema Validator
- ✅ Batch Writer
- ✅ Retry Utility
- ✅ Idempotency Utility
- ✅ Logging Utility
- ✅ Settings
- ✅ Worker Tasks (structure)

### Integration Tests
- ✅ Complete Pipeline Flow
- ✅ API Endpoints
- ✅ Worker Tasks

## Writing New Tests

### Unit Test Example

```python
import pytest
from pipeline.ingestion import BatchIngestor

@pytest.mark.unit
class TestMyComponent:
    def test_my_function(self, sample_dataframe):
        # Test implementation
        assert True
```

### Integration Test Example

```python
import pytest

@pytest.mark.integration
@pytest.mark.requires_db
class TestMyIntegration:
    def test_integration_flow(self, test_db):
        # Integration test
        pass
```

## Continuous Integration

Tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run tests
  run: |
    pytest --cov=pipeline --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
```

## Notes

- Unit tests use mocks and in-memory databases where possible
- Integration tests may require external services (PostgreSQL, Redis)
- Use markers to skip tests that require unavailable services
- All tests should be deterministic and isolated

