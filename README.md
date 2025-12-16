# Data Pipeline Platform

A production-ready data pipeline platform for ETL development with batch ingestion, transformation, and persistence. Built with Python, Celery, PostgreSQL, and Redis.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Usage](#usage)
- [Components](#components)
- [Configuration](#configuration)
- [API Documentation](#api-documentation)
- [CLI Documentation](#cli-documentation)
- [Testing](#testing)
- [Docker](#docker)
- [Contributing](#contributing)
- [License](#license)

## Overview

The Data Pipeline Platform is a comprehensive ETL solution that provides:

- **Batch Data Ingestion** from multiple database sources
- **Flexible Transformations** using SQL, Python code, or declarative configs
- **Efficient Persistence** with multiple write strategies
- **Asynchronous Processing** with Celery workers
- **Schema Evolution** support for changing data structures
- **Idempotency** to prevent duplicate processing
- **Retry Logic** with exponential backoff
- **REST API** and **CLI** for pipeline management

## Architecture

### System Architecture

```mermaid
graph TB
    subgraph Client[Client Layer]
        API[REST API<br/>FastAPI]
        CLI[CLI<br/>Command Line]
    end
    
    subgraph Queue[Task Queue Layer]
        Redis[(Redis<br/>Broker & Results)]
        Queue[Task Queue<br/>Celery]
    end
    
    subgraph Workers[Worker Pool]
        W1[Worker 1]
        W2[Worker 2]
        WN[Worker N]
    end
    
    subgraph Pipeline[Pipeline Stages]
        Ingest[Ingestion<br/>Batch Ingestor]
        Transform[Transformation<br/>SQL/Code/Config]
        Persist[Persistence<br/>Batch Writer]
    end
    
    subgraph Data[Data Layer]
        SourceDB[(Source<br/>Database)]
        DestDB[(Destination<br/>PostgreSQL)]
        SchemaReg[Schema Registry]
    end
    
    subgraph Utils[Utilities]
        Retry[Retry Logic]
        Idempotency[Idempotency]
        Logging[Structured Logging]
    end
    
    API --> Queue
    CLI --> Queue
    Queue --> Workers
    Workers --> Pipeline
    Pipeline --> Ingest
    Pipeline --> Transform
    Pipeline --> Persist
    Ingest --> SourceDB
    Transform --> Utils
    Persist --> DestDB
    Persist --> SchemaReg
    Redis -.->|Broker| Queue
    Redis -.->|Idempotency| Idempotency
    Utils --> Retry
    Utils --> Idempotency
    Utils --> Logging
```

### Pipeline Flow

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Queue
    participant Worker
    participant Ingest
    participant Transform
    participant Persist
    participant DB
    
    Client->>API: POST /pipelines
    API->>Queue: Submit pipeline task
    Queue->>Worker: Assign task
    Worker->>Ingest: Execute ingestion
    Ingest->>DB: Query source database
    DB-->>Ingest: Return data
    Ingest-->>Worker: Serialized DataFrame
    Worker->>Transform: Execute transformation
    Transform-->>Worker: Transformed data
    Worker->>Persist: Execute persistence
    Persist->>DB: Write to destination
    DB-->>Persist: Confirmation
    Persist-->>Worker: Write statistics
    Worker-->>Queue: Task complete
    Queue-->>API: Result
    API-->>Client: Pipeline status
```

### Component Architecture

```mermaid
graph LR
    subgraph Ingestion[Ingestion Layer]
        CM[Connection Manager<br/>Pool Management]
        BI[Batch Ingestor<br/>Chunked Reading]
    end
    
    subgraph Transformation[Transformation Layer]
        SQL[SQL Transformer<br/>SQL Queries]
        Code[Code Transformer<br/>Python Functions]
        Config[Config Transformer<br/>YAML/JSON]
        Schema[Schema Validator<br/>Validation & Evolution]
    end
    
    subgraph Persistence[Persistence Layer]
        BW[Batch Writer<br/>Efficient Writes]
    end
    
    subgraph Workers[Worker Layer]
        Tasks[Celery Tasks<br/>Async Execution]
        Monitor[Worker Monitor<br/>Health Tracking]
    end
    
    subgraph Utils[Utility Layer]
        Retry[Retry Logic<br/>Exponential Backoff]
        Idempotency[Idempotency<br/>Redis-based]
        Logging[Structured Logging<br/>Correlation IDs]
    end
    
    CM --> BI
    BI --> SQL
    BI --> Code
    BI --> Config
    SQL --> Schema
    Code --> Schema
    Config --> Schema
    Schema --> BW
    Tasks --> Ingestion
    Tasks --> Transformation
    Tasks --> Persistence
    Tasks --> Retry
    Tasks --> Idempotency
    Tasks --> Logging
    Monitor --> Tasks
```

## Features

### Core Features

- ✅ **Multi-Database Support**: PostgreSQL, MySQL, and more via SQLAlchemy
- ✅ **Batch Processing**: Efficient chunked reading and writing
- ✅ **Connection Pooling**: Optimized database connections
- ✅ **Multiple Transformation Types**: SQL, Python code, or declarative configs
- ✅ **Schema Evolution**: Handle schema changes gracefully
- ✅ **Write Strategies**: INSERT, UPSERT, REPLACE, APPEND
- ✅ **Asynchronous Processing**: Celery-based task queue
- ✅ **Idempotency**: Prevent duplicate processing
- ✅ **Retry Logic**: Automatic retry with exponential backoff
- ✅ **Structured Logging**: Correlation IDs and pipeline tracking
- ✅ **REST API**: FastAPI-based API for pipeline management
- ✅ **CLI**: Command-line interface for operations
- ✅ **Docker Support**: Complete Docker Compose setup
- ✅ **Health Monitoring**: Worker health checks and statistics

### Advanced Features

- **Schema Registry**: Versioned schema management
- **Task Versioning**: Track transformation versions
- **Pipeline Orchestration**: Chain multiple stages
- **Error Handling**: Comprehensive error recovery
- **Performance Optimization**: COPY operations, batch writes
- **Monitoring**: Flower integration for Celery monitoring

## Quick Start

### Using Docker (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd data-pipeline-platform

# Start all services
docker-compose up -d

# Start with API
docker-compose --profile api up -d

# View logs
docker-compose logs -f worker
```

### Local Installation

```bash
# Install dependencies
pip install -r requirements.txt
pip install -e .

# Set up environment
cp .env.example .env
# Edit .env with your configuration

# Start Redis and PostgreSQL (or use Docker)
# Then start workers
python -m pipeline.workers.run_worker
```

## Installation

### Prerequisites

- Python 3.11+
- PostgreSQL 15+ (for destination database)
- Redis 7+ (for task queue)
- Docker & Docker Compose (optional)

### Install from Source

```bash
# Clone repository
git clone <repository-url>
cd data-pipeline-platform

# Install package
pip install -e .

# Install development dependencies
pip install -e ".[dev]"
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Usage

### Using the REST API

```python
import requests

# Submit a pipeline
response = requests.post("http://localhost:8000/pipelines", json={
    "pipeline_config": {
        "ingestion": {
            "query": "SELECT * FROM users WHERE created_at > :start_date",
            "source_db_url": "postgresql://user:pass@localhost:5432/source_db",
            "parameters": {"start_date": "2024-01-01"}
        },
        "transformation": {
            "type": "sql",
            "config": {
                "sql_query": "SELECT id, UPPER(name) as name_upper FROM input_data"
            }
        },
        "persistence": {
            "table_name": "users_staging",
            "strategy": "upsert",
            "upsert_keys": ["id"]
        }
    }
})

result = response.json()
pipeline_id = result["pipeline_id"]
task_id = result["task_id"]

# Check status
status = requests.get(f"http://localhost:8000/pipelines/{pipeline_id}/status")
print(status.json())
```

### Using the CLI

```bash
# Run complete pipeline
pipeline-cli run \
  --query "SELECT * FROM users" \
  --source-db-url "postgresql://user:pass@localhost:5432/source_db" \
  --transform-type sql \
  --transform-config '{"sql_query": "SELECT id, UPPER(name) FROM input_data"}' \
  --table-name users_staging \
  --strategy insert \
  --wait

# Or use config file
pipeline-cli run --config pipeline_config.json --wait

# Check task status
pipeline-cli status <task_id>

# Health check
pipeline-cli health

# List workers
pipeline-cli workers
```

### Using Python API

```python
from pipeline.ingestion import BatchIngestor
from pipeline.transformation import SQLTransformer
from pipeline.persistence import BatchWriter, WriteStrategy

# Ingest
ingestor = BatchIngestor(database_url="postgresql://...")
df = ingestor.ingest("SELECT * FROM users")

# Transform
transformer = SQLTransformer(database_url="sqlite:///:memory:")
transformed = transformer.transform(
    df,
    "SELECT id, UPPER(name) as name_upper FROM input_data"
)

# Persist
writer = BatchWriter(database_url="postgresql://...")
rows_written = writer.write(
    transformed,
    "users_staging",
    strategy=WriteStrategy.UPSERT,
    upsert_keys=["id"]
)
```

### Using Celery Tasks

```python
from pipeline.workers.tasks import run_pipeline

# Submit pipeline
result = run_pipeline.delay(
    pipeline_config={
        "ingestion": {"query": "SELECT * FROM users", ...},
        "transformation": {"type": "sql", "config": {...}},
        "persistence": {"table_name": "users_staging", ...}
    },
    pipeline_id="pipeline_001"
)

# Wait for result
pipeline_result = result.get(timeout=3600)
print(f"Processed {pipeline_result['ingestion']['row_count']} rows")
```

## Components

### Ingestion Layer

**ConnectionManager**: Manages database connection pools
- Connection pooling with SQLAlchemy
- Health checks and automatic reconnection
- Configurable pool sizes

**BatchIngestor**: Batch data ingestion
- Chunked reading for large datasets
- Support for multiple database types
- Query parameter support
- Streaming and batch modes

### Transformation Layer

**SQLTransformer**: SQL-based transformations
- Execute SQL queries on data
- Parameterized queries
- Version tracking
- In-memory option for small datasets

**CodeTransformer**: Python code transformations
- Function-based transformations
- Class-based transformations
- Module loading
- Version tracking

**ConfigTransformer**: Declarative transformations
- YAML/JSON configuration
- Field mappings, filters, aggregations
- Version tracking
- Config migration

**SchemaValidator**: Schema validation and evolution
- Schema validation
- Type conversion
- Schema evolution (additive changes)
- Schema registry

### Persistence Layer

**BatchWriter**: Efficient PostgreSQL writes
- Multiple write strategies (INSERT, UPSERT, REPLACE, APPEND)
- PostgreSQL COPY for performance
- Transaction management
- Retry logic

### Worker Layer

**Celery Tasks**: Asynchronous task execution
- Ingest, transform, persist tasks
- Pipeline orchestration
- Task chaining
- Result tracking

**Worker Monitor**: Health and statistics
- Task tracking
- Health checks
- Performance metrics

### Utilities

**Retry Logic**: Exponential backoff retry
- Configurable max attempts
- Exception-specific retry
- Async support

**Idempotency**: Redis-based deduplication
- SHA-256 key generation
- Atomic check-and-set
- TTL-based expiration

**Structured Logging**: Correlation tracking
- Correlation IDs
- Pipeline stage tracking
- Multiple output formats (JSON, text, console)

## Configuration

Configuration is managed via environment variables. See `.env.example` for all options.

### Key Configuration

```bash
# Database
SOURCE_DB_URL=postgresql://user:pass@localhost:5432/source_db
DEST_DB_URL=postgresql://user:pass@localhost:5432/dest_db

# Redis
REDIS_URL=redis://localhost:6379/0

# Celery
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Pipeline
BATCH_SIZE=10000
WORKER_CONCURRENCY=4

# Retry
MAX_RETRIES=3
RETRY_BACKOFF_BASE=2.0

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

See [Configuration Documentation](docs/configuration.md) for complete details.

## API Documentation

The REST API provides endpoints for:

- **Pipeline Management**: Create and monitor pipelines
- **Task Management**: Submit and track individual tasks
- **Worker Management**: Monitor worker health and statistics
- **Health Checks**: System health monitoring

### Interactive API Docs

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

See [API Documentation](API_CLI.md) for complete API reference.

## CLI Documentation

The CLI provides commands for:

- **Pipeline Execution**: Run complete pipelines
- **Task Execution**: Run individual tasks (ingest, transform, persist)
- **Status Checking**: Check task and pipeline status
- **Health Monitoring**: System health checks
- **Worker Management**: List and monitor workers

See [CLI Documentation](API_CLI.md) for complete CLI reference.

## Testing

### Run Tests

```bash
# All tests
pytest

# Unit tests only
pytest -m unit

# Integration tests
pytest -m integration

# With coverage
pytest --cov=pipeline --cov-report=html
```

### Test Structure

- **Unit Tests**: `tests/unit/` - Fast, isolated component tests
- **Integration Tests**: `tests/integration/` - End-to-end pipeline tests
- **Fixtures**: `tests/conftest.py` - Shared test fixtures

See [Test Documentation](tests/README.md) for details.

## Docker

### Quick Start

```bash
# Start all services
docker-compose up -d

# Start with API
docker-compose --profile api up -d

# Start with monitoring
docker-compose --profile monitoring up -d

# Scale workers
docker-compose up -d --scale worker=3
```

### Services

- **PostgreSQL**: Data persistence and schema registry
- **Redis**: Task queue and idempotency storage
- **Workers**: Celery worker instances
- **API** (optional): REST API service
- **Flower** (optional): Celery monitoring

See [Docker Documentation](DOCKER.md) for complete Docker guide.

## Project Structure

```
data-pipeline-platform/
├── src/
│   ├── pipeline/
│   │   ├── ingestion/          # Batch ingestion
│   │   ├── transformation/     # Data transformation
│   │   ├── persistence/        # Data persistence
│   │   ├── workers/            # Celery workers
│   │   ├── utils/              # Utilities (retry, idempotency, logging)
│   │   └── config/             # Configuration
│   ├── api/                    # REST API
│   └── cli/                    # CLI
├── tests/                      # Test suite
├── docker/                     # Docker configuration
├── examples/                   # Usage examples
├── requirements.txt            # Dependencies
├── setup.py                    # Package setup
├── docker-compose.yml          # Docker Compose
├── Dockerfile                  # Docker image
└── README.md                   # This file
```

## Design Principles

### 1. Modularity
Each component is independently testable and replaceable.

### 2. Scalability
- Horizontal scaling with multiple workers
- Batch processing for large datasets
- Connection pooling for efficiency

### 3. Reliability
- Automatic retry with exponential backoff
- Idempotency to prevent duplicates
- Transaction management for data integrity

### 4. Observability
- Structured logging with correlation IDs
- Health checks and monitoring
- Task tracking and statistics

### 5. Flexibility
- Multiple transformation types
- Configurable write strategies
- Schema evolution support

## Examples

### Example 1: Simple Pipeline

```python
from pipeline.workers.tasks import run_pipeline

config = {
    "ingestion": {
        "query": "SELECT * FROM users",
        "source_db_url": "postgresql://..."
    },
    "transformation": {
        "type": "sql",
        "config": {"sql_query": "SELECT id, UPPER(name) FROM input_data"}
    },
    "persistence": {
        "table_name": "users_staging",
        "strategy": "insert"
    }
}

result = run_pipeline.delay(config, pipeline_id="example_001")
print(result.get())
```

### Example 2: Code Transformation

```python
from pipeline.transformation import CodeTransformer

def clean_data(df):
    df['name'] = df['name'].str.strip()
    df['email'] = df['email'].str.lower()
    return df

transformer = CodeTransformer()
result = transformer.transform(data, clean_data)
```

### Example 3: Config Transformation

```yaml
# transform_config.yaml
select:
  - id
  - name
  - email
rename:
  name: full_name
filter:
  status: active
add_columns:
  created_year: "EXTRACT(YEAR FROM created_at)"
```

```python
from pipeline.transformation import ConfigTransformer

transformer = ConfigTransformer(config="transform_config.yaml")
result = transformer.transform(data)
```

## Performance Considerations

### Batch Sizing
- Default: 10,000 rows per batch
- Adjust based on memory and network capacity
- Larger batches = fewer round trips but more memory

### Connection Pooling
- Default pool size: 5 connections
- Max overflow: 10 connections
- Tune based on concurrent workload

### Worker Scaling
- Default concurrency: 4 tasks per worker
- Scale horizontally: `docker-compose up --scale worker=5`
- Monitor worker utilization

### Write Performance
- Use COPY for large inserts (default enabled)
- UPSERT is slower than INSERT
- Batch writes are more efficient than row-by-row

## Monitoring

### Health Checks

```bash
# API health
curl http://localhost:8000/health

# Worker health
pipeline-cli health

# Worker statistics
pipeline-cli workers
```

### Logging

Logs include:
- Correlation IDs for request tracking
- Pipeline IDs for execution tracking
- Task IDs for task tracking
- Structured JSON format (configurable)

### Flower (Celery Monitoring)

Access at http://localhost:5555 when enabled:

```bash
docker-compose --profile monitoring up -d
```

## Troubleshooting

### Common Issues

**Workers not processing tasks:**
- Check Redis connection
- Verify worker logs: `docker-compose logs worker`
- Check Celery status: `pipeline-cli workers`

**Database connection errors:**
- Verify connection strings in `.env`
- Check database is accessible
- Test connection: `pipeline-cli health`

**Memory issues:**
- Reduce `BATCH_SIZE`
- Reduce `WORKER_CONCURRENCY`
- Scale workers horizontally

See [Troubleshooting Guide](docs/troubleshooting.md) for more.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black src/ tests/

# Lint code
flake8 src/ tests/
```

## License

See [LICENSE](LICENSE) file for details.

## Additional Documentation

- [API & CLI Documentation](API_CLI.md)
- [Docker Guide](DOCKER.md)
- [Test Documentation](tests/README.md)
- [Worker Documentation](src/pipeline/workers/README.md)

## Support

For issues and questions:
- Open an issue on GitHub
- Check existing documentation
- Review example code in `examples/`

---

**Built with ❤️ for reliable data processing**
