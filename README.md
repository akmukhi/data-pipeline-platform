# Data Pipeline Platform

A scalable data pipeline platform designed for ETL operations with robust error handling, retry logic, and schema evolution support.

## Overview

**Ingest → Transform → Persist** data pipeline designed for scale with graceful failure handling.

### Features

- **Batch Ingestion**: Efficient batch processing from database sources
- **Background Workers**: Asynchronous task processing with Celery
- **Retry Logic**: Exponential backoff retry mechanism
- **Idempotency**: Prevent duplicate processing with Redis-based deduplication
- **Schema Evolution**: Automatic schema validation and migration support

### Tech Stack

- **Python 3.11+**
- **PostgreSQL**: Data persistence and schema registry
- **Redis**: Task queue and idempotency storage
- **Docker**: Containerized deployment

## Project Structure

```
data-pipeline-platform/
├── src/
│   ├── pipeline/          # Core pipeline components
│   │   ├── ingestion/     # Batch data ingestion
│   │   ├── transformation/ # Data transformations
│   │   ├── persistence/   # Data persistence
│   │   ├── workers/       # Background workers
│   │   ├── utils/         # Utilities (retry, idempotency, logging)
│   │   └── config/        # Configuration management
│   ├── api/               # REST API (optional)
│   └── cli/               # CLI interface
├── tests/                 # Test suite
├── docker/                # Docker configuration
├── requirements.txt       # Python dependencies
├── setup.py              # Package setup
└── .env.example          # Environment variables template
```

## Setup

### Prerequisites

- Python 3.11 or higher
- PostgreSQL
- Redis
- Docker (optional, for containerized deployment)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd data-pipeline-platform
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Install the package in development mode:
```bash
pip install -e .
```

## Configuration

Copy `.env.example` to `.env` and configure:

- Database connections (source and destination)
- Redis connection
- Celery worker settings
- Pipeline batch sizes
- Retry and idempotency settings

## Development

Run tests:
```bash
pytest
```

Format code:
```bash
black src tests
isort src tests
```

Type checking:
```bash
mypy src
```

## License

See LICENSE file for details.
