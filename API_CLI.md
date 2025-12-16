# API and CLI Documentation

This document describes the REST API and CLI for managing data pipelines.

## REST API

The REST API is built with FastAPI and provides endpoints for pipeline management.

### Starting the API

```bash
# Using Docker
docker-compose --profile api up -d

# Or directly
uvicorn pipeline.api.main:app --host 0.0.0.0 --port 8000
```

### API Endpoints

#### Health Check

```http
GET /health
```

Returns the health status of the API and dependent services.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00",
  "version": "0.1.0",
  "services": {
    "api": "healthy",
    "workers": "healthy",
    "redis": "healthy"
  }
}
```

#### Create Pipeline

```http
POST /pipelines
Content-Type: application/json

{
  "pipeline_config": {
    "ingestion": {
      "query": "SELECT * FROM users",
      "source_db_url": "postgresql://...",
      "parameters": {},
      "batch_size": 10000
    },
    "transformation": {
      "type": "sql",
      "config": {
        "sql_query": "SELECT id, UPPER(name) FROM input_data"
      }
    },
    "persistence": {
      "table_name": "users_staging",
      "schema": "public",
      "strategy": "insert",
      "upsert_keys": ["id"]
    }
  },
  "pipeline_id": "pipeline_001"
}
```

**Response:**
```json
{
  "status": "submitted",
  "pipeline_id": "pipeline_001",
  "task_id": "abc123...",
  "message": "Pipeline task submitted successfully"
}
```

#### Get Pipeline Status

```http
GET /pipelines/{pipeline_id}/status
```

**Response:**
```json
{
  "task_id": "abc123...",
  "status": "completed",
  "result": {
    "status": "success",
    "pipeline_id": "pipeline_001",
    "ingestion": {...},
    "transformation": {...},
    "persistence": {...}
  }
}
```

#### Run Individual Tasks

**Ingest:**
```http
POST /tasks/ingest
Content-Type: application/json

{
  "query": "SELECT * FROM users",
  "source_db_url": "postgresql://...",
  "parameters": {},
  "batch_size": 10000
}
```

**Transform:**
```http
POST /tasks/transform
Content-Type: application/json

{
  "transformation_type": "sql",
  "transformation_config": {
    "sql_query": "SELECT id, UPPER(name) FROM input_data"
  },
  "data_id": "data_abc123"
}
```

**Persist:**
```http
POST /tasks/persist
Content-Type: application/json

{
  "table_name": "users_staging",
  "schema": "public",
  "strategy": "insert",
  "upsert_keys": ["id"],
  "data_id": "data_abc123"
}
```

#### Get Task Status

```http
GET /tasks/{task_id}
```

#### List Workers

```http
GET /workers
```

#### Worker Health

```http
GET /workers/health
```

### API Documentation

FastAPI automatically generates interactive API documentation:

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## CLI

The CLI provides a command-line interface for pipeline operations.

### Installation

The CLI is installed as part of the package:

```bash
pip install -e .
```

Or use directly:

```bash
python -m cli.main <command>
```

### Commands

#### Run Pipeline

Run a complete pipeline (ingest → transform → persist):

```bash
# Using config file
pipeline-cli run --config pipeline_config.json

# Using command-line arguments
pipeline-cli run \
  --query "SELECT * FROM users" \
  --source-db-url "postgresql://..." \
  --transform-type sql \
  --transform-config '{"sql_query": "SELECT id, UPPER(name) FROM input_data"}' \
  --table-name users_staging \
  --strategy insert \
  --wait
```

**Config file format (JSON):**
```json
{
  "ingestion": {
    "query": "SELECT * FROM users",
    "source_db_url": "postgresql://...",
    "parameters": {"start_date": "2024-01-01"},
    "batch_size": 10000
  },
  "transformation": {
    "type": "sql",
    "config": {
      "sql_query": "SELECT id, UPPER(name) FROM input_data"
    }
  },
  "persistence": {
    "table_name": "users_staging",
    "schema": "public",
    "strategy": "insert"
  }
}
```

**Config file format (YAML):**
```yaml
ingestion:
  query: "SELECT * FROM users"
  source_db_url: "postgresql://..."
  parameters:
    start_date: "2024-01-01"
  batch_size: 10000

transformation:
  type: sql
  config:
    sql_query: "SELECT id, UPPER(name) FROM input_data"

persistence:
  table_name: users_staging
  schema: public
  strategy: insert
```

#### Run Individual Tasks

**Ingest:**
```bash
pipeline-cli ingest \
  --query "SELECT * FROM users" \
  --source-db-url "postgresql://..." \
  --wait
```

**Transform:**
```bash
pipeline-cli transform \
  --type sql \
  --config '{"sql_query": "SELECT id, UPPER(name) FROM input_data"}' \
  --wait
```

**Persist:**
```bash
pipeline-cli persist \
  --table-name users_staging \
  --schema public \
  --strategy insert \
  --wait
```

#### Check Status

```bash
pipeline-cli status <task_id>
```

#### Health Check

```bash
pipeline-cli health
```

#### List Workers

```bash
pipeline-cli workers
```

### CLI Options

Common options:
- `--wait`: Wait for task completion
- `--timeout <seconds>`: Timeout for waiting (default: 3600)
- `--pipeline-id <id>`: Custom pipeline identifier

### Examples

**Example 1: Run pipeline with config file**
```bash
pipeline-cli run --config examples/pipeline_config.json --wait
```

**Example 2: Run ingestion only**
```bash
pipeline-cli ingest \
  --query "SELECT * FROM users WHERE created_at > '2024-01-01'" \
  --source-db-url "postgresql://user:pass@localhost:5432/db" \
  --wait
```

**Example 3: Check task status**
```bash
pipeline-cli status abc123-def456-ghi789
```

**Example 4: Monitor system health**
```bash
pipeline-cli health
pipeline-cli workers
```

## Integration Examples

### Python Client

```python
import requests

# Submit pipeline
response = requests.post(
    "http://localhost:8000/pipelines",
    json={
        "pipeline_config": {
            "ingestion": {"query": "SELECT * FROM users"},
            "transformation": {"type": "sql", "config": {...}},
            "persistence": {"table_name": "users_staging"},
        }
    }
)
result = response.json()
task_id = result["task_id"]

# Check status
status_response = requests.get(f"http://localhost:8000/tasks/{task_id}")
status = status_response.json()
```

### Shell Script

```bash
#!/bin/bash

# Submit pipeline
RESPONSE=$(curl -X POST http://localhost:8000/pipelines \
  -H "Content-Type: application/json" \
  -d @pipeline_config.json)

TASK_ID=$(echo $RESPONSE | jq -r '.task_id')

# Wait for completion
while true; do
  STATUS=$(curl -s http://localhost:8000/tasks/$TASK_ID | jq -r '.status')
  if [ "$STATUS" = "completed" ]; then
    echo "Pipeline completed!"
    break
  elif [ "$STATUS" = "failed" ]; then
    echo "Pipeline failed!"
    exit 1
  fi
  sleep 2
done
```

## Error Handling

Both API and CLI handle errors gracefully:

- **API**: Returns appropriate HTTP status codes (400, 404, 500, etc.)
- **CLI**: Returns exit codes (0 for success, non-zero for failure)

Common errors:
- `400 Bad Request`: Invalid request parameters
- `404 Not Found`: Pipeline or task not found
- `500 Internal Server Error`: Server-side error
- `503 Service Unavailable`: Service dependencies unavailable

## Authentication

Currently, the API has no authentication. For production:

1. Add API key authentication
2. Use OAuth2/JWT tokens
3. Implement role-based access control
4. Use HTTPS/TLS

## Rate Limiting

Rate limiting is not currently implemented. For production, consider:

- Per-IP rate limiting
- Per-user rate limiting
- Queue-based throttling

## Monitoring

Monitor API and CLI usage:

- API: Use `/health` and `/workers` endpoints
- CLI: Check exit codes and logs
- Workers: Use Flower (http://localhost:5555)
- Logs: Structured logging with correlation IDs

