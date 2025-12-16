# Celery Workers and Tasks

This module implements Celery workers and task definitions for asynchronous pipeline processing.

## Architecture

The pipeline uses Celery for distributed task processing with the following components:

- **Tasks**: Individual pipeline stages (ingest, transform, persist)
- **Workers**: Celery workers that execute tasks
- **Queues**: Separate queues for different pipeline stages
- **Monitoring**: Health checks and task tracking

## Task Types

### 1. Ingest Task (`ingest_task`)
Reads data from source databases.

**Input:**
- SQL query
- Source database URL
- Optional query parameters
- Batch size

**Output:**
- Serialized DataFrame
- Row count and column information
- Data identifier for tracking

### 2. Transform Task (`transform_task`)
Transforms data using SQL, code, or config-based transformations.

**Input:**
- Data identifier or serialized DataFrame
- Transformation type (sql/code/config)
- Transformation configuration

**Output:**
- Transformed serialized DataFrame
- Row count and column information
- New data identifier

### 3. Persist Task (`persist_task`)
Writes data to destination database.

**Input:**
- Data identifier or serialized DataFrame
- Table name and schema
- Write strategy (insert/upsert/replace/append)
- Upsert keys (for upsert strategy)

**Output:**
- Number of rows written
- Table information

### 4. Run Pipeline (`run_pipeline`)
Orchestrates the complete pipeline: ingest → transform → persist.

**Input:**
- Complete pipeline configuration
- Pipeline identifier

**Output:**
- Combined results from all stages

## Running Workers

### Using the run script:

```bash
python -m pipeline.workers.run_worker
```

### Using Celery directly:

```bash
celery -A pipeline.workers.celery_app worker \
    --loglevel=info \
    --concurrency=4 \
    --queues=ingestion,transformation,persistence,pipeline
```

### Using Docker:

```bash
docker-compose up worker
```

## Configuration

Workers are configured via environment variables (see `.env.example`):

- `CELERY_BROKER_URL`: Redis broker URL
- `CELERY_RESULT_BACKEND`: Result backend URL
- `WORKER_CONCURRENCY`: Number of concurrent workers
- `WORKER_MAX_TASKS_PER_CHILD`: Max tasks before worker restart

## Task Queues

Tasks are routed to different queues:

- `ingestion`: Ingest tasks
- `transformation`: Transform tasks
- `persistence`: Persist tasks
- `pipeline`: Complete pipeline tasks

## Monitoring

Workers include built-in monitoring:

- Task success/failure tracking
- Health check endpoint
- Statistics collection

Run health check:

```python
from pipeline.workers.tasks import health_check_task
result = health_check_task.delay()
print(result.get())
```

## Example Usage

```python
from pipeline.workers.tasks import run_pipeline

pipeline_config = {
    "ingestion": {
        "query": "SELECT * FROM users",
        "source_db_url": "postgresql://...",
    },
    "transformation": {
        "type": "sql",
        "config": {
            "sql_query": "SELECT id, UPPER(name) FROM input_data",
        },
    },
    "persistence": {
        "table_name": "users_staging",
        "strategy": "insert",
    },
}

result = run_pipeline.delay(pipeline_config, pipeline_id="pipeline_001")
pipeline_result = result.get()
```

## Error Handling

Tasks include automatic retry with exponential backoff:

- Maximum retries: Configurable via `MAX_RETRIES`
- Retry delay: Exponential backoff based on `RETRY_BACKOFF_BASE`
- Failed tasks: Logged and tracked in worker statistics

## Task Chaining

Tasks can be chained together for complex workflows:

```python
from pipeline.workers.tasks import ingest_task, transform_task, persist_task

# Chain tasks
ingest_result = ingest_task.delay(query="SELECT * FROM users")
transform_result = transform_task.s(ingest_result.get())
persist_result = persist_task.s(transform_result.get())
```

For most use cases, use `run_pipeline` which handles chaining automatically.

