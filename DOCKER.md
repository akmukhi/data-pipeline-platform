# Docker Setup Guide

This guide explains how to run the data pipeline platform using Docker and Docker Compose.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 4GB of available RAM
- 10GB of available disk space

## Quick Start

1. **Clone the repository and navigate to the project directory:**
   ```bash
   cd data-pipeline-platform
   ```

2. **Create a `.env` file** (optional, uses defaults if not provided):
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start all services:**
   ```bash
   docker-compose up -d
   ```

4. **Check service status:**
   ```bash
   docker-compose ps
   ```

5. **View logs:**
   ```bash
   docker-compose logs -f worker
   ```

## Services

### PostgreSQL (`postgres`)
- **Image**: `postgres:15-alpine`
- **Port**: 5432 (configurable via `POSTGRES_PORT`)
- **Database**: `pipeline_db` (configurable via `POSTGRES_DB`)
- **User**: `pipeline` (configurable via `POSTGRES_USER`)
- **Password**: `pipeline_password` (configurable via `POSTGRES_PASSWORD`)
- **Volumes**: Persistent data in `postgres_data` volume
- **Initialization**: SQL scripts in `docker/init-db/` are automatically executed

### Redis (`redis`)
- **Image**: `redis:7-alpine`
- **Port**: 6379 (configurable via `REDIS_PORT`)
- **Password**: `redis_password` (configurable via `REDIS_PASSWORD`)
- **Persistence**: AOF (Append Only File) enabled
- **Volumes**: Persistent data in `redis_data` volume

### Worker (`worker`)
- **Image**: Built from `Dockerfile`
- **Command**: Runs Celery workers
- **Scaling**: Use `docker-compose up --scale worker=3` to run multiple workers
- **Health Check**: Basic Python availability check
- **Dependencies**: Waits for Postgres and Redis to be healthy

### API (`api`) - Optional
- **Image**: Built from `Dockerfile`
- **Port**: 8000 (configurable via `API_PORT`)
- **Start**: `docker-compose --profile api up -d`
- **Access**: http://localhost:8000

### Flower (`flower`) - Optional
- **Image**: Built from `Dockerfile`
- **Port**: 5555 (configurable via `FLOWER_PORT`)
- **Start**: `docker-compose --profile monitoring up -d`
- **Access**: http://localhost:5555
- **Purpose**: Celery task monitoring and management

## Common Commands

### Start Services
```bash
# Start all services
docker-compose up -d

# Start with API
docker-compose --profile api up -d

# Start with monitoring (Flower)
docker-compose --profile monitoring up -d

# Start with both API and monitoring
docker-compose --profile api --profile monitoring up -d
```

### Stop Services
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: deletes data)
docker-compose down -v
```

### Scaling Workers
```bash
# Run 3 worker instances
docker-compose up -d --scale worker=3

# Run 5 worker instances
docker-compose up -d --scale worker=5
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f worker
docker-compose logs -f postgres
docker-compose logs -f redis

# Last 100 lines
docker-compose logs --tail=100 worker
```

### Execute Commands
```bash
# Run a command in worker container
docker-compose exec worker python -c "from pipeline.workers.tasks import health_check_task; print(health_check_task.delay().get())"

# Access PostgreSQL
docker-compose exec postgres psql -U pipeline -d pipeline_db

# Access Redis CLI
docker-compose exec redis redis-cli -a redis_password
```

### Rebuild Images
```bash
# Rebuild all images
docker-compose build

# Rebuild specific service
docker-compose build worker

# Rebuild without cache
docker-compose build --no-cache worker
```

## Environment Variables

Key environment variables (see `.env.example` for full list):

### Database
- `POSTGRES_USER`: PostgreSQL username (default: `pipeline`)
- `POSTGRES_PASSWORD`: PostgreSQL password (default: `pipeline_password`)
- `POSTGRES_DB`: PostgreSQL database name (default: `pipeline_db`)
- `POSTGRES_PORT`: PostgreSQL port (default: `5432`)

### Redis
- `REDIS_PASSWORD`: Redis password (default: `redis_password`)
- `REDIS_PORT`: Redis port (default: `6379`)

### Application
- `SOURCE_DB_URL`: Source database connection URL
- `DEST_DB_URL`: Destination database connection URL
- `REDIS_URL`: Redis connection URL
- `CELERY_BROKER_URL`: Celery broker URL
- `CELERY_RESULT_BACKEND`: Celery result backend URL
- `LOG_LEVEL`: Logging level (default: `INFO`)
- `LOG_FORMAT`: Log format - json, text, or console (default: `json`)
- `WORKER_CONCURRENCY`: Number of concurrent workers per instance (default: `4`)
- `BATCH_SIZE`: Batch size for processing (default: `10000`)

## Volumes

### Persistent Data
- `postgres_data`: PostgreSQL data directory
- `redis_data`: Redis data directory

### Development Mounts
- `./src:/app/src:ro`: Source code mounted read-only (for development)

## Networks

All services are connected to the `pipeline-network` bridge network, allowing them to communicate using service names:
- `postgres` (hostname for PostgreSQL)
- `redis` (hostname for Redis)
- `worker` (hostname for workers)
- `api` (hostname for API)

## Health Checks

All services include health checks:
- **Postgres**: Checks if database is ready
- **Redis**: Checks if Redis is responding
- **Worker**: Basic availability check

Services wait for dependencies to be healthy before starting.

## Troubleshooting

### Services won't start
```bash
# Check logs
docker-compose logs

# Check service status
docker-compose ps

# Restart services
docker-compose restart
```

### Database connection issues
```bash
# Verify Postgres is running
docker-compose exec postgres pg_isready -U pipeline

# Check connection from worker
docker-compose exec worker python -c "from pipeline.ingestion import ConnectionManager; cm = ConnectionManager('postgresql://pipeline:pipeline_password@postgres:5432/pipeline_db'); print(cm.health_check())"
```

### Redis connection issues
```bash
# Verify Redis is running
docker-compose exec redis redis-cli -a redis_password ping

# Check connection from worker
docker-compose exec worker python -c "import redis; r = redis.from_url('redis://:redis_password@redis:6379/0'); print(r.ping())"
```

### Worker not processing tasks
```bash
# Check worker logs
docker-compose logs worker

# Check Celery status
docker-compose exec worker celery -A pipeline.workers.celery_app inspect active

# Check registered tasks
docker-compose exec worker celery -A pipeline.workers.celery_app inspect registered
```

### Out of memory
If you encounter memory issues:
- Reduce `WORKER_CONCURRENCY` in `.env`
- Reduce number of worker instances
- Increase Docker memory limit

### Port conflicts
If ports are already in use:
- Change port mappings in `docker-compose.yml`
- Or stop conflicting services

## Production Considerations

1. **Security**:
   - Change all default passwords
   - Use secrets management (Docker secrets, Kubernetes secrets, etc.)
   - Don't expose database ports publicly
   - Use TLS for database connections

2. **Performance**:
   - Adjust `WORKER_CONCURRENCY` based on CPU cores
   - Scale workers horizontally
   - Configure PostgreSQL connection pooling
   - Use Redis persistence appropriately

3. **Monitoring**:
   - Enable Flower for task monitoring
   - Set up log aggregation
   - Monitor resource usage
   - Set up alerts

4. **Backup**:
   - Regular PostgreSQL backups
   - Redis persistence configuration
   - Volume backups

5. **High Availability**:
   - Use managed database services
   - Redis cluster for high availability
   - Multiple worker instances
   - Load balancer for API

## Development

For development, source code is mounted read-only. To make changes:
1. Edit files in `./src/`
2. Changes are reflected immediately (no rebuild needed)
3. Restart services to pick up changes: `docker-compose restart worker`

## Building Custom Images

```bash
# Build image
docker build -t data-pipeline-platform:custom .

# Tag for registry
docker tag data-pipeline-platform:custom registry.example.com/data-pipeline-platform:latest

# Push to registry
docker push registry.example.com/data-pipeline-platform:latest
```

## Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove images
docker-compose down --rmi all

# Remove everything including volumes
docker-compose down -v --rmi all
```

