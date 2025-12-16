# Docker Setup for Data Pipeline Platform

This directory contains Docker configuration files for running the data pipeline platform.

## Files

- `Dockerfile`: Multi-stage Docker image for the Python application
- `docker-compose.yml`: Complete stack with Postgres, Redis, and workers
- `init-db/`: PostgreSQL initialization scripts

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Environment variables configured (see `.env.example` in project root)

### Basic Usage

1. **Start all services:**
   ```bash
   docker-compose up -d
   ```

2. **Start with API service:**
   ```bash
   docker-compose --profile api up -d
   ```

3. **Start with monitoring (Flower):**
   ```bash
   docker-compose --profile monitoring up -d
   ```

4. **View logs:**
   ```bash
   docker-compose logs -f worker
   ```

5. **Stop services:**
   ```bash
   docker-compose down
   ```

### Services

#### Postgres
- **Port**: 5432 (configurable via `POSTGRES_PORT`)
- **Database**: `pipeline_db` (configurable)
- **User**: `pipeline` (configurable)
- **Password**: `pipeline_password` (configurable via `POSTGRES_PASSWORD`)

#### Redis
- **Port**: 6379 (configurable via `REDIS_PORT`)
- **Password**: `redis_password` (configurable via `REDIS_PASSWORD`)

#### Worker
- **Replicas**: 2 (configurable via `WORKER_REPLICAS`)
- **Concurrency**: 4 per worker (configurable via `WORKER_CONCURRENCY`)

#### API (Optional)
- **Port**: 8000 (configurable via `API_PORT`)
- Start with: `docker-compose --profile api up -d`

#### Flower (Optional)
- **Port**: 5555 (configurable via `FLOWER_PORT`)
- Start with: `docker-compose --profile monitoring up -d`

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Postgres
POSTGRES_USER=pipeline
POSTGRES_PASSWORD=pipeline_password
POSTGRES_DB=pipeline_db
POSTGRES_PORT=5432

# Redis
REDIS_PASSWORD=redis_password
REDIS_PORT=6379

# Application
SOURCE_DB_URL=postgresql://pipeline:pipeline_password@postgres:5432/pipeline_db
DEST_DB_URL=postgresql://pipeline:pipeline_password@postgres:5432/pipeline_db
REDIS_URL=redis://:redis_password@redis:6379/0

# Workers
WORKER_REPLICAS=2
WORKER_CONCURRENCY=4

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

### Scaling Workers

Scale worker instances:

```bash
docker-compose up -d --scale worker=4
```

### Health Checks

All services include health checks:

```bash
# Check service status
docker-compose ps

# Check health
docker-compose exec postgres pg_isready
docker-compose exec redis redis-cli ping
```

### Data Persistence

Data is persisted in Docker volumes:

- `postgres_data`: PostgreSQL data
- `redis_data`: Redis persistence (AOF)

To backup data:

```bash
# Backup Postgres
docker-compose exec postgres pg_dump -U pipeline pipeline_db > backup.sql

# Backup Redis
docker-compose exec redis redis-cli --rdb /data/dump.rdb
```

### Development

For development, source code is mounted as a volume (read-only) so changes are reflected immediately:

```yaml
volumes:
  - ./src:/app/src:ro
```

### Production Considerations

1. **Security:**
   - Change all default passwords
   - Use secrets management (Docker secrets, etc.)
   - Limit network exposure

2. **Performance:**
   - Adjust worker concurrency based on workload
   - Configure PostgreSQL connection pool sizes
   - Tune Redis memory settings

3. **Monitoring:**
   - Enable Flower for Celery monitoring
   - Add application metrics
   - Set up log aggregation

4. **Backup:**
   - Regular database backups
   - Redis persistence configuration
   - Volume backups

### Troubleshooting

**Worker not starting:**
```bash
docker-compose logs worker
```

**Database connection issues:**
```bash
docker-compose exec postgres psql -U pipeline -d pipeline_db
```

**Redis connection issues:**
```bash
docker-compose exec redis redis-cli -a redis_password ping
```

**Rebuild images:**
```bash
docker-compose build --no-cache
```

