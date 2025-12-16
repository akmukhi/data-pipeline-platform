"""REST API for pipeline management using FastAPI."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from pipeline.config.settings import settings
from pipeline.utils.logging import get_logger, setup_logging
from pipeline.workers.celery_app import celery_app
from pipeline.workers.tasks import (
    health_check_task,
    ingest_task,
    persist_task,
    run_pipeline,
    transform_task,
)

# Setup logging
setup_logging(level=settings.LOG_LEVEL, format_type=settings.LOG_FORMAT)
logger = get_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Data Pipeline Platform API",
    description="REST API for managing data pipelines",
    version="0.1.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models for request/response
class PipelineConfig(BaseModel):
    """Pipeline configuration model."""

    ingestion: Dict[str, Any] = Field(..., description="Ingestion configuration")
    transformation: Dict[str, Any] = Field(..., description="Transformation configuration")
    persistence: Dict[str, Any] = Field(..., description="Persistence configuration")


class PipelineRequest(BaseModel):
    """Request model for running a pipeline."""

    pipeline_config: PipelineConfig = Field(..., description="Complete pipeline configuration")
    pipeline_id: Optional[str] = Field(None, description="Optional pipeline identifier")


class TaskRequest(BaseModel):
    """Request model for running individual tasks."""

    query: Optional[str] = Field(None, description="SQL query for ingestion")
    source_db_url: Optional[str] = Field(None, description="Source database URL")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Query parameters")
    batch_size: Optional[int] = Field(None, description="Batch size")


class TransformRequest(BaseModel):
    """Request model for transformation."""

    transformation_type: str = Field(..., description="Type: sql, code, or config")
    transformation_config: Dict[str, Any] = Field(..., description="Transformation configuration")
    data_id: Optional[str] = Field(None, description="Data identifier from previous task")


class PersistRequest(BaseModel):
    """Request model for persistence."""

    table_name: str = Field(..., description="Target table name")
    schema: Optional[str] = Field(None, description="Database schema")
    strategy: str = Field("insert", description="Write strategy: insert, upsert, replace, append")
    upsert_keys: Optional[List[str]] = Field(None, description="Keys for upsert strategy")
    data_id: Optional[str] = Field(None, description="Data identifier from previous task")


class PipelineResponse(BaseModel):
    """Response model for pipeline execution."""

    status: str
    pipeline_id: str
    task_id: Optional[str] = None
    message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class TaskStatusResponse(BaseModel):
    """Response model for task status."""

    task_id: str
    status: str
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    timestamp: str
    version: str
    services: Dict[str, str]


# API Routes


@app.get("/", tags=["General"])
async def root():
    """Root endpoint."""
    return {
        "name": "Data Pipeline Platform API",
        "version": "0.1.0",
        "status": "running",
    }


@app.get("/health", response_model=HealthResponse, tags=["General"])
async def health_check():
    """
    Health check endpoint.

    Checks the health of the API and dependent services.
    """
    try:
        # Check Celery workers
        inspect = celery_app.control.inspect()
        stats = inspect.stats()
        worker_status = "healthy" if stats else "unhealthy"

        # Check Redis
        try:
            from pipeline.utils.idempotency import get_idempotency_manager

            manager = get_idempotency_manager()
            redis_status = "healthy" if manager.test_connection() else "unhealthy"
        except Exception:
            redis_status = "unknown"

        overall_status = "healthy" if worker_status == "healthy" else "degraded"

        return HealthResponse(
            status=overall_status,
            timestamp=datetime.utcnow().isoformat(),
            version="0.1.0",
            services={
                "api": "healthy",
                "workers": worker_status,
                "redis": redis_status,
            },
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Health check failed: {str(e)}",
        )


@app.post("/pipelines", response_model=PipelineResponse, tags=["Pipelines"])
async def create_pipeline(request: PipelineRequest):
    """
    Create and run a new pipeline.

    This endpoint orchestrates the complete pipeline: ingest → transform → persist.
    """
    try:
        pipeline_id = request.pipeline_id or f"pipeline_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

        logger.info(f"Creating pipeline: {pipeline_id}")

        # Submit pipeline task
        task = run_pipeline.delay(
            pipeline_config=request.pipeline_config.dict(),
            pipeline_id=pipeline_id,
        )

        return PipelineResponse(
            status="submitted",
            pipeline_id=pipeline_id,
            task_id=task.id,
            message="Pipeline task submitted successfully",
        )
    except Exception as e:
        logger.error(f"Failed to create pipeline: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create pipeline: {str(e)}",
        )


@app.get("/pipelines/{pipeline_id}/status", response_model=TaskStatusResponse, tags=["Pipelines"])
async def get_pipeline_status(pipeline_id: str):
    """
    Get the status of a pipeline execution.

    Args:
        pipeline_id: Pipeline identifier

    Returns:
        Task status and result if available
    """
    try:
        # Try to find task by pipeline_id
        # In production, you might want to store task_id -> pipeline_id mapping
        inspect = celery_app.control.inspect()
        active = inspect.active()
        scheduled = inspect.scheduled()
        reserved = inspect.reserved()

        # Search for tasks (simplified - in production use a task registry)
        task_id = None
        for worker, tasks in (active or {}).items():
            for task in tasks:
                if pipeline_id in str(task.get("kwargs", {})):
                    task_id = task["id"]
                    break

        if not task_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline {pipeline_id} not found",
            )

        # Get task result
        from celery.result import AsyncResult

        result = AsyncResult(task_id, app=celery_app)

        if result.ready():
            if result.successful():
                return TaskStatusResponse(
                    task_id=task_id,
                    status="completed",
                    result=result.result,
                )
            else:
                return TaskStatusResponse(
                    task_id=task_id,
                    status="failed",
                    error=str(result.result),
                )
        else:
            return TaskStatusResponse(
                task_id=task_id,
                status="pending",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get pipeline status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get pipeline status: {str(e)}",
        )


@app.post("/tasks/ingest", response_model=PipelineResponse, tags=["Tasks"])
async def create_ingest_task(request: TaskRequest):
    """
    Create an ingestion task.

    Ingests data from a source database.
    """
    try:
        if not request.query:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Query is required for ingestion",
            )

        task = ingest_task.delay(
            query=request.query,
            source_db_url=request.source_db_url,
            parameters=request.parameters,
            batch_size=request.batch_size,
        )

        return PipelineResponse(
            status="submitted",
            pipeline_id="ingest_task",
            task_id=task.id,
            message="Ingestion task submitted successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ingest task: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create ingest task: {str(e)}",
        )


@app.post("/tasks/transform", response_model=PipelineResponse, tags=["Tasks"])
async def create_transform_task(request: TransformRequest):
    """
    Create a transformation task.

    Transforms data using SQL, code, or config-based transformations.
    """
    try:
        task = transform_task.delay(
            data_id=request.data_id or "unknown",
            transformation_type=request.transformation_type,
            transformation_config=request.transformation_config,
        )

        return PipelineResponse(
            status="submitted",
            pipeline_id="transform_task",
            task_id=task.id,
            message="Transformation task submitted successfully",
        )
    except Exception as e:
        logger.error(f"Failed to create transform task: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create transform task: {str(e)}",
        )


@app.post("/tasks/persist", response_model=PipelineResponse, tags=["Tasks"])
async def create_persist_task(request: PersistRequest):
    """
    Create a persistence task.

    Persists data to the destination database.
    """
    try:
        task = persist_task.delay(
            data_id=request.data_id or "unknown",
            table_name=request.table_name,
            schema=request.schema,
            strategy=request.strategy,
            upsert_keys=request.upsert_keys,
        )

        return PipelineResponse(
            status="submitted",
            pipeline_id="persist_task",
            task_id=task.id,
            message="Persistence task submitted successfully",
        )
    except Exception as e:
        logger.error(f"Failed to create persist task: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create persist task: {str(e)}",
        )


@app.get("/tasks/{task_id}", response_model=TaskStatusResponse, tags=["Tasks"])
async def get_task_status(task_id: str):
    """
    Get the status of a task.

    Args:
        task_id: Task identifier

    Returns:
        Task status and result if available
    """
    try:
        from celery.result import AsyncResult

        result = AsyncResult(task_id, app=celery_app)

        if result.ready():
            if result.successful():
                return TaskStatusResponse(
                    task_id=task_id,
                    status="completed",
                    result=result.result,
                )
            else:
                return TaskStatusResponse(
                    task_id=task_id,
                    status="failed",
                    error=str(result.result),
                )
        else:
            return TaskStatusResponse(
                task_id=task_id,
                status="pending",
            )
    except Exception as e:
        logger.error(f"Failed to get task status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get task status: {str(e)}",
        )


@app.get("/workers", tags=["Workers"])
async def get_workers():
    """
    Get information about Celery workers.

    Returns:
        List of active workers and their statistics
    """
    try:
        inspect = celery_app.control.inspect()
        stats = inspect.stats()
        active = inspect.active()
        scheduled = inspect.scheduled()
        reserved = inspect.reserved()

        workers = []
        for worker_name in (stats or {}).keys():
            workers.append(
                {
                    "name": worker_name,
                    "stats": stats.get(worker_name, {}),
                    "active_tasks": len(active.get(worker_name, [])),
                    "scheduled_tasks": len(scheduled.get(worker_name, [])),
                    "reserved_tasks": len(reserved.get(worker_name, [])),
                }
            )

        return {"workers": workers, "total": len(workers)}
    except Exception as e:
        logger.error(f"Failed to get workers: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get workers: {str(e)}",
        )


@app.get("/workers/health", tags=["Workers"])
async def get_workers_health():
    """
    Get health status of workers.

    Returns:
        Health check result from workers
    """
    try:
        task = health_check_task.delay()
        result = task.get(timeout=10)

        return {"status": "healthy", "result": result}
    except Exception as e:
        logger.error(f"Worker health check failed: {e}", exc_info=True)
        return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    import os
    import uvicorn

    api_host = os.getenv("API_HOST", "0.0.0.0")
    api_port = int(os.getenv("API_PORT", 8000))
    api_reload = os.getenv("API_RELOAD", "false").lower() == "true"

    uvicorn.run(
        "pipeline.api.main:app",
        host=api_host,
        port=api_port,
        reload=api_reload,
    )

