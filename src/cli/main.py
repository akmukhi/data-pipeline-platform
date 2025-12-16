"""CLI for pipeline operations."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from pipeline.config.settings import settings
from pipeline.utils.logging import get_logger, setup_logging
from pipeline.workers.tasks import (
    health_check_task,
    ingest_task,
    persist_task,
    run_pipeline,
    transform_task,
)

# Setup logging
setup_logging(level=settings.LOG_LEVEL, format_type="console")
logger = get_logger(__name__)


def load_config_file(config_path: str) -> Dict[str, Any]:
    """Load configuration from JSON or YAML file."""
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    if path.suffix in [".json"]:
        with open(path, "r") as f:
            return json.load(f)
    elif path.suffix in [".yaml", ".yml"]:
        try:
            import yaml

            with open(path, "r") as f:
                return yaml.safe_load(f)
        except ImportError:
            raise ImportError("PyYAML is required for YAML config files. Install with: pip install pyyaml")
    else:
        raise ValueError(f"Unsupported config file format: {path.suffix}")


def run_pipeline_command(args: argparse.Namespace) -> int:
    """Run a complete pipeline."""
    try:
        # Load config
        if args.config:
            config = load_config_file(args.config)
        else:
            # Use command-line arguments
            config = {
                "ingestion": {
                    "query": args.query,
                    "source_db_url": args.source_db_url,
                    "parameters": json.loads(args.parameters) if args.parameters else None,
                    "batch_size": args.batch_size,
                },
                "transformation": {
                    "type": args.transform_type,
                    "config": json.loads(args.transform_config) if args.transform_config else {},
                },
                "persistence": {
                    "table_name": args.table_name,
                    "schema": args.schema,
                    "strategy": args.strategy,
                    "upsert_keys": args.upsert_keys.split(",") if args.upsert_keys else None,
                },
            }

        pipeline_id = args.pipeline_id or f"pipeline_cli_{Path(__file__).stem}"

        logger.info(f"Submitting pipeline: {pipeline_id}")

        # Submit pipeline
        result = run_pipeline.delay(pipeline_config=config, pipeline_id=pipeline_id)

        print(f"Pipeline submitted successfully!")
        print(f"Pipeline ID: {pipeline_id}")
        print(f"Task ID: {result.id}")

        if args.wait:
            print("Waiting for pipeline to complete...")
            try:
                pipeline_result = result.get(timeout=args.timeout or 3600)

                if pipeline_result.get("status") == "success":
                    print("\n✓ Pipeline completed successfully!")
                    print(f"  Ingestion: {pipeline_result.get('ingestion', {}).get('row_count', 0)} rows")
                    print(
                        f"  Transformation: {pipeline_result.get('transformation', {}).get('row_count', 0)} rows"
                    )
                    print(
                        f"  Persistence: {pipeline_result.get('persistence', {}).get('rows_written', 0)} rows written"
                    )
                    return 0
                else:
                    print(f"\n✗ Pipeline failed: {pipeline_result.get('error')}")
                    return 1
            except Exception as e:
                print(f"\n✗ Error waiting for pipeline: {e}")
                return 1
        else:
            print(f"\nUse 'pipeline-cli status {result.id}' to check status")
            return 0

    except Exception as e:
        logger.error(f"Failed to run pipeline: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        return 1


def ingest_command(args: argparse.Namespace) -> int:
    """Run ingestion task."""
    try:
        logger.info("Submitting ingestion task")

        result = ingest_task.delay(
            query=args.query,
            source_db_url=args.source_db_url,
            parameters=json.loads(args.parameters) if args.parameters else None,
            batch_size=args.batch_size,
        )

        print(f"Ingestion task submitted!")
        print(f"Task ID: {result.id}")

        if args.wait:
            print("Waiting for task to complete...")
            try:
                task_result = result.get(timeout=args.timeout or 3600)
                print(f"\n✓ Ingestion completed: {task_result.get('row_count', 0)} rows")
                return 0
            except Exception as e:
                print(f"\n✗ Error: {e}")
                return 1

        return 0
    except Exception as e:
        logger.error(f"Failed to run ingestion: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        return 1


def transform_command(args: argparse.Namespace) -> int:
    """Run transformation task."""
    try:
        transform_config = json.loads(args.config) if args.config else {}

        logger.info("Submitting transformation task")

        result = transform_task.delay(
            data_id=args.data_id or "unknown",
            transformation_type=args.type,
            transformation_config=transform_config,
        )

        print(f"Transformation task submitted!")
        print(f"Task ID: {result.id}")

        if args.wait:
            print("Waiting for task to complete...")
            try:
                task_result = result.get(timeout=args.timeout or 3600)
                print(f"\n✓ Transformation completed: {task_result.get('row_count', 0)} rows")
                return 0
            except Exception as e:
                print(f"\n✗ Error: {e}")
                return 1

        return 0
    except Exception as e:
        logger.error(f"Failed to run transformation: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        return 1


def persist_command(args: argparse.Namespace) -> int:
    """Run persistence task."""
    try:
        logger.info("Submitting persistence task")

        result = persist_task.delay(
            data_id=args.data_id or "unknown",
            table_name=args.table_name,
            schema=args.schema,
            strategy=args.strategy,
            upsert_keys=args.upsert_keys.split(",") if args.upsert_keys else None,
        )

        print(f"Persistence task submitted!")
        print(f"Task ID: {result.id}")

        if args.wait:
            print("Waiting for task to complete...")
            try:
                task_result = result.get(timeout=args.timeout or 3600)
                print(f"\n✓ Persistence completed: {task_result.get('rows_written', 0)} rows written")
                return 0
            except Exception as e:
                print(f"\n✗ Error: {e}")
                return 1

        return 0
    except Exception as e:
        logger.error(f"Failed to run persistence: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        return 1


def status_command(args: argparse.Namespace) -> int:
    """Check task status."""
    try:
        from celery.result import AsyncResult

        from pipeline.workers.celery_app import celery_app

        result = AsyncResult(args.task_id, app=celery_app)

        if result.ready():
            if result.successful():
                print(f"Status: ✓ Completed")
                print(f"Result: {json.dumps(result.result, indent=2, default=str)}")
                return 0
            else:
                print(f"Status: ✗ Failed")
                print(f"Error: {result.result}")
                return 1
        else:
            print(f"Status: ⏳ Pending")
            return 0
    except Exception as e:
        logger.error(f"Failed to get status: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        return 1


def health_command(args: argparse.Namespace) -> int:
    """Check system health."""
    try:
        print("Checking system health...")

        # Check workers
        task = health_check_task.delay()
        result = task.get(timeout=10)

        if result.get("status") == "healthy":
            print("✓ Workers: Healthy")
            stats = result.get("stats", {})
            print(f"  Tasks processed: {stats.get('tasks_processed', 0)}")
            print(f"  Success rate: {stats.get('success_rate', 0):.2%}")
        else:
            print("✗ Workers: Unhealthy")
            return 1

        return 0
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        print(f"✗ Health check failed: {e}", file=sys.stderr)
        return 1


def workers_command(args: argparse.Namespace) -> int:
    """List workers."""
    try:
        from pipeline.workers.celery_app import celery_app

        inspect = celery_app.control.inspect()
        stats = inspect.stats()
        active = inspect.active()

        if not stats:
            print("No workers found")
            return 1

        print(f"Found {len(stats)} worker(s):\n")

        for worker_name, worker_stats in stats.items():
            print(f"Worker: {worker_name}")
            print(f"  Active tasks: {len(active.get(worker_name, []))}")
            if "pool" in worker_stats:
                print(f"  Pool: {worker_stats['pool']}")
            print()

        return 0
    except Exception as e:
        logger.error(f"Failed to list workers: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        return 1


def create_parser() -> argparse.ArgumentParser:
    """Create CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="pipeline-cli",
        description="CLI for data pipeline platform operations",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Pipeline command
    pipeline_parser = subparsers.add_parser("run", help="Run a complete pipeline")
    pipeline_parser.add_argument(
        "--config",
        type=str,
        help="Path to pipeline configuration file (JSON or YAML)",
    )
    pipeline_parser.add_argument("--pipeline-id", type=str, help="Pipeline identifier")
    pipeline_parser.add_argument("--query", type=str, help="SQL query for ingestion")
    pipeline_parser.add_argument("--source-db-url", type=str, help="Source database URL")
    pipeline_parser.add_argument("--parameters", type=str, help="Query parameters (JSON)")
    pipeline_parser.add_argument("--batch-size", type=int, help="Batch size")
    pipeline_parser.add_argument("--transform-type", type=str, choices=["sql", "code", "config"], help="Transformation type")
    pipeline_parser.add_argument("--transform-config", type=str, help="Transformation config (JSON)")
    pipeline_parser.add_argument("--table-name", type=str, help="Target table name")
    pipeline_parser.add_argument("--schema", type=str, help="Database schema")
    pipeline_parser.add_argument("--strategy", type=str, choices=["insert", "upsert", "replace", "append"], default="insert", help="Write strategy")
    pipeline_parser.add_argument("--upsert-keys", type=str, help="Comma-separated upsert keys")
    pipeline_parser.add_argument("--wait", action="store_true", help="Wait for completion")
    pipeline_parser.add_argument("--timeout", type=int, help="Timeout in seconds")

    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Run ingestion task")
    ingest_parser.add_argument("--query", type=str, required=True, help="SQL query")
    ingest_parser.add_argument("--source-db-url", type=str, help="Source database URL")
    ingest_parser.add_argument("--parameters", type=str, help="Query parameters (JSON)")
    ingest_parser.add_argument("--batch-size", type=int, help="Batch size")
    ingest_parser.add_argument("--wait", action="store_true", help="Wait for completion")
    ingest_parser.add_argument("--timeout", type=int, help="Timeout in seconds")

    # Transform command
    transform_parser = subparsers.add_parser("transform", help="Run transformation task")
    transform_parser.add_argument("--type", type=str, required=True, choices=["sql", "code", "config"], help="Transformation type")
    transform_parser.add_argument("--config", type=str, required=True, help="Transformation config (JSON)")
    transform_parser.add_argument("--data-id", type=str, help="Data identifier")
    transform_parser.add_argument("--wait", action="store_true", help="Wait for completion")
    transform_parser.add_argument("--timeout", type=int, help="Timeout in seconds")

    # Persist command
    persist_parser = subparsers.add_parser("persist", help="Run persistence task")
    persist_parser.add_argument("--table-name", type=str, required=True, help="Target table name")
    persist_parser.add_argument("--schema", type=str, help="Database schema")
    persist_parser.add_argument("--strategy", type=str, choices=["insert", "upsert", "replace", "append"], default="insert", help="Write strategy")
    persist_parser.add_argument("--upsert-keys", type=str, help="Comma-separated upsert keys")
    persist_parser.add_argument("--data-id", type=str, help="Data identifier")
    persist_parser.add_argument("--wait", action="store_true", help="Wait for completion")
    persist_parser.add_argument("--timeout", type=int, help="Timeout in seconds")

    # Status command
    status_parser = subparsers.add_parser("status", help="Check task status")
    status_parser.add_argument("task_id", type=str, help="Task identifier")

    # Health command
    health_parser = subparsers.add_parser("health", help="Check system health")

    # Workers command
    workers_parser = subparsers.add_parser("workers", help="List workers")

    return parser


def main() -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    try:
        if args.command == "run":
            return run_pipeline_command(args)
        elif args.command == "ingest":
            return ingest_command(args)
        elif args.command == "transform":
            return transform_command(args)
        elif args.command == "persist":
            return persist_command(args)
        elif args.command == "status":
            return status_command(args)
        elif args.command == "health":
            return health_command(args)
        elif args.command == "workers":
            return workers_command(args)
        else:
            parser.print_help()
            return 1
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        return 130
    except Exception as e:
        logger.error(f"CLI error: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

