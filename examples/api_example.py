"""Example demonstrating REST API usage."""

import requests
import json
import time

# API base URL
API_BASE_URL = "http://localhost:8000"


def run_pipeline_via_api():
    """Run a pipeline using the REST API."""
    url = f"{API_BASE_URL}/pipelines"

    pipeline_config = {
        "ingestion": {
            "query": "SELECT id, name, email, created_at FROM users WHERE created_at > :start_date",
            "source_db_url": "postgresql://user:password@localhost:5432/source_db",
            "parameters": {"start_date": "2024-01-01"},
            "batch_size": 10000,
        },
        "transformation": {
            "type": "sql",
            "config": {
                "sql_query": "SELECT id, UPPER(name) as name_upper, email, EXTRACT(YEAR FROM created_at) as created_year FROM input_data",
            },
        },
        "persistence": {
            "table_name": "users_staging",
            "schema": "public",
            "strategy": "upsert",
            "upsert_keys": ["id"],
        },
    }

    payload = {
        "pipeline_config": pipeline_config,
        "pipeline_id": "example_pipeline_api_001",
    }

    print("Submitting pipeline via API...")
    response = requests.post(url, json=payload)
    response.raise_for_status()

    result = response.json()
    print(f"Pipeline submitted: {result['pipeline_id']}")
    print(f"Task ID: {result['task_id']}")

    # Check status
    pipeline_id = result["pipeline_id"]
    status_url = f"{API_BASE_URL}/pipelines/{pipeline_id}/status"

    print("\nWaiting for pipeline to complete...")
    while True:
        status_response = requests.get(status_url)
        status_response.raise_for_status()
        status = status_response.json()

        if status["status"] == "completed":
            print("✓ Pipeline completed successfully!")
            print(json.dumps(status["result"], indent=2))
            break
        elif status["status"] == "failed":
            print(f"✗ Pipeline failed: {status.get('error')}")
            break
        else:
            print(f"Status: {status['status']}...")
            time.sleep(2)


def check_health():
    """Check API health."""
    url = f"{API_BASE_URL}/health"
    response = requests.get(url)
    response.raise_for_status()
    health = response.json()
    print("Health Status:")
    print(json.dumps(health, indent=2))


def list_workers():
    """List Celery workers."""
    url = f"{API_BASE_URL}/workers"
    response = requests.get(url)
    response.raise_for_status()
    workers = response.json()
    print("Workers:")
    print(json.dumps(workers, indent=2))


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "health":
            check_health()
        elif command == "workers":
            list_workers()
        elif command == "pipeline":
            run_pipeline_via_api()
        else:
            print("Usage: python api_example.py [health|workers|pipeline]")
    else:
        print("Checking health...")
        check_health()
        print("\nListing workers...")
        list_workers()

