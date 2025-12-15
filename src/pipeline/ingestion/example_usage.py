"""Example usage of the batch ingestion components.

This file demonstrates how to use ConnectionManager and BatchIngestor.
It's not part of the main package but serves as documentation.
"""

# Example 1: Basic connection manager usage
from pipeline.ingestion import BatchIngestor, ConnectionManager
from pipeline.config import settings

# Create connection manager
connection_manager = ConnectionManager.from_settings(
    database_url=settings.database.source_db_url
)

# Health check
if connection_manager.health_check():
    print("Database connection is healthy")

# Get pool status
pool_status = connection_manager.get_pool_status()
print(f"Pool status: {pool_status}")

# Example 2: Batch ingestion from a query
ingestor = BatchIngestor.from_settings(
    database_url=settings.database.source_db_url
)

# Ingest data in batches
for batch_df in ingestor.ingest_query(
    "SELECT * FROM users WHERE status = :status",
    parameters={"status": "active"},
    chunk_size=5000,
):
    print(f"Processing batch with {len(batch_df)} rows")
    # Process batch_df here

# Example 3: Ingest from a table
for batch_df in ingestor.ingest_table(
    table_name="orders",
    schema="public",
    where_clause="created_at > '2024-01-01'",
    order_by="id",
):
    print(f"Processing batch with {len(batch_df)} rows")
    # Process batch_df here

# Example 4: Offset-based pagination
query_template = """
    SELECT * FROM large_table
    WHERE status = :status AND id > :offset
    ORDER BY id
    LIMIT :limit
"""

for batch_df in ingestor.ingest_with_offset(
    query=query_template,
    offset_column="id",
    initial_offset=0,
    parameters={"status": "active"},
    chunk_size=10000,
):
    print(f"Processing batch with {len(batch_df)} rows")
    # Process batch_df here

# Example 5: Get row count without fetching all data
row_count = ingestor.get_row_count(
    "SELECT * FROM users WHERE status = :status",
    parameters={"status": "active"},
)
print(f"Total rows: {row_count}")

