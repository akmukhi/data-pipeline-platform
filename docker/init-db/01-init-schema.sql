-- Initialize database schema for data pipeline platform

-- Create schema registry table if it doesn't exist
CREATE TABLE IF NOT EXISTS schema_versions (
    schema_name VARCHAR(255) PRIMARY KEY,
    version INTEGER NOT NULL,
    schema_json TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on schema_name for faster lookups
CREATE INDEX IF NOT EXISTS idx_schema_versions_name ON schema_versions(schema_name);

-- Create a function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to auto-update updated_at
DROP TRIGGER IF EXISTS update_schema_versions_updated_at ON schema_versions;
CREATE TRIGGER update_schema_versions_updated_at
    BEFORE UPDATE ON schema_versions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions (adjust as needed for your setup)
-- GRANT ALL PRIVILEGES ON schema_versions TO pipeline;

