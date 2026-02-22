-- Agent metadata/control-plane tables in PostgreSQL.

DROP TABLE IF EXISTS agent_query_traces CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS mining_snapshots CASCADE;
DROP TABLE IF EXISTS agent_plan_sql_cache CASCADE;
DROP TABLE IF EXISTS agent_quality_reports CASCADE;
DROP TABLE IF EXISTS agent_ingestion_runs CASCADE;
DROP TABLE IF EXISTS agent_semantic_maps CASCADE;
DROP TABLE IF EXISTS agent_schema_metadata CASCADE;
DROP TABLE IF EXISTS agent_datasets CASCADE;

CREATE TABLE agent_datasets (
    dataset_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    source_type VARCHAR(64) NOT NULL,
    db_engine VARCHAR(64) NOT NULL,
    schema_name VARCHAR(256) NOT NULL,
    description TEXT,
    status VARCHAR(64) NOT NULL,
    source_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata_path TEXT,
    schema_hash VARCHAR(128),
    semantic_map_path TEXT,
    last_ingested_at TIMESTAMPTZ,
    row_count BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE agent_schema_metadata (
    dataset_id VARCHAR(64) PRIMARY KEY REFERENCES agent_datasets(dataset_id) ON DELETE CASCADE,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    schema_hash VARCHAR(128) NOT NULL,
    metadata JSONB NOT NULL
);

CREATE TABLE agent_semantic_maps (
    dataset_id VARCHAR(64) PRIMARY KEY REFERENCES agent_datasets(dataset_id) ON DELETE CASCADE,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    semantic_map JSONB NOT NULL
);

CREATE TABLE agent_ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    dataset_id VARCHAR(64) NOT NULL REFERENCES agent_datasets(dataset_id) ON DELETE CASCADE,
    run_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE agent_quality_reports (
    dataset_id VARCHAR(64) PRIMARY KEY REFERENCES agent_datasets(dataset_id) ON DELETE CASCADE,
    report_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE agent_plan_sql_cache (
    cache_key TEXT PRIMARY KEY,
    dataset_id VARCHAR(64),
    schema_hash VARCHAR(128),
    plan_key TEXT NOT NULL,
    sql_text TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE agent_query_traces (
    id BIGSERIAL PRIMARY KEY,
    trace_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agent_query_traces_created_at ON agent_query_traces(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_ingestion_runs_dataset_created ON agent_ingestion_runs(dataset_id, created_at DESC);
