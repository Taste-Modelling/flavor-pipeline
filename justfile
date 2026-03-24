# Flavor Pipeline Justfile

# Default recipe
default:
    @just --list

# Install dependencies
install:
    pip install -e ".[dev]"

# Run all tests
test:
    pytest

# Run tests with coverage
test-cov:
    pytest --cov=flavor_pipeline --cov-report=term-missing

# Run unit tests only
test-unit:
    pytest tests/unit/

# Run integration tests only
test-integration:
    pytest tests/integration/

# Lint code
lint:
    ruff check src/ tests/

# Format code
fmt:
    ruff format src/ tests/

# Start Dagster dev server
dev:
    dagster dev

# Materialize a specific asset
materialize asset:
    dagster asset materialize -m flavor_pipeline.definitions --select {{asset}}

# Materialize all acquisition assets
materialize-acquisition:
    dagster asset materialize -m flavor_pipeline.definitions --select flavordb2_raw bitterdb_raw fenaroli_raw panten_raw vcf_raw fsbi_raw

# Materialize all tier1 assets
materialize-tier1:
    dagster asset materialize -m flavor_pipeline.definitions --select flavordb2_tier1 bitterdb_tier1 fenaroli_tier1 panten_tier1 vcf_tier1 fsbi_tier1 leffingwell_tier1

# Query consolidated data with DuckDB
query-consolidated:
    duckdb -c "SELECT * FROM 'data/tier1/consolidated.parquet' LIMIT 10"

# Clean raw data
clean-raw:
    rm -rf raw_data/

# Clean processed data
clean-data:
    rm -rf data/

# Clean all generated data
clean-all: clean-raw clean-data
