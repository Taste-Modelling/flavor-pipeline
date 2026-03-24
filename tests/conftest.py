"""Shared pytest fixtures for flavor pipeline tests."""

from datetime import datetime
from pathlib import Path

import pytest

from flavor_pipeline.schemas import AttributedValue, IngestMetadata, SourceMetadata


@pytest.fixture
def tmp_raw_data_dir(tmp_path: Path) -> Path:
    """Create a temporary raw data directory."""
    raw_dir = tmp_path / "raw_data"
    raw_dir.mkdir()
    return raw_dir


@pytest.fixture
def sample_source_metadata() -> SourceMetadata:
    """Sample source metadata for testing."""
    return SourceMetadata(
        name="test_source",
        version="1.0.0",
        url="https://example.com",
        retrieved_at=datetime(2024, 1, 1, 12, 0, 0),
        parser_version="0.1.0",
    )


@pytest.fixture
def sample_ingest_metadata() -> IngestMetadata:
    """Sample ingest metadata for testing."""
    return IngestMetadata(
        ingested_at=datetime(2024, 1, 1, 12, 0, 0),
        pipeline_version="0.1.0",
    )


@pytest.fixture
def sample_avo() -> AttributedValue:
    """Sample AttributedValue for testing."""
    return AttributedValue(value="test_value", sources=["source1"])
