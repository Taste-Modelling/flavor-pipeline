"""Tests for Tier 2 molecule schema."""

from datetime import UTC, datetime

import pytest

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.tier1 import SourceMetadata
from flavor_pipeline.schemas.tier2 import MergeMetadata, Tier2Molecule


class TestMergeMetadata:
    """Tests for MergeMetadata model."""

    def test_merge_metadata_creation(self):
        """Test creating MergeMetadata with all fields."""
        now = datetime.now(UTC)
        meta = MergeMetadata(
            merged_at=now,
            pipeline_version="1.0.0",
            source_count=3,
            conflict_count=2,
        )

        assert meta.merged_at == now
        assert meta.pipeline_version == "1.0.0"
        assert meta.source_count == 3
        assert meta.conflict_count == 2

    def test_merge_metadata_auto_timestamp(self):
        """Test that merged_at defaults to current time."""
        meta = MergeMetadata(
            pipeline_version="1.0.0",
            source_count=1,
            conflict_count=0,
        )

        assert meta.merged_at is not None
        assert meta.merged_at.tzinfo == UTC


class TestTier2Molecule:
    """Tests for Tier2Molecule model."""

    @pytest.fixture
    def sample_source_meta(self):
        """Create sample source metadata."""
        return SourceMetadata(
            name="test_source",
            version="1.0.0",
            url="https://example.com",
            retrieved_at=datetime.now(UTC),
            parser_version="1.0.0",
        )

    @pytest.fixture
    def sample_merge_meta(self):
        """Create sample merge metadata."""
        return MergeMetadata(
            pipeline_version="1.0.0",
            source_count=2,
            conflict_count=1,
        )

    def test_tier2_minimal_molecule(self, sample_merge_meta, sample_source_meta):
        """Test creating a minimal Tier2Molecule."""
        mol = Tier2Molecule(
            molecule_id="cas:121-33-5",
            _merge_metadata=sample_merge_meta,
            _sources={"test": sample_source_meta},
        )

        assert mol.molecule_id == "cas:121-33-5"
        assert mol.merge_metadata.source_count == 2
        assert "test" in mol.sources

    def test_tier2_with_list_attributed_values(self, sample_merge_meta, sample_source_meta):
        """Test Tier2Molecule with list[AttributedValue] fields."""
        mol = Tier2Molecule(
            molecule_id="pubchem:1234",
            _merge_metadata=sample_merge_meta,
            _sources={"vcf": sample_source_meta, "fenaroli": sample_source_meta},
            name=[
                AttributedValue(value="vanillin", sources=["vcf"]),
                AttributedValue(value="VANILLIN", sources=["fenaroli"]),
            ],
            cas=[
                AttributedValue(value="121-33-5", sources=["vcf", "fenaroli"]),
            ],
        )

        assert len(mol.name) == 2
        assert mol.name[0].value == "vanillin"
        assert mol.name[1].value == "VANILLIN"
        assert len(mol.cas) == 1
        assert set(mol.cas[0].sources) == {"vcf", "fenaroli"}

    def test_tier2_with_extra_fields(self, sample_merge_meta, sample_source_meta):
        """Test Tier2Molecule with extra fields."""
        mol = Tier2Molecule(
            molecule_id="cas:123-45-6",
            _merge_metadata=sample_merge_meta,
            _sources={"test": sample_source_meta},
            extra={
                "fema_number": [
                    AttributedValue(value="1234", sources=["vcf"]),
                    AttributedValue(value="1235", sources=["fenaroli"]),
                ],
            },
        )

        assert "fema_number" in mol.extra
        assert len(mol.extra["fema_number"]) == 2

    def test_tier2_model_dump(self, sample_merge_meta, sample_source_meta):
        """Test that Tier2Molecule can be serialized."""
        mol = Tier2Molecule(
            molecule_id="cas:121-33-5",
            _merge_metadata=sample_merge_meta,
            _sources={"test": sample_source_meta},
            name=[AttributedValue(value="vanillin", sources=["test"])],
        )

        # Default dump uses field names (not aliases)
        data = mol.model_dump()
        assert data["molecule_id"] == "cas:121-33-5"
        assert "merge_metadata" in data
        assert "sources" in data
        assert data["name"][0]["value"] == "vanillin"

        # With by_alias=True uses aliases
        data_aliased = mol.model_dump(by_alias=True)
        assert "_merge_metadata" in data_aliased
        assert "_sources" in data_aliased

    def test_tier2_json_serialization(self, sample_merge_meta, sample_source_meta):
        """Test that Tier2Molecule can be serialized to JSON."""
        mol = Tier2Molecule(
            molecule_id="cas:121-33-5",
            _merge_metadata=sample_merge_meta,
            _sources={"test": sample_source_meta},
            molecular_weight=[AttributedValue(value=152.15, unit="g/mol", sources=["test"])],
        )

        data = mol.model_dump(mode="json", by_alias=True)

        assert data["molecule_id"] == "cas:121-33-5"
        assert isinstance(data["_merge_metadata"]["merged_at"], str)
        assert data["molecular_weight"][0]["value"] == 152.15
