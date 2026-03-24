"""Tests for Tier1Merger."""

from datetime import UTC, datetime

import pytest

from flavor_pipeline.consolidation.merger import Tier1Merger, _normalize_value
from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.tier1 import IngestMetadata, SourceMetadata, Tier1Molecule


class TestNormalizeValue:
    """Tests for the _normalize_value helper function."""

    def test_normalize_string_lowercase(self):
        """Test that strings are lowercased."""
        assert _normalize_value("HELLO") == "hello"

    def test_normalize_string_strip(self):
        """Test that strings are stripped."""
        assert _normalize_value("  hello  ") == "hello"

    def test_normalize_float_rounds(self):
        """Test that floats are rounded to 2 decimal places."""
        assert _normalize_value(152.1567) == 152.16
        assert _normalize_value(152.1) == 152.1

    def test_normalize_list_sorts(self):
        """Test that lists are sorted and converted to tuples."""
        assert _normalize_value(["c", "a", "b"]) == ("a", "b", "c")

    def test_normalize_none(self):
        """Test that None is passed through."""
        assert _normalize_value(None) is None

    def test_normalize_int(self):
        """Test that ints are passed through."""
        assert _normalize_value(123) == 123


class TestTier1Merger:
    """Tests for the Tier1Merger class."""

    @pytest.fixture
    def merger(self):
        """Create a Tier1Merger instance."""
        return Tier1Merger(pipeline_version="1.0.0")

    @pytest.fixture
    def source_meta_vcf(self):
        """Create VCF source metadata."""
        return SourceMetadata(
            name="vcf",
            version="1.0.0",
            url="https://vcf.example.com",
            retrieved_at=datetime.now(UTC),
            parser_version="1.0.0",
        )

    @pytest.fixture
    def source_meta_fenaroli(self):
        """Create Fenaroli source metadata."""
        return SourceMetadata(
            name="fenaroli",
            version="1.0.0",
            url="https://fenaroli.example.com",
            retrieved_at=datetime.now(UTC),
            parser_version="1.0.0",
        )

    @pytest.fixture
    def ingest_meta(self):
        """Create ingest metadata."""
        return IngestMetadata(pipeline_version="1.0.0")

    def test_merge_single_molecule(self, merger, source_meta_vcf, ingest_meta):
        """Test merging a single molecule (no actual merge)."""
        mol = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"vcf": source_meta_vcf},
            name=AttributedValue(value="vanillin", sources=["vcf"]),
        )

        result = merger.merge_all([mol])

        assert len(result) == 1
        assert result[0].molecule_id == "cas:121-33-5"
        assert result[0].merge_metadata.source_count == 1
        assert result[0].merge_metadata.conflict_count == 0
        assert result[0].name[0].value == "vanillin"

    def test_merge_identical_values(
        self, merger, source_meta_vcf, source_meta_fenaroli, ingest_meta
    ):
        """Test merging molecules with identical values."""
        mol1 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"vcf": source_meta_vcf},
            cas=AttributedValue(value="121-33-5", sources=["vcf"]),
        )
        mol2 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"fenaroli": source_meta_fenaroli},
            cas=AttributedValue(value="121-33-5", sources=["fenaroli"]),
        )

        result = merger.merge_all([mol1, mol2])

        assert len(result) == 1
        assert result[0].merge_metadata.source_count == 2
        assert result[0].merge_metadata.conflict_count == 0
        # Should be one AVO with both sources
        assert len(result[0].cas) == 1
        assert set(result[0].cas[0].sources) == {"vcf", "fenaroli"}

    def test_merge_conflicting_names(
        self, merger, source_meta_vcf, source_meta_fenaroli, ingest_meta
    ):
        """Test merging molecules with conflicting names."""
        mol1 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"vcf": source_meta_vcf},
            name=AttributedValue(value="vanillin", sources=["vcf"]),
        )
        mol2 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"fenaroli": source_meta_fenaroli},
            name=AttributedValue(value="VANILLIN", sources=["fenaroli"]),
        )

        result = merger.merge_all([mol1, mol2])

        assert len(result) == 1
        # Names should be normalized and combined since vanillin == VANILLIN after normalization
        assert len(result[0].name) == 1
        assert result[0].merge_metadata.conflict_count == 0

    def test_merge_truly_different_values(
        self, merger, source_meta_vcf, source_meta_fenaroli, ingest_meta
    ):
        """Test merging molecules with truly different values."""
        mol1 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"vcf": source_meta_vcf},
            molecular_weight=AttributedValue(value=152.15, unit="g/mol", sources=["vcf"]),
        )
        mol2 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"fenaroli": source_meta_fenaroli},
            molecular_weight=AttributedValue(value=152.20, unit="g/mol", sources=["fenaroli"]),
        )

        result = merger.merge_all([mol1, mol2])

        assert len(result) == 1
        # Should have 2 distinct MW values (conflict)
        assert len(result[0].molecular_weight) == 2
        assert result[0].merge_metadata.conflict_count == 1

    def test_merge_list_fields_union(
        self, merger, source_meta_vcf, source_meta_fenaroli, ingest_meta
    ):
        """Test that list fields (synonyms, descriptors) are unioned."""
        mol1 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"vcf": source_meta_vcf},
            flavor_descriptors=AttributedValue(value=["sweet", "vanilla"], sources=["vcf"]),
        )
        mol2 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"fenaroli": source_meta_fenaroli},
            flavor_descriptors=AttributedValue(value=["vanilla", "creamy"], sources=["fenaroli"]),
        )

        result = merger.merge_all([mol1, mol2])

        assert len(result) == 1
        # Should have 3 unique descriptors (sweet, vanilla, creamy)
        # vanilla appears in both so should have combined sources
        descriptors = result[0].flavor_descriptors
        assert len(descriptors) == 3
        # List fields don't count as conflicts
        assert result[0].merge_metadata.conflict_count == 0

    def test_merge_different_molecule_ids(
        self, merger, source_meta_vcf, source_meta_fenaroli, ingest_meta
    ):
        """Test that different molecule_ids stay separate."""
        mol1 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"vcf": source_meta_vcf},
            name=AttributedValue(value="vanillin", sources=["vcf"]),
        )
        mol2 = Tier1Molecule(
            molecule_id="cas:100-52-7",
            _ingest_metadata=ingest_meta,
            _sources={"fenaroli": source_meta_fenaroli},
            name=AttributedValue(value="benzaldehyde", sources=["fenaroli"]),
        )

        result = merger.merge_all([mol1, mol2])

        assert len(result) == 2
        ids = {m.molecule_id for m in result}
        assert ids == {"cas:121-33-5", "cas:100-52-7"}

    def test_merge_extra_fields(self, merger, source_meta_vcf, source_meta_fenaroli, ingest_meta):
        """Test that extra fields are merged correctly."""
        mol1 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"vcf": source_meta_vcf},
            extra={
                "fema_number": AttributedValue(value="3107", sources=["vcf"]),
            },
        )
        mol2 = Tier1Molecule(
            molecule_id="cas:121-33-5",
            _ingest_metadata=ingest_meta,
            _sources={"fenaroli": source_meta_fenaroli},
            extra={
                "fema_number": AttributedValue(value="3107", sources=["fenaroli"]),
            },
        )

        result = merger.merge_all([mol1, mol2])

        assert len(result) == 1
        assert "fema_number" in result[0].extra
        assert len(result[0].extra["fema_number"]) == 1
        assert set(result[0].extra["fema_number"][0].sources) == {"vcf", "fenaroli"}
