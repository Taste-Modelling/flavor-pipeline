"""Tests for Tier1FoodMerger."""

from datetime import UTC, datetime

import pytest

from flavor_pipeline.food_consolidation.merger import Tier1FoodMerger, _normalize_value
from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.food import IngestMetadata, MoleculeComposition, Tier1Food
from flavor_pipeline.schemas.tier1 import SourceMetadata


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


class TestTier1FoodMerger:
    """Tests for the Tier1FoodMerger class."""

    @pytest.fixture
    def merger(self):
        """Create a Tier1FoodMerger instance."""
        return Tier1FoodMerger(pipeline_version="1.0.0")

    @pytest.fixture
    def source_meta_foodb(self):
        """Create FooDB source metadata."""
        return SourceMetadata(
            name="foodb_food",
            version="2020.04",
            url="https://foodb.ca",
            retrieved_at=datetime.now(UTC),
            parser_version="0.1.0",
        )

    @pytest.fixture
    def source_meta_usda(self):
        """Create USDA source metadata."""
        return SourceMetadata(
            name="usda_food",
            version="2025.12",
            url="https://fdc.nal.usda.gov",
            retrieved_at=datetime.now(UTC),
            parser_version="0.1.0",
        )

    @pytest.fixture
    def ingest_meta(self):
        """Create ingest metadata."""
        return IngestMetadata(pipeline_version="0.1.0")

    def test_merge_single_food(self, merger, source_meta_foodb, ingest_meta):
        """Test merging a single food (no actual merge)."""
        food = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"foodb_food": source_meta_foodb},
            name=AttributedValue(value="Apple", sources=["foodb_food"]),
        )

        result = merger.merge_all([food])

        assert len(result) == 1
        assert result[0].food_id == "foodb_food:FOOD00001"
        assert result[0].merge_metadata.source_count == 1
        assert result[0].merge_metadata.conflict_count == 0
        assert result[0].name is not None
        assert result[0].name[0].value == "Apple"

    def test_merge_identical_values(
        self, merger, source_meta_foodb, source_meta_usda, ingest_meta
    ):
        """Test merging foods with identical values."""
        food1 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"foodb_food": source_meta_foodb},
            category=AttributedValue(value="Fruits", sources=["foodb_food"]),
        )
        food2 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"usda_food": source_meta_usda},
            category=AttributedValue(value="Fruits", sources=["usda_food"]),
        )

        result = merger.merge_all([food1, food2])

        assert len(result) == 1
        assert result[0].merge_metadata.source_count == 2
        assert result[0].merge_metadata.conflict_count == 0
        # Should be one AVO with both sources
        assert result[0].category is not None
        assert len(result[0].category) == 1
        assert set(result[0].category[0].sources) == {"foodb_food", "usda_food"}

    def test_merge_conflicting_names(
        self, merger, source_meta_foodb, source_meta_usda, ingest_meta
    ):
        """Test merging foods with conflicting names."""
        food1 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"foodb_food": source_meta_foodb},
            name=AttributedValue(value="Apple, raw", sources=["foodb_food"]),
        )
        food2 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"usda_food": source_meta_usda},
            name=AttributedValue(value="Apples, raw, with skin", sources=["usda_food"]),
        )

        result = merger.merge_all([food1, food2])

        assert len(result) == 1
        # Names are different, so we have a conflict
        assert result[0].name is not None
        assert len(result[0].name) == 2
        assert result[0].merge_metadata.conflict_count == 1

    def test_merge_case_insensitive(
        self, merger, source_meta_foodb, source_meta_usda, ingest_meta
    ):
        """Test that values are merged case-insensitively."""
        food1 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"foodb_food": source_meta_foodb},
            name=AttributedValue(value="Apple", sources=["foodb_food"]),
        )
        food2 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"usda_food": source_meta_usda},
            name=AttributedValue(value="APPLE", sources=["usda_food"]),
        )

        result = merger.merge_all([food1, food2])

        assert len(result) == 1
        # Names should be normalized and combined since apple == APPLE
        assert result[0].name is not None
        assert len(result[0].name) == 1
        assert result[0].merge_metadata.conflict_count == 0

    def test_merge_composition(
        self, merger, source_meta_foodb, source_meta_usda, ingest_meta
    ):
        """Test merging molecular composition from multiple sources."""
        caffeine_id = "inchikey:RYYVLZVUVIJVGH-UHFFFAOYSA-N"
        sucrose_id = "inchikey:CZMRCDWAGMRECN-UGDNZRGBSA-N"

        food1 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"foodb_food": source_meta_foodb},
            name=AttributedValue(value="Coffee", sources=["foodb_food"]),
            composition=[
                MoleculeComposition(
                    molecule_id=caffeine_id,
                    concentration=AttributedValue(value=95.0, unit="mg/100g", sources=["foodb_food"]),
                ),
            ],
        )
        food2 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"usda_food": source_meta_usda},
            name=AttributedValue(value="Coffee", sources=["usda_food"]),
            composition=[
                MoleculeComposition(
                    molecule_id=caffeine_id,
                    concentration=AttributedValue(value=100.0, unit="mg/100g", sources=["usda_food"]),
                ),
                MoleculeComposition(
                    molecule_id=sucrose_id,
                    concentration=AttributedValue(value=2.0, unit="g/100g", sources=["usda_food"]),
                ),
            ],
        )

        result = merger.merge_all([food1, food2])

        assert len(result) == 1
        assert result[0].merge_metadata.molecule_count == 2
        # Caffeine should have 2 entries (from both sources)
        assert caffeine_id in result[0].composition
        assert len(result[0].composition[caffeine_id]) == 2
        # Sucrose should have 1 entry (only from USDA)
        assert sucrose_id in result[0].composition
        assert len(result[0].composition[sucrose_id]) == 1

    def test_merge_different_food_ids(
        self, merger, source_meta_foodb, source_meta_usda, ingest_meta
    ):
        """Test that different food_ids stay separate."""
        food1 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"foodb_food": source_meta_foodb},
            name=AttributedValue(value="Apple", sources=["foodb_food"]),
        )
        food2 = Tier1Food(
            food_id="usda:12345",
            _ingest_metadata=ingest_meta,
            _sources={"usda_food": source_meta_usda},
            name=AttributedValue(value="Banana", sources=["usda_food"]),
        )

        result = merger.merge_all([food1, food2])

        assert len(result) == 2
        ids = {f.food_id for f in result}
        assert ids == {"foodb_food:FOOD00001", "usda:12345"}

    def test_merge_extra_fields(
        self, merger, source_meta_foodb, source_meta_usda, ingest_meta
    ):
        """Test that extra fields are merged correctly."""
        food1 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"foodb_food": source_meta_foodb},
            extra={
                "foodb_id": AttributedValue(value="FOOD00001", sources=["foodb_food"]),
            },
        )
        food2 = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=ingest_meta,
            _sources={"usda_food": source_meta_usda},
            extra={
                "usda_fdc_id": AttributedValue(value="12345", sources=["usda_food"]),
            },
        )

        result = merger.merge_all([food1, food2])

        assert len(result) == 1
        assert "foodb_id" in result[0].extra
        assert "usda_fdc_id" in result[0].extra
        assert len(result[0].extra["foodb_id"]) == 1
        assert len(result[0].extra["usda_fdc_id"]) == 1

    def test_merge_empty_list(self, merger):
        """Test merging an empty list returns empty list."""
        result = merger.merge_all([])
        assert result == []
