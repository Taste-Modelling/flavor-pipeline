"""Tests for Food schemas."""

from datetime import UTC, datetime

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.food import (
    FoodMergeMetadata,
    IngestMetadata,
    MoleculeComposition,
    Tier1Food,
    Tier2Food,
)
from flavor_pipeline.schemas.tier1 import SourceMetadata


class TestMoleculeComposition:
    """Tests for MoleculeComposition."""

    def test_minimal_composition(self):
        """Test composition with just molecule_id."""
        mc = MoleculeComposition(molecule_id="inchikey:ABCDEF-GHIJKL-N")
        assert mc.molecule_id == "inchikey:ABCDEF-GHIJKL-N"
        assert mc.concentration is None
        assert mc.concentration_min is None
        assert mc.concentration_max is None

    def test_full_composition(self):
        """Test composition with all concentration values."""
        mc = MoleculeComposition(
            molecule_id="inchikey:ABCDEF-GHIJKL-N",
            concentration=AttributedValue(value=250.0, unit="mg/100g", sources=["foodb_food"]),
            concentration_min=AttributedValue(value=100.0, unit="mg/100g", sources=["foodb_food"]),
            concentration_max=AttributedValue(value=500.0, unit="mg/100g", sources=["foodb_food"]),
        )
        assert mc.concentration is not None
        assert mc.concentration.value == 250.0
        assert mc.concentration.unit == "mg/100g"
        assert mc.concentration_min is not None
        assert mc.concentration_min.value == 100.0
        assert mc.concentration_max is not None
        assert mc.concentration_max.value == 500.0


class TestIngestMetadata:
    """Tests for IngestMetadata."""

    def test_auto_timestamp(self):
        """Test that ingested_at defaults to current time."""
        before = datetime.now(UTC)
        meta = IngestMetadata(pipeline_version="0.1.0")
        after = datetime.now(UTC)

        assert before <= meta.ingested_at <= after

    def test_explicit_timestamp(self):
        """Test explicit ingested_at timestamp."""
        timestamp = datetime(2024, 1, 1, 12, 0, 0)
        meta = IngestMetadata(
            ingested_at=timestamp,
            pipeline_version="0.1.0",
        )
        assert meta.ingested_at == timestamp


class TestTier1Food:
    """Tests for Tier1Food."""

    def test_minimal_food(self):
        """Test creating a food with only required fields."""
        food = Tier1Food(
            food_id="usda:12345",
            _ingest_metadata=IngestMetadata(pipeline_version="0.1.0"),
        )
        assert food.food_id == "usda:12345"
        assert food.name is None
        assert food.composition == []

    def test_full_food(self):
        """Test creating a food with all fields populated."""
        source_meta = SourceMetadata(
            name="foodb_food",
            version="2020.04",
            url="https://foodb.ca/",
            retrieved_at=datetime(2024, 1, 15),
            parser_version="0.1.0",
        )

        composition = [
            MoleculeComposition(
                molecule_id="inchikey:RYYVLZVUVIJVGH-UHFFFAOYSA-N",  # Caffeine
                concentration=AttributedValue(value=95.0, unit="mg/100g", sources=["foodb_food"]),
            ),
            MoleculeComposition(
                molecule_id="inchikey:CZMRCDWAGMRECN-UGDNZRGBSA-N",  # Sucrose
                concentration=AttributedValue(value=5.0, unit="g/100g", sources=["foodb_food"]),
            ),
        ]

        food = Tier1Food(
            food_id="foodb_food:FOOD00001",
            _ingest_metadata=IngestMetadata(pipeline_version="0.1.0"),
            _sources={"foodb_food": source_meta},
            name=AttributedValue(value="Coffee, brewed", sources=["foodb_food"]),
            description=AttributedValue(value="Brewed coffee beverage", sources=["foodb_food"]),
            category=AttributedValue(value="Beverages", sources=["foodb_food"]),
            composition=composition,
        )

        assert food.food_id == "foodb_food:FOOD00001"
        assert food.name is not None
        assert food.name.value == "Coffee, brewed"
        assert len(food.composition) == 2
        assert food.composition[0].molecule_id == "inchikey:RYYVLZVUVIJVGH-UHFFFAOYSA-N"

    def test_sparse_fields(self):
        """Test that sparse schema allows missing fields."""
        food = Tier1Food(
            food_id="usda:99999",
            _ingest_metadata=IngestMetadata(pipeline_version="0.1.0"),
            name=AttributedValue(value="Mystery food", sources=["usda_food"]),
        )

        # Only populated fields
        assert food.name is not None
        assert food.name.value == "Mystery food"

        # Sparse fields are None
        assert food.scientific_name is None
        assert food.description is None
        assert food.category is None
        assert food.subcategory is None

    def test_extra_fields(self):
        """Test escape hatch for source-specific fields."""
        food = Tier1Food(
            food_id="usda:12345",
            _ingest_metadata=IngestMetadata(pipeline_version="0.1.0"),
            extra={
                "usda_fdc_id": AttributedValue(value="12345", sources=["usda_food"]),
                "data_type": AttributedValue(value="foundation_food", sources=["usda_food"]),
            },
        )

        assert food.extra["usda_fdc_id"].value == "12345"
        assert food.extra["data_type"].sources == ["usda_food"]


class TestFoodMergeMetadata:
    """Tests for FoodMergeMetadata."""

    def test_merge_metadata(self):
        """Test merge metadata creation."""
        meta = FoodMergeMetadata(
            pipeline_version="1.0.0",
            source_count=2,
            conflict_count=1,
            molecule_count=50,
        )
        assert meta.pipeline_version == "1.0.0"
        assert meta.source_count == 2
        assert meta.conflict_count == 1
        assert meta.molecule_count == 50


class TestTier2Food:
    """Tests for Tier2Food."""

    def test_minimal_tier2_food(self):
        """Test creating a Tier2Food with only required fields."""
        food = Tier2Food(
            food_id="usda:12345",
            _merge_metadata=FoodMergeMetadata(
                pipeline_version="1.0.0",
                source_count=1,
                conflict_count=0,
                molecule_count=0,
            ),
        )
        assert food.food_id == "usda:12345"
        assert food.name is None
        assert food.composition == {}

    def test_multi_source_tier2_food(self):
        """Test Tier2Food with data from multiple sources."""
        source_meta_foodb = SourceMetadata(
            name="foodb_food",
            version="2020.04",
            retrieved_at=datetime.now(UTC),
            parser_version="0.1.0",
        )
        source_meta_usda = SourceMetadata(
            name="usda_food",
            version="2025.12",
            retrieved_at=datetime.now(UTC),
            parser_version="0.1.0",
        )

        # Names from different sources
        names = [
            AttributedValue(value="Apple, raw", sources=["foodb_food"]),
            AttributedValue(value="Apples, raw, with skin", sources=["usda_food"]),
        ]

        # Composition from different sources
        composition = {
            "inchikey:CZMRCDWAGMRECN-UGDNZRGBSA-N": [  # Sucrose
                MoleculeComposition(
                    molecule_id="inchikey:CZMRCDWAGMRECN-UGDNZRGBSA-N",
                    concentration=AttributedValue(value=2.1, unit="g/100g", sources=["foodb_food"]),
                ),
                MoleculeComposition(
                    molecule_id="inchikey:CZMRCDWAGMRECN-UGDNZRGBSA-N",
                    concentration=AttributedValue(value=2.07, unit="g/100g", sources=["usda_food"]),
                ),
            ],
        }

        food = Tier2Food(
            food_id="foodb_food:FOOD00123",
            _merge_metadata=FoodMergeMetadata(
                pipeline_version="1.0.0",
                source_count=2,
                conflict_count=1,  # Different names
                molecule_count=1,
            ),
            _sources={
                "foodb_food": source_meta_foodb,
                "usda_food": source_meta_usda,
            },
            name=names,
            composition=composition,
        )

        assert food.food_id == "foodb_food:FOOD00123"
        assert food.name is not None
        assert len(food.name) == 2
        assert food.merge_metadata.source_count == 2
        assert len(food.composition) == 1
        assert len(food.composition["inchikey:CZMRCDWAGMRECN-UGDNZRGBSA-N"]) == 2

    def test_model_dump(self):
        """Test serialization of Tier2Food."""
        food = Tier2Food(
            food_id="test:1",
            _merge_metadata=FoodMergeMetadata(
                merged_at=datetime(2024, 1, 1),
                pipeline_version="1.0.0",
                source_count=1,
                conflict_count=0,
                molecule_count=5,
            ),
            name=[AttributedValue(value="Test Food", sources=["source1"])],
        )

        data = food.model_dump()
        assert data["food_id"] == "test:1"
        assert data["name"][0]["value"] == "Test Food"
