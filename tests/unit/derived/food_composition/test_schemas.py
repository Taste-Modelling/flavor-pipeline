"""Tests for food flavor composition schemas."""

from flavor_pipeline.derived.food_composition.schemas import FoodFlavorComposition


class TestFoodFlavorComposition:
    """Tests for FoodFlavorComposition model."""

    def test_minimal_valid_record(self):
        """Test creating a record with only required fields."""
        record = FoodFlavorComposition(
            food_name="Apple",
            molecule_id="cas:123-45-6",
            source="foodb",
        )
        assert record.food_name == "Apple"
        assert record.molecule_id == "cas:123-45-6"
        assert record.source == "foodb"
        assert record.scientific_name is None
        assert record.cas is None
        assert record.concentration is None
        assert record.flavor_descriptors == []

    def test_full_record(self):
        """Test creating a record with all fields."""
        record = FoodFlavorComposition(
            food_name="Apple",
            scientific_name="Malus domestica",
            food_part="peel",
            food_category="Fruits",
            molecule_name="Limonene",
            cas="138-86-3",
            pubchem_id=22311,
            inchikey="XMGQYMWWDOXHJM-UHFFFAOYSA-N",
            molecule_id="inchikey:XMGQYMWWDOXHJM-UHFFFAOYSA-N",
            concentration=15.5,
            concentration_min=10.0,
            concentration_max=25.0,
            concentration_unit="mg/100g",
            flavor_descriptors=["citrus", "orange", "fresh"],
            source="foodb",
            source_food_id="FOOD00001",
            source_molecule_id="FDB012345",
        )

        assert record.food_name == "Apple"
        assert record.scientific_name == "Malus domestica"
        assert record.food_part == "peel"
        assert record.food_category == "Fruits"
        assert record.molecule_name == "Limonene"
        assert record.cas == "138-86-3"
        assert record.pubchem_id == 22311
        assert record.inchikey == "XMGQYMWWDOXHJM-UHFFFAOYSA-N"
        assert record.molecule_id == "inchikey:XMGQYMWWDOXHJM-UHFFFAOYSA-N"
        assert record.concentration == 15.5
        assert record.concentration_min == 10.0
        assert record.concentration_max == 25.0
        assert record.concentration_unit == "mg/100g"
        assert record.flavor_descriptors == ["citrus", "orange", "fresh"]
        assert record.source == "foodb"
        assert record.source_food_id == "FOOD00001"
        assert record.source_molecule_id == "FDB012345"

    def test_model_dump_json(self):
        """Test JSON serialization."""
        record = FoodFlavorComposition(
            food_name="Apple",
            molecule_id="cas:123-45-6",
            source="foodb",
            concentration=15.5,
            flavor_descriptors=["citrus"],
        )
        data = record.model_dump(mode="json")

        assert data["food_name"] == "Apple"
        assert data["molecule_id"] == "cas:123-45-6"
        assert data["concentration"] == 15.5
        assert data["flavor_descriptors"] == ["citrus"]

    def test_molecule_id_formats(self):
        """Test various molecule_id formats."""
        # InChIKey format
        r1 = FoodFlavorComposition(
            food_name="Test",
            molecule_id="inchikey:XMGQYMWWDOXHJM-UHFFFAOYSA-N",
            source="foodb",
        )
        assert r1.molecule_id.startswith("inchikey:")

        # CAS format
        r2 = FoodFlavorComposition(
            food_name="Test",
            molecule_id="cas:138-86-3",
            source="foodb",
        )
        assert r2.molecule_id.startswith("cas:")

        # PubChem format
        r3 = FoodFlavorComposition(
            food_name="Test",
            molecule_id="pubchem:22311",
            source="foodatlas",
        )
        assert r3.molecule_id.startswith("pubchem:")

        # Source-specific fallback
        r4 = FoodFlavorComposition(
            food_name="Test",
            molecule_id="foodb:FDB012345",
            source="foodb",
        )
        assert r4.molecule_id.startswith("foodb:")

    def test_empty_flavor_descriptors_default(self):
        """Test that flavor_descriptors defaults to empty list."""
        record = FoodFlavorComposition(
            food_name="Test",
            molecule_id="cas:123-45-6",
            source="foodb",
        )
        assert record.flavor_descriptors == []
        assert isinstance(record.flavor_descriptors, list)
