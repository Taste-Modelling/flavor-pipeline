"""Tests for Attributed Value Object schema."""

import pytest

from flavor_pipeline.schemas.avo import AttributedValue


class TestAttributedValue:
    """Tests for AttributedValue."""

    def test_avo_creation(self):
        """Test basic AVO creation with value and sources."""
        avo = AttributedValue(value=100.5, unit="g/mol", sources=["pubchem"])
        assert avo.value == 100.5
        assert avo.unit == "g/mol"
        assert avo.sources == ["pubchem"]

    def test_avo_list_value(self):
        """Test AVO with list value (e.g., flavor descriptors)."""
        avo = AttributedValue(value=["sweet", "fruity"], sources=["flavordb2"])
        assert "sweet" in avo.value
        assert "fruity" in avo.value
        assert avo.sources == ["flavordb2"]

    def test_avo_multiple_sources(self):
        """Test AVO with multiple contributing sources."""
        avo = AttributedValue(
            value="CCO",
            sources=["pubchem", "flavordb2", "bitterdb"],
        )
        assert len(avo.sources) == 3
        assert "pubchem" in avo.sources

    def test_avo_with_conditions(self):
        """Test AVO with experimental conditions."""
        avo = AttributedValue(
            value=1.5,
            unit="ppm",
            sources=["fenaroli"],
            conditions={"temperature": 25, "medium": "water"},
        )
        assert avo.conditions["temperature"] == 25
        assert avo.conditions["medium"] == "water"

    def test_avo_no_unit(self):
        """Test AVO without unit (e.g., string values)."""
        avo = AttributedValue(value="vanillin", sources=["flavordb2"])
        assert avo.unit is None

    def test_avo_is_immutable(self):
        """Test that AVO is immutable (frozen)."""
        avo = AttributedValue(value="test", sources=["source1"])
        with pytest.raises(Exception):  # ValidationError for frozen model
            avo.value = "new_value"

    def test_avo_dict_value(self):
        """Test AVO with dict value for complex data."""
        avo = AttributedValue(
            value={"min": 0.1, "max": 10.0},
            unit="ppm",
            sources=["fenaroli"],
        )
        assert avo.value["min"] == 0.1
        assert avo.value["max"] == 10.0
