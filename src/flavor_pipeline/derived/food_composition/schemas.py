"""Schemas for food flavor composition export.

This module defines the flattened export schema for food-to-flavor-molecule
composition data.
"""

from pydantic import BaseModel, Field


class FoodFlavorComposition(BaseModel):
    """Flattened food-flavor composition record for export.

    This schema provides a denormalized view of food-to-flavor-molecule
    relationships with concentration data, suitable for CSV/parquet export.
    """

    # Food identification
    food_name: str = Field(description="Common name of the food")
    scientific_name: str | None = Field(
        default=None, description="Scientific/botanical name"
    )
    food_part: str | None = Field(
        default=None, description="Part of food (e.g., 'peel', 'seed', 'whole')"
    )
    food_category: str | None = Field(
        default=None, description="Food category/group"
    )

    # Molecule identification
    molecule_name: str | None = Field(
        default=None, description="Common name of the molecule"
    )
    cas: str | None = Field(
        default=None, description="CAS registry number"
    )
    pubchem_id: int | None = Field(
        default=None, description="PubChem compound ID"
    )
    inchikey: str | None = Field(
        default=None, description="InChIKey identifier"
    )
    molecule_id: str = Field(
        description="Internal molecule ID (inchikey:X, cas:X, pubchem:X, or source:X)"
    )

    # Concentration data
    concentration: float | None = Field(
        default=None, description="Concentration value (average or single measurement)"
    )
    concentration_min: float | None = Field(
        default=None, description="Minimum concentration value"
    )
    concentration_max: float | None = Field(
        default=None, description="Maximum concentration value"
    )
    concentration_unit: str | None = Field(
        default=None, description="Unit of concentration (e.g., 'mg/100g', 'ppm')"
    )

    # Flavor information
    flavor_descriptors: list[str] = Field(
        default_factory=list, description="Associated flavor/aroma descriptors"
    )

    # Provenance
    source: str = Field(description="Data source (e.g., 'foodb', 'foodatlas')")
    source_food_id: str | None = Field(
        default=None, description="Original food ID from source"
    )
    source_molecule_id: str | None = Field(
        default=None, description="Original molecule ID from source"
    )
