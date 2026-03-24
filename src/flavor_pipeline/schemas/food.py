"""Food schemas for Tier 1 and Tier 2 with molecular composition."""

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.tier1 import SourceMetadata


class MoleculeComposition(BaseModel):
    """A molecule's presence in a food with optional concentration.

    Links to Tier2Molecule via molecule_id (e.g., "inchikey:XXXX").
    """

    molecule_id: str  # FK to Tier2Molecule.molecule_id
    concentration: AttributedValue | None = None  # e.g., value=250, unit="mg/100g"
    concentration_min: AttributedValue | None = None
    concentration_max: AttributedValue | None = None


class IngestMetadata(BaseModel):
    """Metadata about the food ingestion process."""

    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    pipeline_version: str


class FoodMergeMetadata(BaseModel):
    """Metadata about the food merge process."""

    merged_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    pipeline_version: str
    source_count: int  # Number of contributing sources
    conflict_count: int  # Fields with differing values
    molecule_count: int  # Total unique molecules in composition


class Tier1Food(BaseModel):
    """Sparse Tier 1 food from a single source. All fields nullable.

    Similar to Tier1Molecule - each field is an optional AttributedValue
    for provenance tracking.
    """

    food_id: str  # e.g., "foodb_food:FOOD00001", "usda:1105904"
    ingest_metadata: IngestMetadata = Field(alias="_ingest_metadata")
    sources: dict[str, SourceMetadata] = Field(default_factory=dict, alias="_sources")

    # Food identification (all nullable)
    name: AttributedValue | None = None
    scientific_name: AttributedValue | None = None
    description: AttributedValue | None = None
    category: AttributedValue | None = None
    subcategory: AttributedValue | None = None

    # Molecular composition
    composition: list[MoleculeComposition] = Field(default_factory=list)

    # Source-specific fields
    extra: dict[str, AttributedValue] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}


class Tier2Food(BaseModel):
    """Merged Tier 2 food with multi-source attribution.

    Unlike Tier1Food where each field has a single AttributedValue,
    Tier2Food uses list[AttributedValue] for all fields to preserve
    values from different sources.
    """

    food_id: str
    merge_metadata: FoodMergeMetadata = Field(alias="_merge_metadata")
    sources: dict[str, SourceMetadata] = Field(default_factory=dict, alias="_sources")

    # Food identification (all nullable, list for multi-source)
    name: list[AttributedValue] | None = None
    scientific_name: list[AttributedValue] | None = None
    description: list[AttributedValue] | None = None
    category: list[AttributedValue] | None = None
    subcategory: list[AttributedValue] | None = None

    # Merged composition keyed by molecule_id for O(1) lookup
    # Each molecule_id maps to list of MoleculeComposition from different sources
    composition: dict[str, list[MoleculeComposition]] = Field(default_factory=dict)

    # Source-specific fields
    extra: dict[str, list[AttributedValue]] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}
