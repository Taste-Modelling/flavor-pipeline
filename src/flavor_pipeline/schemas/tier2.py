"""Tier 2 merged molecule schema with multi-source attribution."""

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.tier1 import SourceMetadata


class MergeMetadata(BaseModel):
    """Metadata about the merge process."""

    merged_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    pipeline_version: str
    source_count: int  # Number of contributing sources
    conflict_count: int  # Fields with differing values


class Tier2Molecule(BaseModel):
    """Merged Tier 2 molecule with multi-source attribution.

    Unlike Tier1Molecule where each field has a single AttributedValue,
    Tier2Molecule uses list[AttributedValue] for all fields to preserve
    conflicting values from different sources.
    """

    molecule_id: str
    merge_metadata: MergeMetadata = Field(alias="_merge_metadata")
    sources: dict[str, SourceMetadata] = Field(default_factory=dict, alias="_sources")

    # Chemical identifiers - lists to handle conflicts
    pubchem_cid: list[AttributedValue] | None = None
    cas: list[AttributedValue] | None = None
    smiles: list[AttributedValue] | None = None
    inchi: list[AttributedValue] | None = None
    inchi_key: list[AttributedValue] | None = None

    # Names
    name: list[AttributedValue] | None = None
    iupac_name: list[AttributedValue] | None = None
    synonyms: list[AttributedValue] | None = None

    # Sensory data
    flavor_descriptors: list[AttributedValue] | None = None
    odor_descriptors: list[AttributedValue] | None = None
    taste_descriptors: list[AttributedValue] | None = None

    # Physical properties
    molecular_weight: list[AttributedValue] | None = None
    molecular_formula: list[AttributedValue] | None = None

    # Escape hatch for source-specific fields
    extra: dict[str, list[AttributedValue]] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}
