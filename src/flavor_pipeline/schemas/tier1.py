"""Tier 1 molecule schema with sparse fields and source metadata."""

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from flavor_pipeline.schemas.avo import AttributedValue


class SourceMetadata(BaseModel):
    """Metadata about a data source."""

    name: str
    version: str
    url: str | None = None
    retrieved_at: datetime
    parser_version: str


class IngestMetadata(BaseModel):
    """Metadata about the ingestion process."""

    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    pipeline_version: str


class Tier1Molecule(BaseModel):
    """Sparse Tier 1 molecule document.

    All chemical/sensory fields are optional to support sparse data from
    different sources. Each field is wrapped in an AttributedValue to
    track data provenance.
    """

    molecule_id: str
    ingest_metadata: IngestMetadata = Field(alias="_ingest_metadata")
    sources: dict[str, SourceMetadata] = Field(default_factory=dict, alias="_sources")

    # Chemical identifiers - all optional for sparse schema
    pubchem_cid: AttributedValue | None = None
    cas: AttributedValue | None = None
    smiles: AttributedValue | None = None
    inchi: AttributedValue | None = None
    inchi_key: AttributedValue | None = None

    # Names
    name: AttributedValue | None = None
    iupac_name: AttributedValue | None = None
    synonyms: AttributedValue | None = None

    # Sensory data
    flavor_descriptors: AttributedValue | None = None
    odor_descriptors: AttributedValue | None = None
    taste_descriptors: AttributedValue | None = None

    # Physical properties
    molecular_weight: AttributedValue | None = None
    molecular_formula: AttributedValue | None = None

    # Escape hatch for source-specific fields
    extra: dict[str, AttributedValue] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}
