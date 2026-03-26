"""MetaboLights source adapter for Tier 1 molecules."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.tier1 import IngestMetadata, SourceMetadata, Tier1Molecule


class MetaboLightsSource:
    """Parse MetaboLights compound data to Tier 1 molecules.

    Expects raw_data/MetaboLights/compounds.json produced by the acquirer.

    MetaboLights provides ~33,000 reference metabolites with:
    - Chemical identifiers (InChI, InChIKey, ChEBI ID)
    - Molecular formulas
    - Compound names
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")
    PARSER_VERSION = "0.1.0"
    PIPELINE_VERSION = "0.1.0"

    COMPOUNDS_FILE = "compounds.json"

    def __init__(self, raw_data_base: str | Path | None = None):
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    def name(self) -> str:
        return "metabolights"

    @property
    def version(self) -> str:
        return "2025.1"

    @property
    def url(self) -> str:
        return "https://www.ebi.ac.uk/metabolights/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "MetaboLights"

    def validate(self) -> list[str]:
        errors = []
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE

        if not compounds_path.exists():
            errors.append(
                f"Missing file: {compounds_path}. "
                "Run the metabolights acquisition first."
            )

        return errors

    def get_source_metadata(self, retrieved_at: datetime | None = None) -> SourceMetadata:
        """Create source metadata for this adapter."""
        return SourceMetadata(
            name=self.name,
            version=self.version,
            url=self.url,
            retrieved_at=retrieved_at or datetime.now(UTC),
            parser_version=self.PARSER_VERSION,
        )

    def get_ingest_metadata(self) -> IngestMetadata:
        """Create ingest metadata for molecules."""
        return IngestMetadata(pipeline_version=self.PIPELINE_VERSION)

    def parse(self) -> list[Tier1Molecule]:
        """Parse MetaboLights compounds to Tier 1 molecules."""
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE
        if not compounds_path.exists():
            return []

        with open(compounds_path, encoding="utf-8") as f:
            compounds = json.load(f)

        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for compound in compounds:
            mol = self._compound_to_molecule(compound, source_meta, ingest_meta)
            if mol:
                molecules.append(mol)

        return molecules

    def _compound_to_molecule(
        self,
        compound: dict,
        source_meta: SourceMetadata,
        ingest_meta: IngestMetadata,
    ) -> Tier1Molecule | None:
        """Convert a MetaboLights compound to Tier1Molecule."""
        accession = compound.get("accession")
        name = self._nonempty(compound.get("name"))
        inchi = self._nonempty(compound.get("inchi"))
        inchi_key = self._nonempty(compound.get("inchikey"))
        chebi_id = self._nonempty(compound.get("chebiId"))
        formula = self._nonempty(compound.get("formula"))
        description = self._nonempty(compound.get("description"))

        if not accession:
            return None

        # Determine molecule_id (prefer InChIKey, then ChEBI, then MTBLC)
        if inchi_key:
            molecule_id = f"inchikey:{inchi_key}"
        elif chebi_id:
            # ChEBI ID format: "CHEBI:12345"
            chebi_num = chebi_id.replace("CHEBI:", "")
            molecule_id = f"chebi:{chebi_num}"
        else:
            molecule_id = f"metabolights:{accession}"

        # Build extra fields
        extra: dict[str, AttributedValue] = {}

        extra["metabolights_id"] = self._av(accession)

        if chebi_id:
            extra["chebi_id"] = self._av(chebi_id)

        if description:
            extra["description"] = self._av(description)

        # Study status and flags
        study_status = compound.get("studyStatus")
        if study_status:
            extra["study_status"] = self._av(study_status)

        # Presence indicators
        if compound.get("hasNMR"):
            extra["has_nmr_spectra"] = self._av(True)
        if compound.get("hasMS"):
            extra["has_ms_spectra"] = self._av(True)
        if compound.get("hasSpecies"):
            extra["has_species_data"] = self._av(True)
        if compound.get("hasPathways"):
            extra["has_pathway_data"] = self._av(True)

        mol = Tier1Molecule(
            molecule_id=molecule_id,
            _ingest_metadata=ingest_meta,
            _sources={self.name: source_meta},
            inchi=self._av(inchi) if inchi else None,
            inchi_key=self._av(inchi_key) if inchi_key else None,
            name=self._av(name) if name else None,
            molecular_formula=self._av(formula) if formula else None,
            extra=extra if extra else {},
        )

        return mol

    def _av(self, value: Any, unit: str | None = None) -> AttributedValue:
        """Create an AttributedValue with this source."""
        return AttributedValue(value=value, unit=unit, sources=[self.name])

    def _nonempty(self, val: Any) -> str | None:
        """Return value if not empty/null, else None."""
        if val is None or val == "":
            return None
        return str(val).strip()
