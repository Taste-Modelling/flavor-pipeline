"""Dr. Duke's Phytochemical Database source adapter for Tier 1 molecules."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.tier1 import IngestMetadata, SourceMetadata, Tier1Molecule


class DukePhytochemSource:
    """Parse Dr. Duke's Phytochemical database to Tier 1 molecules.

    Expects raw_data/DukePhytochem/*.csv files produced by the acquirer.

    Parses:
    - CHEMICALS.csv: ~29,000 phytochemicals with CAS numbers
    - AGGREGAC.csv: Chemical-activity relationships (biological activities)
    - FARMACY_NEW.csv: Chemical-plant relationships (what plants contain each chemical)

    Each chemical becomes a Tier1Molecule with:
    - name, CAS number as identifiers
    - Biological activities stored in extra fields
    - Plant sources stored in extra fields
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")
    PARSER_VERSION = "0.1.0"
    PIPELINE_VERSION = "0.1.0"

    CHEMICALS_FILE = "CHEMICALS.csv"
    ACTIVITIES_FILE = "ACTIVITIES.csv"
    AGGREGAC_FILE = "AGGREGAC.csv"
    FARMACY_FILE = "FARMACY_NEW.csv"
    FNFTAX_FILE = "FNFTAX.csv"

    def __init__(self, raw_data_base: str | Path | None = None):
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    def name(self) -> str:
        return "duke_phytochem"

    @property
    def version(self) -> str:
        return "2023.1"

    @property
    def url(self) -> str:
        return "https://phytochem.nal.usda.gov/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "DukePhytochem"

    def validate(self) -> list[str]:
        errors = []
        chemicals_path = self.raw_data_dir / self.CHEMICALS_FILE

        if not chemicals_path.exists():
            errors.append(
                f"Missing file: {chemicals_path}. "
                "Run the duke_phytochem acquisition first."
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
        """Parse Dr. Duke's chemicals to Tier 1 molecules."""
        chemicals_path = self.raw_data_dir / self.CHEMICALS_FILE
        if not chemicals_path.exists():
            return []

        # Load chemicals
        df = pd.read_csv(chemicals_path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        # Load activity mappings (chemical -> list of activities)
        activity_map = self._load_activity_map()

        # Load plant mappings (chemical -> list of plants)
        plant_map = self._load_plant_map()

        molecules = []
        for _, row in df.iterrows():
            chem_name = self._nonempty(row.get("CHEM"))
            chem_id = self._nonempty(row.get("CHEMID"))
            cas = self._parse_cas(row.get("CASNUM"))

            if not chem_name:
                continue

            # Determine molecule_id (prefer CAS, then duke ID)
            if cas:
                molecule_id = f"cas:{cas}"
            elif chem_id:
                molecule_id = f"duke:{chem_id}"
            else:
                molecule_id = f"duke:{chem_name.replace(' ', '_').upper()}"

            # Get biological activities for this chemical
            activities = activity_map.get(chem_name, [])

            # Get plant sources for this chemical
            plants = plant_map.get(chem_name, [])

            # Build extra fields
            extra: dict[str, AttributedValue] = {}

            if chem_id:
                extra["duke_id"] = self._av(chem_id)

            if activities:
                # Store activities as a list
                extra["biological_activities"] = self._av(activities)
                # Count of activities
                extra["activity_count"] = self._av(len(activities))

            if plants:
                # Store plant sources (limit to first 20 to avoid huge records)
                extra["plant_sources"] = self._av(plants[:20])
                extra["plant_count"] = self._av(len(plants))

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                cas=self._av(cas) if cas else None,
                name=self._av(chem_name),
                extra=extra if extra else {},
            )
            molecules.append(mol)

        return molecules

    def _load_activity_map(self) -> dict[str, list[str]]:
        """Load chemical-activity relationships from AGGREGAC.csv.

        Returns:
            Dict mapping chemical name to list of biological activities.
        """
        aggregac_path = self.raw_data_dir / self.AGGREGAC_FILE
        if not aggregac_path.exists():
            return {}

        df = pd.read_csv(aggregac_path, dtype=str)
        activity_map: dict[str, list[str]] = {}

        for _, row in df.iterrows():
            chem = self._nonempty(row.get("CHEM"))
            activity = self._nonempty(row.get("ACTIVITY"))

            if chem and activity:
                if chem not in activity_map:
                    activity_map[chem] = []
                if activity not in activity_map[chem]:
                    activity_map[chem].append(activity)

        return activity_map

    def _load_plant_map(self) -> dict[str, list[str]]:
        """Load chemical-plant relationships from FARMACY_NEW.csv + FNFTAX.csv.

        Returns:
            Dict mapping chemical name to list of plant scientific names.
        """
        farmacy_path = self.raw_data_dir / self.FARMACY_FILE
        fnftax_path = self.raw_data_dir / self.FNFTAX_FILE

        if not farmacy_path.exists():
            return {}

        # Load plant taxonomy (FNFNUM -> scientific name)
        plant_names: dict[str, str] = {}
        if fnftax_path.exists():
            tax_df = pd.read_csv(fnftax_path, dtype=str)
            for _, row in tax_df.iterrows():
                fnfnum = self._nonempty(row.get("FNFNUM"))
                taxon = self._nonempty(row.get("TAXON"))
                if fnfnum and taxon:
                    plant_names[fnfnum] = taxon

        # Load chemical-plant relationships
        df = pd.read_csv(farmacy_path, dtype=str)
        plant_map: dict[str, list[str]] = {}

        for _, row in df.iterrows():
            chem = self._nonempty(row.get("CHEM"))
            fnfnum = self._nonempty(row.get("FNFNUM"))

            if chem and fnfnum:
                plant_name = plant_names.get(fnfnum, f"Plant #{fnfnum}")
                if chem not in plant_map:
                    plant_map[chem] = []
                if plant_name not in plant_map[chem]:
                    plant_map[chem].append(plant_name)

        return plant_map

    def _av(self, value: Any, unit: str | None = None) -> AttributedValue:
        """Create an AttributedValue with this source."""
        return AttributedValue(value=value, unit=unit, sources=[self.name])

    def _nonempty(self, val: Any) -> str | None:
        """Return value if not empty/null, else None."""
        if val is None or pd.isna(val) or val == "":
            return None
        return str(val).strip()

    def _parse_cas(self, val: Any) -> str | None:
        """Parse and validate CAS number.

        CAS numbers have format: 2-7 digits, hyphen, 2 digits, hyphen, 1 check digit
        e.g., 50-00-0 (formaldehyde) or 7732-18-5 (water)

        Duke database stores them without hyphens (e.g., 55739690 for 5573-96-90).
        """
        if val is None or pd.isna(val) or val == "":
            return None

        cas = str(val).strip()

        # If already has hyphens, validate format
        if "-" in cas:
            parts = cas.split("-")
            if len(parts) == 3 and all(p.isdigit() for p in parts):
                return cas
            return None

        # Duke stores CAS without hyphens - convert to standard format
        # CAS format: 2-7 digits + 2 digits + 1 digit = 5-10 digits total
        if cas.isdigit() and 5 <= len(cas) <= 10:
            # Format as XXX...-XX-X (last digit is check digit, 2 before that, rest is prefix)
            return f"{cas[:-3]}-{cas[-3:-1]}-{cas[-1]}"

        return None
