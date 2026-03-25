"""FooDB source adapter for food compound data."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule

from .base import BaseSource


class FooDBSource(BaseSource):
    """Load food compound data from FooDB 2020 release.

    Expects raw_data/FooDB/foodb_2020_04_07_csv/*.csv files produced by
    acquisition/foodb.py download.

    Key files used:
    - Compound.csv: Main compound data with structures
    - Flavor.csv: Flavor descriptors
    - CompoundsFlavor.csv: Compound-flavor associations
    """

    COMPOUND_FILE = "Compound.csv"
    FLAVOR_FILE = "Flavor.csv"
    COMPOUNDS_FLAVOR_FILE = "CompoundsFlavor.csv"

    @property
    def name(self) -> str:
        return "foodb"

    @property
    def version(self) -> str:
        return "2020.04"

    @property
    def url(self) -> str:
        return "https://foodb.ca/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "FooDB" / "foodb_2020_04_07_csv"

    def validate(self) -> list[str]:
        errors = []
        compound_path = self.raw_data_dir / self.COMPOUND_FILE

        if not compound_path.exists():
            errors.append(
                f"Missing file: {compound_path}. "
                "Run: python -m flavor_pipeline.acquisition.foodb"
            )

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse FooDB compounds to Tier 1 format.

        Note: The FooDB CSV has shifted columns due to unquoted commas in InChI
        fields. The actual column mapping is:
        - description -> CAS number
        - cas_number -> SMILES
        - moldb_inchikey -> InChI (truncated)
        - moldb_inchi -> molecular weight
        - moldb_smiles -> InChIKey
        """
        compound_path = self.raw_data_dir / self.COMPOUND_FILE
        if not compound_path.exists():
            return []

        # Load compound data
        df = pd.read_csv(compound_path, dtype=str, low_memory=False)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        # Load flavor associations
        flavor_map = self._load_flavor_map()

        molecules = []
        for _, row in df.iterrows():
            # FooDB uses 'id' as internal ID and 'public_id' as FDB identifier
            foodb_id = self._nonempty(row.get("id"))
            public_id = self._nonempty(row.get("public_id"))

            if foodb_id is None:
                continue

            # Due to CSV column shift: InChIKey is in 'moldb_smiles' column
            inchi_key = self._extract_inchikey(row.get("moldb_smiles"))
            if inchi_key:
                molecule_id = f"inchikey:{inchi_key}"
            elif public_id:
                molecule_id = f"foodb:{public_id}"
            else:
                molecule_id = f"foodb:{foodb_id}"

            # Due to CSV column shift: CAS is in 'description' column
            cas = self._parse_cas(row.get("description"))

            # Due to CSV column shift: SMILES is in 'cas_number' column
            smiles = self._nonempty(row.get("cas_number"))

            # Due to CSV column shift: molecular weight is in 'moldb_inchi' column
            mol_weight = self._parse_float(row.get("moldb_inchi"))

            # Get flavor descriptors for this compound
            flavor_descriptors = flavor_map.get(foodb_id, [])

            # Parse common/IUPAC names (these columns are not shifted)
            name = self._nonempty(row.get("name"))
            iupac = self._nonempty(row.get("moldb_iupac"))

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                cas=self._av(cas) if cas else None,
                smiles=self._av(smiles) if smiles else None,
                inchi_key=self._av(inchi_key) if inchi_key else None,
                name=self._av(name) if name else None,
                iupac_name=self._av(iupac) if iupac else None,
                molecular_weight=self._av(mol_weight, unit="g/mol") if mol_weight else None,
                flavor_descriptors=self._av(flavor_descriptors) if flavor_descriptors else None,
                extra={
                    "foodb_id": self._av(public_id or foodb_id),
                },
            )
            molecules.append(mol)

        return molecules

    def _parse_cas(self, value: object) -> str | None:
        """Parse CAS number, handling multiple values."""
        if value is None or pd.isna(value) or value == "":
            return None
        cas = str(value).strip()
        # If multiple CAS numbers (comma-separated), take first
        if "," in cas:
            cas = cas.split(",")[0].strip()
        # Validate CAS format (digits-digits-digit)
        parts = cas.split("-")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            return cas
        return None

    def _extract_inchikey(self, value: object) -> str | None:
        """Extract and validate InChIKey from a value.

        InChIKey format: 14 uppercase letters, hyphen, 10 uppercase letters,
        hyphen, 1 uppercase letter (e.g., XXXXXXXXXXXXXX-XXXXXXXXXX-X).
        """
        import re

        if value is None or pd.isna(value) or value == "":
            return None

        val = str(value).strip()
        # InChIKey pattern
        if re.match(r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$", val):
            return val
        return None

    def _load_flavor_map(self) -> dict[str, list[str]]:
        """Load compound-flavor associations.

        Returns:
            Dict mapping compound_id to list of flavor descriptors.
        """
        compounds_flavor_path = self.raw_data_dir / self.COMPOUNDS_FLAVOR_FILE
        flavor_path = self.raw_data_dir / self.FLAVOR_FILE

        if not compounds_flavor_path.exists() or not flavor_path.exists():
            return {}

        # Load flavor names
        flavor_df = pd.read_csv(flavor_path, dtype=str)
        flavor_names: dict[str, str] = {}
        for _, row in flavor_df.iterrows():
            fid = self._nonempty(row.get("id"))
            fname = self._nonempty(row.get("name"))
            if fid and fname:
                flavor_names[fid] = fname

        # Load compound-flavor associations
        cf_df = pd.read_csv(compounds_flavor_path, dtype=str)
        flavor_map: dict[str, list[str]] = {}

        for _, row in cf_df.iterrows():
            compound_id = self._nonempty(row.get("compound_id"))
            flavor_id = self._nonempty(row.get("flavor_id"))

            if compound_id and flavor_id and flavor_id in flavor_names:
                if compound_id not in flavor_map:
                    flavor_map[compound_id] = []
                flavor_map[compound_id].append(flavor_names[flavor_id])

        return flavor_map
