"""FlavorDB2 source adapter for molecule-flavor data."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule

from .base import BaseSource


class FlavorDB2Source(BaseSource):
    """Load molecule-flavor data from FlavorDB2 dataset.

    Expects raw_data/FlavorDB2/molecules.csv produced by
    acquisition/flavordb2.py.
    """

    MOLECULES_FILE = "molecules.csv"
    REQUIRED_COLUMNS = {"pubchem_id", "common_name", "smiles", "flavor_profile"}

    # Placeholder flavors to exclude
    _PLACEHOLDER_FLAVORS = {"not available", "n/a", "none", "unknown", "-", ""}

    @property
    def name(self) -> str:
        return "flavordb2"

    @property
    def url(self) -> str:
        return "https://cosylab.iiitd.edu.in/flavordb2/"

    @property
    def raw_data_dir(self) -> Path:
        """Override because capitalize() gives 'Flavordb2', not 'FlavorDB2'."""
        return self._raw_data_base / "FlavorDB2"

    def validate(self) -> list[str]:
        errors = []
        molecules_path = self.raw_data_dir / self.MOLECULES_FILE

        if not molecules_path.exists():
            errors.append(
                f"Missing file: {molecules_path}. "
                "Run: python -m flavor_pipeline.acquisition.flavordb2"
            )
            return errors

        try:
            df = pd.read_csv(molecules_path, nrows=0)
            missing = self.REQUIRED_COLUMNS - set(df.columns)
            if missing:
                errors.append(f"{self.MOLECULES_FILE} missing columns: {sorted(missing)}")
        except Exception as e:
            errors.append(f"Error reading {self.MOLECULES_FILE}: {e}")

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse FlavorDB2 molecules CSV to Tier 1 format."""
        molecules_path = self.raw_data_dir / self.MOLECULES_FILE
        if not molecules_path.exists():
            raise FileNotFoundError(f"Molecules file not found: {molecules_path}")

        df = pd.read_csv(molecules_path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for _, row in df.iterrows():
            flavors = self._parse_flavor_profile(row.get("flavor_profile", ""))
            if not flavors:
                continue  # Skip molecules without flavor data

            pubchem_cid = self._parse_int(row.get("pubchem_id"))
            if pubchem_cid is None:
                continue  # Skip molecules without valid PubChem ID

            molecule_id = f"pubchem:{pubchem_cid}"

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                pubchem_cid=self._av(pubchem_cid),
                name=self._av(self._nonempty(row.get("common_name")))
                if self._nonempty(row.get("common_name"))
                else None,
                iupac_name=self._av(self._nonempty(row.get("iupac_name")))
                if self._nonempty(row.get("iupac_name"))
                else None,
                smiles=self._av(self._nonempty(row.get("smiles")))
                if self._nonempty(row.get("smiles"))
                else None,
                inchi=self._av(self._nonempty(row.get("inchi")))
                if self._nonempty(row.get("inchi"))
                else None,
                flavor_descriptors=self._av(flavors),
            )
            molecules.append(mol)

        return molecules

    def _parse_flavor_profile(self, value: str) -> list[str]:
        """Parse @ delimited flavor profile into normalized list."""
        if pd.isna(value) or not value:
            return []
        parts = [p.strip().lower() for p in str(value).split("@") if p.strip()]
        return [p for p in parts if p not in self._PLACEHOLDER_FLAVORS]
