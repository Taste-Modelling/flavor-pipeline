"""Leffingwell source adapter for flavor/fragrance data."""

import ast
from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule

from .base import BaseSource


class LeffingwellSource(BaseSource):
    """Load molecule-flavor data from Leffingwell dataset.

    This is proprietary data - no download functionality.
    Expects raw_data/Leffingwell/ with:
      - molecules.csv (CID, IsomericSMILES, name)
      - behavior_sparse.csv (Stimulus, Labels)
    """

    MOLECULES_FILE = "molecules.csv"
    BEHAVIOR_FILE = "behavior_sparse.csv"

    @property
    def name(self) -> str:
        return "leffingwell"

    @property
    def url(self) -> str:
        return "https://www.leffingwell.com/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "Leffingwell"

    def validate(self) -> list[str]:
        errors = []
        molecules_path = self.raw_data_dir / self.MOLECULES_FILE
        behavior_path = self.raw_data_dir / self.BEHAVIOR_FILE

        if not molecules_path.exists():
            errors.append(f"Missing file: {molecules_path} (proprietary data)")
        if not behavior_path.exists():
            errors.append(f"Missing file: {behavior_path} (proprietary data)")

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse Leffingwell data to Tier 1 format."""
        molecules_path = self.raw_data_dir / self.MOLECULES_FILE
        behavior_path = self.raw_data_dir / self.BEHAVIOR_FILE

        if not molecules_path.exists() or not behavior_path.exists():
            # Return empty list if proprietary data not available
            return []

        molecules_df = pd.read_csv(molecules_path, dtype=str)
        behavior_df = pd.read_csv(behavior_path, dtype=str)

        # Join on CID = Stimulus
        merged = molecules_df.merge(
            behavior_df, left_on="CID", right_on="Stimulus", how="inner"
        )

        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for _, row in merged.iterrows():
            cid_str = self._nonempty(row.get("CID"))
            pubchem_cid = self._parse_int(cid_str)

            if pubchem_cid is None or pubchem_cid <= 0:
                continue  # Skip invalid or internal IDs

            molecule_id = f"pubchem:{pubchem_cid}"

            # Parse flavor labels
            labels = self._parse_labels(row.get("Labels", "[]"))

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                pubchem_cid=self._av(pubchem_cid),
                name=self._av(self._nonempty(row.get("name")))
                if self._nonempty(row.get("name"))
                else None,
                smiles=self._av(self._nonempty(row.get("IsomericSMILES")))
                if self._nonempty(row.get("IsomericSMILES"))
                else None,
                flavor_descriptors=self._av(labels) if labels else None,
            )
            molecules.append(mol)

        return molecules

    def _parse_labels(self, labels_str: str) -> list[str]:
        """Parse Labels column which is a string representation of a list."""
        if pd.isna(labels_str) or not labels_str:
            return []
        try:
            labels = ast.literal_eval(labels_str)
            if isinstance(labels, list):
                return [str(label).lower().strip() for label in labels]
        except (ValueError, SyntaxError):
            pass
        return []
