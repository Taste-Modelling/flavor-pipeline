"""SweetenersDB source adapter for sweet molecules.

SweetenersDB is a curated database of 316 sweet molecules with:
- Compound names
- Relative sweetness (logSw) - logarithmic sweetness relative to sucrose
- SMILES structures

Based on research by Bouysset et al. (2020) Food Chemistry and
Chéron et al. (2017) Food Chemistry.

This is a curated subset of the original SuperSweet database
(http://bioinf-applied.charite.de/sweet/) which is no longer available.
"""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule
from flavor_pipeline.sources.base import BaseSource


class SweetenersDBSource(BaseSource):
    """Parse SweetenersDB to Tier1Molecule format.

    Extracts sweet molecules with their relative sweetness values.
    The logSw value represents the logarithm of relative sweetness
    compared to sucrose (logSw=0 means equivalent to sucrose).
    """

    SWEETENERS_FILE = "sweeteners.csv"

    @property
    def name(self) -> str:
        return "sweetenersdb"

    @property
    def version(self) -> str:
        return "2.0"

    @property
    def url(self) -> str:
        return "https://github.com/chemosim-lab/SweetenersDB"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "Sweetenersdb"

    def validate(self) -> list[str]:
        errors = []
        csv_path = self.raw_data_dir / self.SWEETENERS_FILE

        if not csv_path.exists():
            errors.append(
                f"Missing file: {csv_path}. "
                "Run: python -m flavor_pipeline.acquisition.sweetenersdb"
            )

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse SweetenersDB to Tier 1 molecules."""
        csv_path = self.raw_data_dir / self.SWEETENERS_FILE
        if not csv_path.exists():
            return []

        df = pd.read_csv(csv_path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for _, row in df.iterrows():
            # Get identifiers
            source_id = self._nonempty(row.get("ID"))
            name = self._nonempty(row.get("Name"))
            smiles = self._nonempty(row.get("Smiles"))
            log_sw = self._parse_float(row.get("logSw"))

            if not source_id:
                continue

            # Use source-specific ID since no PubChem/CAS available
            molecule_id = f"sweetenersdb:{source_id}"

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                name=self._av(name) if name else None,
                smiles=self._av(smiles) if smiles else None,
                # Sweet taste is the defining characteristic
                taste_descriptors=self._av(["sweet"]),
                extra={
                    "sweetenersdb_id": self._av(source_id),
                    # logSw is relative sweetness: 0 = sucrose, positive = sweeter
                    # e.g., logSw=2 means 100x sweeter than sucrose
                    **(
                        {"log_sweetness": self._av(log_sw)}
                        if log_sw is not None
                        else {}
                    ),
                    # Convert to relative sweetness factor if available
                    **(
                        {
                            "relative_sweetness": self._av(
                                round(10**log_sw, 2), unit="x sucrose"
                            )
                        }
                        if log_sw is not None
                        else {}
                    ),
                },
            )
            molecules.append(mol)

        return molecules
