"""Fenaroli source adapter for flavor substances from the handbook."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule

from .base import BaseSource


class FenaroliSource(BaseSource):
    """Load flavor substances from Fenaroli's Handbook.

    Expects raw_data/Fenaroli/flavor_substances.csv produced by
    acquisition/fenaroli.py PDF extraction.
    """

    SUBSTANCES_FILE = "flavor_substances.csv"

    @property
    def name(self) -> str:
        return "fenaroli"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "Fenaroli"

    def validate(self) -> list[str]:
        errors = []
        substances_path = self.raw_data_dir / self.SUBSTANCES_FILE

        if not substances_path.exists():
            errors.append(
                f"Missing file: {substances_path}. "
                "Run: python -m flavor_pipeline.acquisition.fenaroli"
            )

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse Fenaroli flavor substances to Tier 1 format."""
        substances_path = self.raw_data_dir / self.SUBSTANCES_FILE
        if not substances_path.exists():
            # Return empty list if no data available
            return []

        df = pd.read_csv(substances_path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for _, row in df.iterrows():
            # Try to find a unique identifier
            # Support both extraction script columns (CAS_No, FEMA_No, FL_No) and legacy names
            cas = self._nonempty(row.get("CAS_No") or row.get("cas") or row.get("CAS"))
            fema = self._nonempty(row.get("FEMA_No") or row.get("fema") or row.get("FEMA"))
            fl_no = self._nonempty(row.get("FL_No") or row.get("fl_no") or row.get("FL"))
            name = self._nonempty(row.get("Name") or row.get("name"))

            # Determine molecule_id
            if cas:
                molecule_id = f"cas:{cas}"
            elif fema:
                molecule_id = f"fema:{fema}"
            elif fl_no:
                molecule_id = f"fl:{fl_no}"
            elif name:
                molecule_id = f"fenaroli:{name.lower().replace(' ', '_')[:50]}"
            else:
                continue  # Skip records without any identifier

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                cas=self._av(cas) if cas else None,
                name=self._av(name) if name else None,
                synonyms=self._av(self._parse_synonyms(str(row.get("Synonyms") or row.get("synonyms") or "")))
                if self._nonempty(row.get("Synonyms") or row.get("synonyms"))
                else None,
                molecular_formula=self._av(
                    self._nonempty(row.get("Empirical_Formula") or row.get("formula") or row.get("molecular_formula"))
                )
                if self._nonempty(row.get("Empirical_Formula") or row.get("formula") or row.get("molecular_formula"))
                else None,
                molecular_weight=self._av(
                    self._parse_float(row.get("Molecular_Weight") or row.get("mw") or row.get("molecular_weight")),
                    unit="g/mol",
                )
                if self._parse_float(row.get("Molecular_Weight") or row.get("mw") or row.get("molecular_weight"))
                else None,
                extra={
                    **({"fema_no": self._av(fema)} if fema else {}),
                    **({"fl_no": self._av(fl_no)} if fl_no else {}),
                },
            )
            molecules.append(mol)

        return molecules

    def _parse_synonyms(self, value: str) -> list[str]:
        """Parse synonyms string into list."""
        if pd.isna(value) or not value:
            return []
        # Try semicolon first, then comma
        if ";" in str(value):
            return [s.strip() for s in str(value).split(";") if s.strip()]
        return [s.strip() for s in str(value).split(",") if s.strip()]
