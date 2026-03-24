"""Panten source adapter for fragrance and flavor materials."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule

from .base import BaseSource


class PantenSource(BaseSource):
    """Load fragrance/flavor data from Panten's handbook.

    Expects raw_data/Panten/compounds.csv produced by
    acquisition/panten.py PDF extraction.
    """

    COMPOUNDS_FILE = "compounds.csv"

    @property
    def name(self) -> str:
        return "panten"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "Panten"

    def validate(self) -> list[str]:
        errors = []
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE

        if not compounds_path.exists():
            errors.append(
                f"Missing file: {compounds_path}. "
                "Run: python -m flavor_pipeline.acquisition.panten"
            )

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse Panten compounds to Tier 1 format."""
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE
        if not compounds_path.exists():
            # Return empty list if no data available
            return []

        df = pd.read_csv(compounds_path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for _, row in df.iterrows():
            # Support both extraction script columns (cas_numbers) and legacy names
            cas = self._nonempty(row.get("cas_numbers") or row.get("cas") or row.get("CAS"))
            name = self._nonempty(row.get("name") or row.get("Name"))

            # Determine molecule_id
            if cas:
                molecule_id = f"cas:{cas}"
            elif name:
                molecule_id = f"panten:{name.lower().replace(' ', '_')[:50]}"
            else:
                continue

            # Parse odor description as descriptors
            # Support both extraction script columns (odor_description) and legacy names
            odor_desc = self._nonempty(row.get("odor_description") or row.get("odor"))
            odor_descriptors = self._parse_odor_description(odor_desc) if odor_desc else []

            # Support both extraction script columns and legacy names
            formula = self._nonempty(row.get("empirical_formula") or row.get("formula"))
            mw = self._parse_float(row.get("molecular_weight") or row.get("mw"))

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                cas=self._av(cas) if cas else None,
                name=self._av(name) if name else None,
                synonyms=self._av(self._parse_list(str(row.get("synonyms") or "")))
                if self._nonempty(row.get("synonyms"))
                else None,
                molecular_formula=self._av(formula)
                if formula
                else None,
                molecular_weight=self._av(mw, unit="g/mol")
                if mw
                else None,
                odor_descriptors=self._av(odor_descriptors) if odor_descriptors else None,
            )
            molecules.append(mol)

        return molecules

    def _parse_list(self, value: str) -> list[str]:
        """Parse comma or semicolon delimited string into list."""
        if pd.isna(value) or not value:
            return []
        sep = ";" if ";" in str(value) else ","
        return [s.strip() for s in str(value).split(sep) if s.strip()]

    def _parse_odor_description(self, value: str) -> list[str]:
        """Extract odor descriptors from description text."""
        if pd.isna(value) or not value:
            return []
        # Simple extraction - split on common delimiters
        text = str(value).lower()
        for sep in [",", ";", "and", "with"]:
            text = text.replace(sep, "|")
        return [s.strip() for s in text.split("|") if s.strip() and len(s.strip()) > 2]
