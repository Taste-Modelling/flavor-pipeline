"""FSBI-DB source adapter for flavor compound data."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule

from .base import BaseSource


class FSBISource(BaseSource):
    """Load flavor compound data from FSBI-DB.

    Expects raw_data/FSBI/compounds.csv produced by acquisition/fsbi.py.
    """

    COMPOUNDS_FILE = "compounds.csv"

    @property
    def name(self) -> str:
        return "fsbi"

    @property
    def url(self) -> str:
        return "https://fsbi-db.de/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "FSBI"

    def validate(self) -> list[str]:
        errors = []
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE

        if not compounds_path.exists():
            errors.append(
                f"Missing file: {compounds_path}. "
                "Run: python -m flavor_pipeline.acquisition.fsbi"
            )

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse FSBI compounds to Tier 1 format."""
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE
        if not compounds_path.exists():
            raise FileNotFoundError(f"Compounds file not found: {compounds_path}")

        df = pd.read_csv(compounds_path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for _, row in df.iterrows():
            fsbi_id = self._nonempty(row.get("fsbi_id"))
            pubchem_cid = self._parse_int(row.get("pubchem_id"))

            if fsbi_id is None:
                continue

            # Use PubChem ID as molecule_id if available
            if pubchem_cid:
                molecule_id = f"pubchem:{pubchem_cid}"
            else:
                molecule_id = f"fsbi:{fsbi_id}"

            # Parse odor and taste qualities (pipe-delimited)
            odor_descriptors = self._parse_pipe_delimited(row.get("odor_qualities", ""))
            taste_descriptors = self._parse_pipe_delimited(row.get("taste_qualities", ""))

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                pubchem_cid=self._av(pubchem_cid) if pubchem_cid else None,
                cas=self._av(self._nonempty(row.get("cas")))
                if self._nonempty(row.get("cas"))
                else None,
                smiles=self._av(self._nonempty(row.get("smiles")))
                if self._nonempty(row.get("smiles"))
                else None,
                inchi_key=self._av(self._nonempty(row.get("inchi_key")))
                if self._nonempty(row.get("inchi_key"))
                else None,
                name=self._av(self._nonempty(row.get("name")))
                if self._nonempty(row.get("name"))
                else None,
                synonyms=self._av(self._parse_comma_delimited(row.get("synonyms", "")))
                if self._nonempty(row.get("synonyms"))
                else None,
                molecular_weight=self._av(
                    self._parse_float(row.get("molecular_weight")), unit="g/mol"
                )
                if self._parse_float(row.get("molecular_weight"))
                else None,
                molecular_formula=self._av(self._nonempty(row.get("molecular_formula")))
                if self._nonempty(row.get("molecular_formula"))
                else None,
                odor_descriptors=self._av(odor_descriptors) if odor_descriptors else None,
                taste_descriptors=self._av(taste_descriptors) if taste_descriptors else None,
                extra={
                    "fsbi_id": self._av(fsbi_id),
                    **(
                        {"flavordb_id": self._av(row.get("flavordb_id"))}
                        if self._nonempty(row.get("flavordb_id"))
                        else {}
                    ),
                },
            )
            molecules.append(mol)

        return molecules

    def _parse_pipe_delimited(self, value: str) -> list[str]:
        """Parse pipe-delimited string into list."""
        if pd.isna(value) or not value:
            return []
        return [p.strip().lower() for p in str(value).split("|") if p.strip()]

    def _parse_comma_delimited(self, value: str) -> list[str]:
        """Parse comma-delimited string into list."""
        if pd.isna(value) or not value:
            return []
        return [p.strip() for p in str(value).split(",") if p.strip()]
