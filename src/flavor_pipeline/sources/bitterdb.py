"""BitterDB source adapter for bitter compound data."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule

from .base import BaseSource


class BitterDBSource(BaseSource):
    """Load bitter compound data from BitterDB 2024 release.

    Expects raw_data/BitterDB/*.csv files produced by acquisition/bitterdb.py.
    Primary data comes from BitterCompoundsPropA_2024.csv.
    """

    COMPOUNDS_FILE = "BitterCompoundsPropA_2024.csv"
    NAMES_FILE = "compoundsnamesA_2024.csv"

    @property
    def name(self) -> str:
        return "bitterdb"

    @property
    def version(self) -> str:
        return "2024"

    @property
    def url(self) -> str:
        return "https://bitterdb.agri.huji.ac.il/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "BitterDB"

    def validate(self) -> list[str]:
        errors = []
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE

        if not compounds_path.exists():
            errors.append(
                f"Missing file: {compounds_path}. "
                "Run: python -m flavor_pipeline.acquisition.bitterdb"
            )

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse BitterDB compounds to Tier 1 format."""
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE
        if not compounds_path.exists():
            raise FileNotFoundError(f"Compounds file not found: {compounds_path}")

        df = pd.read_csv(compounds_path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        # Load compound names for synonyms
        names_df = self._load_names()

        molecules = []
        for _, row in df.iterrows():
            # BitterDB uses 'cid' as internal ID and 'pubChemID' for PubChem
            bitterdb_id = self._nonempty(row.get("cid"))
            pubchem_cid = self._parse_int(row.get("pubChemID"))

            if bitterdb_id is None:
                continue

            # Use PubChem ID as molecule_id if available, else BitterDB ID
            if pubchem_cid:
                molecule_id = f"pubchem:{pubchem_cid}"
            else:
                molecule_id = f"bitterdb:{bitterdb_id}"

            # Get synonyms from names file
            synonyms = names_df.get(bitterdb_id, []) if names_df else []

            # All BitterDB compounds are bitter by definition
            taste_descriptors = ["bitter"]

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                pubchem_cid=self._av(pubchem_cid) if pubchem_cid else None,
                cas=self._av(self._nonempty(row.get("Cas_Number_Final")))
                if self._nonempty(row.get("Cas_Number_Final"))
                else None,
                smiles=self._av(self._nonempty(row.get("canonical_smiles")))
                if self._nonempty(row.get("canonical_smiles"))
                else None,
                inchi_key=self._av(self._nonempty(row.get("InChiKey")))
                if self._nonempty(row.get("InChiKey"))
                else None,
                name=self._av(self._nonempty(row.get("IUPAC")))
                if self._nonempty(row.get("IUPAC"))
                else None,
                iupac_name=self._av(self._nonempty(row.get("IUPAC")))
                if self._nonempty(row.get("IUPAC"))
                else None,
                synonyms=self._av(synonyms) if synonyms else None,
                molecular_weight=self._av(self._parse_float(row.get("MW")), unit="g/mol")
                if self._parse_float(row.get("MW"))
                else None,
                molecular_formula=self._av(self._nonempty(row.get("cFormula")))
                if self._nonempty(row.get("cFormula"))
                else None,
                taste_descriptors=self._av(taste_descriptors),
                extra={
                    "bitterdb_id": self._av(bitterdb_id),
                },
            )
            molecules.append(mol)

        return molecules

    def _load_names(self) -> dict[str, list[str]]:
        """Load compound names/synonyms from names file."""
        names_path = self.raw_data_dir / self.NAMES_FILE
        if not names_path.exists():
            return {}

        df = pd.read_csv(names_path, dtype=str)
        names_dict: dict[str, list[str]] = {}

        for _, row in df.iterrows():
            cid = self._nonempty(row.get("cid"))
            name = self._nonempty(row.get("compound_name") or row.get("name"))
            if cid and name:
                if cid not in names_dict:
                    names_dict[cid] = []
                names_dict[cid].append(name)

        return names_dict
