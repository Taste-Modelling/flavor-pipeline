"""VCF (Volatile Compounds in Food) source adapter."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule

from .base import BaseSource


class VCFSource(BaseSource):
    """Load volatile compound data from VCF EU-Flavis database.

    Expects raw_data/VCF/compounds.csv produced by acquisition/vcf.py.
    """

    COMPOUNDS_FILE = "compounds.csv"

    @property
    def name(self) -> str:
        return "vcf"

    @property
    def url(self) -> str:
        return "https://www.vcf-online.nl/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "VCF"

    def validate(self) -> list[str]:
        errors = []
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE

        if not compounds_path.exists():
            errors.append(
                f"Missing file: {compounds_path}. "
                "Run: python -m flavor_pipeline.acquisition.vcf"
            )

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse VCF compounds to Tier 1 format."""
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE
        if not compounds_path.exists():
            raise FileNotFoundError(f"Compounds file not found: {compounds_path}")

        df = pd.read_csv(compounds_path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for _, row in df.iterrows():
            fl_no = self._nonempty(row.get("fl_no"))
            cas = self._nonempty(row.get("cas"))

            if fl_no is None:
                continue

            # Use CAS as molecule_id if available, else FL number
            if cas:
                molecule_id = f"cas:{cas}"
            else:
                molecule_id = f"vcf:{fl_no}"

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                cas=self._av(cas) if cas else None,
                name=self._av(self._nonempty(row.get("compound_name")))
                if self._nonempty(row.get("compound_name"))
                else None,
                extra={
                    "fl_no": self._av(fl_no),
                    **(
                        {"fema_no": self._av(row.get("fema"))}
                        if self._nonempty(row.get("fema"))
                        else {}
                    ),
                    **(
                        {"chemical_group": self._av(row.get("chemical_group"))}
                        if self._nonempty(row.get("chemical_group"))
                        else {}
                    ),
                },
            )
            molecules.append(mol)

        return molecules
