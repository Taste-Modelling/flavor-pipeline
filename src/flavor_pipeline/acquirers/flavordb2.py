"""FlavorDB2 acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class FlavorDB2Acquirer(BaseAcquirer):
    """Acquire molecule data from FlavorDB2 with PubChem enrichment.

    Scrapes the FlavorDB2 molecules table (~25k molecules) and enriches
    with structural data (SMILES, InChI, IUPAC name) from PubChem.
    """

    name = "flavordb2"
    description = "Fetch FlavorDB2 molecules with PubChem structural enrichment (~25k molecules)"
    url = "https://cosylab.iiitd.edu.in/flavordb2/"

    @property
    def output_dir(self) -> Path:
        # FlavorDB2 uses capital DB in directory name
        return self._raw_data_base / "FlavorDB2"

    @property
    def output_files(self) -> list[str]:
        return ["molecules.csv"]

    def fetch(self) -> Path:
        """Scrape FlavorDB2 and enrich with PubChem data."""
        from flavor_pipeline.acquisition.flavordb2 import fetch_flavordb2

        try:
            result = fetch_flavordb2(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch FlavorDB2: {e}") from e

    def validate(self) -> list[str]:
        """Validate FlavorDB2 output has expected columns."""
        errors = super().validate()
        if errors:
            return errors

        import pandas as pd

        csv_path = self.output_dir / "molecules.csv"
        try:
            df = pd.read_csv(csv_path, nrows=0)
            required = {"pubchem_id", "common_name", "flavor_profile"}
            missing = required - set(df.columns)
            if missing:
                errors.append(f"Missing columns in molecules.csv: {missing}")
        except Exception as e:
            errors.append(f"Error reading molecules.csv: {e}")

        return errors
