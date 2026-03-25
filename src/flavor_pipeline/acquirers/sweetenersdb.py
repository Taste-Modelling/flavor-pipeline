"""SweetenersDB acquirer implementation."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class SweetenersDBAcquirer(BaseAcquirer):
    """Acquire SweetenersDB sweet compound data from GitHub.

    Downloads a curated database of 316 sweet molecules with:
    - Compound names
    - Relative sweetness (logSw)
    - SMILES structures

    This is a curated subset of the original SuperSweet database.

    License: MIT
    """

    name = "sweetenersdb"
    description = "Download SweetenersDB sweet compound data (~316 molecules)"
    url = "https://github.com/chemosim-lab/SweetenersDB"

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "Sweetenersdb"

    @property
    def output_files(self) -> list[str]:
        return ["sweeteners.csv"]

    def fetch(self) -> Path:
        """Download SweetenersDB data from GitHub."""
        from flavor_pipeline.acquisition.sweetenersdb import fetch_sweetenersdb

        try:
            result = fetch_sweetenersdb(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch SweetenersDB: {e}") from e

    def validate(self) -> list[str]:
        """Validate the fetched SweetenersDB data."""
        errors = super().validate()
        if errors:
            return errors

        csv_path = self.output_dir / "sweeteners.csv"
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, nrows=5)
                required = {"ID", "Name", "logSw", "Smiles"}
                missing = required - set(df.columns)
                if missing:
                    errors.append(f"Missing columns in sweeteners.csv: {missing}")
            except Exception as e:
                errors.append(f"Error reading sweeteners.csv: {e}")

        return errors
