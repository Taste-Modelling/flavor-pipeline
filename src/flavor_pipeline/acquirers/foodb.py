"""FooDB acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class FooDBDAcquirer(BaseAcquirer):
    """Acquire food compound data from FooDB.

    Downloads the FooDB 2020 CSV data archive containing ~26,000 compounds
    with flavor associations, chemical properties, and food sources.
    """

    name = "foodb"
    description = "Download FooDB food compound data (~26k compounds with flavors)"
    url = "https://foodb.ca/"

    @property
    def csv_dir(self) -> Path:
        """Directory containing extracted CSV files."""
        return self.output_dir / "foodb_2020_04_07_csv"

    @property
    def output_files(self) -> list[str]:
        return [
            "foodb_2020_04_07_csv/Compound.csv",
            "foodb_2020_04_07_csv/Flavor.csv",
        ]

    @property
    def all_files(self) -> list[str]:
        """All files in the FooDB archive (used for complete validation)."""
        return [
            "foodb_2020_04_07_csv/Compound.csv",
            "foodb_2020_04_07_csv/CompoundsFlavor.csv",
            "foodb_2020_04_07_csv/Flavor.csv",
            "foodb_2020_04_07_csv/Content.csv",
            "foodb_2020_04_07_csv/Food.csv",
        ]

    def fetch(self) -> Path:
        """Download and extract FooDB data."""
        from flavor_pipeline.acquisition.foodb import fetch_foodb

        try:
            result = fetch_foodb(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch FooDB: {e}") from e
