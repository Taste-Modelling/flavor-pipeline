"""USDA FoodData Central acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class USDAcquirer(BaseAcquirer):
    """Acquire USDA FoodData Central full dataset.

    Downloads the full CSV dataset (~458MB compressed, ~3.1GB uncompressed)
    containing Foundation Foods, SR Legacy Foods, Survey Foods (FNDDS),
    and Branded Foods.
    """

    name = "usda"
    description = "USDA FoodData Central full CSV dataset (~458MB, ~3.1GB uncompressed)"
    url = "https://fdc.nal.usda.gov/download-datasets"

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "USDA"

    @property
    def csv_dir(self) -> Path:
        """Directory containing extracted CSV files."""
        return self.output_dir / "FoodData_Central_csv_2025-12-18"

    @property
    def output_files(self) -> list[str]:
        # Key files expected after extraction
        return [
            "FoodData_Central_csv_2025-12-18/food.csv",
            "FoodData_Central_csv_2025-12-18/nutrient.csv",
            "FoodData_Central_csv_2025-12-18/food_nutrient.csv",
        ]

    def fetch(self) -> Path:
        """Download and extract USDA FoodData Central data."""
        from flavor_pipeline.acquisition.usda import fetch_usda

        try:
            result = fetch_usda(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch USDA: {e}") from e
