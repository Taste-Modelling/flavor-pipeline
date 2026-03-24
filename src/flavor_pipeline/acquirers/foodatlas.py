"""FoodAtlas acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class FoodAtlasAcquirer(BaseAcquirer):
    """Acquire FoodAtlas knowledge graph data.

    Downloads FoodAtlas v3.2.0 containing 1,430 foods linked to 3,610 chemicals
    with 96,981 provenance-tracked relationships. Data sourced from UC Davis
    and licensed under Apache-2.0.
    """

    name = "foodatlas"
    description = "Download FoodAtlas knowledge graph v3.2.0 (~1.4k foods, ~3.6k chemicals)"
    url = "https://www.foodatlas.ai/"

    # Version subdirectory in the extracted archive
    DATA_SUBDIR = "v3.2_20250211"

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "Foodatlas"

    @property
    def data_dir(self) -> Path:
        """Directory containing the actual data files (versioned subdirectory)."""
        return self.output_dir / self.DATA_SUBDIR

    @property
    def output_files(self) -> list[str]:
        # Core files we expect from the archive (in versioned subdirectory)
        return [
            f"{self.DATA_SUBDIR}/entities.tsv",
            f"{self.DATA_SUBDIR}/triplets.tsv",
            f"{self.DATA_SUBDIR}/metadata_flavor.tsv",
        ]

    def fetch(self) -> Path:
        """Download and extract FoodAtlas data."""
        from flavor_pipeline.acquisition.foodatlas import fetch_foodatlas

        try:
            result = fetch_foodatlas(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch FoodAtlas: {e}") from e

    def validate(self) -> list[str]:
        """Validate the fetched FoodAtlas data."""
        errors = super().validate()

        # Additional validation: check that entities file has expected columns
        entities_path = self.data_dir / "entities.tsv"
        if entities_path.exists():
            with open(entities_path, encoding="utf-8") as f:
                header = f.readline().strip()
                if "foodatlas_id" not in header:
                    errors.append(
                        f"Unexpected entities.tsv format - header: {header[:100]}"
                    )

        return errors
