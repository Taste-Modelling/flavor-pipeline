"""CulinaryDB acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class CulinaryDBAcquirer(BaseAcquirer):
    """Acquire CulinaryDB recipe and ingredient data.

    Downloads CulinaryDB containing 45k+ recipes from 22 world regions
    with ingredients linked to FlavorDB flavor molecules.
    Licensed under CC BY-NC-SA 3.0.
    """

    name = "culinarydb"
    description = "Download CulinaryDB recipes (~46k recipes, 22 cuisines)"
    url = "https://cosylab.iiitd.edu.in/culinarydb/"

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "Culinarydb"

    @property
    def output_files(self) -> list[str]:
        return [
            "01_Recipe_Details.csv",
            "02_Ingredients.csv",
            "03_Compound_Ingredients.csv",
            "04_Recipe-Ingredients_Aliases.csv",
        ]

    def fetch(self) -> Path:
        """Download and extract CulinaryDB data."""
        from flavor_pipeline.acquisition.culinarydb import fetch_culinarydb

        try:
            result = fetch_culinarydb(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch CulinaryDB: {e}") from e

    def validate(self) -> list[str]:
        """Validate the fetched CulinaryDB data."""
        errors = super().validate()

        # Additional validation: check recipe details has expected columns
        recipe_path = self.output_dir / "01_Recipe_Details.csv"
        if recipe_path.exists():
            with open(recipe_path, encoding="utf-8") as f:
                header = f.readline().strip()
                if "Recipe ID" not in header:
                    errors.append(
                        f"Unexpected 01_Recipe_Details.csv format - header: {header[:100]}"
                    )

        return errors
