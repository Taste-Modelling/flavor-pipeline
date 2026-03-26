"""UmamiDB acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class UmamiDBAcquirer(BaseAcquirer):
    """Acquire UmamiDB food amino acid and nucleotide data.

    Downloads UmamiDB (~700 foods) with measurements for:
    - 20 free amino acids (glutamate, aspartate, etc.)
    - 3 nucleotides (IMP, GMP, AMP)

    All values in mg/100g of edible portion.

    License: Free to use (Umami Information Center)
    """

    name = "umamidb"
    description = "Download UmamiDB food amino acid/nucleotide data (~700 foods)"
    url = "https://www.umamiinfo.com/umamidb/"

    @property
    def output_files(self) -> list[str]:
        return ["foods.json"]

    def fetch(self) -> Path:
        """Download UmamiDB data from API endpoint."""
        from flavor_pipeline.acquisition.umamidb import fetch_umamidb

        try:
            result = fetch_umamidb(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch UmamiDB: {e}") from e

    def validate(self) -> list[str]:
        """Validate the fetched UmamiDB data."""
        import json

        errors = super().validate()
        if errors:
            return errors

        foods_path = self.output_dir / "foods.json"
        if foods_path.exists():
            try:
                with open(foods_path) as f:
                    data = json.load(f)
                if not isinstance(data, list):
                    errors.append("foods.json is not a JSON array")
                elif len(data) == 0:
                    errors.append("foods.json is empty")
                else:
                    # Check first record has expected fields
                    first = data[0]
                    required = ["sample_name_en", "category_en", "free_amino_acid05_Glu"]
                    missing = [f for f in required if f not in first]
                    if missing:
                        errors.append(f"Missing fields in foods.json: {missing}")
            except json.JSONDecodeError as e:
                errors.append(f"Invalid JSON in foods.json: {e}")
            except Exception as e:
                errors.append(f"Error reading foods.json: {e}")

        return errors
