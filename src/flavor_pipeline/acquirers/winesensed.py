"""WineSensed acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class WineSensedAcquirer(BaseAcquirer):
    """Acquire WineSensed dataset from Hugging Face.

    Downloads WineSensed (NeurIPS 2023) containing 350k+ wine vintages with
    metadata, 824k reviews, and 5k+ pairwise flavor similarity rankings
    from tasting experiments.

    License: CC BY-NC-ND 4.0
    """

    name = "winesensed"
    description = "Download WineSensed dataset (350k wines, 824k reviews, flavor similarities)"
    url = "https://huggingface.co/datasets/Dakhoo/L2T-NeurIPS-2023"

    @property
    def output_files(self) -> list[str]:
        return [
            "images_reviews_attributes.csv",
            "napping.csv",
            "participants.csv",
        ]

    def fetch(self) -> Path:
        """Download WineSensed data from Hugging Face."""
        from flavor_pipeline.acquisition.winesensed import fetch_winesensed

        try:
            result = fetch_winesensed(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch WineSensed: {e}") from e

    def validate(self) -> list[str]:
        """Validate the fetched WineSensed data."""
        errors = super().validate()

        # Check that wines file has expected columns
        wines_path = self.output_dir / "images_reviews_attributes.csv"
        if wines_path.exists():
            try:
                import pandas as pd

                # Read just the header to check columns
                df = pd.read_csv(wines_path, nrows=0)
                required_cols = ["vintage_id", "wine", "country"]
                missing = [c for c in required_cols if c not in df.columns]
                if missing:
                    errors.append(
                        f"Missing columns in images_reviews_attributes.csv: {missing}"
                    )
            except Exception as e:
                errors.append(f"Error reading images_reviews_attributes.csv: {e}")

        return errors
