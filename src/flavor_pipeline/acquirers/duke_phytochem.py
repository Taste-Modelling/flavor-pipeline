"""Dr. Duke's Phytochemical Database acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class DukePhytochemAcquirer(BaseAcquirer):
    """Acquire phytochemical data from Dr. Duke's database.

    Downloads the bulk CSV archive containing:
    - ~29,000 phytochemicals with CAS numbers
    - ~2,400 biological activities
    - ~29,000 chemical-activity relationships
    - ~104,000 chemical-plant relationships
    - ~2,400 plant species with taxonomy
    """

    name = "duke_phytochem"
    description = "Download Dr. Duke's Phytochemical database (~29k chemicals)"
    url = "https://phytochem.nal.usda.gov/"

    @property
    def output_files(self) -> list[str]:
        return [
            "CHEMICALS.csv",
            "ACTIVITIES.csv",
            "AGGREGAC.csv",
            "FARMACY_NEW.csv",
            "FNFTAX.csv",
        ]

    def fetch(self) -> Path:
        """Download Dr. Duke's Phytochemical database."""
        from flavor_pipeline.acquisition.duke_phytochem import fetch_duke_phytochem

        try:
            result = fetch_duke_phytochem(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch Duke Phytochem: {e}") from e
