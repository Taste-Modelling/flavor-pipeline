"""FSBI-DB acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class FSBIAcquirer(BaseAcquirer):
    """Acquire flavor compound data from FSBI-DB.

    Scrapes compound detail pages from the Flavor Science Basic Information
    Database, extracting sensory qualities and chemical identifiers.
    """

    name = "fsbi"
    description = "Scrape FSBI-DB flavor compounds (~2.5k compounds with sensory data)"
    url = "https://fsbi-db.de/"

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "FSBI"

    @property
    def output_files(self) -> list[str]:
        return ["compounds.csv"]

    def fetch(self) -> Path:
        """Scrape FSBI-DB compound pages."""
        from flavor_pipeline.acquisition.fsbi import fetch_fsbi

        try:
            result = fetch_fsbi(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch FSBI: {e}") from e
