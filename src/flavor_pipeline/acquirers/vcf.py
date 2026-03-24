"""VCF (Volatile Compounds in Food) acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class VCFAcquirer(BaseAcquirer):
    """Acquire volatile compound data from VCF EU-Flavis database.

    Scrapes the EU-Flavis compound tables organized by chemical group.
    """

    name = "vcf"
    description = "Scrape VCF EU-Flavis volatile compounds (~2.7k compounds)"
    url = "https://www.vcf-online.nl/"

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "VCF"

    @property
    def output_files(self) -> list[str]:
        return ["compounds.csv"]

    def fetch(self) -> Path:
        """Scrape VCF Flavis compound tables."""
        from flavor_pipeline.acquisition.vcf import fetch_vcf

        try:
            result = fetch_vcf(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch VCF: {e}") from e
