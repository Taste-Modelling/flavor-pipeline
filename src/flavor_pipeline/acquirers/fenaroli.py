"""Fenaroli handbook acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class FenaroliAcquirer(BaseAcquirer):
    """Extract flavor substance data from Fenaroli's Handbook PDF.

    Requires the PDF to be manually placed in the output directory.
    """

    name = "fenaroli"
    description = "Extract Fenaroli handbook flavor substances from PDF"
    url = None  # PDF extraction, no URL

    @property
    def output_files(self) -> list[str]:
        return ["flavor_substances.csv"]

    @property
    def input_pdf(self) -> Path:
        return self.output_dir / "fenaroli_handbook_flavor.pdf"

    def is_cached(self) -> bool:
        """Check if output exists. Also checks if input PDF is available."""
        if not self.input_pdf.exists():
            return True  # No input = skip (not an error)
        return super().is_cached()

    def fetch(self) -> Path:
        """Extract data from Fenaroli PDF."""
        if not self.input_pdf.exists():
            # PDF not available - this is expected, not an error
            self.output_dir.mkdir(parents=True, exist_ok=True)
            return self.output_dir

        from flavor_pipeline.acquisition.fenaroli import fetch_fenaroli

        try:
            result = fetch_fenaroli(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to extract Fenaroli: {e}") from e
