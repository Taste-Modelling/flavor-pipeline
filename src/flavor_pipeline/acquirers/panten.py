"""Panten handbook acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class PantenAcquirer(BaseAcquirer):
    """Extract fragrance/flavor data from Panten's handbook PDF.

    Requires the PDF to be manually placed in the output directory.
    """

    name = "panten"
    description = "Extract Panten handbook fragrance/flavor compounds from PDF (~350 compounds)"
    url = None  # PDF extraction, no URL

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "Panten"

    @property
    def output_files(self) -> list[str]:
        return ["compounds.csv"]

    def _find_input_pdf(self) -> Path | None:
        """Find the input PDF (filename varies)."""
        if not self.output_dir.exists():
            return None
        pdfs = list(self.output_dir.glob("*.pdf"))
        return pdfs[0] if pdfs else None

    def is_cached(self) -> bool:
        """Check if output exists. Also checks if input PDF is available."""
        if self._find_input_pdf() is None:
            return True  # No input = skip
        return super().is_cached()

    def fetch(self) -> Path:
        """Extract data from Panten PDF."""
        input_pdf = self._find_input_pdf()
        if input_pdf is None:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            return self.output_dir

        from flavor_pipeline.acquisition.panten import fetch_panten

        try:
            result = fetch_panten(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to extract Panten: {e}") from e
