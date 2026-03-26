"""MetaboLights acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class MetaboLightsAcquirer(BaseAcquirer):
    """Acquire metabolite/compound data from MetaboLights.

    Downloads compound data via the MetaboLights REST API:
    - ~33,000 reference compounds with chemical identifiers
    - InChI, InChIKey, ChEBI IDs, molecular formulas
    - Compound names and descriptions

    Note: Full download takes significant time due to API rate limits.
    """

    name = "metabolights"
    description = "Download MetaboLights compound data (~33k metabolites)"
    url = "https://www.ebi.ac.uk/metabolights/"

    @property
    def output_files(self) -> list[str]:
        return ["compounds.json"]

    def fetch(self) -> Path:
        """Download MetaboLights compound data."""
        from flavor_pipeline.acquisition.metabolights import fetch_metabolights

        try:
            result = fetch_metabolights(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch MetaboLights: {e}") from e
