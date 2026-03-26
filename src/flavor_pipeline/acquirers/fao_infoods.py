"""FAO INFOODS acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class FAOINFOODSAcquirer(BaseAcquirer):
    """Acquire food composition data from FAO/INFOODS databases.

    Downloads multiple FAO/INFOODS food composition databases:
    - AnFooD 2.0: Analytical Food Composition Database
    - uFiSh 1.0: Global fish/seafood nutrient database
    - uPulses 1.0: Global pulse nutrient database
    - BioFoodComp 4.0: Food biodiversity repository
    - Density Database v2.0: Food density values
    """

    name = "fao_infoods"
    description = "Download FAO/INFOODS food composition databases (8 databases)"
    url = "https://www.fao.org/infoods/infoods/tables-and-databases/faoinfoods-databases/en/"

    @property
    def output_files(self) -> list[str]:
        return [
            "AnFooD2.0.xlsx",
            "uFiSh1.0.xlsx",
            "uPulses1.0.xlsx",
            "BioFoodComp4.0.xlsx",
            "Density_DB_v2.0.xlsx",
            "WAFCT_2019.xlsx",
            "PhyFoodComp_1.0.xlsx",
            "PulsesDM1.0.xlsx",
        ]

    def fetch(self) -> Path:
        """Download FAO INFOODS databases."""
        from flavor_pipeline.acquisition.fao_infoods import fetch_fao_infoods

        try:
            result = fetch_fao_infoods(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch FAO INFOODS: {e}") from e
