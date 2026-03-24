"""BitterDB acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class BitterDBAcquirer(BaseAcquirer):
    """Acquire bitter compound data from BitterDB 2024 release.

    Downloads 12 CSV files containing bitter compound properties,
    receptor associations, and related data.
    """

    name = "bitterdb"
    description = "Download BitterDB 2024 compound data (12 CSV files, ~2.3k compounds)"
    url = "https://bitterdb.agri.huji.ac.il/"

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "BitterDB"

    @property
    def output_files(self) -> list[str]:
        return [
            "BitterCompoundsPropA_2024.csv",
            "compoundsnamesA_2024.csv",
            "ligandReceptorsA_2024.csv",
        ]

    @property
    def all_files(self) -> list[str]:
        """All files downloaded (used for complete validation)."""
        return [
            "BitterCompoundsPropA_2024.csv",
            "cbitterresourceA_2024.csv",
            "compoundsnamesA_2024.csv",
            "compRefA_2024.csv",
            "compzinclinksA_2024.csv",
            "dbreferencesA_2024.csv",
            "drugbankcidA_2024.csv",
            "IUPHAR_cidA_2024.csv",
            "ligandReceptorsA_2024.csv",
            "mutationData_2024.csv",
            "ReceptorSearchA_2024.csv",
            "snpData_2024.csv",
        ]

    def fetch(self) -> Path:
        """Download all BitterDB CSV files."""
        from flavor_pipeline.acquisition.bitterdb import fetch_bitterdb

        try:
            result = fetch_bitterdb(output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch BitterDB: {e}") from e
