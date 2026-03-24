"""Base acquirer class for data acquisition with Dagster integration."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass
class AcquisitionMetadata:
    """Metadata about an acquisition run."""

    acquired_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    source_url: str | None = None
    record_count: int | None = None
    file_sizes: dict[str, int] = field(default_factory=dict)


class BaseAcquirer(ABC):
    """Abstract base class for data acquirers.

    Subclasses implement the fetch() method to acquire data from external sources.
    The base class provides:
    - Caching/idempotency logic
    - Output directory management
    - Validation framework
    - Metadata generation for Dagster assets

    Example:
        class FlavorDB2Acquirer(BaseAcquirer):
            name = "flavordb2"
            description = "Fetch FlavorDB2 molecules with PubChem enrichment"

            @property
            def output_files(self) -> list[str]:
                return ["molecules.csv"]

            def fetch(self) -> Path:
                # Acquisition logic here
                ...
    """

    # Override these in subclasses
    name: str
    description: str
    group_name: str = "acquisition"

    def __init__(self, raw_data_base: Path | str = Path("raw_data")):
        self._raw_data_base = Path(raw_data_base)

    @property
    def output_dir(self) -> Path:
        """Directory where this acquirer writes data."""
        # Default: raw_data/{Name} (capitalized)
        return self._raw_data_base / self.name.capitalize()

    @property
    @abstractmethod
    def output_files(self) -> list[str]:
        """List of files this acquirer produces (relative to output_dir).

        Used for cache checking and validation.
        """
        ...

    @abstractmethod
    def fetch(self) -> Path:
        """Fetch data from the external source.

        Returns:
            Path to the output directory containing fetched data.

        Raises:
            AcquisitionError: If fetching fails.
        """
        ...

    def is_cached(self) -> bool:
        """Check if output already exists (idempotency check).

        Returns True if ALL expected output files exist.
        """
        for filename in self.output_files:
            if not (self.output_dir / filename).exists():
                return False
        return True

    def validate(self) -> list[str]:
        """Validate the fetched data.

        Override in subclasses to add source-specific validation.

        Returns:
            List of validation error messages. Empty if valid.
        """
        errors = []
        for filename in self.output_files:
            filepath = self.output_dir / filename
            if not filepath.exists():
                errors.append(f"Missing output file: {filepath}")
            elif filepath.stat().st_size == 0:
                errors.append(f"Empty output file: {filepath}")
        return errors

    def get_metadata(self) -> AcquisitionMetadata:
        """Get metadata about the current acquisition state."""
        file_sizes = {}
        for filename in self.output_files:
            filepath = self.output_dir / filename
            if filepath.exists():
                file_sizes[filename] = filepath.stat().st_size

        return AcquisitionMetadata(
            source_url=getattr(self, "url", None),
            file_sizes=file_sizes,
        )

    def get_asset_metadata(self) -> dict[str, Any]:
        """Get metadata dict for Dagster asset materialization."""
        meta = self.get_metadata()
        return {
            "output_dir": str(self.output_dir),
            "files": list(meta.file_sizes.keys()),
            "total_bytes": sum(meta.file_sizes.values()),
            "cached": self.is_cached(),
        }


class AcquisitionError(Exception):
    """Raised when data acquisition fails."""

    pass
