"""Base class for food source adapters."""

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.food import IngestMetadata, MoleculeComposition, Tier1Food
from flavor_pipeline.schemas.tier1 import SourceMetadata


class BaseFoodSource(ABC):
    """Abstract base class for food source adapters.

    Each food source adapter is responsible for:
    1. Parsing raw data files from the acquisition layer
    2. Transforming records to Tier1Food format with proper attribution
    3. Providing source metadata for provenance tracking
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")
    PARSER_VERSION = "0.1.0"
    PIPELINE_VERSION = "0.1.0"

    def __init__(self, raw_data_base: str | Path | None = None):
        """Initialize the source with a base path for raw data.

        Args:
            raw_data_base: Base directory containing source-specific subdirectories.
                          Defaults to "raw_data".
        """
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the source name (e.g., 'foodb_food', 'usda_food')."""
        ...

    @property
    def version(self) -> str:
        """Return the source version. Override in subclasses."""
        return "2024.1"

    @property
    def url(self) -> str | None:
        """Return the source URL. Override in subclasses."""
        return None

    @property
    def raw_data_dir(self) -> Path:
        """Return the directory containing raw data for this source."""
        return self._raw_data_base / self.name.capitalize()

    def get_source_metadata(self, retrieved_at: datetime | None = None) -> SourceMetadata:
        """Create source metadata for this adapter."""
        return SourceMetadata(
            name=self.name,
            version=self.version,
            url=self.url,
            retrieved_at=retrieved_at or datetime.now(UTC),
            parser_version=self.PARSER_VERSION,
        )

    def get_ingest_metadata(self) -> IngestMetadata:
        """Create ingest metadata for foods."""
        return IngestMetadata(pipeline_version=self.PIPELINE_VERSION)

    def validate(self) -> list[str]:
        """Validate that raw data exists and is in expected format.

        Returns:
            List of validation error messages. Empty list if valid.
        """
        return []

    @abstractmethod
    def parse(self) -> list[Tier1Food]:
        """Parse raw data and return Tier 1 foods.

        Returns:
            List of Tier1Food objects with attributed fields.
        """
        ...

    def to_dataframe(self) -> pd.DataFrame:
        """Parse foods and convert to DataFrame."""
        foods = self.parse()
        if not foods:
            return pd.DataFrame()
        return pd.DataFrame([f.model_dump() for f in foods])

    # Helper methods for creating AttributedValues
    def _av(self, value, unit: str | None = None) -> AttributedValue:
        """Create an AttributedValue with this source."""
        return AttributedValue(value=value, unit=unit, sources=[self.name])

    def _mc(
        self,
        molecule_id: str,
        concentration: float | None = None,
        unit: str | None = None,
        concentration_min: float | None = None,
        concentration_max: float | None = None,
    ) -> MoleculeComposition:
        """Create a MoleculeComposition entry."""
        return MoleculeComposition(
            molecule_id=molecule_id,
            concentration=self._av(concentration, unit) if concentration is not None else None,
            concentration_min=self._av(concentration_min, unit)
            if concentration_min is not None
            else None,
            concentration_max=self._av(concentration_max, unit)
            if concentration_max is not None
            else None,
        )

    def _nonempty(self, val) -> str | None:
        """Return value if not empty/null, else None."""
        if pd.isna(val) or val == "":
            return None
        return str(val).strip()

    def _parse_int(self, val) -> int | None:
        """Parse a value as integer, return None on failure."""
        if pd.isna(val) or val == "":
            return None
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None

    def _parse_float(self, val) -> float | None:
        """Parse a value as float, return None on failure."""
        if pd.isna(val) or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
