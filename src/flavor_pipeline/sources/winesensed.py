"""WineSensed source adapter for wines.

WineSensed is a multimodal wine dataset from NeurIPS 2023 containing
350k+ wines with metadata, reviews, and pairwise flavor similarity data
from tasting experiments.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.food import IngestMetadata as FoodIngestMetadata
from flavor_pipeline.schemas.food import Tier1Food
from flavor_pipeline.schemas.tier1 import SourceMetadata


def _is_missing(value: Any) -> bool:
    """Check if a value is missing/empty."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value == "":
        return True
    return False


class WineSensedSource:
    """Parse WineSensed wines to Tier1Food format.

    Extracts wine vintages with metadata (grapes, region, country, alcohol,
    price, rating) and reviews containing flavor descriptions.

    Note: This does not inherit from BaseSource since it parses foods (wines),
    not molecules. It provides a compatible interface for the Tier1 food
    asset pipeline.
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")
    PARSER_VERSION = "0.1.0"
    PIPELINE_VERSION = "0.1.0"

    WINES_FILE = "images_reviews_attributes.csv"

    def __init__(self, raw_data_base: str | Path | None = None):
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    def name(self) -> str:
        return "winesensed"

    @property
    def version(self) -> str:
        return "1.0.0"  # NeurIPS 2023 release

    @property
    def url(self) -> str:
        return "https://huggingface.co/datasets/Dakhoo/L2T-NeurIPS-2023"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "WineSensed"

    def validate(self) -> list[str]:
        errors = []
        wines_path = self.raw_data_dir / self.WINES_FILE

        if not wines_path.exists():
            errors.append(
                f"Missing file: {wines_path}. "
                "Run: python -m flavor_pipeline.acquisition.winesensed"
            )

        return errors

    def get_source_metadata(self, retrieved_at: datetime | None = None) -> SourceMetadata:
        """Create source metadata for this adapter."""
        return SourceMetadata(
            name=self.name,
            version=self.version,
            url=self.url,
            retrieved_at=retrieved_at or datetime.now(UTC),
            parser_version=self.PARSER_VERSION,
        )

    def get_ingest_metadata(self) -> FoodIngestMetadata:
        """Create ingest metadata for foods."""
        return FoodIngestMetadata(pipeline_version=self.PIPELINE_VERSION)

    def parse(self) -> list[Tier1Food]:
        """Parse WineSensed wines to Tier 1 foods.

        The dataset has multiple rows per vintage (one per review). We group by
        vintage_id, take metadata from the first complete row, and aggregate reviews.
        """
        wines_path = self.raw_data_dir / self.WINES_FILE
        if not wines_path.exists():
            return []

        # Load wines data
        df = pd.read_csv(wines_path, dtype=str, low_memory=False)

        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        # Filter to rows with actual wine metadata (name required)
        # This gives us ~50k wines instead of 1M+ review rows
        wines_with_metadata = df[df["wine"].notna()].copy()

        # Group by vintage_id and aggregate
        # Take first non-null value for each metadata field
        # Collect all reviews
        grouped = wines_with_metadata.groupby("vintage_id", as_index=False).agg({
            "wine": "first",
            "year": "first",
            "country": "first",
            "region": "first",
            "grape": "first",
            "wine_alcohol": "first",
            "price": "first",
            "rating": "first",
            "winery_id": "first",
            "review": lambda x: " | ".join(r for r in x.dropna().head(3)),  # First 3 reviews
        })

        foods = []
        for _, row in grouped.iterrows():
            vintage_id = self._nonempty(row.get("vintage_id"))
            if vintage_id is None:
                continue

            food_id = f"winesensed:{vintage_id}"

            # Wine name
            wine_name = self._nonempty(row.get("wine"))
            if not wine_name:
                continue

            # Build full name with year if available
            year = self._nonempty(row.get("year"))
            full_name = wine_name
            if year:
                # Year may be stored as float string like "2018.0"
                try:
                    year_int = int(float(year))
                    full_name = f"{wine_name} {year_int}"
                    year = str(year_int)
                except (ValueError, TypeError):
                    pass

            # Region info
            country = self._nonempty(row.get("country"))
            region = self._nonempty(row.get("region"))

            # Category from grapes (e.g., "Pinot Noir, Chardonnay")
            grapes = self._nonempty(row.get("grape"))

            # Aggregated reviews
            reviews = self._nonempty(row.get("review"))

            # Numeric attributes
            alcohol = self._parse_float(row.get("wine_alcohol"))
            price = self._parse_float(row.get("price"))
            rating = self._parse_float(row.get("rating"))

            # Winery info
            winery_id = self._nonempty(row.get("winery_id"))

            # Build extra fields
            extra: dict[str, AttributedValue] = {
                "vintage_id": self._av(vintage_id),
            }

            if year:
                extra["year"] = self._av(int(year) if year.isdigit() else year)
            if grapes:
                extra["grapes"] = self._av(grapes)
            if region:
                extra["region"] = self._av(region.strip())
            if country:
                extra["country"] = self._av(country)
            if alcohol is not None:
                extra["alcohol"] = self._av(alcohol, unit="%")
            if price is not None:
                extra["price"] = self._av(price, unit="USD")
            if rating is not None:
                extra["rating"] = self._av(rating)
            if winery_id:
                extra["winery_id"] = self._av(winery_id)
            if reviews:
                # Truncate combined reviews
                review_text = reviews[:2000] if len(reviews) > 2000 else reviews
                extra["reviews"] = self._av(review_text)

            food = Tier1Food(
                food_id=food_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                name=self._av(full_name),
                # Category is "Wine" with grape varieties as subcategory
                category=self._av("Wine"),
                subcategory=self._av(grapes) if grapes else None,
                # Use first review excerpt as description
                description=self._av(reviews[:500]) if reviews else None,
                extra=extra,
            )
            foods.append(food)

        return foods

    def _av(self, value: Any, unit: str | None = None) -> AttributedValue:
        """Create an AttributedValue with this source."""
        return AttributedValue(value=value, unit=unit, sources=[self.name])

    def _nonempty(self, val: Any) -> str | None:
        """Return value if not empty/null, else None."""
        if _is_missing(val):
            return None
        return str(val).strip()

    def _parse_float(self, val: Any) -> float | None:
        """Parse a value as float, return None on failure."""
        if _is_missing(val):
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
