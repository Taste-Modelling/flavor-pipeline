"""FAO INFOODS source adapter for food composition data."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.food import IngestMetadata, Tier1Food
from flavor_pipeline.schemas.tier1 import SourceMetadata


class FAOINFOODSSource:
    """Parse FAO/INFOODS food composition data to Tier 1 foods.

    Expects raw_data/FAO_INFOODS/*.xlsx files produced by the acquirer.

    Parses multiple FAO INFOODS databases:
    - uFiSh 1.0: Global fish/seafood nutrient database
    - uPulses 1.0: Global pulse nutrient database
    - WAFCT 2019: West African Food Composition Table

    Note: Some databases (AnFooD, BioFoodComp) have complex multi-sheet structures
    that require specialized parsing.
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")
    PARSER_VERSION = "0.1.0"
    PIPELINE_VERSION = "0.1.0"

    ANFOOD_FILE = "AnFooD2.0.xlsx"
    UFISH_FILE = "uFiSh1.0.xlsx"
    UPULSES_FILE = "uPulses1.0.xlsx"
    BIOFOODCOMP_FILE = "BioFoodComp4.0.xlsx"
    DENSITY_FILE = "Density_DB_v2.0.xlsx"
    WAFCT_FILE = "WAFCT_2019.xlsx"
    PHYFOODCOMP_FILE = "PhyFoodComp_1.0.xlsx"
    PULSESDM_FILE = "PulsesDM1.0.xlsx"

    def __init__(self, raw_data_base: str | Path | None = None):
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    def name(self) -> str:
        return "fao_infoods"

    @property
    def version(self) -> str:
        return "2024.1"

    @property
    def url(self) -> str:
        return "https://www.fao.org/infoods/infoods/tables-and-databases/faoinfoods-databases/en/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "FAO_INFOODS"

    def validate(self) -> list[str]:
        errors = []
        # Check if at least one database file exists
        files = [
            self.UFISH_FILE,
            self.UPULSES_FILE,
            self.WAFCT_FILE,
        ]
        found = False
        for f in files:
            if (self.raw_data_dir / f).exists():
                found = True
                break
        if not found:
            errors.append(
                f"No FAO INFOODS database files found in {self.raw_data_dir}. "
                "Run the fao_infoods acquisition first."
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

    def get_ingest_metadata(self) -> IngestMetadata:
        """Create ingest metadata for foods."""
        return IngestMetadata(pipeline_version=self.PIPELINE_VERSION)

    def parse(self) -> list[Tier1Food]:
        """Parse FAO INFOODS databases to Tier 1 foods.

        Returns a combined list of foods from all available databases.
        """
        foods = []

        # Parse each database if available
        if (self.raw_data_dir / self.UFISH_FILE).exists():
            foods.extend(self._parse_ufish())

        if (self.raw_data_dir / self.UPULSES_FILE).exists():
            foods.extend(self._parse_upulses())

        if (self.raw_data_dir / self.WAFCT_FILE).exists():
            foods.extend(self._parse_wafct())

        return foods

    def _parse_ufish(self) -> list[Tier1Food]:
        """Parse uFiSh 1.0 (fish/seafood nutrient database)."""
        filepath = self.raw_data_dir / self.UFISH_FILE
        foods = []
        source_meta = SourceMetadata(
            name="fao_infoods_ufish",
            version="1.0",
            url="https://www.fao.org/infoods/infoods/tables-and-databases/faoinfoods-databases/en/",
            retrieved_at=self.get_source_metadata().retrieved_at,
            parser_version=self.PARSER_VERSION,
        )
        ingest_meta = self.get_ingest_metadata()

        try:
            # uFiSh has data in sheet "04 NV_sum (per 100 g EP)" with headers in row 0
            df = pd.read_excel(
                filepath,
                sheet_name="04 NV_sum (per 100 g EP)",
                header=0,
                dtype=str,
            )
            # Clean column names
            df.columns = [str(c).strip().lower() for c in df.columns]
            # Skip the second header row (row index 0 after header)
            df = df.iloc[1:].reset_index(drop=True)

            for idx, row in df.iterrows():
                food_id_val = self._nonempty(row.get("food item id"))
                name_val = self._nonempty(row.get("food name in english"))

                if not name_val:
                    continue

                food_id = f"fao_ufish:{food_id_val}" if food_id_val else f"fao_ufish:row_{idx}"

                # Build extra with nutrient data
                extra: dict[str, AttributedValue] = {}

                # Energy
                energy = self._parse_float(row.get("enerc(kj)"))
                if energy:
                    extra["energy_kj"] = self._av(energy, unit="kJ/100g")

                # Protein
                protein = self._parse_float(row.get("prot"))
                if protein:
                    extra["protein"] = self._av(protein, unit="g/100g")

                # Fat
                fat = self._parse_float(row.get("fat"))
                if fat:
                    extra["fat"] = self._av(fat, unit="g/100g")

                # State of food (raw, cooked, etc.)
                state = self._nonempty(row.get("state of food"))
                if state:
                    extra["state"] = self._av(state)

                # Habitat (wild/farmed)
                habitat = self._nonempty(row.get("habitat"))
                if habitat:
                    extra["habitat"] = self._av(habitat)

                food = Tier1Food(
                    food_id=food_id,
                    _ingest_metadata=ingest_meta,
                    _sources={source_meta.name: source_meta},
                    name=self._av(name_val),
                    category=self._av("Fish and Seafood"),
                    extra=extra,
                )
                foods.append(food)

        except Exception as e:
            print(f"Warning: Error parsing {filepath}: {e}")

        return foods

    def _parse_upulses(self) -> list[Tier1Food]:
        """Parse uPulses 1.0 (pulse/legume nutrient database)."""
        filepath = self.raw_data_dir / self.UPULSES_FILE
        foods = []
        source_meta = SourceMetadata(
            name="fao_infoods_upulses",
            version="1.0",
            url="https://www.fao.org/infoods/infoods/tables-and-databases/faoinfoods-databases/en/",
            retrieved_at=self.get_source_metadata().retrieved_at,
            parser_version=self.PARSER_VERSION,
        )
        ingest_meta = self.get_ingest_metadata()

        try:
            # uPulses has data in sheet "04 NV_sum (per 100 g EP on FW)"
            df = pd.read_excel(
                filepath,
                sheet_name="04 NV_sum (per 100 g EP on FW)",
                header=0,
                dtype=str,
            )
            df.columns = [str(c).strip().lower() for c in df.columns]
            # Skip descriptor row
            df = df.iloc[1:].reset_index(drop=True)

            for idx, row in df.iterrows():
                food_id_val = self._nonempty(row.get("food item id"))
                name_val = self._nonempty(row.get("food name in english"))

                if not name_val:
                    continue

                food_id = f"fao_upulses:{food_id_val}" if food_id_val else f"fao_upulses:row_{idx}"

                extra: dict[str, AttributedValue] = {}

                # Energy
                energy = self._parse_float(row.get("enerc(kj)"))
                if energy:
                    extra["energy_kj"] = self._av(energy, unit="kJ/100g")

                # Protein
                protein = self._parse_float(row.get("prot"))
                if protein:
                    extra["protein"] = self._av(protein, unit="g/100g")

                # Fat
                fat = self._parse_float(row.get("fat"))
                if fat:
                    extra["fat"] = self._av(fat, unit="g/100g")

                # Carbohydrates
                carbs = self._parse_float(row.get("cho"))
                if carbs:
                    extra["carbohydrate"] = self._av(carbs, unit="g/100g")

                # Fiber
                fiber = self._parse_float(row.get("fibtg"))
                if fiber:
                    extra["fiber"] = self._av(fiber, unit="g/100g")

                food = Tier1Food(
                    food_id=food_id,
                    _ingest_metadata=ingest_meta,
                    _sources={source_meta.name: source_meta},
                    name=self._av(name_val),
                    category=self._av("Legumes and Pulses"),
                    extra=extra,
                )
                foods.append(food)

        except Exception as e:
            print(f"Warning: Error parsing {filepath}: {e}")

        return foods

    def _parse_wafct(self) -> list[Tier1Food]:
        """Parse West African Food Composition Table 2019."""
        filepath = self.raw_data_dir / self.WAFCT_FILE
        foods = []
        source_meta = SourceMetadata(
            name="fao_infoods_wafct",
            version="2019",
            url="https://www.fao.org/infoods/infoods/tables-and-databases/faoinfoods-databases/en/",
            retrieved_at=self.get_source_metadata().retrieved_at,
            parser_version=self.PARSER_VERSION,
        )
        ingest_meta = self.get_ingest_metadata()

        try:
            # WAFCT has main data in sheet "03 NV_sum_39 (per 100g EP)"
            df = pd.read_excel(
                filepath,
                sheet_name="03 NV_sum_39 (per 100g EP)",
                header=0,
                dtype=str,
            )
            df.columns = [str(c).strip().lower() for c in df.columns]
            # Skip descriptor row
            df = df.iloc[1:].reset_index(drop=True)

            for idx, row in df.iterrows():
                food_code = self._nonempty(row.get("food code"))
                name_val = self._nonempty(row.get("food name in english"))

                if not name_val:
                    continue

                food_id = f"fao_wafct:{food_code}" if food_code else f"fao_wafct:row_{idx}"

                # Get scientific name and category
                scientific_name = self._nonempty(row.get("scientific name"))
                food_group = self._nonempty(row.get("food group"))
                food_subgroup = self._nonempty(row.get("food subgroup"))

                extra: dict[str, AttributedValue] = {}

                # Energy
                energy = self._parse_float(row.get("enerc (kj)"))
                if energy:
                    extra["energy_kj"] = self._av(energy, unit="kJ/100g")

                # Water
                water = self._parse_float(row.get("water (g)"))
                if water:
                    extra["water"] = self._av(water, unit="g/100g")

                # Protein
                protein = self._parse_float(row.get("prot (g)"))
                if protein:
                    extra["protein"] = self._av(protein, unit="g/100g")

                # Fat
                fat = self._parse_float(row.get("fat (g)"))
                if fat:
                    extra["fat"] = self._av(fat, unit="g/100g")

                # Carbohydrates
                carbs = self._parse_float(row.get("cho (g)"))
                if carbs:
                    extra["carbohydrate"] = self._av(carbs, unit="g/100g")

                # Fiber
                fiber = self._parse_float(row.get("fibtg (g)"))
                if fiber:
                    extra["fiber"] = self._av(fiber, unit="g/100g")

                food = Tier1Food(
                    food_id=food_id,
                    _ingest_metadata=ingest_meta,
                    _sources={source_meta.name: source_meta},
                    name=self._av(name_val),
                    scientific_name=self._av(scientific_name) if scientific_name else None,
                    category=self._av(food_group) if food_group else None,
                    subcategory=self._av(food_subgroup) if food_subgroup else None,
                    extra=extra,
                )
                foods.append(food)

        except Exception as e:
            print(f"Warning: Error parsing {filepath}: {e}")

        return foods

    def _av(self, value: Any, unit: str | None = None) -> AttributedValue:
        """Create an AttributedValue with this source."""
        return AttributedValue(value=value, unit=unit, sources=[self.name])

    def _nonempty(self, val: Any) -> str | None:
        """Return value if not empty/null, else None."""
        if val is None or pd.isna(val) or val == "":
            return None
        return str(val).strip()

    def _parse_float(self, val: Any) -> float | None:
        """Parse a value as float, return None on failure."""
        if val is None or pd.isna(val) or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
