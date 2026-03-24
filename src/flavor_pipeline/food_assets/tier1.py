"""Tier 1 food assets - parse raw data to Tier1Food format.

These assets depend on the acquisition assets and transform raw CSV data
into Tier1Food format with AttributedValue provenance tracking.
"""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetExecutionContext, asset

from flavor_pipeline.food_sources import FooDBFoodSource, USDAFoodSource

# Output directory for Tier 1 food files
TIER1_FOOD_OUTPUT_DIR = Path("data/tier1_food")


def _save_foods_to_parquet(foods: list, output_path: Path) -> int:
    """Save foods to parquet file and return count."""
    if not foods:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert to dicts for parquet
    records = []
    for f in foods:
        record = f.model_dump()
        # PyArrow can't write empty structs to Parquet, so convert empty dicts to None
        if "extra" in record and (record["extra"] is None or record["extra"] == {}):
            record["extra"] = None
        if "composition" in record and (record["composition"] is None or record["composition"] == []):
            record["composition"] = None
        records.append(record)

    # Create table and write
    table = pa.Table.from_pylist(records)
    pq.write_table(table, output_path)

    return len(foods)


def _save_foods_to_json(foods: list, output_path: Path) -> int:
    """Save foods to JSON file and return count."""
    if not foods:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Serialize using Pydantic's model_dump with JSON-safe settings
    records = [f.model_dump(mode="json") for f in foods]

    with open(output_path, "w") as fh:
        json.dump(records, fh, indent=2, default=str)

    return len(foods)


@asset(
    group_name="tier1_food",
    deps=["foodb_raw"],
    description="FooDB foods parsed to Tier 1 format with molecular composition",
)
def foodb_tier1_food(context: AssetExecutionContext) -> None:
    """Parse FooDB raw data to Tier 1 foods.

    Depends on: foodb_raw
    """
    source = FooDBFoodSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.FOOD_FILE).exists():
            context.log.info("No FooDB food data available, skipping")
            return

    foods = source.parse()
    parquet_path = TIER1_FOOD_OUTPUT_DIR / "foodb_food.parquet"
    json_path = TIER1_FOOD_OUTPUT_DIR / "foodb_food.json"

    _save_foods_to_parquet(foods, parquet_path)
    count = _save_foods_to_json(foods, json_path)

    # Log statistics
    foods_with_composition = sum(1 for f in foods if f.composition)
    total_molecules = sum(len(f.composition) for f in foods)
    context.log.info(f"Saved {count} foods to {parquet_path} and {json_path}")
    context.log.info(f"  {foods_with_composition} foods with molecular composition")
    context.log.info(f"  {total_molecules} total molecule associations")


@asset(
    group_name="tier1_food",
    deps=["usda_raw"],
    description="USDA foods parsed to Tier 1 format with nutrient composition",
)
def usda_tier1_food(context: AssetExecutionContext) -> None:
    """Parse USDA raw data to Tier 1 foods.

    Depends on: usda_raw
    """
    source = USDAFoodSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.FOOD_FILE).exists():
            context.log.info("No USDA food data available, skipping")
            return

    foods = source.parse()
    parquet_path = TIER1_FOOD_OUTPUT_DIR / "usda_food.parquet"
    json_path = TIER1_FOOD_OUTPUT_DIR / "usda_food.json"

    _save_foods_to_parquet(foods, parquet_path)
    count = _save_foods_to_json(foods, json_path)

    # Log statistics
    foods_with_composition = sum(1 for f in foods if f.composition)
    total_molecules = sum(len(f.composition) for f in foods)
    context.log.info(f"Saved {count} foods to {parquet_path} and {json_path}")
    context.log.info(f"  {foods_with_composition} foods with molecular composition")
    context.log.info(f"  {total_molecules} total molecule associations")
