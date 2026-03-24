"""Tier 2 merged food assets.

This module merges Tier 1 foods by food_id to produce unified records
with multi-source attribution.
"""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetExecutionContext, asset

from flavor_pipeline.food_consolidation import Tier1FoodMerger
from flavor_pipeline.schemas.food import Tier1Food

# Output directory for Tier 2 food files
TIER2_FOOD_OUTPUT_DIR = Path("data/tier2_food")

# Input directory for Tier 1 food files
TIER1_FOOD_INPUT_DIR = Path("data/tier1_food")


def _load_tier1_foods() -> list[Tier1Food]:
    """Load all Tier 1 foods from JSON files."""
    all_foods = []

    for source_file in TIER1_FOOD_INPUT_DIR.glob("*.json"):
        with open(source_file) as f:
            records = json.load(f)
            for record in records:
                food = Tier1Food.model_validate(record)
                all_foods.append(food)

    return all_foods


def _save_tier2_foods_to_parquet(foods: list, output_path: Path) -> int:
    """Save Tier2 foods to parquet file and return count."""
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
        if "composition" in record and (record["composition"] is None or record["composition"] == {}):
            record["composition"] = None
        records.append(record)

    # Create table and write
    table = pa.Table.from_pylist(records)
    pq.write_table(table, output_path)

    return len(foods)


def _save_tier2_foods_to_json(foods: list, output_path: Path) -> int:
    """Save Tier2 foods to JSON file and return count."""
    if not foods:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Serialize using Pydantic's model_dump with JSON-safe settings
    records = [f.model_dump(mode="json") for f in foods]

    with open(output_path, "w") as fh:
        json.dump(records, fh, indent=2, default=str)

    return len(foods)


@asset(
    group_name="tier2_food",
    deps=[
        "foodb_tier1_food",
        "usda_tier1_food",
    ],
    description="Merged foods by ID with multi-source attribution",
)
def merged_tier2_food(context: AssetExecutionContext) -> None:
    """Merge all Tier 1 foods by food_id into Tier 2.

    This asset:
    1. Loads all Tier 1 food JSON files
    2. Groups foods by food_id
    3. Merges conflicting values with source attribution
    4. Outputs merged foods to JSON and Parquet
    """
    # Load all tier1 foods
    tier1_foods = _load_tier1_foods()
    context.log.info(f"Loaded {len(tier1_foods)} Tier 1 foods")

    if not tier1_foods:
        context.log.warning("No Tier 1 foods found, skipping merge")
        return

    # Merge by food_id
    merger = Tier1FoodMerger(pipeline_version="1.0.0")
    tier2_foods = merger.merge_all(tier1_foods)
    context.log.info(f"Merged into {len(tier2_foods)} Tier 2 foods")

    # Count statistics
    multi_source = sum(1 for f in tier2_foods if f.merge_metadata.source_count > 1)
    with_conflicts = sum(1 for f in tier2_foods if f.merge_metadata.conflict_count > 0)
    with_composition = sum(1 for f in tier2_foods if f.composition)
    total_molecules = sum(len(f.composition) for f in tier2_foods)

    context.log.info(f"  {multi_source} foods from multiple sources")
    context.log.info(f"  {with_conflicts} foods with conflicts")
    context.log.info(f"  {with_composition} foods with molecular composition")
    context.log.info(f"  {total_molecules} unique molecule associations")

    # Save outputs
    json_path = TIER2_FOOD_OUTPUT_DIR / "merged_food.json"
    parquet_path = TIER2_FOOD_OUTPUT_DIR / "merged_food.parquet"

    _save_tier2_foods_to_json(tier2_foods, json_path)
    _save_tier2_foods_to_parquet(tier2_foods, parquet_path)

    context.log.info(f"Saved Tier 2 foods to {json_path} and {parquet_path}")
