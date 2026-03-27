"""Dagster assets for food flavor composition export.

This module defines the Dagster assets for generating the food-to-flavor-molecule
composition dataset.
"""

import csv
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetExecutionContext, asset

from flavor_pipeline.derived.food_composition.schemas import FoodFlavorComposition
from flavor_pipeline.derived.food_composition.sources import (
    FoodAtlasFlavorFoodSource,
    FooDBFlavorFoodSource,
)

# Output directory for food flavor composition
OUTPUT_DIR = Path("data/derived/food_composition")


def _save_to_csv(records: list[FoodFlavorComposition], output_path: Path) -> int:
    """Save records to CSV file.

    Args:
        records: List of FoodFlavorComposition records.
        output_path: Path to output CSV file.

    Returns:
        Number of records saved.
    """
    if not records:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "food_name",
        "scientific_name",
        "food_part",
        "food_category",
        "molecule_name",
        "cas",
        "pubchem_id",
        "inchikey",
        "molecule_id",
        "concentration",
        "concentration_min",
        "concentration_max",
        "concentration_unit",
        "flavor_descriptors",
        "source",
        "source_food_id",
        "source_molecule_id",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for record in records:
            row = record.model_dump()
            # Convert list to semicolon-separated string for CSV
            if row.get("flavor_descriptors"):
                row["flavor_descriptors"] = ";".join(row["flavor_descriptors"])
            else:
                row["flavor_descriptors"] = ""
            writer.writerow(row)

    return len(records)


def _save_to_parquet(records: list[FoodFlavorComposition], output_path: Path) -> int:
    """Save records to Parquet file.

    Args:
        records: List of FoodFlavorComposition records.
        output_path: Path to output Parquet file.

    Returns:
        Number of records saved.
    """
    if not records:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert to dicts for parquet
    rows = [r.model_dump() for r in records]

    table = pa.Table.from_pylist(rows)
    pq.write_table(table, output_path)

    return len(records)


@asset(
    group_name="derived",
    deps=["foodb_raw"],
    description="FooDB food-flavor-molecule composition data",
)
def foodb_food_flavor_composition(context: AssetExecutionContext) -> None:
    """Parse FooDB food-compound associations filtered to flavor compounds.

    Outputs:
    - data/derived/food_composition/foodb_food_flavor.csv
    - data/derived/food_composition/foodb_food_flavor.parquet
    """
    source = FooDBFlavorFoodSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        # Check if critical files are missing
        if not (source.raw_data_dir / source.FOOD_FILE).exists():
            context.log.info("FooDB data not available, skipping")
            return

    records = source.parse()

    csv_path = OUTPUT_DIR / "foodb_food_flavor.csv"
    parquet_path = OUTPUT_DIR / "foodb_food_flavor.parquet"

    _save_to_csv(records, csv_path)
    count = _save_to_parquet(records, parquet_path)

    # Log statistics
    unique_foods = len({r.food_name for r in records})
    unique_molecules = len({r.molecule_id for r in records})
    with_concentration = sum(1 for r in records if r.concentration is not None)
    with_flavor = sum(1 for r in records if r.flavor_descriptors)

    context.log.info(f"Saved {count} FooDB food-flavor composition records")
    context.log.info(f"  {unique_foods} unique foods")
    context.log.info(f"  {unique_molecules} unique flavor molecules")
    context.log.info(f"  {with_concentration} records with concentration data")
    context.log.info(f"  {with_flavor} records with flavor descriptors")


@asset(
    group_name="derived",
    deps=["foodatlas_raw"],
    description="FoodAtlas food-flavor-molecule composition data",
)
def foodatlas_food_flavor_composition(context: AssetExecutionContext) -> None:
    """Parse FoodAtlas food-chemical associations filtered to flavor compounds.

    Outputs:
    - data/derived/food_composition/foodatlas_food_flavor.csv
    - data/derived/food_composition/foodatlas_food_flavor.parquet
    """
    source = FoodAtlasFlavorFoodSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.ENTITIES_FILE).exists():
            context.log.info("FoodAtlas data not available, skipping")
            return

    records = source.parse()

    csv_path = OUTPUT_DIR / "foodatlas_food_flavor.csv"
    parquet_path = OUTPUT_DIR / "foodatlas_food_flavor.parquet"

    _save_to_csv(records, csv_path)
    count = _save_to_parquet(records, parquet_path)

    # Log statistics
    unique_foods = len({r.food_name for r in records})
    unique_molecules = len({r.molecule_id for r in records})
    with_concentration = sum(1 for r in records if r.concentration is not None)
    with_flavor = sum(1 for r in records if r.flavor_descriptors)

    context.log.info(f"Saved {count} FoodAtlas food-flavor composition records")
    context.log.info(f"  {unique_foods} unique foods")
    context.log.info(f"  {unique_molecules} unique flavor molecules")
    context.log.info(f"  {with_concentration} records with concentration data")
    context.log.info(f"  {with_flavor} records with flavor descriptors")


@asset(
    group_name="derived",
    deps=[
        "foodb_food_flavor_composition",
        "foodatlas_food_flavor_composition",
    ],
    description="Merged food-flavor-molecule composition from all sources",
)
def food_flavor_composition(context: AssetExecutionContext) -> None:
    """Merge food-flavor composition from FooDB and FoodAtlas.

    This asset combines records from both sources, keeping all records
    with source attribution. No deduplication is performed since the
    same food-molecule pair from different sources may have different
    concentration values.

    Outputs:
    - data/derived/food_composition/food_flavor_composition.csv
    - data/derived/food_composition/food_flavor_composition.parquet
    """
    all_records: list[FoodFlavorComposition] = []

    # Load FooDB records
    foodb_parquet = OUTPUT_DIR / "foodb_food_flavor.parquet"
    if foodb_parquet.exists():
        foodb_table = pq.read_table(foodb_parquet)
        for row in foodb_table.to_pylist():
            all_records.append(FoodFlavorComposition.model_validate(row))
        context.log.info(f"Loaded {len(foodb_table)} records from FooDB")

    # Load FoodAtlas records
    foodatlas_parquet = OUTPUT_DIR / "foodatlas_food_flavor.parquet"
    if foodatlas_parquet.exists():
        foodatlas_table = pq.read_table(foodatlas_parquet)
        for row in foodatlas_table.to_pylist():
            all_records.append(FoodFlavorComposition.model_validate(row))
        context.log.info(f"Loaded {len(foodatlas_table)} records from FoodAtlas")

    if not all_records:
        context.log.warning("No food-flavor composition records found")
        return

    # Save merged output
    csv_path = OUTPUT_DIR / "food_flavor_composition.csv"
    parquet_path = OUTPUT_DIR / "food_flavor_composition.parquet"

    _save_to_csv(all_records, csv_path)
    count = _save_to_parquet(all_records, parquet_path)

    # Log statistics
    unique_foods = len({r.food_name for r in all_records})
    unique_molecules = len({r.molecule_id for r in all_records})
    with_cas = sum(1 for r in all_records if r.cas)
    with_concentration = sum(1 for r in all_records if r.concentration is not None)
    sources = {r.source for r in all_records}

    context.log.info(f"Saved {count} merged food-flavor composition records")
    context.log.info(f"  Sources: {', '.join(sorted(sources))}")
    context.log.info(f"  {unique_foods} unique foods")
    context.log.info(f"  {unique_molecules} unique flavor molecules")
    context.log.info(f"  {with_cas} records with CAS numbers")
    context.log.info(f"  {with_concentration} records with concentration data")
    context.log.info(f"  Output: {csv_path}")
