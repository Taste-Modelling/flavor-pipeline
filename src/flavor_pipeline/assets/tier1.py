"""Tier 1 molecule assets - parse raw data to Tier1Molecule format.

These assets depend on the acquisition assets and transform raw CSV data
into Tier1Molecule format with AttributedValue provenance tracking.
"""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetExecutionContext, asset

from flavor_pipeline.sources import (
    BitterDBSource,
    FenaroliSource,
    FlavorDB2Source,
    FooDBSource,
    FSBISource,
    LeffingwellSource,
    PantenSource,
    VCFSource,
)
from flavor_pipeline.sources.culinarydb import CulinaryDBRecipeSource
from flavor_pipeline.sources.foodatlas import FoodAtlasFoodSource, FoodAtlasMoleculeSource
from flavor_pipeline.sources.winesensed import WineSensedSource

# Output directory for Tier 1 parquet files
TIER1_OUTPUT_DIR = Path("data/tier1")


def _save_molecules_to_parquet(molecules: list, output_path: Path) -> int:
    """Save molecules to parquet file and return count."""
    if not molecules:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert to dicts for parquet
    records = []
    for m in molecules:
        record = m.model_dump()
        # PyArrow can't write empty structs to Parquet, so convert empty dicts to None
        if "extra" in record and (record["extra"] is None or record["extra"] == {}):
            record["extra"] = None
        records.append(record)

    # Create table and write
    table = pa.Table.from_pylist(records)
    pq.write_table(table, output_path)

    return len(molecules)


def _save_molecules_to_json(molecules: list, output_path: Path) -> int:
    """Save molecules to JSON file and return count."""
    if not molecules:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Serialize using Pydantic's model_dump with JSON-safe settings
    records = [m.model_dump(mode="json") for m in molecules]

    with open(output_path, "w") as f:
        json.dump(records, f, indent=2, default=str)

    return len(molecules)


@asset(
    group_name="tier1",
    deps=["flavordb2_raw"],
    description="FlavorDB2 molecules parsed to Tier 1 format with flavor descriptors",
)
def flavordb2_tier1(context: AssetExecutionContext) -> None:
    """Parse FlavorDB2 raw data to Tier 1 molecules.

    Depends on: flavordb2_raw
    """
    source = FlavorDB2Source()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "flavordb2.parquet"
    json_path = TIER1_OUTPUT_DIR / "flavordb2.json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")


@asset(
    group_name="tier1",
    deps=["bitterdb_raw"],
    description="BitterDB compounds parsed to Tier 1 format with bitter taste attribution",
)
def bitterdb_tier1(context: AssetExecutionContext) -> None:
    """Parse BitterDB raw data to Tier 1 molecules.

    Depends on: bitterdb_raw
    """
    source = BitterDBSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "bitterdb.parquet"
    json_path = TIER1_OUTPUT_DIR / "bitterdb.json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")


@asset(
    group_name="tier1",
    deps=["fsbi_raw"],
    description="FSBI-DB compounds parsed to Tier 1 format with odor/taste descriptors",
)
def fsbi_tier1(context: AssetExecutionContext) -> None:
    """Parse FSBI raw data to Tier 1 molecules.

    Depends on: fsbi_raw
    """
    source = FSBISource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "fsbi.parquet"
    json_path = TIER1_OUTPUT_DIR / "fsbi.json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")


@asset(
    group_name="tier1",
    deps=["vcf_raw"],
    description="VCF EU-Flavis compounds parsed to Tier 1 format",
)
def vcf_tier1(context: AssetExecutionContext) -> None:
    """Parse VCF raw data to Tier 1 molecules.

    Depends on: vcf_raw
    """
    source = VCFSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "vcf.parquet"
    json_path = TIER1_OUTPUT_DIR / "vcf.json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")


@asset(
    group_name="tier1",
    deps=["fenaroli_raw"],
    description="Fenaroli handbook substances parsed to Tier 1 format",
)
def fenaroli_tier1(context: AssetExecutionContext) -> None:
    """Parse Fenaroli raw data to Tier 1 molecules.

    Depends on: fenaroli_raw
    """
    source = FenaroliSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.SUBSTANCES_FILE).exists():
            context.log.info("No Fenaroli data available, skipping")
            return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "fenaroli.parquet"
    json_path = TIER1_OUTPUT_DIR / "fenaroli.json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")


@asset(
    group_name="tier1",
    deps=["panten_raw"],
    description="Panten handbook compounds parsed to Tier 1 format",
)
def panten_tier1(context: AssetExecutionContext) -> None:
    """Parse Panten raw data to Tier 1 molecules.

    Depends on: panten_raw
    """
    source = PantenSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.COMPOUNDS_FILE).exists():
            context.log.info("No Panten data available, skipping")
            return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "panten.parquet"
    json_path = TIER1_OUTPUT_DIR / "panten.json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")


@asset(
    group_name="tier1",
    deps=["foodb_raw"],
    description="FooDB food compounds parsed to Tier 1 format with flavor associations",
)
def foodb_tier1(context: AssetExecutionContext) -> None:
    """Parse FooDB raw data to Tier 1 molecules.

    Depends on: foodb_raw
    """
    source = FooDBSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.COMPOUND_FILE).exists():
            context.log.info("No FooDB data available, skipping")
            return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "foodb.parquet"
    json_path = TIER1_OUTPUT_DIR / "foodb.json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")


@asset(
    group_name="tier1",
    description="Leffingwell flavor/fragrance data parsed to Tier 1 format (proprietary)",
)
def leffingwell_tier1(context: AssetExecutionContext) -> None:
    """Parse Leffingwell raw data to Tier 1 molecules.

    Note: Leffingwell is proprietary data with no acquisition asset.
    """
    source = LeffingwellSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.MOLECULES_FILE).exists():
            context.log.info("No Leffingwell data available (proprietary), skipping")
            return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "leffingwell.parquet"
    json_path = TIER1_OUTPUT_DIR / "leffingwell.json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")


@asset(
    group_name="tier1",
    deps=["foodatlas_raw"],
    description="FoodAtlas chemicals parsed to Tier 1 format with flavor descriptors",
)
def foodatlas_tier1(context: AssetExecutionContext) -> None:
    """Parse FoodAtlas raw data to Tier 1 molecules.

    Depends on: foodatlas_raw
    """
    source = FoodAtlasMoleculeSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.ENTITIES_FILE).exists():
            context.log.info("No FoodAtlas data available, skipping")
            return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "foodatlas.parquet"
    json_path = TIER1_OUTPUT_DIR / "foodatlas.json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")


@asset(
    group_name="tier1",
    deps=["foodatlas_raw"],
    description="FoodAtlas foods parsed to Tier 1 format with molecular composition",
)
def foodatlas_food_tier1(context: AssetExecutionContext) -> None:
    """Parse FoodAtlas raw data to Tier 1 foods.

    Depends on: foodatlas_raw
    """
    source = FoodAtlasFoodSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.ENTITIES_FILE).exists():
            context.log.info("No FoodAtlas data available, skipping")
            return

    foods = source.parse()

    # Save foods to JSON
    json_path = TIER1_OUTPUT_DIR / "foodatlas_food.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)

    records = [f.model_dump(mode="json") for f in foods]
    with open(json_path, "w") as f:
        json.dump(records, f, indent=2, default=str)

    context.log.info(f"Saved {len(foods)} foods to {json_path}")


@asset(
    group_name="tier1",
    deps=["culinarydb_raw"],
    description="CulinaryDB recipes parsed to Tier 1 format with ingredient composition",
)
def culinarydb_tier1(context: AssetExecutionContext) -> None:
    """Parse CulinaryDB raw data to Tier 1 foods (recipes).

    Depends on: culinarydb_raw
    """
    source = CulinaryDBRecipeSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.RECIPE_FILE).exists():
            context.log.info("No CulinaryDB data available, skipping")
            return

    foods = source.parse()

    # Save foods to JSON
    json_path = TIER1_OUTPUT_DIR / "culinarydb.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)

    records = [f.model_dump(mode="json") for f in foods]
    with open(json_path, "w") as f:
        json.dump(records, f, indent=2, default=str)

    context.log.info(f"Saved {len(foods)} recipes to {json_path}")


@asset(
    group_name="tier1",
    deps=["winesensed_raw"],
    description="WineSensed wines parsed to Tier 1 format with flavor reviews",
)
def winesensed_tier1(context: AssetExecutionContext) -> None:
    """Parse WineSensed raw data to Tier 1 foods (wines).

    Depends on: winesensed_raw
    """
    source = WineSensedSource()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        if not (source.raw_data_dir / source.WINES_FILE).exists():
            context.log.info("No WineSensed data available, skipping")
            return

    foods = source.parse()

    # Save wines to JSON
    json_path = TIER1_OUTPUT_DIR / "winesensed.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)

    records = [f.model_dump(mode="json") for f in foods]
    with open(json_path, "w") as f:
        json.dump(records, f, indent=2, default=str)

    context.log.info(f"Saved {len(foods)} wines to {json_path}")


