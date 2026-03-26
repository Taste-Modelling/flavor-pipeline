"""Tier 2 merged molecule assets.

This module merges Tier 1 molecules by molecule_id to produce unified records
with multi-source attribution.
"""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetExecutionContext, asset

from flavor_pipeline.consolidation import Tier1Merger
from flavor_pipeline.schemas.tier1 import Tier1Molecule

# Output directory for Tier 2 files
TIER2_OUTPUT_DIR = Path("data/tier2")

# Input directory for Tier 1 files
TIER1_INPUT_DIR = Path("data/tier1")


def _load_tier1_molecules() -> list[Tier1Molecule]:
    """Load all Tier 1 molecules from JSON files.

    Only loads molecule files, skipping food files (which use Tier1Food schema).
    """
    all_molecules = []

    # Files that contain Tier1Food, not Tier1Molecule
    food_files = {
        "culinarydb.json",
        "foodatlas_food.json",
        "winesensed.json",
        "consolidated.json",  # Old consolidated file
    }

    for source_file in TIER1_INPUT_DIR.glob("*.json"):
        if source_file.name in food_files:
            continue

        with open(source_file) as f:
            records = json.load(f)
            for record in records:
                mol = Tier1Molecule.model_validate(record)
                all_molecules.append(mol)

    return all_molecules


def _save_tier2_to_parquet(molecules: list, output_path: Path) -> int:
    """Save Tier2 molecules to parquet file and return count."""
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


def _save_tier2_to_json(molecules: list, output_path: Path) -> int:
    """Save Tier2 molecules to JSON file and return count."""
    if not molecules:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Serialize using Pydantic's model_dump with JSON-safe settings
    records = [m.model_dump(mode="json") for m in molecules]

    with open(output_path, "w") as f:
        json.dump(records, f, indent=2, default=str)

    return len(molecules)


@asset(
    group_name="tier2",
    deps=[
        "bitterdb_tier1",
        "duke_phytochem_tier1",
        "fenaroli_tier1",
        "flavordb2_tier1",
        "foodb_tier1",
        "foodatlas_tier1",
        "fsbi_tier1",
        "leffingwell_tier1",
        "metabolights_tier1",
        "panten_tier1",
        "sweetenersdb_tier1",
        "vcf_tier1",
    ],
    description="Merged molecules by ID with multi-source attribution",
)
def merged_tier2(context: AssetExecutionContext) -> None:
    """Merge all Tier 1 molecules by molecule_id into Tier 2.

    This asset:
    1. Loads all Tier 1 JSON files
    2. Groups molecules by molecule_id
    3. Merges conflicting values with source attribution
    4. Outputs merged molecules to JSON and Parquet
    """
    # Load all tier1 molecules
    tier1_molecules = _load_tier1_molecules()
    context.log.info(f"Loaded {len(tier1_molecules)} Tier 1 molecules")

    # Merge by molecule_id
    merger = Tier1Merger(pipeline_version="1.0.0")
    tier2_molecules = merger.merge_all(tier1_molecules)
    context.log.info(f"Merged into {len(tier2_molecules)} Tier 2 molecules")

    # Count multi-source and conflicting molecules
    multi_source = sum(1 for m in tier2_molecules if m.merge_metadata.source_count > 1)
    with_conflicts = sum(1 for m in tier2_molecules if m.merge_metadata.conflict_count > 0)
    context.log.info(f"  {multi_source} molecules from multiple sources")
    context.log.info(f"  {with_conflicts} molecules with conflicts")

    # Save outputs
    json_path = TIER2_OUTPUT_DIR / "merged.json"
    parquet_path = TIER2_OUTPUT_DIR / "merged.parquet"

    _save_tier2_to_json(tier2_molecules, json_path)
    _save_tier2_to_parquet(tier2_molecules, parquet_path)

    context.log.info(f"Saved Tier 2 molecules to {json_path} and {parquet_path}")
