"""Dagster assets for the flavor pipeline.

Assets are organized in three groups:
- acquisition: Fetch raw data from external sources
- tier1: Parse raw data to Tier1Molecule format with provenance tracking
- tier2: Merge molecules by ID with multi-source attribution
"""

from flavor_pipeline.assets.acquisition import (
    bitterdb_raw,
    culinarydb_raw,
    fenaroli_raw,
    flavordb2_raw,
    foodb_raw,
    foodatlas_raw,
    fsbi_raw,
    panten_raw,
    sweetenersdb_raw,
    umamidb_raw,
    vcf_raw,
    winesensed_raw,
)
from flavor_pipeline.assets.tier1 import (
    bitterdb_tier1,
    culinarydb_tier1,
    fenaroli_tier1,
    flavordb2_tier1,
    foodb_tier1,
    foodatlas_food_tier1,
    foodatlas_tier1,
    fsbi_tier1,
    leffingwell_tier1,
    panten_tier1,
    sweetenersdb_tier1,
    umamidb_tier1,
    vcf_tier1,
    winesensed_tier1,
)
from flavor_pipeline.assets.tier2 import merged_tier2

__all__ = [
    # Acquisition assets
    "flavordb2_raw",
    "bitterdb_raw",
    "fsbi_raw",
    "vcf_raw",
    "fenaroli_raw",
    "panten_raw",
    "foodb_raw",
    "foodatlas_raw",
    "culinarydb_raw",
    "sweetenersdb_raw",
    "umamidb_raw",
    "winesensed_raw",
    # Tier 1 assets
    "flavordb2_tier1",
    "bitterdb_tier1",
    "fsbi_tier1",
    "vcf_tier1",
    "fenaroli_tier1",
    "panten_tier1",
    "foodb_tier1",
    "leffingwell_tier1",
    "foodatlas_tier1",
    "foodatlas_food_tier1",
    "culinarydb_tier1",
    "sweetenersdb_tier1",
    "umamidb_tier1",
    "winesensed_tier1",
    # Tier 2 assets
    "merged_tier2",
]
