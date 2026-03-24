"""Dagster assets for the flavor pipeline.

Assets are organized in two groups:
- acquisition: Fetch raw data from external sources
- tier1: Parse raw data to Tier1Molecule format with provenance tracking
"""

from flavor_pipeline.assets.acquisition import (
    bitterdb_raw,
    fenaroli_raw,
    flavordb2_raw,
    fsbi_raw,
    panten_raw,
    vcf_raw,
)
from flavor_pipeline.assets.tier1 import (
    bitterdb_tier1,
    fenaroli_tier1,
    flavordb2_tier1,
    fsbi_tier1,
    leffingwell_tier1,
    panten_tier1,
    vcf_tier1,
)

__all__ = [
    # Acquisition assets
    "flavordb2_raw",
    "bitterdb_raw",
    "fsbi_raw",
    "vcf_raw",
    "fenaroli_raw",
    "panten_raw",
    # Tier 1 assets
    "flavordb2_tier1",
    "bitterdb_tier1",
    "fsbi_tier1",
    "vcf_tier1",
    "fenaroli_tier1",
    "panten_tier1",
    "leffingwell_tier1",
]
