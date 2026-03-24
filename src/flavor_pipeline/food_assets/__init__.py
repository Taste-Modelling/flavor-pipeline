"""Dagster assets for the food pipeline.

Assets are organized in two tiers:
- tier1: Parse raw data to Tier1Food format with provenance tracking
- tier2: Merge foods by ID with multi-source attribution
"""

from flavor_pipeline.food_assets.tier1 import (
    foodb_tier1_food,
    usda_tier1_food,
)
from flavor_pipeline.food_assets.tier2 import merged_tier2_food

__all__ = [
    # Tier 1 food assets
    "foodb_tier1_food",
    "usda_tier1_food",
    # Tier 2 food assets
    "merged_tier2_food",
]
