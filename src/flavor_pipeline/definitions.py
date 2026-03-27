"""Dagster definitions for the flavor pipeline.

This module provides two approaches for asset registration:

1. Factory approach (recommended): Auto-generate assets from acquirers
2. Manual approach: Explicitly define assets with @asset decorator

The factory approach ensures consistent behavior across all acquisition assets.
"""

from dagster import Definitions, load_assets_from_modules

from flavor_pipeline.acquirers.factory import create_acquisition_assets
from flavor_pipeline.assets import tier1, tier2
from flavor_pipeline.derived.food_composition import assets as food_composition_assets
from flavor_pipeline.food_assets import tier1 as food_tier1
from flavor_pipeline.food_assets import tier2 as food_tier2

# Approach 1: Generate acquisition assets from acquirer classes
# This ensures all acquirers follow the same patterns
acquisition_assets = create_acquisition_assets()

# Approach 2: Load tier1 and tier2 assets from the assets module
# (these still use the manual @asset decorator approach)
# Molecule pipeline assets
tier1_assets = load_assets_from_modules([tier1])
tier2_assets = load_assets_from_modules([tier2])

# Food pipeline assets (separate from molecule pipeline)
food_tier1_assets = load_assets_from_modules([food_tier1])
food_tier2_assets = load_assets_from_modules([food_tier2])

# Derived assets (specialized datasets built on core pipeline)
derived_food_composition_assets = load_assets_from_modules([food_composition_assets])

# Combine all assets
all_assets = [
    *acquisition_assets,
    *tier1_assets,
    *tier2_assets,
    *food_tier1_assets,
    *food_tier2_assets,
    *derived_food_composition_assets,
]

defs = Definitions(
    assets=all_assets,
)
