"""Dagster definitions for the flavor pipeline.

This module provides two approaches for asset registration:

1. Factory approach (recommended): Auto-generate assets from acquirers
2. Manual approach: Explicitly define assets with @asset decorator

The factory approach ensures consistent behavior across all acquisition assets.
"""

from dagster import Definitions, load_assets_from_modules

from flavor_pipeline.acquirers.factory import create_acquisition_assets
from flavor_pipeline.assets import tier1

# Approach 1: Generate acquisition assets from acquirer classes
# This ensures all acquirers follow the same patterns
acquisition_assets = create_acquisition_assets()

# Approach 2: Load tier1 assets from the assets module
# (these still use the manual @asset decorator approach)
tier1_assets = load_assets_from_modules([tier1])

# Combine all assets
all_assets = acquisition_assets + tier1_assets

defs = Definitions(
    assets=all_assets,
)
