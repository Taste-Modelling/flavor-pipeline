"""Factory for generating Dagster assets from acquirers.

This module provides functions to automatically generate Dagster assets
from BaseAcquirer subclasses, ensuring consistent behavior across all
acquisition assets.
"""

from pathlib import Path
from typing import Callable

from dagster import AssetExecutionContext, AssetsDefinition, asset

from flavor_pipeline.acquirers.base import BaseAcquirer


def create_asset_for_acquirer(acquirer: BaseAcquirer) -> AssetsDefinition:
    """Create a Dagster asset from an acquirer instance.

    The generated asset:
    - Checks cache before fetching
    - Logs acquisition progress
    - Validates output after fetching
    - Reports metadata to Dagster

    Args:
        acquirer: An instance of a BaseAcquirer subclass.

    Returns:
        A Dagster AssetsDefinition that can be included in Definitions.
    """

    @asset(
        name=f"{acquirer.name}_raw",
        group_name=acquirer.group_name,
        description=acquirer.description,
    )
    def _asset_fn(context: AssetExecutionContext) -> Path:
        # Check cache first
        if acquirer.is_cached():
            context.log.info(f"Using cached data in {acquirer.output_dir}")
            # Still validate cached data
            errors = acquirer.validate()
            if errors:
                context.log.warning(f"Validation warnings: {errors}")
            return acquirer.output_dir

        # Fetch fresh data
        context.log.info(f"Fetching {acquirer.name} data...")
        result_path = acquirer.fetch()
        context.log.info(f"Data saved to {result_path}")

        # Validate output
        errors = acquirer.validate()
        if errors:
            context.log.warning(f"Validation warnings: {errors}")

        # Report metadata
        meta = acquirer.get_asset_metadata()
        context.log.info(f"Acquisition metadata: {meta}")

        return acquirer.output_dir

    return _asset_fn


def create_acquisition_assets(
    acquirers: dict[str, BaseAcquirer] | None = None,
) -> list[AssetsDefinition]:
    """Create Dagster assets for all registered acquirers.

    Args:
        acquirers: Optional dict of acquirers. If None, uses all registered acquirers.

    Returns:
        List of Dagster AssetsDefinition objects.

    Usage:
        # In definitions.py
        from flavor_pipeline.acquirers.factory import create_acquisition_assets

        acquisition_assets = create_acquisition_assets()
        defs = Definitions(assets=acquisition_assets + other_assets)
    """
    if acquirers is None:
        from flavor_pipeline.acquirers import ACQUIRERS

        acquirers = ACQUIRERS

    return [create_asset_for_acquirer(acq) for acq in acquirers.values()]


def create_acquisition_asset(
    acquirer_class: type[BaseAcquirer],
    **kwargs,
) -> Callable[[AssetExecutionContext], Path]:
    """Decorator-style factory for creating an acquisition asset.

    This allows defining assets inline while still using the acquirer pattern.

    Usage:
        from flavor_pipeline.acquirers.factory import create_acquisition_asset
        from flavor_pipeline.acquirers import FlavorDB2Acquirer

        flavordb2_raw = create_acquisition_asset(FlavorDB2Acquirer)
    """
    acquirer = acquirer_class(**kwargs)
    return create_asset_for_acquirer(acquirer)
