"""Data acquirers for fetching raw data from external sources.

This module provides a class-based interface for data acquisition with:
- Abstract base class enforcing consistent interface
- Built-in caching/idempotency
- Validation framework
- Dagster asset generation via factory function
- Archive management (compression and checksumming)

Usage:
    from flavor_pipeline.acquirers import ACQUIRERS, create_acquisition_assets

    # Get all acquirer instances
    for name, acquirer in ACQUIRERS.items():
        print(f"{name}: {acquirer.description}")

    # Generate Dagster assets
    assets = create_acquisition_assets()

    # Archive management
    from flavor_pipeline.acquirers.archive import (
        create_archive, extract_archive, verify_archive,
        load_manifest, save_manifest
    )
"""

from flavor_pipeline.acquirers.archive import (
    ARCHIVES_DIR,
    MANIFEST_PATH,
    compute_sha256,
    create_archive,
    extract_archive,
    get_archive_entry,
    load_manifest,
    save_manifest,
    update_manifest_entry,
    verify_archive,
)
from flavor_pipeline.acquirers.base import (
    AcquisitionError,
    AcquisitionMetadata,
    BaseAcquirer,
)
from flavor_pipeline.acquirers.bitterdb import BitterDBAcquirer
from flavor_pipeline.acquirers.culinarydb import CulinaryDBAcquirer
from flavor_pipeline.acquirers.duke_phytochem import DukePhytochemAcquirer
from flavor_pipeline.acquirers.fao_infoods import FAOINFOODSAcquirer
from flavor_pipeline.acquirers.fenaroli import FenaroliAcquirer
from flavor_pipeline.acquirers.flavordb2 import FlavorDB2Acquirer
from flavor_pipeline.acquirers.foodatlas import FoodAtlasAcquirer
from flavor_pipeline.acquirers.foodb import FooDBDAcquirer
from flavor_pipeline.acquirers.fsbi import FSBIAcquirer
from flavor_pipeline.acquirers.metabolights import MetaboLightsAcquirer
from flavor_pipeline.acquirers.panten import PantenAcquirer
from flavor_pipeline.acquirers.sweetenersdb import SweetenersDBAcquirer
from flavor_pipeline.acquirers.umamidb import UmamiDBAcquirer
from flavor_pipeline.acquirers.usda import USDAcquirer
from flavor_pipeline.acquirers.vcf import VCFAcquirer
from flavor_pipeline.acquirers.winesensed import WineSensedAcquirer

# Registry of all acquirers
ACQUIRER_CLASSES: dict[str, type[BaseAcquirer]] = {
    "flavordb2": FlavorDB2Acquirer,
    "bitterdb": BitterDBAcquirer,
    "fsbi": FSBIAcquirer,
    "vcf": VCFAcquirer,
    "fenaroli": FenaroliAcquirer,
    "panten": PantenAcquirer,
    "foodb": FooDBDAcquirer,
    "usda": USDAcquirer,
    "foodatlas": FoodAtlasAcquirer,
    "culinarydb": CulinaryDBAcquirer,
    "sweetenersdb": SweetenersDBAcquirer,
    "umamidb": UmamiDBAcquirer,
    "winesensed": WineSensedAcquirer,
    "fao_infoods": FAOINFOODSAcquirer,
    "duke_phytochem": DukePhytochemAcquirer,
    "metabolights": MetaboLightsAcquirer,
}


def get_acquirers() -> dict[str, BaseAcquirer]:
    """Get instances of all registered acquirers."""
    return {name: cls() for name, cls in ACQUIRER_CLASSES.items()}


# Instantiated acquirers for convenience
ACQUIRERS = get_acquirers()

__all__ = [
    # Base classes and errors
    "BaseAcquirer",
    "AcquisitionError",
    "AcquisitionMetadata",
    # Registry
    "ACQUIRER_CLASSES",
    "ACQUIRERS",
    "get_acquirers",
    # Archive utilities
    "ARCHIVES_DIR",
    "MANIFEST_PATH",
    "compute_sha256",
    "create_archive",
    "extract_archive",
    "verify_archive",
    "load_manifest",
    "save_manifest",
    "update_manifest_entry",
    "get_archive_entry",
    # Acquirer classes
    "FlavorDB2Acquirer",
    "BitterDBAcquirer",
    "FSBIAcquirer",
    "VCFAcquirer",
    "FenaroliAcquirer",
    "PantenAcquirer",
    "FooDBDAcquirer",
    "USDAcquirer",
    "FoodAtlasAcquirer",
    "CulinaryDBAcquirer",
    "SweetenersDBAcquirer",
    "UmamiDBAcquirer",
    "WineSensedAcquirer",
    "FAOINFOODSAcquirer",
    "DukePhytochemAcquirer",
    "MetaboLightsAcquirer",
]
