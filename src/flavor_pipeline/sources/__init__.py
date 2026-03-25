"""Source adapters for transforming raw data to Tier 1 molecules."""

from flavor_pipeline.sources.base import BaseSource
from flavor_pipeline.sources.bitterdb import BitterDBSource
from flavor_pipeline.sources.culinarydb import CulinaryDBRecipeSource
from flavor_pipeline.sources.fenaroli import FenaroliSource
from flavor_pipeline.sources.flavordb2 import FlavorDB2Source
from flavor_pipeline.sources.foodatlas import FoodAtlasFoodSource, FoodAtlasMoleculeSource
from flavor_pipeline.sources.foodb import FooDBSource
from flavor_pipeline.sources.fsbi import FSBISource
from flavor_pipeline.sources.leffingwell import LeffingwellSource
from flavor_pipeline.sources.panten import PantenSource
from flavor_pipeline.sources.vcf import VCFSource
from flavor_pipeline.sources.winesensed import WineSensedSource

SOURCES: dict[str, type[BaseSource]] = {
    "flavordb2": FlavorDB2Source,
    "bitterdb": BitterDBSource,
    "fenaroli": FenaroliSource,
    "panten": PantenSource,
    "vcf": VCFSource,
    "fsbi": FSBISource,
    "leffingwell": LeffingwellSource,
    "foodb": FooDBSource,
    "foodatlas": FoodAtlasMoleculeSource,
}

__all__ = [
    "BaseSource",
    "SOURCES",
    "FlavorDB2Source",
    "BitterDBSource",
    "FenaroliSource",
    "PantenSource",
    "VCFSource",
    "FSBISource",
    "LeffingwellSource",
    "FooDBSource",
    "FoodAtlasMoleculeSource",
    "FoodAtlasFoodSource",
    "CulinaryDBRecipeSource",
    "WineSensedSource",
]
