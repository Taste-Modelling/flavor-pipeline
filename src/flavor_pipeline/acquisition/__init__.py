"""Data acquisition modules for fetching raw data from various sources."""

from flavor_pipeline.acquisition.bitterdb import fetch_bitterdb
from flavor_pipeline.acquisition.fenaroli import fetch_fenaroli
from flavor_pipeline.acquisition.flavordb2 import fetch_flavordb2
from flavor_pipeline.acquisition.fsbi import fetch_fsbi
from flavor_pipeline.acquisition.panten import fetch_panten
from flavor_pipeline.acquisition.usda import fetch_usda
from flavor_pipeline.acquisition.vcf import fetch_vcf

__all__ = [
    "fetch_flavordb2",
    "fetch_bitterdb",
    "fetch_fenaroli",
    "fetch_panten",
    "fetch_vcf",
    "fetch_fsbi",
    "fetch_usda",
]
