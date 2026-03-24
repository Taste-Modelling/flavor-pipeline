"""Food source adapters for transforming raw data to Tier 1 foods."""

from flavor_pipeline.food_sources.base import BaseFoodSource
from flavor_pipeline.food_sources.foodb_food import FooDBFoodSource
from flavor_pipeline.food_sources.usda_food import USDAFoodSource

FOOD_SOURCES: dict[str, type[BaseFoodSource]] = {
    "foodb_food": FooDBFoodSource,
    "usda_food": USDAFoodSource,
}

__all__ = [
    "BaseFoodSource",
    "FOOD_SOURCES",
    "FooDBFoodSource",
    "USDAFoodSource",
]
