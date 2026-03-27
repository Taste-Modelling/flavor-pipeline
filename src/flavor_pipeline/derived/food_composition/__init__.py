"""Food flavor composition derived asset.

Provides food-to-flavor-molecule mass composition data:
- Food name
- Molecule identifier (CAS preferred, fallback to PubChem/InChIKey)
- Concentration (percentage/mg per 100g)

This asset filters to flavor/aroma compounds only, excluding general
nutrients and macronutrients.
"""

from flavor_pipeline.derived.food_composition.schemas import FoodFlavorComposition
from flavor_pipeline.derived.food_composition.sources import (
    FoodAtlasFlavorFoodSource,
    FooDBFlavorFoodSource,
)

__all__ = [
    "FoodFlavorComposition",
    "FooDBFlavorFoodSource",
    "FoodAtlasFlavorFoodSource",
]
