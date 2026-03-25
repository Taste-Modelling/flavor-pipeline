"""CulinaryDB source adapter for recipes.

CulinaryDB is a repository of structured recipe data from 22 world regions
with ingredients linked to FlavorDB flavor molecules.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.food import IngestMetadata as FoodIngestMetadata
from flavor_pipeline.schemas.food import MoleculeComposition, Tier1Food
from flavor_pipeline.schemas.tier1 import SourceMetadata


def _is_missing(value: Any) -> bool:
    """Check if a value is missing/empty."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value == "":
        return True
    return False


class CulinaryDBRecipeSource:
    """Parse CulinaryDB recipes to Tier1Food format.

    Extracts recipes with their ingredients mapped to FlavorDB entity IDs.
    Each recipe becomes a Tier1Food with ingredient composition linking
    to FlavorDB molecules.
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")
    PARSER_VERSION = "0.1.0"
    PIPELINE_VERSION = "0.1.0"

    RECIPE_FILE = "01_Recipe_Details.csv"
    INGREDIENTS_FILE = "02_Ingredients.csv"
    COMPOUND_FILE = "03_Compound_Ingredients.csv"
    RECIPE_INGREDIENTS_FILE = "04_Recipe-Ingredients_Aliases.csv"

    def __init__(self, raw_data_base: str | Path | None = None):
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    def name(self) -> str:
        return "culinarydb"

    @property
    def version(self) -> str:
        return "2018.03"

    @property
    def url(self) -> str:
        return "https://cosylab.iiitd.edu.in/culinarydb/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "Culinarydb"

    def validate(self) -> list[str]:
        errors = []
        recipe_path = self.raw_data_dir / self.RECIPE_FILE

        if not recipe_path.exists():
            errors.append(
                f"Missing file: {recipe_path}. "
                "Run: python -m flavor_pipeline.acquisition.culinarydb"
            )

        return errors

    def get_source_metadata(self, retrieved_at: datetime | None = None) -> SourceMetadata:
        """Create source metadata for this adapter."""
        return SourceMetadata(
            name=self.name,
            version=self.version,
            url=self.url,
            retrieved_at=retrieved_at or datetime.now(UTC),
            parser_version=self.PARSER_VERSION,
        )

    def get_ingest_metadata(self) -> FoodIngestMetadata:
        """Create ingest metadata for foods."""
        return FoodIngestMetadata(pipeline_version=self.PIPELINE_VERSION)

    def parse(self) -> list[Tier1Food]:
        """Parse CulinaryDB recipes to Tier 1 foods."""
        recipe_path = self.raw_data_dir / self.RECIPE_FILE
        if not recipe_path.exists():
            return []

        # Load all data files
        recipes_df = pd.read_csv(recipe_path, dtype=str)
        ingredients_df = pd.read_csv(
            self.raw_data_dir / self.INGREDIENTS_FILE, dtype=str
        )
        compound_df = pd.read_csv(
            self.raw_data_dir / self.COMPOUND_FILE, dtype=str
        )
        recipe_ing_df = pd.read_csv(
            self.raw_data_dir / self.RECIPE_INGREDIENTS_FILE, dtype=str
        )

        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        # Build ingredient entity ID to FlavorDB molecule ID map
        # Entity IDs 0-999 are simple ingredients, 2000+ are compound ingredients
        ingredient_map = self._build_ingredient_map(ingredients_df, compound_df)

        # Build recipe -> ingredients mapping
        recipe_ingredients = self._build_recipe_ingredients(recipe_ing_df)

        foods = []
        for _, row in recipes_df.iterrows():
            recipe_id = self._nonempty(row.get("Recipe ID"))
            if recipe_id is None:
                continue

            food_id = f"culinarydb:{recipe_id}"
            title = self._nonempty(row.get("Title"))
            cuisine = self._nonempty(row.get("Cuisine"))
            source = self._nonempty(row.get("Source"))

            # Build composition from recipe ingredients
            composition = []
            ing_list = recipe_ingredients.get(recipe_id, [])
            for entity_id, _original_name, _aliased_name in ing_list:
                # Map entity ID to molecule_id
                mol_id = ingredient_map.get(entity_id)
                if mol_id:
                    comp = MoleculeComposition(
                        molecule_id=mol_id,
                        # No concentration data in CulinaryDB
                    )
                    composition.append(comp)

            food = Tier1Food(
                food_id=food_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                name=self._av(title) if title else None,
                category=self._av(cuisine) if cuisine else None,
                composition=composition,
                extra={
                    "culinarydb_id": self._av(recipe_id),
                    **({"source_site": self._av(source)} if source else {}),
                },
            )
            foods.append(food)

        return foods

    def _av(self, value: Any, unit: str | None = None) -> AttributedValue:
        """Create an AttributedValue with this source."""
        return AttributedValue(value=value, unit=unit, sources=[self.name])

    def _nonempty(self, val: Any) -> str | None:
        """Return value if not empty/null, else None."""
        if _is_missing(val):
            return None
        return str(val).strip()

    def _build_ingredient_map(
        self, ingredients_df: pd.DataFrame, compound_df: pd.DataFrame
    ) -> dict[str, str]:
        """Build map from entity ID to molecule_id.

        CulinaryDB entity IDs correspond to FlavorDB entity IDs for simple
        ingredients. For compound ingredients, we use a culinarydb prefix.

        Returns:
            Dict mapping entity_id string to molecule_id.
        """
        id_map: dict[str, str] = {}

        # Simple ingredients - entity IDs link to FlavorDB
        for _, row in ingredients_df.iterrows():
            entity_id = self._nonempty(row.get("Entity ID"))
            if entity_id:
                # FlavorDB uses entity IDs - map to flavordb namespace
                id_map[entity_id] = f"flavordb_entity:{entity_id}"

        # Compound ingredients - custom IDs (2000+)
        for _, row in compound_df.iterrows():
            entity_id = self._nonempty(row.get("entity_id"))
            if entity_id:
                id_map[entity_id] = f"culinarydb_compound:{entity_id}"

        return id_map

    def _build_recipe_ingredients(
        self, recipe_ing_df: pd.DataFrame
    ) -> dict[str, list[tuple[str, str, str]]]:
        """Build map from recipe ID to list of ingredients.

        Returns:
            Dict mapping recipe_id to list of (entity_id, original_name, aliased_name).
        """
        recipe_map: dict[str, list[tuple[str, str, str]]] = {}

        for _, row in recipe_ing_df.iterrows():
            recipe_id = self._nonempty(row.get("Recipe ID"))
            entity_id = self._nonempty(row.get("Entity ID"))
            original = self._nonempty(row.get("Original Ingredient Name"))
            aliased = self._nonempty(row.get("Aliased Ingredient Name"))

            if recipe_id and entity_id:
                if recipe_id not in recipe_map:
                    recipe_map[recipe_id] = []
                recipe_map[recipe_id].append((entity_id, original or "", aliased or ""))

        return recipe_map
