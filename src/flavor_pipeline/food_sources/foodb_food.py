"""FooDB food source adapter for food-compound associations."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.food import MoleculeComposition, Tier1Food

from .base import BaseFoodSource


class FooDBFoodSource(BaseFoodSource):
    """Load food data and compound associations from FooDB 2020 release.

    Expects raw_data/FooDB/foodb_2020_04_07_csv/*.csv files produced by
    acquisition/foodb.py download.

    Key files used:
    - Food.csv: Food metadata (name, category, description)
    - Content.csv: Food-compound associations with concentrations
    - Compound.csv: Compound data (for InChIKey mapping to molecule_id)
    """

    FOOD_FILE = "Food.csv"
    CONTENT_FILE = "Content.csv"
    COMPOUND_FILE = "Compound.csv"

    @property
    def name(self) -> str:
        return "foodb_food"

    @property
    def version(self) -> str:
        return "2020.04"

    @property
    def url(self) -> str:
        return "https://foodb.ca/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "FooDB" / "foodb_2020_04_07_csv"

    def validate(self) -> list[str]:
        errors = []
        food_path = self.raw_data_dir / self.FOOD_FILE

        if not food_path.exists():
            errors.append(
                f"Missing file: {food_path}. "
                "Run: python -m flavor_pipeline.acquisition.foodb"
            )

        return errors

    def parse(self) -> list[Tier1Food]:
        """Parse FooDB foods to Tier 1 format."""
        food_path = self.raw_data_dir / self.FOOD_FILE
        if not food_path.exists():
            return []

        # Load food data
        food_df = pd.read_csv(food_path, dtype=str, low_memory=False)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        # Load compound ID -> InChIKey mapping
        inchikey_map = self._load_compound_inchikey_map()

        # Load food-compound associations
        content_map = self._load_content_map(inchikey_map)

        foods = []
        for _, row in food_df.iterrows():
            foodb_id = self._nonempty(row.get("id"))
            public_id = self._nonempty(row.get("public_id"))

            if foodb_id is None:
                continue

            # Use FooDB public_id as food_id
            food_id = f"foodb_food:{public_id}" if public_id else f"foodb_food:{foodb_id}"

            # Parse food metadata
            name = self._nonempty(row.get("name"))
            scientific_name = self._nonempty(row.get("name_scientific"))
            description = self._nonempty(row.get("description"))
            category = self._nonempty(row.get("food_group"))
            subcategory = self._nonempty(row.get("food_subgroup"))

            # Get composition for this food
            composition = content_map.get(foodb_id, [])

            food = Tier1Food(
                food_id=food_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                name=self._av(name) if name else None,
                scientific_name=self._av(scientific_name) if scientific_name else None,
                description=self._av(description) if description else None,
                category=self._av(category) if category else None,
                subcategory=self._av(subcategory) if subcategory else None,
                composition=composition,
                extra={
                    "foodb_id": self._av(public_id or foodb_id),
                },
            )
            foods.append(food)

        return foods

    def _load_compound_inchikey_map(self) -> dict[str, str]:
        """Load compound ID to InChIKey mapping.

        Returns:
            Dict mapping compound_id -> molecule_id (inchikey:XXX or foodb:XXX)
        """
        compound_path = self.raw_data_dir / self.COMPOUND_FILE
        if not compound_path.exists():
            return {}

        df = pd.read_csv(compound_path, dtype=str, low_memory=False)
        mapping: dict[str, str] = {}

        for _, row in df.iterrows():
            compound_id = self._nonempty(row.get("id"))
            if compound_id is None:
                continue

            inchi_key = self._nonempty(row.get("moldb_inchikey"))
            public_id = self._nonempty(row.get("public_id"))

            if inchi_key:
                mapping[compound_id] = f"inchikey:{inchi_key}"
            elif public_id:
                mapping[compound_id] = f"foodb:{public_id}"
            else:
                mapping[compound_id] = f"foodb:{compound_id}"

        return mapping

    def _load_content_map(
        self, inchikey_map: dict[str, str]
    ) -> dict[str, list[MoleculeComposition]]:
        """Load food-compound content associations.

        Returns:
            Dict mapping food_id -> list of MoleculeComposition
        """
        content_path = self.raw_data_dir / self.CONTENT_FILE
        if not content_path.exists():
            return {}

        df = pd.read_csv(content_path, dtype=str, low_memory=False)
        content_map: dict[str, list[MoleculeComposition]] = {}

        for _, row in df.iterrows():
            food_id = self._nonempty(row.get("food_id"))
            source_id = self._nonempty(row.get("source_id"))
            source_type = self._nonempty(row.get("source_type"))

            if food_id is None or source_id is None:
                continue

            # Only include Compound associations (not Nutrient)
            if source_type != "Compound":
                continue

            # Get molecule_id from compound mapping
            molecule_id = inchikey_map.get(source_id)
            if molecule_id is None:
                continue

            # Parse concentration values
            orig_content = self._parse_float(row.get("orig_content"))
            orig_min = self._parse_float(row.get("orig_min"))
            orig_max = self._parse_float(row.get("orig_max"))
            orig_unit = self._nonempty(row.get("orig_unit"))

            # Normalize units to standard form
            unit = self._normalize_unit(orig_unit)

            # Create composition entry
            composition = MoleculeComposition(
                molecule_id=molecule_id,
                concentration=self._av(orig_content, unit) if orig_content is not None else None,
                concentration_min=self._av(orig_min, unit) if orig_min is not None else None,
                concentration_max=self._av(orig_max, unit) if orig_max is not None else None,
            )

            if food_id not in content_map:
                content_map[food_id] = []
            content_map[food_id].append(composition)

        return content_map

    def _normalize_unit(self, unit: str | None) -> str | None:
        """Normalize concentration units to standard form."""
        if unit is None:
            return None

        unit = unit.strip().lower()

        # Common unit mappings
        unit_map = {
            "mg/100g": "mg/100g",
            "mg/100 g": "mg/100g",
            "ug/100g": "ug/100g",
            "ug/100 g": "ug/100g",
            "g/100g": "g/100g",
            "g/100 g": "g/100g",
            "ppm": "ppm",
            "ppb": "ppb",
        }

        return unit_map.get(unit, unit)
