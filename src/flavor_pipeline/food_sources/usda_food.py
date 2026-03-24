"""USDA FoodData Central food source adapter."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.food import MoleculeComposition, Tier1Food

from .base import BaseFoodSource

# Mapping of USDA nutrient IDs to molecule identifiers (InChIKey where known)
# These are common compounds with well-established InChIKeys
NUTRIENT_TO_MOLECULE_ID: dict[str, str] = {
    # Organic acids
    "1026": "inchikey:QTBSBXVTEAMEQO-UHFFFAOYSA-N",  # Acetic acid
    "1028": "inchikey:WPYMKLBDIGXBTP-UHFFFAOYSA-N",  # Benzoic acid
    "1030": "inchikey:CWVRJTMFETXNAD-JUHZACGLSA-N",  # Chlorogenic acid
    "1031": "inchikey:WBYWAXJHAXSJNI-VOTSOKGWSA-N",  # Cinnamic acid
    "1032": "inchikey:KRKNYBCHXYNGOX-UHFFFAOYSA-N",  # Citric acid
    "1033": "inchikey:VZCYOOQTPOCHFL-OWOJBTEDSA-N",  # Fumaric acid
    "1035": "inchikey:LNTHITQWFMADLM-UHFFFAOYSA-N",  # Gallic acid
    "1038": "inchikey:BJHIKXHVCXFQLS-UYFOZJQFSA-N",  # Lactic acid
    "1039": "inchikey:BJEPYKJPYRNKOW-REOHCLBHSA-N",  # Malic acid
    "1041": "inchikey:MUBZPKHOEPUJKR-UHFFFAOYSA-N",  # Oxalic acid
    "1043": "inchikey:LCTONWCANYUPML-UHFFFAOYSA-N",  # Pyruvic acid
    "1044": "inchikey:AAWZDTNXLSGCEK-LNVDRNJUSA-N",  # Quinic acid
    "1045": "inchikey:YGSDEFSMJLZEOE-UHFFFAOYSA-N",  # Salicylic acid
    "1046": "inchikey:KDYFGRWQOYBRFD-UHFFFAOYSA-N",  # Succinic acid
    "1047": "inchikey:FEWJPZIEWOKRBE-JCYAYHJZSA-N",  # Tartaric acid
    # Sugars
    "1010": "inchikey:CZMRCDWAGMRECN-UGDNZRGBSA-N",  # Sucrose
    "1011": "inchikey:WQZGKKKJIJFFOK-GASJEMHNSA-N",  # Glucose
    "1012": "inchikey:RFSUNEUAIZKAJO-ARQDHWQXSA-N",  # Fructose
    "1013": "inchikey:GUBGYTABKSRVRQ-DCSYEGIMSA-N",  # Lactose
    "1014": "inchikey:GUBGYTABKSRVRQ-QUYVBRFLSA-N",  # Maltose
    "1075": "inchikey:WQZGKKKJIJFFOK-SVZMEOIVSA-N",  # Galactose
    # Alcohols and polyols
    "1018": "inchikey:LFQSCWFLJHTTHZ-UHFFFAOYSA-N",  # Ethanol
    "1055": "inchikey:FBPFZTCFMRRESA-JGWLITMVSA-N",  # Mannitol
    "1056": "inchikey:FBPFZTCFMRRESA-KVTDHHQDSA-N",  # Sorbitol
    "1078": "inchikey:HEBKCHPVOBER2T-UHFFFAOYSA-N",  # Xylitol
    # Alkaloids
    "1057": "inchikey:RYYVLZVUVIJVGH-UHFFFAOYSA-N",  # Caffeine
    "1058": "inchikey:YAPQBXQYLJRXSA-UHFFFAOYSA-N",  # Theobromine
    # Vitamins (some have well-known InChIKeys)
    "1162": "inchikey:CIWBSHSKHKDKBQ-JLAZNSOCSA-N",  # Vitamin C (ascorbic acid)
    "1165": "inchikey:JZRWCGZRTZMZEH-UHFFFAOYSA-N",  # Thiamin (B1)
    "1166": "inchikey:AUNGANRZJHBGPY-SCRDCRAPSA-N",  # Riboflavin (B2)
    "1167": "inchikey:PVNIIMVLHYAWGP-UHFFFAOYSA-N",  # Niacin (B3)
    "1170": "inchikey:SNPLKNRPJHDVJA-ZETCQYMHSA-N",  # Pantothenic acid (B5)
    "1175": "inchikey:QDGAVODICPCDMU-XVFCMESISA-N",  # Pyridoxine (B6)
    "1177": "inchikey:BLGXFZZNTVWLAY-UHFFFAOYSA-N",  # Folate
    "1178": "inchikey:BPKIGYQJPYCAOW-FFJTTWKXSA-I",  # Vitamin B12
    # Amino acids
    "1210": "inchikey:QWCKQJZIFLGMSD-UHFFFAOYSA-N",  # Tryptophan
    "1211": "inchikey:FFEARJCKVFRZRR-BYPYZUCNSA-N",  # Threonine
    "1212": "inchikey:AGPKZVBTJJNPAG-WHFBIAKZSA-N",  # Isoleucine
    "1213": "inchikey:ROHFNLRQFUQHCH-YFKPBYRVSA-N",  # Leucine
    "1214": "inchikey:KDXKERNSBIXSRK-YFKPBYRVSA-N",  # Lysine
    "1215": "inchikey:FFEARJCKVFRZRR-UHFFFAOYSA-N",  # Methionine
    "1216": "inchikey:LKDRXBCSQODPBY-UHFFFAOYSA-N",  # Cystine
    "1217": "inchikey:COLNVLDHVKWLRT-QMMMGPOBSA-N",  # Phenylalanine
    "1218": "inchikey:OUYCCCASQSFEME-QMMMGPOBSA-N",  # Tyrosine
    "1219": "inchikey:KZSNJWFQEVHDMF-BYPYZUCNSA-N",  # Valine
    "1220": "inchikey:DCXYFEDJOCDNAF-REOHCLBHSA-N",  # Arginine
    "1221": "inchikey:CKLJMWTZIZZHCS-REOHCLBHSA-N",  # Histidine
    "1222": "inchikey:CKLJMWTZIZZHCS-UHFFFAOYSA-N",  # Alanine
    "1223": "inchikey:CKLJMWTZIZZHCS-UHFFFAOYSA-N",  # Aspartic acid
    "1224": "inchikey:WHUUTDBJXJRKMK-VKHMYHEASA-N",  # Glutamic acid
    "1225": "inchikey:DHMQDGOQFOQNFH-UHFFFAOYSA-N",  # Glycine
    "1226": "inchikey:ONIBWKKTOPOVIA-BYPYZUCNSA-N",  # Proline
    "1227": "inchikey:MTCFGRXMJLQNBG-REOHCLBHSA-N",  # Serine
}


class USDAFoodSource(BaseFoodSource):
    """Load food data from USDA FoodData Central.

    Expects raw_data/USDA/FoodData_Central_csv_*/*.csv files produced by
    acquisition/usda.py download.

    Key files used:
    - food.csv: Food metadata (name, description, category)
    - food_nutrient.csv: Food-nutrient associations with amounts
    - nutrient.csv: Nutrient definitions
    - food_category.csv: Category definitions
    """

    FOOD_FILE = "food.csv"
    FOOD_NUTRIENT_FILE = "food_nutrient.csv"
    NUTRIENT_FILE = "nutrient.csv"
    FOOD_CATEGORY_FILE = "food_category.csv"

    @property
    def name(self) -> str:
        return "usda_food"

    @property
    def version(self) -> str:
        return "2025.12"

    @property
    def url(self) -> str:
        return "https://fdc.nal.usda.gov/"

    @property
    def raw_data_dir(self) -> Path:
        # Find the extracted directory (name includes date)
        usda_base = self._raw_data_base / "USDA"
        if not usda_base.exists():
            return usda_base

        # Look for FoodData_Central_csv_* directory
        for subdir in usda_base.iterdir():
            if subdir.is_dir() and subdir.name.startswith("FoodData_Central_csv_"):
                return subdir

        return usda_base

    def validate(self) -> list[str]:
        errors = []
        food_path = self.raw_data_dir / self.FOOD_FILE

        if not food_path.exists():
            errors.append(
                f"Missing file: {food_path}. "
                "Run: python -m flavor_pipeline.acquisition.usda"
            )

        return errors

    def parse(self) -> list[Tier1Food]:
        """Parse USDA foods to Tier 1 format.

        Note: This processes only Foundation and SR Legacy foods by default,
        which have the most complete nutrient data. Branded foods are very
        numerous but have limited nutrient profiles.
        """
        food_path = self.raw_data_dir / self.FOOD_FILE
        if not food_path.exists():
            return []

        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        # Load category mapping
        category_map = self._load_category_map()

        # Load nutrient metadata (for units)
        nutrient_meta = self._load_nutrient_metadata()

        # Load food-nutrient associations
        # This is a large file - we'll process in chunks
        nutrient_map = self._load_food_nutrients(nutrient_meta)

        # Process food.csv
        foods = []
        food_df = pd.read_csv(food_path, dtype=str, low_memory=False)

        for _, row in food_df.iterrows():
            fdc_id = self._nonempty(row.get("fdc_id"))
            data_type = self._nonempty(row.get("data_type"))

            if fdc_id is None:
                continue

            # Focus on Foundation and SR Legacy foods (highest quality data)
            # Can be expanded to include branded_food if desired
            if data_type not in ("foundation_food", "sr_legacy_food", "survey_fndds_food"):
                continue

            food_id = f"usda:{fdc_id}"

            # Parse food metadata
            description = self._nonempty(row.get("description"))
            category_id = self._nonempty(row.get("food_category_id"))
            category = category_map.get(category_id) if category_id else None

            # Get composition for this food
            composition = nutrient_map.get(fdc_id, [])

            food = Tier1Food(
                food_id=food_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                name=self._av(description) if description else None,
                category=self._av(category) if category else None,
                composition=composition,
                extra={
                    "usda_fdc_id": self._av(fdc_id),
                    "data_type": self._av(data_type),
                },
            )
            foods.append(food)

        return foods

    def _load_category_map(self) -> dict[str, str]:
        """Load food category ID to description mapping."""
        category_path = self.raw_data_dir / self.FOOD_CATEGORY_FILE
        if not category_path.exists():
            return {}

        df = pd.read_csv(category_path, dtype=str)
        result: dict[str, str] = {}
        for _, row in df.iterrows():
            cat_id = self._nonempty(row.get("id"))
            cat_desc = self._nonempty(row.get("description"))
            if cat_id and cat_desc:
                result[cat_id] = cat_desc
        return result

    def _load_nutrient_metadata(self) -> dict[str, dict]:
        """Load nutrient ID to metadata mapping (name, unit)."""
        nutrient_path = self.raw_data_dir / self.NUTRIENT_FILE
        if not nutrient_path.exists():
            return {}

        df = pd.read_csv(nutrient_path, dtype=str)
        metadata: dict[str, dict] = {}

        for _, row in df.iterrows():
            nutrient_id = self._nonempty(row.get("id"))
            if nutrient_id is None:
                continue

            metadata[nutrient_id] = {
                "name": self._nonempty(row.get("name")),
                "unit": self._nonempty(row.get("unit_name")),
            }

        return metadata

    def _load_food_nutrients(
        self, nutrient_meta: dict[str, dict]
    ) -> dict[str, list[MoleculeComposition]]:
        """Load food-nutrient associations as MoleculeComposition.

        Only includes nutrients that have a known molecule_id mapping.
        """
        nutrient_path = self.raw_data_dir / self.FOOD_NUTRIENT_FILE
        if not nutrient_path.exists():
            return {}

        nutrient_map: dict[str, list[MoleculeComposition]] = {}

        # Process in chunks due to file size
        for chunk in pd.read_csv(nutrient_path, dtype=str, low_memory=False, chunksize=100000):
            for _, row in chunk.iterrows():
                fdc_id = self._nonempty(row.get("fdc_id"))
                nutrient_id = self._nonempty(row.get("nutrient_id"))

                if fdc_id is None or nutrient_id is None:
                    continue

                # Only include nutrients with known molecule mapping
                molecule_id = NUTRIENT_TO_MOLECULE_ID.get(nutrient_id)
                if molecule_id is None:
                    continue

                # Get amount and unit
                amount = self._parse_float(row.get("amount"))
                if amount is None or amount == 0:
                    continue

                # Get unit from nutrient metadata
                meta = nutrient_meta.get(nutrient_id, {})
                unit_name = meta.get("unit", "")

                # Normalize unit to per 100g format
                unit = self._normalize_unit(unit_name)

                # Create composition entry
                composition = MoleculeComposition(
                    molecule_id=molecule_id,
                    concentration=self._av(amount, unit),
                    concentration_min=self._av(self._parse_float(row.get("min")), unit)
                    if self._parse_float(row.get("min"))
                    else None,
                    concentration_max=self._av(self._parse_float(row.get("max")), unit)
                    if self._parse_float(row.get("max"))
                    else None,
                )

                if fdc_id not in nutrient_map:
                    nutrient_map[fdc_id] = []
                nutrient_map[fdc_id].append(composition)

        return nutrient_map

    def _normalize_unit(self, unit: str | None) -> str:
        """Normalize USDA units to standard form (per 100g basis)."""
        if unit is None:
            return "per 100g"

        unit = unit.strip().upper()

        # USDA units are per 100g by default
        unit_map = {
            "G": "g/100g",
            "MG": "mg/100g",
            "UG": "ug/100g",
            "KCAL": "kcal/100g",
            "KJ": "kJ/100g",
            "IU": "IU/100g",
        }

        return unit_map.get(unit, f"{unit}/100g")
