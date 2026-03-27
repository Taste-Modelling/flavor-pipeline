"""Source parsers for food flavor composition data.

These sources parse FooDB and FoodAtlas data, filtering to flavor compounds only.
They produce FoodFlavorComposition records suitable for the derived asset.
"""

import ast
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from flavor_pipeline.derived.food_composition.schemas import FoodFlavorComposition


def _is_missing(value: Any) -> bool:
    """Check if a value is missing/empty."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value == "":
        return True
    return False


def _nonempty(val: Any) -> str | None:
    """Return value if not empty/null, else None."""
    if _is_missing(val):
        return None
    return str(val).strip()


def _parse_float(val: Any) -> float | None:
    """Parse a value as float, return None on failure."""
    if _is_missing(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_int(val: Any) -> int | None:
    """Parse a value as int, return None on failure."""
    if _is_missing(val):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


class FooDBFlavorFoodSource:
    """Parse FooDB food-compound associations filtered to flavor compounds.

    This source:
    1. Loads flavor compound IDs from CompoundsFlavor.csv
    2. Loads compound details (CAS, InChIKey) from Compound.csv
    3. Loads food definitions from Food.csv
    4. Loads composition data from Content.csv
    5. Filters to only flavor compounds
    6. Produces FoodFlavorComposition records
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")

    FOOD_FILE = "Food.csv"
    CONTENT_FILE = "Content.csv"
    COMPOUND_FILE = "Compound.csv"
    FLAVOR_FILE = "Flavor.csv"
    COMPOUNDS_FLAVOR_FILE = "CompoundsFlavor.csv"

    def __init__(self, raw_data_base: str | Path | None = None):
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    def name(self) -> str:
        return "foodb"

    @property
    def version(self) -> str:
        return "2020.04"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "FooDB" / "foodb_2020_04_07_csv"

    def validate(self) -> list[str]:
        """Validate that required files exist."""
        errors = []
        required_files = [
            self.FOOD_FILE,
            self.CONTENT_FILE,
            self.COMPOUND_FILE,
            self.COMPOUNDS_FLAVOR_FILE,
        ]
        for fname in required_files:
            fpath = self.raw_data_dir / fname
            if not fpath.exists():
                errors.append(
                    f"Missing file: {fpath}. "
                    "Run: python -m flavor_pipeline.acquisition.foodb"
                )
        return errors

    def parse(self) -> list[FoodFlavorComposition]:
        """Parse FooDB to FoodFlavorComposition records.

        Returns:
            List of FoodFlavorComposition records for flavor compounds only.
        """
        if not (self.raw_data_dir / self.FOOD_FILE).exists():
            return []

        # Step 1: Load flavor compound IDs from CompoundsFlavor.csv
        flavor_compound_ids = self._load_flavor_compound_ids()
        if not flavor_compound_ids:
            return []

        # Step 2: Load compound details (for CAS, InChIKey, names)
        compound_details = self._load_compound_details()

        # Step 3: Load flavor descriptors per compound
        flavor_descriptors_map = self._load_flavor_descriptors()

        # Step 4: Load food definitions
        food_map = self._load_food_map()

        # Step 5: Load and filter content to flavor compounds only
        records = self._load_content_records(
            flavor_compound_ids, compound_details, flavor_descriptors_map, food_map
        )

        return records

    def _load_flavor_compound_ids(self) -> set[str]:
        """Load IDs of compounds that have flavor associations."""
        cf_path = self.raw_data_dir / self.COMPOUNDS_FLAVOR_FILE
        if not cf_path.exists():
            return set()

        df = pd.read_csv(cf_path, dtype=str)
        compound_ids = set()
        for _, row in df.iterrows():
            compound_id = _nonempty(row.get("compound_id"))
            if compound_id:
                compound_ids.add(compound_id)
        return compound_ids

    def _load_compound_details(self) -> dict[str, dict[str, Any]]:
        """Load compound details from Compound.csv.

        Returns:
            Dict mapping compound_id to details dict with keys:
            - cas: CAS number
            - inchikey: InChIKey
            - pubchem_id: PubChem CID (if available)
            - name: Common name
            - public_id: FooDB public ID
        """
        compound_path = self.raw_data_dir / self.COMPOUND_FILE
        if not compound_path.exists():
            return {}

        df = pd.read_csv(compound_path, dtype=str, low_memory=False)
        details: dict[str, dict[str, Any]] = {}

        for _, row in df.iterrows():
            compound_id = _nonempty(row.get("id"))
            if compound_id is None:
                continue

            # Due to CSV column shift in FooDB:
            # - InChIKey is in 'moldb_smiles' column
            # - CAS is in 'description' column
            # - SMILES is in 'cas_number' column
            inchi_key = self._extract_inchikey(row.get("moldb_smiles"))
            cas = self._parse_cas(row.get("description"))
            name = _nonempty(row.get("name"))
            public_id = _nonempty(row.get("public_id"))

            # Try to get PubChem ID from pubchem_compound_id column if it exists
            pubchem_id = _parse_int(row.get("pubchem_compound_id"))

            details[compound_id] = {
                "cas": cas,
                "inchikey": inchi_key,
                "pubchem_id": pubchem_id,
                "name": name,
                "public_id": public_id,
            }

        return details

    def _load_flavor_descriptors(self) -> dict[str, list[str]]:
        """Load flavor descriptors for each compound.

        Returns:
            Dict mapping compound_id to list of flavor descriptor names.
        """
        cf_path = self.raw_data_dir / self.COMPOUNDS_FLAVOR_FILE
        flavor_path = self.raw_data_dir / self.FLAVOR_FILE

        if not cf_path.exists() or not flavor_path.exists():
            return {}

        # Load flavor names
        flavor_df = pd.read_csv(flavor_path, dtype=str)
        flavor_names: dict[str, str] = {}
        for _, row in flavor_df.iterrows():
            fid = _nonempty(row.get("id"))
            fname = _nonempty(row.get("name"))
            if fid and fname:
                flavor_names[fid] = fname

        # Load compound-flavor associations
        cf_df = pd.read_csv(cf_path, dtype=str)
        flavor_map: dict[str, list[str]] = {}

        for _, row in cf_df.iterrows():
            compound_id = _nonempty(row.get("compound_id"))
            flavor_id = _nonempty(row.get("flavor_id"))

            if compound_id and flavor_id and flavor_id in flavor_names:
                if compound_id not in flavor_map:
                    flavor_map[compound_id] = []
                fname = flavor_names[flavor_id]
                if fname not in flavor_map[compound_id]:
                    flavor_map[compound_id].append(fname)

        return flavor_map

    def _load_food_map(self) -> dict[str, dict[str, Any]]:
        """Load food definitions from Food.csv.

        Returns:
            Dict mapping food_id to details dict.
        """
        food_path = self.raw_data_dir / self.FOOD_FILE
        if not food_path.exists():
            return {}

        df = pd.read_csv(food_path, dtype=str, low_memory=False)
        food_map: dict[str, dict[str, Any]] = {}

        for _, row in df.iterrows():
            food_id = _nonempty(row.get("id"))
            if food_id is None:
                continue

            food_map[food_id] = {
                "name": _nonempty(row.get("name")),
                "scientific_name": _nonempty(row.get("name_scientific")),
                "category": _nonempty(row.get("food_group")),
                "public_id": _nonempty(row.get("public_id")),
            }

        return food_map

    def _load_content_records(
        self,
        flavor_compound_ids: set[str],
        compound_details: dict[str, dict[str, Any]],
        flavor_descriptors_map: dict[str, list[str]],
        food_map: dict[str, dict[str, Any]],
    ) -> list[FoodFlavorComposition]:
        """Load Content.csv filtered to flavor compounds.

        Returns:
            List of FoodFlavorComposition records.
        """
        content_path = self.raw_data_dir / self.CONTENT_FILE
        if not content_path.exists():
            return []

        df = pd.read_csv(content_path, dtype=str, low_memory=False)
        records: list[FoodFlavorComposition] = []

        for _, row in df.iterrows():
            food_id = _nonempty(row.get("food_id"))
            source_id = _nonempty(row.get("source_id"))
            source_type = _nonempty(row.get("source_type"))

            # Skip non-compound associations
            if source_type != "Compound":
                continue

            # Skip non-flavor compounds
            if source_id not in flavor_compound_ids:
                continue

            if food_id is None or source_id is None:
                continue

            # Get food details
            food = food_map.get(food_id, {})
            food_name = food.get("name")
            if not food_name:
                continue

            # Get compound details
            compound = compound_details.get(source_id, {})
            inchikey = compound.get("inchikey")
            cas = compound.get("cas")
            pubchem_id = compound.get("pubchem_id")

            # Determine molecule_id using priority: inchikey > cas > pubchem > foodb
            if inchikey:
                molecule_id = f"inchikey:{inchikey}"
            elif cas:
                molecule_id = f"cas:{cas}"
            elif pubchem_id:
                molecule_id = f"pubchem:{pubchem_id}"
            else:
                foodb_id = compound.get("public_id") or source_id
                molecule_id = f"foodb:{foodb_id}"

            # Parse concentration values
            concentration = _parse_float(row.get("orig_content"))
            concentration_min = _parse_float(row.get("orig_min"))
            concentration_max = _parse_float(row.get("orig_max"))
            concentration_unit = self._normalize_unit(_nonempty(row.get("orig_unit")))

            # Get food part from preparation column if available
            food_part = _nonempty(row.get("preparation"))

            record = FoodFlavorComposition(
                food_name=food_name,
                scientific_name=food.get("scientific_name"),
                food_part=food_part,
                food_category=food.get("category"),
                molecule_name=compound.get("name"),
                cas=cas,
                pubchem_id=pubchem_id,
                inchikey=inchikey,
                molecule_id=molecule_id,
                concentration=concentration,
                concentration_min=concentration_min,
                concentration_max=concentration_max,
                concentration_unit=concentration_unit,
                flavor_descriptors=flavor_descriptors_map.get(source_id, []),
                source=self.name,
                source_food_id=food.get("public_id") or food_id,
                source_molecule_id=compound.get("public_id") or source_id,
            )
            records.append(record)

        return records

    def _extract_inchikey(self, value: Any) -> str | None:
        """Extract and validate InChIKey from a value."""
        if _is_missing(value):
            return None
        val = str(value).strip()
        # InChIKey pattern: 14 uppercase letters, hyphen, 10 uppercase letters, hyphen, 1 letter
        if re.match(r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$", val):
            return val
        return None

    def _parse_cas(self, value: Any) -> str | None:
        """Parse CAS number, handling multiple values."""
        if _is_missing(value):
            return None
        cas = str(value).strip()
        # If multiple CAS numbers (comma-separated), take first
        if "," in cas:
            cas = cas.split(",")[0].strip()
        # Validate CAS format (digits-digits-digit)
        parts = cas.split("-")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            return cas
        return None

    def _normalize_unit(self, unit: str | None) -> str | None:
        """Normalize concentration units to standard form."""
        if unit is None:
            return None

        unit_lower = unit.strip().lower()
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
        return unit_map.get(unit_lower, unit)


class FoodAtlasFlavorFoodSource:
    """Parse FoodAtlas food-chemical associations filtered to flavor compounds.

    This source:
    1. Loads chemicals with flavor metadata from metadata_flavor.tsv
    2. Loads food-chemical relationships from triplets.tsv
    3. Loads concentration data from metadata_contains.tsv
    4. Filters to only chemicals with flavor associations
    5. Produces FoodFlavorComposition records
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")
    DATA_SUBDIR = "v3.2_20250211"

    ENTITIES_FILE = "entities.tsv"
    TRIPLETS_FILE = "triplets.tsv"
    METADATA_CONTAINS_FILE = "metadata_contains.tsv"
    METADATA_FLAVOR_FILE = "metadata_flavor.tsv"

    def __init__(self, raw_data_base: str | Path | None = None):
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    def name(self) -> str:
        return "foodatlas"

    @property
    def version(self) -> str:
        return "3.2.0"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "Foodatlas" / self.DATA_SUBDIR

    def validate(self) -> list[str]:
        """Validate that required files exist."""
        errors = []
        required_files = [
            self.ENTITIES_FILE,
            self.TRIPLETS_FILE,
            self.METADATA_FLAVOR_FILE,
        ]
        for fname in required_files:
            fpath = self.raw_data_dir / fname
            if not fpath.exists():
                errors.append(
                    f"Missing file: {fpath}. "
                    "Run: python -m flavor_pipeline.acquisition.foodatlas"
                )
        return errors

    def parse(self) -> list[FoodFlavorComposition]:
        """Parse FoodAtlas to FoodFlavorComposition records.

        Returns:
            List of FoodFlavorComposition records for flavor compounds only.
        """
        if not (self.raw_data_dir / self.ENTITIES_FILE).exists():
            return []

        # Step 1: Load entities (foods and chemicals)
        entities_df = pd.read_csv(
            self.raw_data_dir / self.ENTITIES_FILE,
            sep="\t",
            dtype=str,
            low_memory=False,
        )

        # Step 2: Identify flavor chemicals from metadata_flavor.tsv
        flavor_chemical_ids = self._load_flavor_chemical_ids(entities_df)
        if not flavor_chemical_ids:
            return []

        # Step 3: Load chemical details
        chemical_details = self._load_chemical_details(entities_df)

        # Step 4: Load food details
        food_details = self._load_food_details(entities_df)

        # Step 5: Load flavor descriptors per chemical
        flavor_descriptors_map = self._load_flavor_descriptors()

        # Step 6: Load concentration data
        concentration_map = self._load_concentration_data()

        # Step 7: Load food-chemical relationships and build records
        records = self._load_relationships(
            flavor_chemical_ids,
            chemical_details,
            food_details,
            flavor_descriptors_map,
            concentration_map,
        )

        return records

    def _load_flavor_chemical_ids(self, entities_df: pd.DataFrame) -> set[str]:
        """Load IDs of chemicals that have flavor metadata.

        FoodAtlas identifies chemicals by PubChem CID in metadata_flavor.tsv.
        We need to map these back to FoodAtlas entity IDs.
        """
        flavor_path = self.raw_data_dir / self.METADATA_FLAVOR_FILE
        if not flavor_path.exists():
            return set()

        flavor_df = pd.read_csv(flavor_path, sep="\t", dtype=str)

        # Get all PubChem CIDs that have flavor metadata
        flavor_pubchem_ids: set[str] = set()
        for _, row in flavor_df.iterrows():
            chem_name = _nonempty(row.get("_chemical_name"))
            if chem_name and chem_name.startswith("PUBCHEM_COMPOUND:"):
                pubchem_id = chem_name.replace("PUBCHEM_COMPOUND:", "")
                flavor_pubchem_ids.add(pubchem_id)

        # Map PubChem CIDs to FoodAtlas entity IDs
        chemicals_df = entities_df[entities_df["entity_type"] == "chemical"]
        flavor_entity_ids: set[str] = set()

        for _, row in chemicals_df.iterrows():
            entity_id = _nonempty(row.get("foodatlas_id"))
            if not entity_id:
                continue

            external_ids = self._parse_external_ids(row.get("external_ids"))
            pubchem_cid = self._get_first_id(external_ids, "pubchem_compound")

            if pubchem_cid and pubchem_cid in flavor_pubchem_ids:
                flavor_entity_ids.add(entity_id)

        return flavor_entity_ids

    def _load_chemical_details(
        self, entities_df: pd.DataFrame
    ) -> dict[str, dict[str, Any]]:
        """Load chemical details from entities.

        Returns:
            Dict mapping foodatlas_id to details dict.
        """
        chemicals_df = entities_df[entities_df["entity_type"] == "chemical"]
        details: dict[str, dict[str, Any]] = {}

        for _, row in chemicals_df.iterrows():
            entity_id = _nonempty(row.get("foodatlas_id"))
            if not entity_id:
                continue

            external_ids = self._parse_external_ids(row.get("external_ids"))
            pubchem_cid = self._get_first_id(external_ids, "pubchem_compound")
            chebi_id = self._get_first_id(external_ids, "chebi")

            details[entity_id] = {
                "name": _nonempty(row.get("common_name")),
                "scientific_name": _nonempty(row.get("scientific_name")),
                "pubchem_id": _parse_int(pubchem_cid) if pubchem_cid else None,
                "chebi_id": chebi_id,
                # FoodAtlas doesn't provide CAS or InChIKey directly
                "cas": None,
                "inchikey": None,
            }

        return details

    def _load_food_details(
        self, entities_df: pd.DataFrame
    ) -> dict[str, dict[str, Any]]:
        """Load food details from entities."""
        foods_df = entities_df[entities_df["entity_type"] == "food"]
        details: dict[str, dict[str, Any]] = {}

        for _, row in foods_df.iterrows():
            entity_id = _nonempty(row.get("foodatlas_id"))
            if not entity_id:
                continue

            details[entity_id] = {
                "name": _nonempty(row.get("common_name")),
                "scientific_name": _nonempty(row.get("scientific_name")),
            }

        return details

    def _load_flavor_descriptors(self) -> dict[str, list[str]]:
        """Load flavor descriptors for each chemical.

        Returns:
            Dict mapping PubChem CID to list of flavor descriptors.
        """
        flavor_path = self.raw_data_dir / self.METADATA_FLAVOR_FILE
        if not flavor_path.exists():
            return {}

        flavor_df = pd.read_csv(flavor_path, sep="\t", dtype=str)
        flavor_map: dict[str, list[str]] = {}

        for _, row in flavor_df.iterrows():
            chem_name = _nonempty(row.get("_chemical_name"))
            flavor_name = _nonempty(row.get("_flavor_name"))

            if chem_name and flavor_name and chem_name.startswith("PUBCHEM_COMPOUND:"):
                pubchem_id = chem_name.replace("PUBCHEM_COMPOUND:", "")
                if pubchem_id not in flavor_map:
                    flavor_map[pubchem_id] = []
                if flavor_name not in flavor_map[pubchem_id]:
                    flavor_map[pubchem_id].append(flavor_name)

        return flavor_map

    def _load_concentration_data(
        self,
    ) -> dict[str, tuple[float | None, str | None]]:
        """Load concentration data from metadata_contains.tsv.

        Returns:
            Dict mapping metadata_id to (concentration, unit) tuple.
        """
        metadata_path = self.raw_data_dir / self.METADATA_CONTAINS_FILE
        if not metadata_path.exists():
            return {}

        meta_df = pd.read_csv(metadata_path, sep="\t", dtype=str, low_memory=False)
        conc_map: dict[str, tuple[float | None, str | None]] = {}

        for _, row in meta_df.iterrows():
            meta_id = _nonempty(row.get("foodatlas_id"))
            conc_value = _parse_float(row.get("conc_value"))
            conc_unit = _nonempty(row.get("conc_unit"))

            if meta_id:
                conc_map[meta_id] = (conc_value, conc_unit)

        return conc_map

    def _load_relationships(
        self,
        flavor_chemical_ids: set[str],
        chemical_details: dict[str, dict[str, Any]],
        food_details: dict[str, dict[str, Any]],
        flavor_descriptors_map: dict[str, list[str]],
        concentration_map: dict[str, tuple[float | None, str | None]],
    ) -> list[FoodFlavorComposition]:
        """Load food-chemical relationships from triplets.tsv."""
        triplets_path = self.raw_data_dir / self.TRIPLETS_FILE
        if not triplets_path.exists():
            return []

        triplets_df = pd.read_csv(triplets_path, sep="\t", dtype=str)
        # Filter to "contains" relationships (r1)
        contains_df = triplets_df[triplets_df["relationship_id"] == "r1"]

        records: list[FoodFlavorComposition] = []

        for _, row in contains_df.iterrows():
            food_id = _nonempty(row.get("head_id"))
            chemical_id = _nonempty(row.get("tail_id"))

            if not food_id or not chemical_id:
                continue

            # Skip non-flavor chemicals
            if chemical_id not in flavor_chemical_ids:
                continue

            # Get food details
            food = food_details.get(food_id, {})
            food_name = food.get("name")
            if not food_name:
                continue

            # Get chemical details
            chemical = chemical_details.get(chemical_id, {})
            pubchem_id = chemical.get("pubchem_id")
            inchikey = chemical.get("inchikey")
            cas = chemical.get("cas")

            # Determine molecule_id using priority: inchikey > cas > pubchem > chebi > foodatlas
            if inchikey:
                molecule_id = f"inchikey:{inchikey}"
            elif cas:
                molecule_id = f"cas:{cas}"
            elif pubchem_id:
                molecule_id = f"pubchem:{pubchem_id}"
            elif chemical.get("chebi_id"):
                molecule_id = f"chebi:{chemical['chebi_id']}"
            else:
                molecule_id = f"foodatlas:{chemical_id}"

            # Get concentration from metadata
            concentration = None
            concentration_unit = None
            metadata_ids = self._parse_list_field(row.get("metadata_ids"))
            for meta_id in metadata_ids:
                if meta_id in concentration_map:
                    concentration, concentration_unit = concentration_map[meta_id]
                    break

            # Get flavor descriptors by PubChem ID
            flavor_descriptors: list[str] = []
            if pubchem_id:
                flavor_descriptors = flavor_descriptors_map.get(str(pubchem_id), [])

            record = FoodFlavorComposition(
                food_name=food_name,
                scientific_name=food.get("scientific_name"),
                food_part=None,  # FoodAtlas doesn't track food parts
                food_category=None,  # Would need ontology lookup
                molecule_name=chemical.get("name") or chemical.get("scientific_name"),
                cas=cas,
                pubchem_id=pubchem_id,
                inchikey=inchikey,
                molecule_id=molecule_id,
                concentration=concentration,
                concentration_min=None,  # FoodAtlas doesn't provide min/max
                concentration_max=None,
                concentration_unit=concentration_unit,
                flavor_descriptors=flavor_descriptors,
                source=self.name,
                source_food_id=food_id,
                source_molecule_id=chemical_id,
            )
            records.append(record)

        return records

    def _parse_external_ids(self, value: Any) -> dict:
        """Parse external_ids column (JSON-like dict string)."""
        if _is_missing(value):
            return {}
        try:
            return ast.literal_eval(str(value))
        except (ValueError, SyntaxError):
            try:
                return json.loads(str(value))
            except json.JSONDecodeError:
                return {}

    def _get_first_id(self, external_ids: dict, key: str) -> str | None:
        """Get the first ID from an external_ids dict for a given key."""
        ids = external_ids.get(key, [])
        if ids and len(ids) > 0:
            return str(ids[0])
        return None

    def _parse_list_field(self, value: Any) -> list[str]:
        """Parse a list field stored as Python list repr."""
        if _is_missing(value):
            return []
        try:
            result = ast.literal_eval(str(value))
            if isinstance(result, list):
                return [str(item) for item in result if item]
            return []
        except (ValueError, SyntaxError):
            return []
