"""FoodAtlas source adapters for molecules and foods.

FoodAtlas is an evidence-based knowledge graph from UC Davis linking foods
to chemicals with provenance-tracked relationships from scientific literature.
"""

import ast
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.food import IngestMetadata as FoodIngestMetadata
from flavor_pipeline.schemas.food import MoleculeComposition, Tier1Food
from flavor_pipeline.schemas.tier1 import SourceMetadata, Tier1Molecule

from .base import BaseSource


def _is_missing(value: Any) -> bool:
    """Check if a value is missing/empty."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value == "":
        return True
    return False


class FoodAtlasMoleculeSource(BaseSource):
    """Parse FoodAtlas chemicals to Tier1Molecule format.

    Extracts chemical entities with ChEBI IDs, PubChem CIDs, MESH IDs,
    names, synonyms, and flavor descriptors from the FoodAtlas knowledge graph.
    """

    # Version subdirectory in the extracted archive
    DATA_SUBDIR = "v3.2_20250211"

    ENTITIES_FILE = "entities.tsv"
    METADATA_FLAVOR_FILE = "metadata_flavor.tsv"

    @property
    def name(self) -> str:
        return "foodatlas"

    @property
    def version(self) -> str:
        return "3.2.0"

    @property
    def url(self) -> str:
        return "https://www.foodatlas.ai/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "Foodatlas" / self.DATA_SUBDIR

    def validate(self) -> list[str]:
        errors = []
        entities_path = self.raw_data_dir / self.ENTITIES_FILE

        if not entities_path.exists():
            errors.append(
                f"Missing file: {entities_path}. "
                "Run: python -m flavor_pipeline.acquisition.foodatlas"
            )

        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse FoodAtlas chemical entities to Tier 1 molecules."""
        entities_path = self.raw_data_dir / self.ENTITIES_FILE
        if not entities_path.exists():
            return []

        # Load entities and filter to chemicals
        df = pd.read_csv(entities_path, sep="\t", dtype=str, low_memory=False)
        chemicals_df = df[df["entity_type"] == "chemical"].copy()

        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        # Load flavor descriptors
        flavor_map = self._load_flavor_map()

        molecules = []
        for _, row in chemicals_df.iterrows():
            foodatlas_id = self._nonempty(row.get("foodatlas_id"))
            if foodatlas_id is None:
                continue

            # Parse external IDs (stored as JSON-like dict string)
            external_ids = self._parse_external_ids(row.get("external_ids"))
            pubchem_cid = self._get_first_id(external_ids, "pubchem_compound")
            chebi_id = self._get_first_id(external_ids, "chebi")
            mesh_id = self._get_first_id(external_ids, "mesh")

            # Use PubChem CID or ChEBI for molecule_id, else FoodAtlas ID
            if pubchem_cid:
                molecule_id = f"pubchem:{pubchem_cid}"
            elif chebi_id:
                molecule_id = f"chebi:{chebi_id}"
            else:
                molecule_id = f"foodatlas:{foodatlas_id}"

            # Names
            common_name = self._nonempty(row.get("common_name"))
            scientific_name = self._nonempty(row.get("scientific_name"))

            # Synonyms (stored as Python list string)
            synonyms = self._parse_list_field(row.get("synonyms"))

            # Get flavor descriptors for this chemical (by PubChem CID)
            flavor_descriptors: list[str] = []
            if pubchem_cid:
                flavor_descriptors = flavor_map.get(f"PUBCHEM_COMPOUND:{pubchem_cid}", [])

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                pubchem_cid=self._av(int(pubchem_cid)) if pubchem_cid else None,
                name=self._av(common_name) if common_name else None,
                iupac_name=self._av(scientific_name)
                if scientific_name and scientific_name != common_name
                else None,
                synonyms=self._av(synonyms) if synonyms else None,
                flavor_descriptors=self._av(flavor_descriptors) if flavor_descriptors else None,
                extra={
                    "foodatlas_id": self._av(foodatlas_id),
                    **({"chebi_id": self._av(chebi_id)} if chebi_id else {}),
                    **({"mesh_id": self._av(mesh_id)} if mesh_id else {}),
                },
            )
            molecules.append(mol)

        return molecules

    def _parse_external_ids(self, value: Any) -> dict:
        """Parse external_ids column (JSON-like dict string)."""
        if _is_missing(value):
            return {}
        try:
            # External IDs are stored as Python dict repr, e.g.:
            # {'chebi': [9349], 'pubchem_compound': [6213], 'mesh': ['C025910']}
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

    def _load_flavor_map(self) -> dict[str, list[str]]:
        """Load flavor descriptors from metadata_flavor.tsv.

        Returns:
            Dict mapping chemical identifier (e.g., "PUBCHEM_COMPOUND:1060")
            to list of flavor descriptors.
        """
        flavor_path = self.raw_data_dir / self.METADATA_FLAVOR_FILE
        if not flavor_path.exists():
            return {}

        flavor_df = pd.read_csv(flavor_path, sep="\t", dtype=str)
        flavor_map: dict[str, list[str]] = {}

        for _, row in flavor_df.iterrows():
            chemical_name = self._nonempty(row.get("_chemical_name"))
            flavor_name = self._nonempty(row.get("_flavor_name"))

            if chemical_name and flavor_name:
                if chemical_name not in flavor_map:
                    flavor_map[chemical_name] = []
                if flavor_name not in flavor_map[chemical_name]:
                    flavor_map[chemical_name].append(flavor_name)

        return flavor_map


class FoodAtlasFoodSource:
    """Parse FoodAtlas foods to Tier1Food format.

    Extracts food entities with names, categories (via ontology), and
    molecular composition from food-chemical relationships.

    Note: This does not inherit from BaseSource since it parses foods,
    not molecules. It provides a compatible interface for the Tier1 food
    asset pipeline.
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")
    PARSER_VERSION = "0.1.0"
    PIPELINE_VERSION = "0.1.0"

    # Version subdirectory in the extracted archive
    DATA_SUBDIR = "v3.2_20250211"

    ENTITIES_FILE = "entities.tsv"
    TRIPLETS_FILE = "triplets.tsv"
    METADATA_CONTAINS_FILE = "metadata_contains.tsv"
    FOOD_ONTOLOGY_FILE = "food_ontology.tsv"

    def __init__(self, raw_data_base: str | Path | None = None):
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    def name(self) -> str:
        return "foodatlas_food"

    @property
    def version(self) -> str:
        return "3.2.0"

    @property
    def url(self) -> str:
        return "https://www.foodatlas.ai/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "Foodatlas" / self.DATA_SUBDIR

    def validate(self) -> list[str]:
        errors = []
        entities_path = self.raw_data_dir / self.ENTITIES_FILE

        if not entities_path.exists():
            errors.append(
                f"Missing file: {entities_path}. "
                "Run: python -m flavor_pipeline.acquisition.foodatlas"
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
        """Parse FoodAtlas food entities to Tier 1 foods."""
        entities_path = self.raw_data_dir / self.ENTITIES_FILE
        if not entities_path.exists():
            return []

        # Load entities and filter to foods
        df = pd.read_csv(entities_path, sep="\t", dtype=str, low_memory=False)
        foods_df = df[df["entity_type"] == "food"].copy()

        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        # Load food categories from ontology
        category_map = self._load_food_categories()

        # Load food-chemical composition
        composition_map = self._load_composition()

        # Build chemical ID map for creating molecule references
        chemical_id_map = self._build_chemical_id_map(df)

        foods = []
        for _, row in foods_df.iterrows():
            foodatlas_id = self._nonempty(row.get("foodatlas_id"))
            if foodatlas_id is None:
                continue

            food_id = f"foodatlas:{foodatlas_id}"

            # Names
            common_name = self._nonempty(row.get("common_name"))
            scientific_name = self._nonempty(row.get("scientific_name"))

            # Category from ontology (parent in is_a hierarchy)
            category = category_map.get(foodatlas_id)

            # Molecular composition
            composition = []
            chemical_ids = composition_map.get(foodatlas_id, [])
            for chem_id, conc_value, conc_unit in chemical_ids:
                # Convert FoodAtlas chemical ID to our molecule_id format
                mol_id = chemical_id_map.get(chem_id)
                if mol_id:
                    comp = MoleculeComposition(
                        molecule_id=mol_id,
                        concentration=self._av(conc_value, unit=conc_unit)
                        if conc_value is not None
                        else None,
                    )
                    composition.append(comp)

            food = Tier1Food(
                food_id=food_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                name=self._av(common_name) if common_name else None,
                scientific_name=self._av(scientific_name) if scientific_name else None,
                category=self._av(category) if category else None,
                composition=composition,
                extra={
                    "foodatlas_id": self._av(foodatlas_id),
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

    def _parse_float(self, val: Any) -> float | None:
        """Parse a value as float, return None on failure."""
        if _is_missing(val):
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _load_food_categories(self) -> dict[str, str]:
        """Load food categories from food_ontology.tsv (is_a relationships).

        Returns:
            Dict mapping food_id to category name.
        """
        ontology_path = self.raw_data_dir / self.FOOD_ONTOLOGY_FILE
        if not ontology_path.exists():
            return {}

        ont_df = pd.read_csv(ontology_path, sep="\t", dtype=str)
        category_map: dict[str, str] = {}

        # food_ontology has: child_id, parent_id, parent_name
        for _, row in ont_df.iterrows():
            child_id = self._nonempty(row.get("child_id"))
            parent_name = self._nonempty(row.get("parent_name"))
            if child_id and parent_name:
                # Take the first category if multiple
                if child_id not in category_map:
                    category_map[child_id] = parent_name

        return category_map

    def _load_composition(self) -> dict[str, list[tuple[str, float | None, str | None]]]:
        """Load food-chemical composition from triplets and metadata_contains.

        Returns:
            Dict mapping food_id to list of (chemical_id, concentration, unit) tuples.
        """
        triplets_path = self.raw_data_dir / self.TRIPLETS_FILE
        metadata_path = self.raw_data_dir / self.METADATA_CONTAINS_FILE

        if not triplets_path.exists():
            return {}

        # Load triplets (food contains chemical relationships)
        triplets_df = pd.read_csv(triplets_path, sep="\t", dtype=str)

        # Filter to "contains" relationships (r1)
        contains_df = triplets_df[triplets_df["relationship_id"] == "r1"]

        # Load metadata for concentrations
        conc_map: dict[str, tuple[float | None, str | None]] = {}
        if metadata_path.exists():
            meta_df = pd.read_csv(metadata_path, sep="\t", dtype=str, low_memory=False)
            for _, row in meta_df.iterrows():
                meta_id = self._nonempty(row.get("foodatlas_id"))
                conc_value = self._parse_float(row.get("conc_value"))
                conc_unit = self._nonempty(row.get("conc_unit"))
                if meta_id:
                    conc_map[meta_id] = (conc_value, conc_unit)

        # Build composition map
        composition_map: dict[str, list[tuple[str, float | None, str | None]]] = {}

        for _, row in contains_df.iterrows():
            food_id = self._nonempty(row.get("head_id"))
            chemical_id = self._nonempty(row.get("tail_id"))
            metadata_ids = self._parse_list_field(row.get("metadata_ids"))

            if food_id and chemical_id:
                # Get concentration from first metadata entry
                conc_value = None
                conc_unit = None
                for meta_id in metadata_ids:
                    if meta_id in conc_map:
                        conc_value, conc_unit = conc_map[meta_id]
                        break

                if food_id not in composition_map:
                    composition_map[food_id] = []
                composition_map[food_id].append((chemical_id, conc_value, conc_unit))

        return composition_map

    def _build_chemical_id_map(self, entities_df: pd.DataFrame) -> dict[str, str]:
        """Build map from FoodAtlas chemical ID to our molecule_id format.

        Returns:
            Dict mapping FoodAtlas ID (e.g., "e10271") to molecule_id
            (e.g., "pubchem:6213").
        """
        chemicals_df = entities_df[entities_df["entity_type"] == "chemical"]
        id_map: dict[str, str] = {}

        for _, row in chemicals_df.iterrows():
            foodatlas_id = self._nonempty(row.get("foodatlas_id"))
            if not foodatlas_id:
                continue

            external_ids = self._parse_external_ids(row.get("external_ids"))
            pubchem_cid = self._get_first_id(external_ids, "pubchem_compound")
            chebi_id = self._get_first_id(external_ids, "chebi")

            if pubchem_cid:
                id_map[foodatlas_id] = f"pubchem:{pubchem_cid}"
            elif chebi_id:
                id_map[foodatlas_id] = f"chebi:{chebi_id}"
            else:
                id_map[foodatlas_id] = f"foodatlas:{foodatlas_id}"

        return id_map

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
