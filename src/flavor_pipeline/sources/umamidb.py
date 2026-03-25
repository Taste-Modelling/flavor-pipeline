"""UmamiDB source adapter for foods with amino acid and nucleotide data.

UmamiDB provides umami content measurements for ~700 foods including:
- 20 free amino acids (mg/100g): Asp, Thr, Ser, Asn, Glu, Gln, Pro, Gly,
  Ala, Val, Cys, Met, Ile, Leu, Tyr, Phe, Trp, Lys, His, Arg
- 3 nucleotides (mg/100g): IMP (inosinate), GMP (guanylate), AMP (adenylic acid)

Foods are categorized into groups like vegetables, seafood, meat, dairy, etc.
Data sources include academic papers and analyses commissioned by the
Umami Information Center.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.food import IngestMetadata as FoodIngestMetadata
from flavor_pipeline.schemas.food import Tier1Food
from flavor_pipeline.schemas.tier1 import SourceMetadata

# Mapping of JSON field names to human-readable amino acid names
AMINO_ACID_FIELDS = {
    "free_amino_acid01_Asp": "aspartate",
    "free_amino_acid02_Thr": "threonine",
    "free_amino_acid03_Ser": "serine",
    "free_amino_acid04_Asn": "asparagine",
    "free_amino_acid05_Glu": "glutamate",
    "free_amino_acid06_Gln": "glutamine",
    "free_amino_acid07_Pro": "proline",
    "free_amino_acid08_Gly": "glycine",
    "free_amino_acid09_Ala": "alanine",
    "free_amino_acid10_Val": "valine",
    "free_amino_acid11_Cys": "cysteine",
    "free_amino_acid12_Met": "methionine",
    "free_amino_acid13_Ile": "isoleucine",
    "free_amino_acid14_Leu": "leucine",
    "free_amino_acid15_Tyr": "tyrosine",
    "free_amino_acid16_Phe": "phenylalanine",
    "free_amino_acid17_Trp": "tryptophan",
    "free_amino_acid18_Lys": "lysine",
    "free_amino_acid19_His": "histidine",
    "free_amino_acid20_Arg": "arginine",
}

NUCLEOTIDE_FIELDS = {
    "nucleic_acid01_IMP": "inosinate",  # IMP - umami enhancer
    "nucleic_acid02_GMP": "guanylate",  # GMP - umami enhancer
    "nucleic_acid03_AMP": "adenylate",  # AMP - flavor modifier
}


class UmamiDBSource:
    """Parse UmamiDB foods to Tier1Food format.

    Extracts foods with their amino acid and nucleotide measurements.
    Key umami compounds tracked:
    - Glutamate (Glu) - primary umami taste
    - Aspartate (Asp) - umami contributor
    - IMP, GMP - umami synergists (enhance glutamate perception 7-8x)
    """

    DEFAULT_RAW_DATA_BASE = Path("raw_data")
    PARSER_VERSION = "0.1.0"
    PIPELINE_VERSION = "0.1.0"

    FOODS_FILE = "foods.json"

    def __init__(self, raw_data_base: str | Path | None = None):
        self._raw_data_base = Path(
            raw_data_base if raw_data_base is not None else self.DEFAULT_RAW_DATA_BASE
        )

    @property
    def name(self) -> str:
        return "umamidb"

    @property
    def version(self) -> str:
        return "2025.03"

    @property
    def url(self) -> str:
        return "https://www.umamiinfo.com/umamidb/"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "Umamidb"

    def validate(self) -> list[str]:
        errors = []
        foods_path = self.raw_data_dir / self.FOODS_FILE

        if not foods_path.exists():
            errors.append(
                f"Missing file: {foods_path}. "
                "Run: python -m flavor_pipeline.acquisition.umamidb"
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
        """Parse UmamiDB foods to Tier 1 format."""
        foods_path = self.raw_data_dir / self.FOODS_FILE
        if not foods_path.exists():
            return []

        with open(foods_path, encoding="utf-8") as f:
            raw_foods = json.load(f)

        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        foods = []
        for idx, record in enumerate(raw_foods):
            food_name = self._nonempty(record.get("sample_name_en"))
            if not food_name:
                continue

            # Generate stable food_id from index and name
            food_id = f"umamidb:{idx}"

            category = self._nonempty(record.get("category_en"))
            producer = self._nonempty(record.get("producer_en"))
            source_ref = self._nonempty(record.get("web_en_remarks"))

            # Extract amino acid measurements
            amino_acids = {}
            for field, aa_name in AMINO_ACID_FIELDS.items():
                value = self._parse_float(record.get(field))
                if value is not None:
                    amino_acids[aa_name] = self._av(value, unit="mg/100g")

            # Extract nucleotide measurements
            nucleotides = {}
            for field, nuc_name in NUCLEOTIDE_FIELDS.items():
                value = self._parse_float(record.get(field))
                if value is not None:
                    nucleotides[nuc_name] = self._av(value, unit="mg/100g")

            # Build extra fields with all measurements
            extra: dict[str, AttributedValue] = {}

            # Add amino acids to extra
            for aa_name, av in amino_acids.items():
                extra[f"amino_acid_{aa_name}"] = av

            # Add nucleotides to extra
            for nuc_name, av in nucleotides.items():
                extra[f"nucleotide_{nuc_name}"] = av

            # Add source reference if available
            if source_ref:
                extra["source_reference"] = self._av(source_ref)

            # Add producer/region
            if producer:
                extra["producer"] = self._av(producer)

            # Calculate total umami score (glutamate is primary)
            glu = amino_acids.get("glutamate")
            asp = amino_acids.get("aspartate")
            if glu or asp:
                # Simple umami intensity estimate
                glu_val = glu.value if glu else 0
                asp_val = asp.value if asp else 0
                extra["total_umami_amino_acids"] = self._av(
                    round(glu_val + asp_val, 2), unit="mg/100g"
                )

            food = Tier1Food(
                food_id=food_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                name=self._av(food_name),
                category=self._av(category) if category else None,
                extra=extra,
            )
            foods.append(food)

        return foods

    def _av(self, value: Any, unit: str | None = None) -> AttributedValue:
        """Create an AttributedValue with this source."""
        return AttributedValue(value=value, unit=unit, sources=[self.name])

    def _nonempty(self, val: Any) -> str | None:
        """Return value if not empty/null, else None."""
        if val is None or val == "":
            return None
        return str(val).strip()

    def _parse_float(self, val: Any) -> float | None:
        """Parse a value as float, return None on failure."""
        if val is None or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
