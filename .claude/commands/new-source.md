# New Source Adapter

Create a new source adapter that parses raw data into Tier1Molecule format.

## Arguments

- `$ARGUMENTS` - Name of the data source (e.g., "goodscents", "superscent")

## Instructions

Create a source adapter that transforms raw CSV data into `Tier1Molecule` objects with `AttributedValue` provenance tracking.

### 1. Understand the Raw Data

First, examine the raw data structure:
```bash
head -5 raw_data/{SourceName}/*.csv
```

Identify:
- **Available fields**: What columns exist?
- **Identifier fields**: PubChem CID, CAS number, or source-specific ID?
- **Sensory data**: Flavor, odor, or taste descriptors?
- **Chemical data**: SMILES, InChI, molecular formula/weight?
- **Delimiter for lists**: Are descriptors comma-separated, pipe-separated, etc.?

### 2. Create the Source Adapter

Create `src/flavor_pipeline/sources/{name}.py`:

```python
"""[SourceName] source adapter for Tier 1 molecules."""

from pathlib import Path

import pandas as pd

from flavor_pipeline.schemas.tier1 import Tier1Molecule

from .base import BaseSource


class [SourceName]Source(BaseSource):
    """Parse [SourceName] raw data to Tier 1 molecules.

    Expects raw_data/[SourceName]/[files] produced by the acquirer.
    """

    COMPOUNDS_FILE = "compounds.csv"

    @property
    def name(self) -> str:
        return "[source_name]"

    @property
    def url(self) -> str:
        return "[source_url]"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "[SourceName]"

    def validate(self) -> list[str]:
        errors = []
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE
        if not compounds_path.exists():
            errors.append(f"Missing file: {compounds_path}")
        return errors

    def parse(self) -> list[Tier1Molecule]:
        """Parse raw data to Tier 1 molecules."""
        compounds_path = self.raw_data_dir / self.COMPOUNDS_FILE
        if not compounds_path.exists():
            return []

        df = pd.read_csv(compounds_path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for _, row in df.iterrows():
            # Determine molecule_id (priority: pubchem > cas > source-specific)
            pubchem_cid = self._parse_int(row.get("pubchem_id"))
            cas = self._nonempty(row.get("cas"))
            source_id = self._nonempty(row.get("id"))

            if pubchem_cid:
                molecule_id = f"pubchem:{pubchem_cid}"
            elif cas:
                molecule_id = f"cas:{cas}"
            elif source_id:
                molecule_id = f"[source_name]:{source_id}"
            else:
                continue  # Skip records without identifiers

            mol = Tier1Molecule(
                molecule_id=molecule_id,
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                # Map fields to Tier1Molecule schema using self._av()
                pubchem_cid=self._av(pubchem_cid) if pubchem_cid else None,
                cas=self._av(cas) if cas else None,
                name=self._av(self._nonempty(row.get("name")))
                if self._nonempty(row.get("name"))
                else None,
                # Add more field mappings as needed...
            )
            molecules.append(mol)

        return molecules
```

### 3. Register the Source

Add to `src/flavor_pipeline/sources/__init__.py`:

```python
from flavor_pipeline.sources.[name] import [SourceName]Source

SOURCES: dict[str, type[BaseSource]] = {
    # ... existing sources ...
    "[name]": [SourceName]Source,
}

__all__ = [
    # ... existing exports ...
    "[SourceName]Source",
]
```

### 4. Create the Tier1 Asset

Add to `src/flavor_pipeline/assets/tier1.py`:

```python
@asset(
    group_name="tier1",
    deps=["[name]_raw"],
    description="[SourceName] data parsed to Tier 1 format",
)
def [name]_tier1(context: AssetExecutionContext) -> None:
    """Parse [SourceName] raw data to Tier 1 molecules."""
    source = [SourceName]Source()

    errors = source.validate()
    if errors:
        context.log.warning(f"Validation warnings: {errors}")
        return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "[name].parquet"
    json_path = TIER1_OUTPUT_DIR / "[name].json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")
```

Don't forget to:
- Import `[SourceName]Source` at the top of tier1.py
- Export the asset in `assets/__init__.py`
- Add `"[name]_tier1"` to the deps list of `merged_tier2` asset in `tier2.py`

### 5. Verify

```bash
python -m py_compile src/flavor_pipeline/sources/{name}.py
```

## Tier1Molecule Field Reference

Available fields to map (all optional, all use `AttributedValue`):

| Field | Type | Description |
|-------|------|-------------|
| `pubchem_cid` | int | PubChem Compound ID |
| `cas` | str | CAS Registry Number |
| `smiles` | str | SMILES string |
| `inchi` | str | InChI string |
| `inchi_key` | str | InChI Key |
| `name` | str | Common name |
| `iupac_name` | str | IUPAC systematic name |
| `synonyms` | list[str] | Alternative names |
| `flavor_descriptors` | list[str] | Flavor notes |
| `odor_descriptors` | list[str] | Odor notes |
| `taste_descriptors` | list[str] | Taste notes (sweet, bitter, etc.) |
| `molecular_weight` | float | MW in g/mol |
| `molecular_formula` | str | Chemical formula |
| `extra` | dict | Source-specific fields |

## Helper Methods from BaseSource

- `self._av(value, unit=None)` - Create AttributedValue with this source
- `self._nonempty(val)` - Return value if not empty/null
- `self._parse_int(val)` - Parse as int, return None on failure
- `self._parse_float(val)` - Parse as float, return None on failure

## Example Field Mapping

```python
# For a CSV with: id,name,cas,flavors,mw
mol = Tier1Molecule(
    molecule_id=f"cas:{cas}",
    _ingest_metadata=ingest_meta,
    _sources={self.name: source_meta},
    cas=self._av(cas),
    name=self._av(self._nonempty(row.get("name"))),
    flavor_descriptors=self._av(self._parse_pipe_delimited(row.get("flavors"))),
    molecular_weight=self._av(self._parse_float(row.get("mw")), unit="g/mol"),
    extra={
        "source_id": self._av(row.get("id")),
    },
)
```
