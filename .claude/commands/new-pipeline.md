# New Data Pipeline

Create a complete pipeline for a new data source: acquirer → source adapter → Dagster assets.

## Arguments

- `$ARGUMENTS` - Name of the data source (e.g., "goodscents", "superscent")

## Instructions

This skill creates all components needed to add a new data source to the flavor pipeline.

### Step 1: Gather Requirements

Ask the user for:

1. **Source Information**
   - Source name (e.g., "goodscents")
   - Source URL
   - Brief description

2. **Acquisition Method**
   - Web scraping?
   - CSV/file download?
   - API calls?
   - PDF extraction?

3. **Data Structure**
   - What identifiers are available? (PubChem CID, CAS, FEMA, etc.)
   - What sensory data? (flavor, odor, taste descriptors)
   - What chemical data? (SMILES, InChI, formula, MW)
   - Expected record count?

### Step 2: Create the Acquisition Function

Create `src/flavor_pipeline/acquisition/{name}.py`:

```python
"""Fetch data from [SourceName]."""

import csv
from pathlib import Path

import requests
from bs4 import BeautifulSoup  # if web scraping

DEFAULT_OUTPUT_DIR = Path("raw_data/[SourceName]")


def fetch_[name](output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Fetch [SourceName] data.

    Args:
        output_dir: Directory to write output files.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "compounds.csv"

    # Implement acquisition logic here...

    print(f"Saved: {output_csv}")
    return output_dir
```

Register in `src/flavor_pipeline/acquisition/__init__.py`:
```python
from flavor_pipeline.acquisition.[name] import fetch_[name]
```

### Step 3: Create the Acquirer Class

Create `src/flavor_pipeline/acquirers/{name}.py`:

```python
"""[SourceName] acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class [SourceName]Acquirer(BaseAcquirer):
    """Acquire data from [SourceName]."""

    name = "[name]"
    description = "[Description]"
    url = "[URL]"

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "[SourceName]"

    @property
    def output_files(self) -> list[str]:
        return ["compounds.csv"]

    def fetch(self) -> Path:
        from flavor_pipeline.acquisition.[name] import fetch_[name]
        try:
            return fetch_[name](output_dir=self.output_dir)
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch [SourceName]: {e}") from e
```

Register in `src/flavor_pipeline/acquirers/__init__.py`.

### Step 4: Create the Source Adapter

Create `src/flavor_pipeline/sources/{name}.py`:

```python
"""[SourceName] source adapter."""

from pathlib import Path
import pandas as pd
from flavor_pipeline.schemas.tier1 import Tier1Molecule
from .base import BaseSource


class [SourceName]Source(BaseSource):
    """Parse [SourceName] to Tier 1 molecules."""

    COMPOUNDS_FILE = "compounds.csv"

    @property
    def name(self) -> str:
        return "[name]"

    @property
    def url(self) -> str:
        return "[URL]"

    @property
    def raw_data_dir(self) -> Path:
        return self._raw_data_base / "[SourceName]"

    def validate(self) -> list[str]:
        errors = []
        if not (self.raw_data_dir / self.COMPOUNDS_FILE).exists():
            errors.append(f"Missing: {self.raw_data_dir / self.COMPOUNDS_FILE}")
        return errors

    def parse(self) -> list[Tier1Molecule]:
        path = self.raw_data_dir / self.COMPOUNDS_FILE
        if not path.exists():
            return []

        df = pd.read_csv(path, dtype=str)
        source_meta = self.get_source_metadata()
        ingest_meta = self.get_ingest_metadata()

        molecules = []
        for _, row in df.iterrows():
            # Map fields to Tier1Molecule...
            mol = Tier1Molecule(
                molecule_id="...",
                _ingest_metadata=ingest_meta,
                _sources={self.name: source_meta},
                # ... field mappings
            )
            molecules.append(mol)

        return molecules
```

Register in `src/flavor_pipeline/sources/__init__.py`.

### Step 5: Create the Tier1 Asset

Add to `src/flavor_pipeline/assets/tier1.py`:

```python
@asset(
    group_name="tier1",
    deps=["[name]_raw"],
    description="[SourceName] parsed to Tier 1",
)
def [name]_tier1(context: AssetExecutionContext) -> None:
    source = [SourceName]Source()
    errors = source.validate()
    if errors:
        context.log.warning(f"Validation: {errors}")
        return

    molecules = source.parse()
    parquet_path = TIER1_OUTPUT_DIR / "[name].parquet"
    json_path = TIER1_OUTPUT_DIR / "[name].json"

    _save_molecules_to_parquet(molecules, parquet_path)
    count = _save_molecules_to_json(molecules, json_path)

    context.log.info(f"Saved {count} molecules to {parquet_path} and {json_path}")
```

Don't forget to add `"[name]_tier1"` to the deps list of the `merged_tier2` asset in `tier2.py`.

### Step 6: Verify Everything

```bash
# Syntax check
python -m py_compile src/flavor_pipeline/acquisition/{name}.py
python -m py_compile src/flavor_pipeline/acquirers/{name}.py
python -m py_compile src/flavor_pipeline/sources/{name}.py

# Run acquisition (if data is accessible)
python -c "from flavor_pipeline.acquirers import [SourceName]Acquirer; [SourceName]Acquirer().fetch()"

# Check Dagster sees the assets
dagster asset list -m flavor_pipeline.definitions | grep [name]
```

## Files Created

After completion, the following files should exist:

```
src/flavor_pipeline/
├── acquisition/
│   └── {name}.py           # fetch_{name}()
├── acquirers/
│   └── {name}.py           # {SourceName}Acquirer
├── sources/
│   └── {name}.py           # {SourceName}Source
└── assets/
    └── tier1.py            # {name}_tier1 asset (updated)
```

## Pipeline Flow

```
[External Source]
       │
       ▼
fetch_{name}() ─────► raw_data/{SourceName}/compounds.csv
       │
       ▼
{name}_raw (Dagster asset via acquirer factory)
       │
       ▼
{SourceName}Source.parse() ─────► list[Tier1Molecule]
       │
       ▼
{name}_tier1 (Dagster asset) ─────► data/tier1/{name}.parquet
                                    data/tier1/{name}.json
       │
       ▼
merged_tier2 ─────► data/tier2/merged.parquet
                    data/tier2/merged.json (multi-source merged)
```

## Checklist

- [ ] Acquisition function created and registered
- [ ] Acquirer class created and registered
- [ ] Source adapter created and registered
- [ ] Tier1 asset created and registered (saves both parquet and JSON)
- [ ] New source added to `merged_tier2` deps in `tier2.py`
- [ ] All files pass syntax check
- [ ] Dagster recognizes new assets
