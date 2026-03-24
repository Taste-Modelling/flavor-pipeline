# New Acquirer

Create a new data acquirer for the flavor pipeline.

## Arguments

- `$ARGUMENTS` - Name of the data source (e.g., "goodscents", "superscent")

## Instructions

Create a new acquirer that fetches raw data from an external source. Follow these steps:

### 1. Gather Information

Ask the user for:
- **Source URL**: Where does the data come from?
- **Data format**: Is it a web scrape, CSV download, PDF extraction, or API?
- **Output files**: What CSV files will be produced?
- **Approximate record count**: How many molecules/compounds?

### 2. Create the Acquirer Class

Create a new file at `src/flavor_pipeline/acquirers/{name}.py` following this template:

```python
"""[SourceName] acquirer implementation."""

from pathlib import Path

from flavor_pipeline.acquirers.base import AcquisitionError, BaseAcquirer


class [SourceName]Acquirer(BaseAcquirer):
    """Acquire data from [SourceName].

    [Description of what this source contains and how it's acquired]
    """

    name = "[source_name]"
    description = "[Brief description for Dagster UI]"
    url = "[source_url]"  # or None for PDF extraction

    @property
    def output_dir(self) -> Path:
        return self._raw_data_base / "[SourceName]"

    @property
    def output_files(self) -> list[str]:
        return ["compounds.csv"]  # List expected output files

    def fetch(self) -> Path:
        """Fetch data from [SourceName]."""
        from flavor_pipeline.acquisition.[name] import fetch_[name]

        try:
            result = fetch_[name](output_dir=self.output_dir)
            return result
        except Exception as e:
            raise AcquisitionError(f"Failed to fetch [SourceName]: {e}") from e
```

### 3. Register the Acquirer

Add to `src/flavor_pipeline/acquirers/__init__.py`:

```python
from flavor_pipeline.acquirers.[name] import [SourceName]Acquirer

ACQUIRER_CLASSES: dict[str, type[BaseAcquirer]] = {
    # ... existing acquirers ...
    "[name]": [SourceName]Acquirer,
}
```

### 4. Create the Acquisition Function (if needed)

If the acquisition logic doesn't exist yet, create `src/flavor_pipeline/acquisition/{name}.py` with the fetch function:

```python
"""Fetch data from [SourceName]."""

from pathlib import Path

def fetch_[name](output_dir: Path) -> Path:
    """Fetch [SourceName] data.

    Args:
        output_dir: Directory to write output files.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Implementation here...

    return output_dir
```

And register it in `src/flavor_pipeline/acquisition/__init__.py`.

### 5. Verify

After creation, verify the syntax:
```bash
python -m py_compile src/flavor_pipeline/acquirers/{name}.py
```

## Architecture Context

The acquirer system uses an abstract base class pattern:

- `BaseAcquirer` (ABC) enforces: `name`, `output_files`, `fetch()`
- Provides shared logic: `is_cached()`, `validate()`, `get_metadata()`
- Factory in `acquirers/factory.py` generates Dagster assets automatically
- Raw data goes to `raw_data/{SourceName}/`

## Example

For a source called "goodscents":
- File: `src/flavor_pipeline/acquirers/goodscents.py`
- Class: `GoodScentsAcquirer`
- Property: `name = "goodscents"`
- Output: `raw_data/GoodScents/compounds.csv`
