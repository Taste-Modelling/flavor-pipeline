# Architecture Overview

Explain the flavor pipeline architecture and design patterns.

## Instructions

Provide a comprehensive overview of the flavor pipeline architecture when the user asks about how the system works.

## Core Concepts

### Three-Tier Data Model

```
Tier 0 (Raw)              Tier 1 (Normalized)         Tier 2 (Merged)
─────────────             ──────────────────          ───────────────
raw_data/{Source}/*.csv   data/tier1/{source}.parquet data/tier2/merged.json
                          data/tier1/{source}.json    data/tier2/merged.parquet
                          │                           │
                          └── Tier1Molecule           └── Tier2Molecule
                              (single source)             (multi-source merged)
```

### Attributed Value Object (AVO) Pattern

Every field value is wrapped in `AttributedValue` to track provenance:

```python
AttributedValue(
    value="sweet",           # The actual data
    unit=None,               # Optional unit
    sources=["flavordb2"],   # Which source(s) provided this
    conditions=None,         # Experimental conditions (future)
)
```

This enables:
- Multi-source consolidation
- Conflict detection
- Audit trail

### Sparse Schema Design

`Tier1Molecule` fields are all optional because different sources provide different data:

```python
class Tier1Molecule:
    molecule_id: str                      # Required
    _ingest_metadata: IngestMetadata      # Required
    _sources: dict[str, SourceMetadata]   # Required

    # All optional - sparse schema
    pubchem_cid: AttributedValue | None
    cas: AttributedValue | None
    smiles: AttributedValue | None
    flavor_descriptors: AttributedValue | None
    # ... etc
```

`Tier2Molecule` uses `list[AttributedValue]` to handle conflicting values from multiple sources:

```python
class Tier2Molecule:
    molecule_id: str                      # Required
    _merge_metadata: MergeMetadata        # Required (source_count, conflict_count)
    _sources: dict[str, SourceMetadata]   # Combined from all contributing sources

    # All fields are lists to preserve conflicting values
    name: list[AttributedValue] | None    # May have different names from each source
    cas: list[AttributedValue] | None     # Usually identical, combined sources
    # ... etc
```

## Module Structure

```
src/flavor_pipeline/
│
├── schemas/                 # Pydantic data models
│   ├── avo.py              # AttributedValue
│   ├── tier1.py            # Tier1Molecule, SourceMetadata
│   └── tier2.py            # Tier2Molecule, MergeMetadata
│
├── acquisition/             # Raw data fetching (pure Python)
│   ├── flavordb2.py        # fetch_flavordb2()
│   ├── bitterdb.py         # fetch_bitterdb()
│   └── ...
│
├── acquirers/               # Class-based acquisition
│   ├── base.py             # BaseAcquirer (ABC)
│   ├── factory.py          # Dagster asset generation
│   ├── flavordb2.py        # FlavorDB2Acquirer
│   └── ...
│
├── sources/                 # CSV → Tier1Molecule parsing
│   ├── base.py             # BaseSource (ABC)
│   ├── flavordb2.py        # FlavorDB2Source
│   └── ...
│
├── consolidation/           # Tier1 → Tier2 merging
│   ├── __init__.py
│   └── merger.py           # Tier1Merger
│
├── assets/                  # Dagster asset definitions
│   ├── tier1.py            # *_tier1 assets
│   └── tier2.py            # merged_tier2 asset
│
└── definitions.py           # Dagster entry point
```

## Data Flow

```
External Sources (Web, API, PDF)
         │
         ▼
┌─────────────────────────────────────┐
│        ACQUISITION LAYER            │
│  acquisition/*.py (fetch functions) │
│  acquirers/*.py (BaseAcquirer)     │
└─────────────────┬───────────────────┘
                  │
                  ▼
         raw_data/{Source}/*.csv
                  │
                  ▼
┌─────────────────────────────────────┐
│         SOURCE LAYER                │
│  sources/*.py (BaseSource)         │
│  Parses CSV → Tier1Molecule        │
└─────────────────┬───────────────────┘
                  │
                  ▼
         data/tier1/{source}.parquet
         data/tier1/{source}.json
                  │
                  ▼
┌─────────────────────────────────────┐
│      CONSOLIDATION LAYER            │
│  consolidation/merger.py            │
│  Merges by molecule_id              │
└─────────────────┬───────────────────┘
                  │
                  ▼
         data/tier2/merged.parquet
         data/tier2/merged.json
```

## Key Design Decisions

1. **Dagster for orchestration**: Asset-based, not task-based
2. **Abstract base classes**: Enforce consistent interface
3. **Factory pattern**: Generate Dagster assets from acquirers
4. **Dual output formats**: Parquet (columnar, efficient) + JSON (portable, readable)
5. **Tier 2 merging**: Molecules merged by ID with multi-source attribution
6. **Conflict preservation**: Keep all conflicting values with sources, let consumers decide
7. **Separation of concerns**: Acquisition vs parsing vs merging

## Data Sources

| Source | Type | Count | Key Data |
|--------|------|-------|----------|
| FlavorDB2 | Web scrape | ~25k | Flavor descriptors |
| BitterDB | CSV download | ~2.3k | Bitter compounds |
| FSBI-DB | Web scrape | ~2.5k | Odor/taste qualities |
| VCF | Web scrape | ~2.7k | EU-Flavis compounds |
| Fenaroli | PDF extract | ~400 | Handbook substances |
| Panten | PDF extract | ~350 | Fragrance materials |
| Leffingwell | Proprietary | ~5k | Flavor labels |

## Running the Pipeline

```bash
# Start Dagster UI
just dev

# Materialize all acquisition assets
just materialize-acquisition

# Materialize all tier1 assets
just materialize-tier1

# Materialize tier2 merged asset
dagster asset materialize -m flavor_pipeline.definitions --select "merged_tier2"

# Query parquet results
duckdb -c "SELECT * FROM 'data/tier1/*.parquet' LIMIT 10"
duckdb -c "SELECT * FROM 'data/tier2/merged.parquet' LIMIT 10"

# Check tier2 merged output
python -c "
import json
d = json.load(open('data/tier2/merged.json'))
print(f'{len(d)} merged molecules')
multi = sum(1 for m in d if m['_merge_metadata']['source_count'] > 1)
print(f'{multi} from multiple sources')
"
```
