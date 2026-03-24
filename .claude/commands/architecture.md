# Architecture Overview

Explain the flavor pipeline architecture and design patterns.

## Instructions

Provide a comprehensive overview of the flavor pipeline architecture when the user asks about how the system works.

## Core Concepts

### Two-Tier Data Model

```
Tier 0 (Raw)              Tier 1 (Normalized)
─────────────             ──────────────────
raw_data/{Source}/*.csv   data/tier1/{source}.parquet
                          data/tier1/{source}.json
                          data/tier1/consolidated.json
                          │
                          └── Tier1Molecule with AttributedValue
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

## Module Structure

```
src/flavor_pipeline/
│
├── schemas/                 # Pydantic data models
│   ├── avo.py              # AttributedValue
│   └── tier1.py            # Tier1Molecule, SourceMetadata
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
├── assets/                  # Dagster asset definitions
│   └── tier1.py            # *_tier1 assets
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
         data/tier1/consolidated.json
```

## Key Design Decisions

1. **Dagster for orchestration**: Asset-based, not task-based
2. **Abstract base classes**: Enforce consistent interface
3. **Factory pattern**: Generate Dagster assets from acquirers
4. **Dual output formats**: Parquet (columnar, efficient) + JSON (portable, readable)
5. **Consolidated JSON**: All sources combined into single file for easy consumption
6. **Separation of concerns**: Acquisition vs parsing

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

# Query parquet results
duckdb -c "SELECT * FROM 'data/tier1/*.parquet' LIMIT 10"

# Check consolidated JSON
python -c "import json; d=json.load(open('data/tier1/consolidated.json')); print(f'{len(d)} molecules')"
```
