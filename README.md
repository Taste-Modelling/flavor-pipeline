# Flavor Pipeline

A Dagster-based data pipeline for aggregating and harmonizing flavor molecule data from multiple public databases into a unified schema.

## Overview

Flavor Pipeline collects flavor compound data from 16+ sources, normalizes it using Attributed Value Objects (AVOs) for provenance tracking, and produces consolidated Parquet files suitable for analysis and machine learning.

### Data Sources

| Source | Description | Compounds |
|--------|-------------|-----------|
| FlavorDB2 | Flavor molecules with sensory descriptors | ~25,000 |
| BitterDB | Bitter compounds with receptor data | ~2,300 |
| FooDB | Food compounds with flavor associations | ~26,000 |
| FSBI-DB | Flavor compounds with sensory qualities | ~2,500 |
| VCF | Volatile compounds in food (EU-Flavis) | ~2,700 |
| FoodAtlas | Food-chemical knowledge graph | ~3,600 |
| CulinaryDB | Recipes with ingredient-flavor links | ~46,000 recipes |
| UmamiDB | Amino acids and nucleotides in foods | ~700 foods |
| SweetenersDB | Sweet compounds with sweetness values | ~316 |
| WineSensed | Wine flavor similarity data | ~350,000 wines |
| FAO INFOODS | Global food composition databases | 8 databases |
| Duke Phytochem | Phytochemicals from Dr. Duke's database | ~29,000 |
| MetaboLights | Reference metabolite compounds | ~33,000 |
| USDA FoodData | Comprehensive food nutrient data | 3.1 GB |
| Fenaroli | Flavor handbook (PDF extraction) | varies |
| Panten | Fragrance handbook (PDF extraction) | ~350 |

## Installation

### Prerequisites

- Python 3.11+
- [Nix](https://nixos.org/) (for system dependencies)
- [uv](https://github.com/astral-sh/uv) (Python package manager)

### Setup

```bash
# Clone the repository
git clone https://github.com/your-org/flavor-pipeline.git
cd flavor-pipeline

# Create and activate virtual environment
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install -e ".[dev]"
```

### Environment Variables

Before running commands, set the library path (required for Nix):

```bash
export LD_LIBRARY_PATH="/nix/store/ihpdbhy4rfxaixiamyb588zfc3vj19al-gcc-15.2.0-lib/lib:/nix/store/vl8jkqpr0l3fac3cxiy4nwc5paiww1lv-zlib-1.3.1/lib:/nix/store/5mnq195cx3cagnpbbvf5ncbp4fjgy0sz-libffi-3.5.2/lib:/nix/store/p96a7p297gmia8zcy4i72qd45wzw8lh6-openssl-3.6.1/lib"
```

For Dagster commands:

```bash
export DAGSTER_HOME=/path/to/flavor_pipeline
```

## Quick Start

### Using Dagster UI

```bash
just dev
# Open http://localhost:3000
```

### Using CLI

```bash
# Restore raw data from archives
just archive-restore

# Run the full pipeline
dagster asset materialize -m flavor_pipeline.definitions --select "*"

# Query consolidated data
duckdb -c "SELECT * FROM 'data/tier1/consolidated.parquet' LIMIT 10"
```

## Project Structure

```
flavor_pipeline/
├── src/flavor_pipeline/
│   ├── acquirers/        # Acquirer classes (wrap acquisition)
│   ├── acquisition/      # Data download scripts
│   ├── sources/          # Source adapters (raw -> Tier1)
│   ├── schemas/          # Pydantic models (AVO, Tier1Molecule)
│   ├── assets/           # Dagster asset definitions
│   ├── consolidation/    # Tier1 -> Tier2 merging
│   ├── cli/              # CLI commands
│   └── definitions.py    # Dagster Definitions entry point
├── raw_data/             # Downloaded raw data (gitignored)
├── data/                 # Processed parquet outputs
├── archives/             # Compressed data archives
└── tests/                # Test suite
```

## Data Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Raw Data Sources                            │
│  (FlavorDB2, BitterDB, FooDB, FSBI, VCF, FoodAtlas, ...)           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Tier 1: Source-Specific                         │
│  - One parquet per source                                           │
│  - Attributed Value Objects (AVOs) for provenance                   │
│  - Normalized identifiers (PubChem, CAS, InChI)                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Tier 2: Consolidated                            │
│  - Merged across sources on chemical identifiers                    │
│  - Deduplicated with source attribution                             │
│  - Ready for analysis and ML                                        │
└─────────────────────────────────────────────────────────────────────┘
```

## CLI Commands

### Archive Management

```bash
# List archive status
flavor-archive list

# Create archives from raw data
flavor-archive create              # All sources
flavor-archive create flavordb2    # Specific source

# Verify archive checksums
flavor-archive verify

# Restore raw data from archives
flavor-archive restore             # All sources
flavor-archive restore flavordb2   # Specific source
```

### Just Recipes

```bash
just                    # List all recipes
just test               # Run all tests
just lint               # Lint code with ruff
just fmt                # Format code with ruff
just dev                # Start Dagster dev server
just archive-list       # List archive status
just archive-restore    # Restore all archives
```

## Development

### Running Tests

```bash
just test              # All tests
just test-unit         # Unit tests only
just test-cov          # With coverage report
```

### Code Quality

```bash
just lint              # Check code style
just fmt               # Format code
```

### Adding a New Data Source

1. Create acquirer in `src/flavor_pipeline/acquirers/{source}.py`
2. Create acquisition script in `src/flavor_pipeline/acquisition/{source}.py`
3. Create source adapter in `src/flavor_pipeline/sources/{source}.py`
4. Register in `src/flavor_pipeline/acquirers/__init__.py`
5. Register in `src/flavor_pipeline/sources/__init__.py`
6. Add Tier 1 asset in `src/flavor_pipeline/assets/tier1.py`

## License

MIT License - see [LICENSE](LICENSE) for details.

## Citation

If you use this pipeline in research, please cite the original data sources appropriately. Each source has its own license and citation requirements.
