# Flavor Pipeline - Claude Code Instructions

## Environment Setup

This project uses **Nix** for system dependencies and **uv** for Python package management.

### Before Running Python Commands

**CRITICAL:** Before running any Python code, tests, or `just` commands, prefix with:

```bash
export LD_LIBRARY_PATH="/nix/store/ihpdbhy4rfxaixiamyb588zfc3vj19al-gcc-15.2.0-lib/lib:/nix/store/vl8jkqpr0l3fac3cxiy4nwc5paiww1lv-zlib-1.3.1/lib:/nix/store/5mnq195cx3cagnpbbvf5ncbp4fjgy0sz-libffi-3.5.2/lib:/nix/store/p96a7p297gmia8zcy4i72qd45wzw8lh6-openssl-3.6.1/lib" && source .venv/bin/activate && <command>
```

For Dagster commands, also set:
```bash
export DAGSTER_HOME=/home/nitish/Development/flavor_pipeline
```

### Quick Setup (if venv doesn't exist)

```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

## Common Commands

After activating the environment, use `just` to run common tasks:

- `just test` - Run all tests
- `just lint` - Lint code with ruff
- `just fmt` - Format code with ruff
- `just dev` - Start Dagster dev server

## Project Structure

- `src/flavor_pipeline/` - Main package
  - `acquisition/` - Data download scripts
  - `acquirers/` - Acquirer classes (wrap acquisition)
  - `sources/` - Source adapters (parse raw data to Tier1)
  - `schemas/` - Pydantic models (AVO, Tier1Molecule)
- `raw_data/` - Downloaded raw data files
- `data/` - Processed data outputs
- `tests/` - Test suite
