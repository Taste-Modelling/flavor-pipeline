"""
Download SweetenersDB sweet compound data from GitHub.

Source: https://github.com/chemosim-lab/SweetenersDB

SweetenersDB is a curated database of 316 sweet molecules with:
- Compound names
- Relative sweetness (logSw)
- SMILES structures

Based on research by Bouysset et al. (2020) and Chéron et al. (2017).

Note: This is a curated subset of the original SuperSweet database
(http://bioinf-applied.charite.de/sweet/) which is no longer available.

License: MIT

Outputs:
    raw_data/Sweetenersdb/sweeteners.csv
"""

import sys
from pathlib import Path

import requests

DEFAULT_OUTPUT_DIR = Path("raw_data/Sweetenersdb")

# GitHub raw file URL for SweetenersDB v2.0
DATA_URL = (
    "https://raw.githubusercontent.com/chemosim-lab/SweetenersDB/"
    "master/SweetenersDB_v2.0.csv"
)


def fetch_sweetenersdb(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Download SweetenersDB from GitHub.

    Args:
        output_dir: Directory to write output files.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "sweeteners.csv"

    print("Fetching SweetenersDB data from GitHub...")
    print(f"Source: {DATA_URL}")

    session = requests.Session()
    session.headers["User-Agent"] = (
        "FlavorPipeline/1.0 (academic research; sweet compound data collection)"
    )

    response = session.get(DATA_URL, timeout=60)
    response.raise_for_status()

    # Save raw CSV
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(response.text)

    # Count records
    lines = response.text.strip().split("\n")
    num_records = len(lines) - 1  # Subtract header

    print(f"Downloaded {num_records} sweet compounds")
    print(f"Saved: {output_path}")

    return output_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    fetch_sweetenersdb(output)
