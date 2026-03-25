"""
Download UmamiDB food amino acid and nucleotide data.

Source: https://www.umamiinfo.com/umamidb/

UmamiDB provides umami content measurements for ~800 foods:
- 20 free amino acids (mg/100g)
- 3 nucleotides: IMP, GMP, AMP (mg/100g)
- Food category classification
- Source references from academic papers

The data is served via a Google Apps Script endpoint that returns JSON.

Outputs:
    raw_data/Umamidb/foods.json
"""

import json
import sys
from pathlib import Path

import requests

DEFAULT_OUTPUT_DIR = Path("raw_data/Umamidb")

# Data is served via Google Apps Script from a spreadsheet
DATA_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbyC16_DGQKuv3eZXWk0Q21zd9qPy4EswpoFNdmzoEv85XwBO1DX/exec"
)


def fetch_umamidb(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Download UmamiDB food data from Google Apps Script endpoint.

    Args:
        output_dir: Directory to write output files.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "foods.json"

    print("Fetching UmamiDB data...")
    print(f"Source: {DATA_URL}")

    session = requests.Session()
    session.headers["User-Agent"] = (
        "FlavorPipeline/1.0 (academic research; umami data collection)"
    )

    response = session.get(DATA_URL, timeout=60)
    response.raise_for_status()

    foods = response.json()
    print(f"Downloaded {len(foods)} food records")

    # Save raw JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(foods, f, ensure_ascii=False, indent=2)

    print(f"Saved: {output_path}")

    # Print summary statistics
    categories = set()
    with_glu = 0
    with_nucleotides = 0

    for food in foods:
        cat = food.get("category_en", "")
        if cat:
            categories.add(cat)
        if food.get("free_amino_acid05_Glu"):
            with_glu += 1
        if any(food.get(f"nucleic_acid0{i}_{n}") for i, n in [(1, "IMP"), (2, "GMP"), (3, "AMP")]):
            with_nucleotides += 1

    print("\nStats:")
    print(f"  Total foods: {len(foods)}")
    print(f"  Categories: {len(categories)}")
    print(f"  With glutamate data: {with_glu}")
    print(f"  With nucleotide data: {with_nucleotides}")

    return output_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    fetch_umamidb(output)
