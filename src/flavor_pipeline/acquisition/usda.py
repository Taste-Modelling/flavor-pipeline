"""
Download USDA FoodData Central full dataset.

Source: https://fdc.nal.usda.gov/download-datasets

Downloads the full CSV dataset containing Foundation, SR Legacy, Survey (FNDDS),
and Branded foods data.

Outputs:
    raw_data/USDA/*.csv (multiple CSV files, ~3.1GB uncompressed)
"""

import sys
import zipfile
from pathlib import Path

import requests

DEFAULT_OUTPUT_DIR = Path("raw_data/USDA")
USDA_URL = "https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_csv_2025-12-18.zip"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research data collection; flavor-pipeline)"
}


def download_usda_archive(output_dir: Path) -> Path:
    """Download the USDA zip archive with progress reporting."""
    archive_path = output_dir / "FoodData_Central_csv.zip"

    if archive_path.exists():
        print(f"Archive already exists: {archive_path}")
        return archive_path

    print(f"Downloading USDA FoodData Central from {USDA_URL}...")

    response = requests.get(USDA_URL, headers=HEADERS, stream=True, timeout=600)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(archive_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size:
                pct = downloaded * 100 // total_size
                mb = downloaded / 1024 / 1024
                print(f"\rDownloading: {pct}% ({mb:.1f} MB)", end="")

    print(f"\nDownloaded: {archive_path} ({archive_path.stat().st_size / 1024 / 1024:.1f} MB)")
    return archive_path


def extract_usda_archive(archive_path: Path, output_dir: Path) -> Path:
    """Extract the USDA zip archive."""
    # Check if already extracted (ZIP contains a subdirectory)
    marker_file = output_dir / "FoodData_Central_csv_2025-12-18" / "food.csv"
    if marker_file.exists():
        print(f"Archive already extracted: {output_dir}")
        return output_dir

    print(f"Extracting archive to {output_dir}...")

    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(output_dir)

    print(f"Extracted to: {output_dir}")
    return output_dir


def fetch_usda(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Download and extract USDA FoodData Central CSV data."""
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching USDA FoodData Central data...")
    print(f"Output directory: {output_dir}\n")

    archive_path = download_usda_archive(output_dir)
    extract_usda_archive(archive_path, output_dir)

    # List extracted files
    csv_files = list(output_dir.glob("*.csv"))
    print(f"\n--- Extracted {len(csv_files)} CSV files ---")

    total_bytes = 0
    for csv_file in sorted(csv_files)[:10]:  # Show first 10
        size = csv_file.stat().st_size
        total_bytes += size
        print(f"  {csv_file.name}: {size / 1024 / 1024:.1f} MB")

    if len(csv_files) > 10:
        print(f"  ... and {len(csv_files) - 10} more files")

    return output_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    fetch_usda(output)
