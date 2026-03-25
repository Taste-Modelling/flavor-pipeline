"""
Download CulinaryDB recipe and ingredient data.

Source: https://cosylab.iiitd.edu.in/culinarydb/

Downloads the CulinaryDB ZIP archive containing recipes from 22 world regions
with ingredients linked to FlavorDB flavor molecules.

Outputs:
    raw_data/Culinarydb/*.csv (4 CSV files)
"""

import sys
import zipfile
from pathlib import Path

import requests

DEFAULT_OUTPUT_DIR = Path("raw_data/Culinarydb")

# CulinaryDB download URL
CULINARYDB_URL = "https://cosylab.iiitd.edu.in/culinarydb/static/data/CulinaryDB.zip"

# Request configuration
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research data collection; flavor-pipeline)"
}

# Expected files in the archive
EXPECTED_FILES = [
    "01_Recipe_Details.csv",
    "02_Ingredients.csv",
    "03_Compound_Ingredients.csv",
    "04_Recipe-Ingredients_Aliases.csv",
]


def download_culinarydb_archive(output_dir: Path) -> Path:
    """Download the CulinaryDB ZIP archive.

    Returns:
        Path to the downloaded archive.
    """
    archive_path = output_dir / "CulinaryDB.zip"

    if archive_path.exists():
        print(f"Archive already exists: {archive_path}")
        return archive_path

    print(f"Downloading CulinaryDB from {CULINARYDB_URL}...")

    response = requests.get(CULINARYDB_URL, headers=HEADERS, stream=True, timeout=120)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(archive_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size:
                pct = downloaded * 100 // total_size
                print(f"\rDownloading: {pct}% ({downloaded:,} / {total_size:,} bytes)", end="")

    print(f"\nDownloaded: {archive_path} ({archive_path.stat().st_size:,} bytes)")
    return archive_path


def extract_culinarydb_archive(archive_path: Path, output_dir: Path) -> Path:
    """Extract the CulinaryDB ZIP archive.

    Returns:
        Path to the output directory containing extracted files.
    """
    # Check if already extracted
    all_exist = all((output_dir / f).exists() for f in EXPECTED_FILES)

    if all_exist:
        print(f"Archive already extracted: {output_dir}")
        return output_dir

    print(f"Extracting archive to {output_dir}...")

    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(output_dir)

    print(f"Extracted to: {output_dir}")
    return output_dir


def fetch_culinarydb(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Download and extract CulinaryDB data.

    Args:
        output_dir: Directory to write output files.

    Returns:
        Path to the output directory containing CSV files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching CulinaryDB data...")
    print(f"Output directory: {output_dir}\n")

    # Download archive
    archive_path = download_culinarydb_archive(output_dir)

    # Extract archive
    extract_culinarydb_archive(archive_path, output_dir)

    # List extracted files
    print("\n--- Extracted files ---")
    total_bytes = 0
    for filename in EXPECTED_FILES:
        filepath = output_dir / filename
        if filepath.exists():
            size = filepath.stat().st_size
            total_bytes += size
            # Count lines
            with open(filepath, encoding="utf-8", errors="ignore") as f:
                line_count = sum(1 for _ in f)
            print(f"  {filename}: {line_count:,} lines ({size:,} bytes)")

    print(f"\nTotal size: {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.2f} MB)")

    return output_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    fetch_culinarydb(output)
