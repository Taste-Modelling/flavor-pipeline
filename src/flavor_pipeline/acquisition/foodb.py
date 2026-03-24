"""
Download compound data from FooDB.

Source: https://foodb.ca/downloads

Downloads the FooDB CSV data archive containing food compound information.

Outputs:
    raw_data/FooDB/foodb_2020_04_07_csv/*.csv
"""

import sys
import tarfile
from pathlib import Path

import requests

DEFAULT_OUTPUT_DIR = Path("raw_data/FooDB")

# FooDB download URL for the 2020 release
FOODB_URL = "https://foodb.ca/public/system/downloads/foodb_2020_04_07_csv.tar.gz"

# Request configuration
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research data collection; flavor-pipeline)"
}


def download_foodb_archive(output_dir: Path) -> Path:
    """Download the FooDB tar.gz archive.

    Returns:
        Path to the downloaded archive.
    """
    archive_path = output_dir / "foodb_2020_04_07_csv.tar.gz"

    if archive_path.exists():
        print(f"Archive already exists: {archive_path}")
        return archive_path

    print(f"Downloading FooDB archive from {FOODB_URL}...")

    response = requests.get(FOODB_URL, headers=HEADERS, stream=True, timeout=300)
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


def extract_foodb_archive(archive_path: Path, output_dir: Path) -> Path:
    """Extract the FooDB tar.gz archive.

    Returns:
        Path to the extracted directory.
    """
    extract_dir = output_dir / "foodb_2020_04_07_csv"

    if extract_dir.exists() and any(extract_dir.iterdir()):
        print(f"Archive already extracted: {extract_dir}")
        return extract_dir

    print(f"Extracting archive to {output_dir}...")

    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=output_dir, filter="data")

    print(f"Extracted to: {extract_dir}")
    return extract_dir


def fetch_foodb(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Download and extract FooDB CSV data.

    Args:
        output_dir: Directory to write output files.

    Returns:
        Path to the output directory containing CSV files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching FooDB data...")
    print(f"Output directory: {output_dir}\n")

    # Download archive
    archive_path = download_foodb_archive(output_dir)

    # Extract archive
    extract_dir = extract_foodb_archive(archive_path, output_dir)

    # List extracted files
    csv_files = list(extract_dir.glob("*.csv"))
    print(f"\n--- Extracted {len(csv_files)} CSV files ---")

    total_bytes = 0
    for csv_file in sorted(csv_files):
        size = csv_file.stat().st_size
        total_bytes += size
        # Count lines for key files
        if csv_file.name in ["Compound.csv", "Flavor.csv", "CompoundsFlavor.csv"]:
            with open(csv_file, encoding="utf-8", errors="ignore") as f:
                line_count = sum(1 for _ in f)
            print(f"  {csv_file.name}: {line_count:,} lines ({size:,} bytes)")
        else:
            print(f"  {csv_file.name}: {size:,} bytes")

    print(f"\nTotal size: {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.2f} MB)")

    return extract_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    fetch_foodb(output)
