"""
Download phytochemical data from Dr. Duke's Phytochemical and Ethnobotanical Databases.

Source: https://phytochem.nal.usda.gov/
Data: https://catalog.data.gov/dataset/dr-dukes-phytochemical-and-ethnobotanical-databases-0849e

Downloads the bulk CSV archive containing:
- Chemical compounds with biological activities
- Plants and their phytochemical composition
- Ethnobotanical uses and applications

Outputs:
    raw_data/DukePhytochem/*.csv
"""

import sys
import zipfile
from pathlib import Path

import requests

DEFAULT_OUTPUT_DIR = Path("raw_data/DukePhytochem")

# Download URLs from Figshare (hosted by USDA Ag Data Commons)
CSV_ZIP_URL = "https://ndownloader.figshare.com/files/43363335"
DATA_DICT_URL = "https://ndownloader.figshare.com/files/43363338"

# Request configuration
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research data collection; flavor-pipeline)"
}


def download_file(url: str, output_path: Path, description: str) -> bool:
    """Download a file from URL.

    Returns:
        True if successful, False otherwise.
    """
    if output_path.exists():
        print(f"  {description}: Already exists ({output_path})")
        return True

    print(f"  Downloading {description}...")
    print(f"    URL: {url}")

    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=300)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    pct = downloaded * 100 // total_size
                    print(
                        f"\r    Progress: {pct}% ({downloaded:,} / {total_size:,} bytes)",
                        end="",
                    )

        print(f"\n    Saved: {output_path} ({output_path.stat().st_size:,} bytes)")
        return True

    except requests.RequestException as e:
        print(f"    Failed to download: {e}")
        return False


def extract_zip(zip_path: Path, output_dir: Path) -> list[Path]:
    """Extract ZIP archive and return list of extracted files."""
    extracted = []

    print(f"  Extracting {zip_path.name}...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            # Skip directories and hidden files
            if member.endswith("/") or member.startswith("__MACOSX"):
                continue

            # Extract to output directory (flatten structure)
            filename = Path(member).name
            target_path = output_dir / filename

            if target_path.exists():
                print(f"    {filename}: Already extracted")
                extracted.append(target_path)
                continue

            with zf.open(member) as src, open(target_path, "wb") as dst:
                dst.write(src.read())

            print(f"    Extracted: {filename}")
            extracted.append(target_path)

    return extracted


def fetch_duke_phytochem(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Download and extract Dr. Duke's Phytochemical database.

    Args:
        output_dir: Directory to write output files.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching Dr. Duke's Phytochemical and Ethnobotanical data...")
    print(f"Output directory: {output_dir}\n")

    # Download the CSV ZIP archive
    zip_path = output_dir / "Duke-Source-CSV.zip"
    if not download_file(CSV_ZIP_URL, zip_path, "Duke-Source-CSV.zip"):
        print("Warning: Failed to download main data archive")

    # Download the data dictionary
    dict_path = output_dir / "DataDictionary.csv"
    download_file(DATA_DICT_URL, dict_path, "Data Dictionary")

    # Extract the ZIP if it exists
    if zip_path.exists():
        print()
        extracted = extract_zip(zip_path, output_dir)
        print(f"\n  Extracted {len(extracted)} files")

    # List all CSV files
    print(f"\n--- Files in {output_dir} ---")
    csv_files = sorted(output_dir.glob("*.csv"))
    total_bytes = 0

    for csv_file in csv_files:
        size = csv_file.stat().st_size
        total_bytes += size
        print(f"  {csv_file.name}: {size:,} bytes")

    print(f"\nTotal size: {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.2f} MB)")

    return output_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    fetch_duke_phytochem(output)
