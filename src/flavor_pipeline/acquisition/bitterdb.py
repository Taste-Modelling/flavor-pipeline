"""
Download compound data from BitterDB.

Source: https://bitterdb.agri.huji.ac.il/dbbitter.php#Download

Downloads all CSV files from the 2024 release containing bitter compound
and receptor data.

Outputs:
    raw_data/BitterDB/*.csv
"""

import sys
import time
from pathlib import Path

import requests

DEFAULT_OUTPUT_DIR = Path("raw_data/BitterDB")

BASE_URL = "https://bitterdb.agri.huji.ac.il/downloads/2024"

# All available CSV files from the 2024 release
CSV_FILES = [
    "BitterCompoundsPropA_2024.csv",
    "cbitterresourceA_2024.csv",
    "compoundsnamesA_2024.csv",
    "compRefA_2024.csv",
    "compzinclinksA_2024.csv",
    "dbreferencesA_2024.csv",
    "drugbankcidA_2024.csv",
    "IUPHAR_cidA_2024.csv",
    "ligandReceptorsA_2024.csv",
    "mutationData_2024.csv",
    "ReceptorSearchA_2024.csv",
    "snpData_2024.csv",
]

# Request configuration
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research data collection; gustatory-datasets)"
}
REQUEST_DELAY = 0.5  # Seconds between requests


def download_file(filename: str, session: requests.Session, output_dir: Path) -> bool:
    """Download a single CSV file. Returns True on success."""
    url = f"{BASE_URL}/?file={filename}"
    output_path = output_dir / filename

    try:
        response = session.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()

        # Verify we got CSV content
        content_type = response.headers.get("Content-Type", "")
        if "csv" not in content_type.lower() and "text" not in content_type.lower():
            print(f"  Warning: Unexpected content type for {filename}: {content_type}")

        # Save file
        output_path.write_bytes(response.content)
        return True

    except requests.RequestException as e:
        print(f"  Error downloading {filename}: {e}", file=sys.stderr)
        return False


def fetch_bitterdb(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Download all BitterDB CSV files.

    Args:
        output_dir: Directory to write output CSV files.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    print("Downloading BitterDB 2024 data files...")
    print(f"Output directory: {output_dir}\n")

    success_count = 0
    total_bytes = 0

    for i, filename in enumerate(CSV_FILES, 1):
        print(f"[{i}/{len(CSV_FILES)}] Downloading {filename}...", end=" ")

        if download_file(filename, session, output_dir):
            file_path = output_dir / filename
            size = file_path.stat().st_size
            total_bytes += size
            print(f"OK ({size:,} bytes)")
            success_count += 1
        else:
            print("FAILED")

        time.sleep(REQUEST_DELAY)

    print("\n--- Summary ---")
    print(f"Downloaded: {success_count}/{len(CSV_FILES)} files")
    print(f"Total size: {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.2f} MB)")
    print(f"Output directory: {output_dir}")

    # Show file details
    print("\n--- Files ---")
    for filename in CSV_FILES:
        file_path = output_dir / filename
        if file_path.exists():
            # Count lines
            with open(file_path, encoding="utf-8", errors="ignore") as f:
                line_count = sum(1 for _ in f)
            print(f"  {filename}: {line_count:,} lines")

    return output_dir
