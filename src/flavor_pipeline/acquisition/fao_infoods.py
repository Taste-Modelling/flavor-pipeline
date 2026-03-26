"""
Download food composition data from FAO/INFOODS databases.

Source: https://www.fao.org/infoods/infoods/tables-and-databases/faoinfoods-databases/en/

Downloads multiple FAO/INFOODS food composition databases in Excel format:
- AnFooD 2.0: Analytical Food Composition Database
- uFiSh 1.0: Global fish/seafood nutrient database
- uPulses 1.0: Global pulse nutrient database
- BioFoodComp 4.0: Food biodiversity repository

Outputs:
    raw_data/FAO_INFOODS/*.xlsx
"""

import sys
from pathlib import Path

import requests

DEFAULT_OUTPUT_DIR = Path("raw_data/FAO_INFOODS")

# FAO INFOODS database URLs (direct Excel download links)
# These are the primary data files from each database
BASE_URL = "https://www.fao.org"
DATABASES = {
    "anfood": {
        "name": "AnFooD 2.0",
        "description": "Analytical Food Composition Database - global compendium",
        "url": f"{BASE_URL}/fileadmin/templates/food_composition/documents/AnFooD2.0.xlsx",
        "filename": "AnFooD2.0.xlsx",
    },
    "ufish": {
        "name": "uFiSh 1.0",
        "description": "Global fish, crustaceans, and molluscs nutrient database",
        "url": f"{BASE_URL}/fileadmin/templates/food_composition/documents/uFiSh1.0.xlsx",
        "filename": "uFiSh1.0.xlsx",
    },
    "upulses": {
        "name": "uPulses 1.0",
        "description": "Global food composition database for pulses",
        "url": f"{BASE_URL}/fileadmin/templates/food_composition/documents/uPulses1.0.xlsx",
        "filename": "uPulses1.0.xlsx",
    },
    "biofoodcomp": {
        "name": "BioFoodComp 4.0",
        "description": "Food biodiversity composition data repository",
        "url": f"{BASE_URL}/fileadmin/templates/food_composition/documents/BioFoodComp4.0.xlsx",
        "filename": "BioFoodComp4.0.xlsx",
    },
    "density": {
        "name": "FAO/INFOODS Density Database v2.0",
        "description": "Food density values for volume to weight conversion",
        "url": f"{BASE_URL}/fileadmin/templates/food_composition/documents/density_DB_v2_0_final-1__1_.xlsx",
        "filename": "Density_DB_v2.0.xlsx",
    },
    "wafct": {
        "name": "West African Food Composition Table 2019",
        "description": "Food composition table for Western Africa",
        "url": f"{BASE_URL}/fileadmin/user_upload/faoweb/2020/WAFCT_2019.xlsx",
        "filename": "WAFCT_2019.xlsx",
    },
    "phyfoodcomp": {
        "name": "PhyFoodComp 1.0",
        "description": "Global phytate analytical data repository",
        "url": f"{BASE_URL}/fileadmin/templates/food_composition/documents/PhyFoodComp_1.0.xlsx",
        "filename": "PhyFoodComp_1.0.xlsx",
    },
    "pulsesdm": {
        "name": "PulsesDM 1.0",
        "description": "Pulse nutrient values on dry matter basis",
        "url": f"{BASE_URL}/fileadmin/templates/food_composition/documents/PulsesDM1.0.xlsx",
        "filename": "PulsesDM1.0.xlsx",
    },
}

# Request configuration
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research data collection; flavor-pipeline)"
}


def download_database(db_key: str, output_dir: Path) -> Path | None:
    """Download a single FAO INFOODS database.

    Args:
        db_key: Key from DATABASES dict
        output_dir: Directory to save the file

    Returns:
        Path to downloaded file, or None if failed
    """
    db_info = DATABASES[db_key]
    output_path = output_dir / db_info["filename"]

    if output_path.exists():
        print(f"  {db_info['name']}: Already exists ({output_path})")
        return output_path

    print(f"  Downloading {db_info['name']}...")
    print(f"    URL: {db_info['url']}")

    try:
        response = requests.get(
            db_info["url"], headers=HEADERS, stream=True, timeout=120
        )
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
        return output_path

    except requests.RequestException as e:
        print(f"    Failed to download: {e}")
        return None


def fetch_fao_infoods(
    output_dir: Path = DEFAULT_OUTPUT_DIR, databases: list[str] | None = None
) -> Path:
    """Download FAO/INFOODS food composition databases.

    Args:
        output_dir: Directory to write output files.
        databases: List of database keys to download. If None, downloads all.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching FAO/INFOODS food composition data...")
    print(f"Output directory: {output_dir}\n")

    # Determine which databases to download
    db_keys = databases if databases else list(DATABASES.keys())

    downloaded = []
    failed = []

    for db_key in db_keys:
        if db_key not in DATABASES:
            print(f"  Unknown database: {db_key}")
            failed.append(db_key)
            continue

        result = download_database(db_key, output_dir)
        if result:
            downloaded.append(db_key)
        else:
            failed.append(db_key)

    print("\n--- Download Summary ---")
    print(f"Downloaded: {len(downloaded)} databases")
    if downloaded:
        for key in downloaded:
            print(f"  - {DATABASES[key]['name']}")

    if failed:
        print(f"Failed: {len(failed)} databases")
        for key in failed:
            db_name = DATABASES.get(key, {}).get("name", key)
            print(f"  - {db_name}")

    # List all files in output directory
    print(f"\nFiles in {output_dir}:")
    total_bytes = 0
    for f in sorted(output_dir.glob("*.xlsx")):
        size = f.stat().st_size
        total_bytes += size
        print(f"  {f.name}: {size:,} bytes")

    print(f"\nTotal size: {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.2f} MB)")

    return output_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    fetch_fao_infoods(output)
