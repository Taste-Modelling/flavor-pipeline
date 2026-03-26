"""
Download metabolite/compound data from MetaboLights.

Source: https://www.ebi.ac.uk/metabolights/

MetaboLights is a database for metabolomics experiments containing ~33,000
reference compounds with chemical identifiers and structures.

Uses the MetaboLights REST API to fetch compound data:
- /ws/compounds/list - Get list of all compound IDs
- /ws/compounds/{id} - Get compound details

Outputs:
    raw_data/MetaboLights/compounds.json
"""

import json
import sys
import time
from pathlib import Path

import requests

DEFAULT_OUTPUT_DIR = Path("raw_data/MetaboLights")

# MetaboLights API endpoints
BASE_URL = "https://www.ebi.ac.uk/metabolights/ws"
COMPOUNDS_LIST_URL = f"{BASE_URL}/compounds/list"
COMPOUND_DETAIL_URL = f"{BASE_URL}/compounds"

# Request configuration
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research data collection; flavor-pipeline)",
    "Accept": "application/json",
}

# Rate limiting - conservative to avoid 403 errors
REQUESTS_PER_SECOND = 1  # 1 request per second to be safe
REQUEST_DELAY = 1.0 / REQUESTS_PER_SECOND
MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds to wait on 403/429 errors


def fetch_compound_list() -> list[str]:
    """Fetch the list of all compound IDs."""
    print("  Fetching compound list...")

    response = requests.get(COMPOUNDS_LIST_URL, headers=HEADERS, timeout=60)
    response.raise_for_status()

    data = response.json()
    compound_ids = data.get("content", [])

    print(f"  Found {len(compound_ids)} compounds")
    return compound_ids


def fetch_compound_details(compound_id: str) -> dict | None:
    """Fetch details for a single compound with retry logic."""
    url = f"{COMPOUND_DETAIL_URL}/{compound_id}"

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)

            # Handle rate limiting
            if response.status_code in (403, 429):
                wait_time = RETRY_BACKOFF * (attempt + 1)
                print(f"    Rate limited, waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            data = response.json()
            return data.get("content")

        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF * (attempt + 1)
                print(f"    Error fetching {compound_id}, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                print(f"    Warning: Failed to fetch {compound_id} after {MAX_RETRIES} attempts: {e}")
                return None

    return None


def fetch_metabolights(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    max_compounds: int | None = None,
    resume: bool = True,
) -> Path:
    """Download MetaboLights compound data.

    Args:
        output_dir: Directory to write output files.
        max_compounds: Maximum number of compounds to fetch (None = all).
        resume: If True, resume from existing partial download.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching MetaboLights compound data...")
    print(f"Output directory: {output_dir}\n")

    compounds_file = output_dir / "compounds.json"
    progress_file = output_dir / ".progress.json"

    # Check for existing complete download
    if compounds_file.exists() and not progress_file.exists():
        with open(compounds_file) as f:
            existing = json.load(f)
        print(f"Using existing data: {len(existing)} compounds in {compounds_file}")
        return output_dir

    # Get list of all compound IDs
    compound_ids = fetch_compound_list()

    if max_compounds:
        compound_ids = compound_ids[:max_compounds]
        print(f"  Limiting to {max_compounds} compounds")

    # Load progress if resuming
    completed: set[str] = set()
    compounds: list[dict] = []

    if resume and progress_file.exists():
        with open(progress_file) as f:
            progress = json.load(f)
        completed = set(progress.get("completed", []))
        compounds = progress.get("compounds", [])
        print(f"  Resuming from {len(completed)} completed compounds")

    # Fetch compound details
    remaining = [cid for cid in compound_ids if cid not in completed]
    total = len(compound_ids)
    fetched = len(completed)

    print(f"\n  Fetching {len(remaining)} remaining compounds...")

    for i, compound_id in enumerate(remaining):
        # Progress update every 100 compounds
        if i > 0 and i % 100 == 0:
            pct = (fetched + i) * 100 // total
            print(f"    Progress: {pct}% ({fetched + i}/{total})")

            # Save progress
            with open(progress_file, "w") as f:
                json.dump(
                    {"completed": list(completed), "compounds": compounds},
                    f,
                )

        # Fetch compound details
        details = fetch_compound_details(compound_id)

        if details:
            compounds.append(details)
            completed.add(compound_id)

        # Rate limiting
        time.sleep(REQUEST_DELAY)

    # Save final results
    print(f"\n  Saving {len(compounds)} compounds...")

    with open(compounds_file, "w") as f:
        json.dump(compounds, f, indent=2)

    # Remove progress file
    if progress_file.exists():
        progress_file.unlink()

    print("\n--- Download Complete ---")
    print(f"Saved: {compounds_file} ({compounds_file.stat().st_size:,} bytes)")
    print(f"Total compounds: {len(compounds)}")

    return output_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    max_compounds = None

    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    if len(sys.argv) > 2:
        max_compounds = int(sys.argv[2])

    fetch_metabolights(output, max_compounds=max_compounds)
