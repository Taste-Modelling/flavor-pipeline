"""
Fetch molecule data from FlavorDB2 and enrich with PubChem structural data.

Step 1 — Scrape the FlavorDB2 molecules HTML table (~25k rows) to collect
         pubchem_id, common_name, and flavor_profile for every molecule.

Step 2 — Batch-fetch IsomericSMILES, InChI, and IUPACName from the PubChem
         PUG REST API (100 CIDs per request) to add structural data.

Outputs:
    raw_data/FlavorDB2/molecules.csv
"""

import csv
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

DEFAULT_OUTPUT_DIR = Path("raw_data/FlavorDB2")

FLAVORDB2_URL = "https://cosylab.iiitd.edu.in/flavordb2/molecules"
PUBCHEM_PROPERTY_URL = (
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid"
    "/{cids}/property/IsomericSMILES,InChI,IUPACName/JSON"
)

PUBCHEM_BATCH_SIZE = 100
DEFAULT_PUBCHEM_DELAY = 0.25  # 4 req/s — PubChem asks for ≤5/s

OUTPUT_FIELDS = [
    "pubchem_id",
    "common_name",
    "iupac_name",
    "smiles",
    "inchi",
    "flavor_profile",
]


def scrape_flavordb2_table(session: requests.Session) -> list[dict]:
    """Download and parse the FlavorDB2 molecules HTML table.

    The page is server-rendered with all ~25k molecules embedded in a
    DataTable. Columns: Common Name | PubChem ID | Flavor Profile | More Info.
    Flavor profile tags are rendered as <a> links inside the cell.
    """
    print(f"Fetching {FLAVORDB2_URL} (this page is large — may take a moment)...")
    resp = session.get(FLAVORDB2_URL, timeout=120)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content) / 1_000_000:.1f} MB")

    print("  Parsing HTML table...")
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table", {"id": "molecules"})
    if table is None:
        raise ValueError(
            "Could not find <table id='molecules'> in the FlavorDB2 molecules page. "
            "The page structure may have changed."
        )

    tbody = table.find("tbody")
    if tbody is None:
        raise ValueError("No <tbody> found in the molecules table.")

    molecules = []
    skipped = 0

    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        common_name = cells[0].get_text(strip=True)

        pubchem_id_text = cells[1].get_text(strip=True)
        try:
            pubchem_id = int(pubchem_id_text)
        except (ValueError, TypeError):
            skipped += 1
            continue

        # Flavor profile: rendered as <a> tag links inside the cell
        flavor_links = cells[2].find_all("a")
        if flavor_links:
            flavor_profile = "@".join(a.get_text(strip=True) for a in flavor_links)
        else:
            # Fallback: plain text, already space/comma separated
            flavor_profile = cells[2].get_text(separator="@", strip=True)

        molecules.append({
            "pubchem_id": pubchem_id,
            "common_name": common_name,
            "flavor_profile": flavor_profile,
            "iupac_name": "",
            "smiles": "",
            "inchi": "",
        })

    if skipped:
        print(f"  Skipped {skipped} rows with non-integer PubChem IDs")

    return molecules


def enrich_with_pubchem(
    molecules: list[dict],
    delay: float = DEFAULT_PUBCHEM_DELAY,
) -> None:
    """Add SMILES, InChI, IUPACName to molecule records in-place via PubChem API."""
    by_cid: dict[int, dict] = {m["pubchem_id"]: m for m in molecules}
    cids = list(by_cid.keys())
    total_batches = (len(cids) + PUBCHEM_BATCH_SIZE - 1) // PUBCHEM_BATCH_SIZE

    print(
        f"\nEnriching {len(cids):,} molecules from PubChem "
        f"({total_batches} batches of {PUBCHEM_BATCH_SIZE})..."
    )

    enriched = 0
    for batch_num, i in enumerate(range(0, len(cids), PUBCHEM_BATCH_SIZE), start=1):
        batch = cids[i : i + PUBCHEM_BATCH_SIZE]
        url = PUBCHEM_PROPERTY_URL.format(cids=",".join(str(c) for c in batch))

        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 404:
                # PubChem returns 404 when none of the CIDs are found
                print(
                    f"  [{batch_num:>4}/{total_batches}] 404 — no CIDs matched in batch",
                    file=sys.stderr,
                )
                time.sleep(delay)
                continue
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as e:
            print(
                f"  [{batch_num:>4}/{total_batches}] Warning: request failed: {e}",
                file=sys.stderr,
            )
            time.sleep(delay)
            continue

        batch_enriched = 0
        for prop in data.get("PropertyTable", {}).get("Properties", []):
            cid = prop.get("CID")
            if cid in by_cid:
                by_cid[cid]["smiles"] = prop.get("SMILES", "")
                by_cid[cid]["inchi"] = prop.get("InChI", "")
                by_cid[cid]["iupac_name"] = prop.get("IUPACName", "")
                batch_enriched += 1
                enriched += 1

        print(
            f"  [{batch_num:>4}/{total_batches}] "
            f"{batch_enriched:>3}/{len(batch)} matched | total enriched: {enriched:,}"
        )
        time.sleep(delay)

    print(f"PubChem enrichment complete: {enriched:,}/{len(cids):,} molecules matched")


def fetch_flavordb2(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    skip_pubchem: bool = False,
    delay: float = DEFAULT_PUBCHEM_DELAY,
) -> Path:
    """Main entry point - scrape FlavorDB2 + enrich with PubChem + save CSV.

    Args:
        output_dir: Directory to write output CSV.
        skip_pubchem: If True, skip PubChem enrichment (flavor data only).
        delay: Seconds between PubChem batch requests.

    Returns:
        Path to the output CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "molecules.csv"

    session = requests.Session()
    session.headers["User-Agent"] = (
        "MoleculeFlavorPipeline/1.0 (academic research; flavor data collection)"
    )
    molecules = scrape_flavordb2_table(session)
    print(f"Scraped {len(molecules):,} molecules from FlavorDB2")

    if not skip_pubchem:
        enrich_with_pubchem(molecules, delay=delay)
    else:
        print("Skipping PubChem enrichment (--skip-pubchem)")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(molecules)

    print(f"\nSaved: {output_path}")

    with_flavors = sum(1 for m in molecules if m.get("flavor_profile"))
    with_smiles = sum(1 for m in molecules if m.get("smiles"))
    print(
        f"Stats: {len(molecules):,} total | "
        f"{with_flavors:,} with flavor_profile | "
        f"{with_smiles:,} with SMILES"
    )

    return output_path
