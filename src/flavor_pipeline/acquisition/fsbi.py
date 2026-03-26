"""
Scrape compound data from FSBI-DB (Flavor Science Basic Information Database).

Source: https://fsbi-db.de/index.php

The database contains ~2,544 flavor-active compounds with sensory data,
receptor associations, and food occurrence information.

Outputs:
    raw_data/FSBI/compounds.csv
"""

import csv
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

DEFAULT_OUTPUT_DIR = Path("raw_data/FSBI")

BASE_URL = "https://fsbi-db.de"
SEARCH_URL = f"{BASE_URL}/search.php"
COMPOUND_URL = f"{BASE_URL}/single.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research scraper; gustatory-datasets)"
}
REQUEST_DELAY = 0.5  # Seconds between requests

FIELDNAMES = [
    "fsbi_id",
    "name",
    "pubchem_id",
    "cas",
    "smiles",
    "inchi_key",
    "molecular_formula",
    "molecular_weight",
    "odor_qualities",
    "taste_qualities",
    "description",
    "synonyms",
    "flavordb_id",
]


def get_soup(url: str, session: requests.Session, params: dict | None = None) -> BeautifulSoup:
    """Fetch URL and return BeautifulSoup parser."""
    response = session.get(url, params=params, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def collect_compound_ids(session: requests.Session) -> set[int]:
    """Collect compound IDs using search with comprehensive coverage."""
    compound_ids = set()

    search_terms = ["e", "a", "o", "i"]

    print("Collecting compound IDs from search results...", flush=True)

    for term in search_terms:
        page = 1
        max_pages = 150

        while page <= max_pages:
            params = {"term": term, "type": "compounds", "page": page}
            try:
                soup = get_soup(SEARCH_URL, session, params)

                links = soup.find_all("a", href=True)
                page_ids = set()
                for link in links:
                    href = str(link.get("href", ""))
                    if "single.php?id=" in href:
                        match = re.search(r"id=(\d+)", href)
                        if match:
                            page_ids.add(int(match.group(1)))

                if not page_ids:
                    break

                new_ids = page_ids - compound_ids
                compound_ids.update(page_ids)

                if new_ids:
                    print(
                        f"  Search '{term}' page {page}: {len(new_ids)} new (total: {len(compound_ids)})",
                        flush=True,
                    )

                pagination = soup.find("div", class_="paginationContainer")
                next_page_link = None
                if pagination:
                    for a in pagination.find_all("a", href=True):
                        if f"page={page + 1}" in str(a.get("href", "")):
                            next_page_link = a
                            break

                if not next_page_link:
                    break

                page += 1
                time.sleep(REQUEST_DELAY)

            except Exception as e:
                print(f"  Error searching '{term}' page {page}: {e}", file=sys.stderr)
                break

        if len(compound_ids) >= 2400:
            print(f"  Reached sufficient coverage ({len(compound_ids)} IDs)", flush=True)
            break

        time.sleep(REQUEST_DELAY)

    return compound_ids


def extract_text(soup: BeautifulSoup, label: str) -> str:
    """Extract text following a label element."""
    for h5 in soup.find_all("h5"):
        if label.lower() in h5.get_text().lower():
            parent = h5.find_parent("div", class_="col-md-3")
            if parent:
                row = parent.find_parent("div", class_="row")
                if row:
                    value_div = row.find("div", class_="col-md-9")
                    if value_div:
                        text = value_div.get_text(strip=True)
                        text = re.sub(r"\s+", " ", text).strip()
                        if text:
                            return text
    return ""


def parse_compound_page(compound_id: int, session: requests.Session) -> dict | None:
    """Parse a compound detail page and extract data."""
    try:
        soup = get_soup(COMPOUND_URL, session, {"id": compound_id})

        if not soup.find("div", class_="singleCompound"):
            return None

        compound = {
            "fsbi_id": compound_id,
            "name": "",
            "pubchem_id": "",
            "cas": "",
            "smiles": "",
            "inchi_key": "",
            "molecular_formula": "",
            "molecular_weight": "",
            "odor_qualities": "",
            "taste_qualities": "",
            "description": "",
            "synonyms": "",
            "flavordb_id": "",
        }

        name_h3 = soup.find("h3")
        if name_h3:
            name_i = name_h3.find("i")
            if name_i:
                compound["name"] = name_i.get_text(strip=True)
            else:
                compound["name"] = name_h3.get_text(strip=True)

        for link in soup.find_all("a", href=True):
            href = str(link.get("href", ""))
            if "pubchem.ncbi.nlm.nih.gov/compound/" in href:
                match = re.search(r"/compound/(\d+)", href)
                if match:
                    compound["pubchem_id"] = match.group(1)
                    break

        compound["molecular_formula"] = extract_text(soup, "Molecular Formula")
        mw_text = extract_text(soup, "Molecular Weight")
        if mw_text:
            mw_match = re.search(r"([\d.]+)", mw_text)
            if mw_match:
                compound["molecular_weight"] = mw_match.group(1)

        compound["smiles"] = extract_text(soup, "Smiles")
        compound["inchi_key"] = extract_text(soup, "Inchikey")

        synonyms_text = ""
        for h5 in soup.find_all("h5"):
            if "Synonyms" in h5.get_text():
                parent = h5.find_parent("div", class_="col-md-9")
                if parent:
                    small = parent.find("small")
                    if small:
                        synonyms_text = small.get_text(strip=True)
                        compound["synonyms"] = synonyms_text[:500]
                break

        cas_match = re.search(r"\b(\d{2,7}-\d{2}-\d)\b", synonyms_text)
        if cas_match:
            compound["cas"] = cas_match.group(1)

        for h5 in soup.find_all("h5"):
            if "Description" in h5.get_text():
                parent = h5.find_parent("div")
                if parent:
                    desc_parts = []
                    for sibling in h5.next_siblings:
                        if hasattr(sibling, "get_text"):
                            desc_parts.append(sibling.get_text(strip=True))
                    compound["description"] = " ".join(desc_parts)[:500]
                break

        for h5 in soup.find_all("h5"):
            if "FlavorDB ID" in h5.get_text():
                parent = h5.find_parent("div", class_="col-md-9")
                if parent:
                    text = parent.get_text(strip=True).replace("FlavorDB ID", "").strip()
                    if text.isdigit():
                        compound["flavordb_id"] = text
                break

        quality_section = soup.find("div", class_="alert", string=re.compile(r"Compound Quality"))
        if quality_section:
            parent = quality_section.find_parent("div", class_="compoundSection")
            if parent:
                odor_qualities = []
                taste_qualities = []
                for table in parent.find_all("table"):
                    for row in table.find_all("tr"):
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            quality_type = cells[0].get_text(strip=True).lower()
                            quality_value = cells[1].get_text(strip=True)
                            if "odor" in quality_type and quality_value:
                                odor_qualities.append(quality_value)
                            elif "taste" in quality_type and quality_value:
                                taste_qualities.append(quality_value)

                if odor_qualities:
                    compound["odor_qualities"] = "|".join(odor_qualities[:20])
                if taste_qualities:
                    compound["taste_qualities"] = "|".join(taste_qualities[:20])

        return compound

    except Exception as e:
        print(f"  Error parsing compound {compound_id}: {e}", file=sys.stderr)
        return None


def scrape_fsbi_data(output_dir: Path) -> Path:
    """Scrape all FSBI compound data.

    Args:
        output_dir: Directory to write output CSV.

    Returns:
        Path to the output CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "compounds.csv"
    checkpoint_file = output_dir / "compound_ids.txt"

    session = requests.Session()

    # Step 1: Collect all compound IDs
    if checkpoint_file.exists():
        print(f"Loading compound IDs from checkpoint: {checkpoint_file}", flush=True)
        with open(checkpoint_file) as f:
            compound_ids = set(int(line.strip()) for line in f if line.strip())
    else:
        compound_ids = collect_compound_ids(session)
        with open(checkpoint_file, "w") as f:
            for cid in sorted(compound_ids):
                f.write(f"{cid}\n")
        print(f"Saved {len(compound_ids)} compound IDs to checkpoint", flush=True)

    print(f"\nTotal unique compound IDs: {len(compound_ids)}", flush=True)

    # Check for already scraped compounds (resume support)
    scraped_ids = set()
    if output_csv.exists():
        with open(output_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("fsbi_id"):
                    scraped_ids.add(int(row["fsbi_id"]))
        print(f"Resuming: {len(scraped_ids)} compounds already scraped", flush=True)

    sorted_ids = sorted(compound_ids)
    remaining_ids = [cid for cid in sorted_ids if cid not in scraped_ids]

    print(f"\nScraping {len(remaining_ids)} compound detail pages...", flush=True)

    write_header = not output_csv.exists() or len(scraped_ids) == 0
    mode = "w" if write_header else "a"

    success_count = len(scraped_ids)

    with open(output_csv, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()

        for i, compound_id in enumerate(remaining_ids, 1):
            if i % 100 == 0 or i == 1:
                print(
                    f"[{i}/{len(remaining_ids)}] Scraping compound {compound_id}... (total saved: {success_count})",
                    flush=True,
                )

            compound = parse_compound_page(compound_id, session)
            if compound and compound.get("name"):
                writer.writerow(compound)
                f.flush()
                success_count += 1

            time.sleep(REQUEST_DELAY)

    print(f"\nSuccessfully scraped {success_count} compounds", flush=True)
    print(f"Saved: {output_csv}", flush=True)

    # Print statistics
    print("\n--- Statistics ---", flush=True)
    with open(output_csv, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        compounds = list(reader)

    print(f"Total compounds: {len(compounds)}")
    with_pubchem = sum(1 for c in compounds if c["pubchem_id"])
    print(f"With PubChem ID: {with_pubchem}")
    with_smiles = sum(1 for c in compounds if c["smiles"])
    print(f"With SMILES: {with_smiles}")
    with_cas = sum(1 for c in compounds if c["cas"])
    print(f"With CAS: {with_cas}")
    with_odor = sum(1 for c in compounds if c["odor_qualities"])
    print(f"With odor qualities: {with_odor}")

    return output_csv


def fetch_fsbi(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Scrape FSBI compound data.

    Args:
        output_dir: Directory to write output CSV.

    Returns:
        Path to the output directory.
    """
    scrape_fsbi_data(output_dir)
    return output_dir
