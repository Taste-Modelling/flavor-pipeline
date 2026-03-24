"""
Scrape compound data from VCF (Volatile Compounds in Food) EU-Flavis database.

Source: https://www.vcf-online.nl/VcfCompounds.cfm?Flavis

Outputs:
    raw_data/VCF/compounds.csv
"""

import csv
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

DEFAULT_OUTPUT_DIR = Path("raw_data/VCF")

BASE_URL = "https://www.vcf-online.nl"
FLAVIS_URL = f"{BASE_URL}/VcfCompounds.cfm?Flavis"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research scraper; gustatory-datasets)"
}
REQUEST_DELAY = 1.0  # Seconds between requests

FIELDNAMES = [
    "fl_no",
    "fema",
    "cas",
    "compound_name",
    "chemical_group",
    "chemical_group_code",
    "flags",
]


def get_soup(url: str, session: requests.Session) -> BeautifulSoup:
    """Fetch URL and return BeautifulSoup parser."""
    response = session.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def get_category_links(soup: BeautifulSoup) -> list[tuple[str, str]]:
    """Extract chemical category links from the main Flavis page."""
    categories = []

    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        if "volatgrp=" in href and "Flavis" in href:
            name = link.get_text(strip=True)
            if name and not name.isdigit():
                full_url = f"{BASE_URL}{href}" if href.startswith("/") else href
                categories.append((name, full_url))

    return categories


def parse_compound_table(soup: BeautifulSoup, category_name: str) -> list[dict]:
    """Parse compound table from a category page."""
    compounds = []

    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            cell_texts = [cell.get_text(strip=True) for cell in cells]
            non_empty = [t for t in cell_texts if t]

            if len(non_empty) < 4:
                continue

            fl_no = None
            fl_idx = None
            for i, text in enumerate(non_empty):
                if re.match(r"^\d{2}\.\d{3}$", text):
                    fl_no = text
                    fl_idx = i
                    break

            if fl_no is None or fl_idx is None:
                continue

            chem_grp_code = ""
            if fl_idx > 1:
                chem_grp_code = non_empty[fl_idx - 1]

            rest = non_empty[fl_idx + 1 :]

            fema = ""
            cas = ""
            compound_name = ""
            flags = ""

            for text in rest:
                if re.match(r"^\d{4}$", text) and not fema:
                    fema = text
                elif re.match(r"^\d+-\d+-\d$", text) and not cas:
                    cas = text
                elif (
                    re.match(r"^[A-Z](\s|[\xa0])*([A-Z](\s|[\xa0])*)*$", text)
                    and len(text) < 25
                ):
                    flags = text.replace("\xa0", " ").strip()
                elif not compound_name and len(text) > 2:
                    compound_name = text

            if compound_name:
                compounds.append({
                    "fl_no": fl_no,
                    "fema": fema,
                    "cas": cas,
                    "compound_name": compound_name,
                    "chemical_group": category_name,
                    "chemical_group_code": chem_grp_code,
                    "flags": flags,
                })

    return compounds


def scrape_vcf_data(output_dir: Path) -> Path:
    """Scrape all VCF compound data.

    Args:
        output_dir: Directory to write output CSV.

    Returns:
        Path to the output CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "compounds.csv"

    session = requests.Session()

    print(f"Fetching main Flavis page: {FLAVIS_URL}")
    main_soup = get_soup(FLAVIS_URL, session)

    categories = get_category_links(main_soup)
    print(f"Found {len(categories)} chemical categories")

    all_compounds = []

    for i, (category_name, url) in enumerate(categories, 1):
        print(f"[{i}/{len(categories)}] Scraping: {category_name}")

        try:
            soup = get_soup(url, session)
            compounds = parse_compound_table(soup, category_name)
            all_compounds.extend(compounds)
            print(f"    Found {len(compounds)} compounds")
        except Exception as e:
            print(f"    Error: {e}")

        time.sleep(REQUEST_DELAY)

    print(f"\nTotal compounds scraped: {len(all_compounds)}")

    # Deduplicate by FL number
    seen_fl = set()
    unique_compounds = []
    for compound in all_compounds:
        fl_no = compound["fl_no"]
        if fl_no not in seen_fl:
            seen_fl.add(fl_no)
            unique_compounds.append(compound)

    print(f"Unique compounds (by FL number): {len(unique_compounds)}")

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(unique_compounds)

    print(f"Saved: {output_csv}")

    # Print statistics
    print(f"\n--- Statistics ---")
    print(f"Total unique compounds: {len(unique_compounds)}")
    with_cas = sum(1 for c in unique_compounds if c["cas"])
    print(f"With CAS number: {with_cas}")
    with_fema = sum(1 for c in unique_compounds if c["fema"])
    print(f"With FEMA number: {with_fema}")

    return output_csv


def fetch_vcf(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Scrape VCF compound data.

    Args:
        output_dir: Directory to write output CSV.

    Returns:
        Path to the output directory.
    """
    scrape_vcf_data(output_dir)
    return output_dir
