"""
Extract compound data from Panten "Common Fragrance and Flavor Materials" PDF.

Uses pdftotext for text extraction.

Outputs:
    raw_data/Panten/compounds.csv
"""

import csv
import re
import subprocess
from pathlib import Path

DEFAULT_OUTPUT_DIR = Path("raw_data/Panten")
DEFAULT_INPUT_PDF = DEFAULT_OUTPUT_DIR / "panten_common_fragrance.pdf"

# Section number pattern for entries (e.g., 2.1.3.1)
SECTION_NUM_PATTERN = re.compile(r"\n[ \t]*(\d+\.\d+\.\d+\.\d+)\s*\n")

# CAS number pattern in brackets [66-25-1]
CAS_PATTERN = re.compile(r"\[(\d{1,7}-\d{2}-\d)\]")

# Physical properties patterns
FORMULA_MW_PATTERN = re.compile(r"(C\d+\s*H\d+(?:\s*[A-Z][a-z]?\d*)*)\s*,\s*M[rw]\s*([\d.]+)")
BP_PATTERN = re.compile(r"bp\s*(?:([\d.]+)\s*kPa\s*)?(\d+(?:[–\-]\d+)?(?:\.?\d*)?)\s*(?:∘|°)?\s*C")
DENSITY_PATTERN = re.compile(r"d\s*(?:\d+)?\s*(?:\d+)?\s*([\d.]+)")
RI_PATTERN = re.compile(r"n\s*(?:\d+)?\s*D\s*([\d.]+)")

# FCT reference pattern
FCT_PATTERN = re.compile(
    r"FCT\s*(\d{4})\s*\((\d+)(?:,\s*suppl\.\s*(\d+))?\)\s*p+\.?\s*([\d\w\-–,\s;S]+?)(?=\.|;|FCT|\n|$)"
)

# Trade names pattern
TRADE_NAMES_PATTERN = re.compile(
    r"Trade\s*Names?\.?\s*([^\n]+(?:\n(?!\d+\.\d+)[^\n]*)*)", re.DOTALL
)

FIELDNAMES = [
    "section",
    "name",
    "cas_numbers",
    "synonyms",
    "empirical_formula",
    "molecular_weight",
    "boiling_point",
    "density",
    "refractive_index",
    "odor_description",
    "natural_occurrence",
    "synthesis",
    "uses",
    "fct_reference",
    "trade_names",
]


def extract_pdf_text(pdf_path: Path, start_page: int = 37, end_page: int = 220) -> str:
    """Extract text using pdftotext with layout mode for proper structure."""
    result = subprocess.run(
        ["pdftotext", "-layout", "-f", str(start_page), "-l", str(end_page), str(pdf_path), "-"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def normalize_text(text: str) -> str:
    """Normalize characters and clean up text."""
    text = text.replace("∘", "°")
    text = text.replace("–", "-")
    text = text.replace("‑", "-")
    text = text.replace("ﬂ", "fl")
    text = text.replace("ﬁ", "fi")
    text = text.replace("ﬀ", "ff")
    text = text.replace("ﬃ", "ffi")
    text = text.replace("\u00a0", " ")
    text = text.replace("®", "")

    lines = text.split("\n")
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in lines]
    text = "\n".join(lines)

    # Join lines that are continuations
    text = re.sub(r"\n(?=[a-z])", " ", text)

    return text


def split_into_entries(text: str) -> list[tuple[str, str]]:
    """Split text by section numbers and return (section_num, entry_text) tuples."""
    matches = list(SECTION_NUM_PATTERN.finditer(text))

    entries = []
    for i, match in enumerate(matches):
        section_num = match.group(1)
        start = match.end()

        if i + 1 < len(matches):
            end = matches[i + 1].start()
        else:
            end = len(text)

        entry_text = text[start:end].strip()

        # Compound sections have 4 levels (2.1.3.1)
        if section_num.count(".") >= 3 and len(entry_text) > 100:
            entries.append((section_num, entry_text))

    return entries


def parse_header_line(text: str) -> tuple[str, list[str], list[str]]:
    """Parse the first line(s) to extract name, CAS numbers, and synonyms."""
    lines = text.split("\n")

    header_lines = []
    for line in lines[:8]:
        line = line.strip()
        if not line:
            continue
        if re.match(r"^[A-Z]{1,2}\d*[A-Z]*\d*\s*$", line):
            continue
        if re.match(r"^\d+\s*$", line):
            continue
        if re.match(r"^\d+\s+\d+\s+Individual", line):
            continue
        if "Aliphatic Compounds" in line or "Individual Fragrance" in line:
            continue
        if re.match(r"^C\d+\s*H\d+", line):
            break
        if CAS_PATTERN.search(line) or (len(line) > 3 and not line.startswith("=")):
            header_lines.append(line)
            if CAS_PATTERN.search(line):
                break

    header = " ".join(header_lines)

    cas_numbers = CAS_PATTERN.findall(header)

    first_cas_match = CAS_PATTERN.search(header)
    if first_cas_match:
        name_part = header[: first_cas_match.start()].strip().rstrip(",")
        last_cas_end = 0
        for m in CAS_PATTERN.finditer(header):
            last_cas_end = m.end()
        synonyms_part = header[last_cas_end:].strip().lstrip(",").strip()
    else:
        parts = header.split(",", 1)
        name_part = parts[0].strip()
        synonyms_part = parts[1].strip() if len(parts) > 1 else ""

    name = name_part.strip()
    name = re.sub(r"\[\s*\]", "", name)
    name = name.strip()

    synonyms = []
    if synonyms_part:
        raw_synonyms = re.split(r",\s+(?=[A-Z(\"])", synonyms_part)
        for s in raw_synonyms:
            s = s.strip().strip(",").strip()
            if s and len(s) > 1:
                synonyms.append(s)

    return name, cas_numbers, synonyms


def parse_physical_properties(text: str) -> dict:
    """Extract physical properties from the entry text."""
    props = {
        "empirical_formula": "",
        "molecular_weight": "",
        "boiling_point": "",
        "density": "",
        "refractive_index": "",
    }

    fm_match = FORMULA_MW_PATTERN.search(text)
    if fm_match:
        props["empirical_formula"] = fm_match.group(1).replace(" ", "")
        props["molecular_weight"] = fm_match.group(2)

    bp_match = BP_PATTERN.search(text)
    if bp_match:
        pressure = bp_match.group(1)
        temp = bp_match.group(2)
        if pressure:
            props["boiling_point"] = f"{temp}°C at {pressure} kPa"
        else:
            props["boiling_point"] = f"{temp}°C"

    d_match = DENSITY_PATTERN.search(text)
    if d_match:
        props["density"] = d_match.group(1)

    ri_match = RI_PATTERN.search(text)
    if ri_match:
        props["refractive_index"] = ri_match.group(1)

    return props


def extract_odor_description(text: str) -> str:
    """Extract odor/sensory description from entry."""
    patterns = [
        r"(?:with|has|having)\s+(?:a|an)\s+([^.]+?(?:odor|note|smell)[^.]*)",
        r"(?:is a|It is a)\s+[^.]*?(?:with|having)\s+(?:a|an)?\s*([^.]+?(?:odor|note|character)[^.]*)",
        r"liquid\s+(?:with|having)\s+(?:a|an)?\s*([^.]+?(?:odor|note)[^.]*)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            desc = match.group(1).strip()
            desc = re.sub(r"\s+", " ", desc)
            desc = desc.rstrip(".")
            if len(desc) > 200:
                desc = desc[:200] + "..."
            return desc

    return ""


def extract_natural_occurrence(text: str) -> str:
    """Extract natural occurrence information."""
    patterns = [
        r"(?:occurs?\s+(?:naturally\s+)?(?:in|as)\s+)([^.]+)",
        r"(?:found\s+(?:in|as)\s+)([^.]+)",
        r"(?:is\s+a\s+(?:component|constituent)\s+of\s+)([^.]+)",
    ]

    occurrences = []
    seen = set()
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            occ = match.group(1).strip()
            occ = re.sub(r"\s+", " ", occ)
            if occ and len(occ) < 150 and occ.lower() not in seen:
                seen.add(occ.lower())
                occurrences.append(occ)

    return "; ".join(occurrences[:2])


def extract_synthesis(text: str) -> str:
    """Extract synthesis/preparation information."""
    patterns = [
        r"(?:is\s+)?(?:synthesized|prepared|obtained|produced)\s+(?:by|from|via)\s+([^.]+)",
        r"(?:can\s+be\s+)(?:synthesized|prepared|obtained|produced)\s+(?:by|from|via)\s+([^.]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            syn = match.group(1).strip()
            syn = re.sub(r"\s+", " ", syn)
            if len(syn) > 200:
                syn = syn[:200] + "..."
            return syn

    return ""


def extract_uses(text: str) -> str:
    """Extract usage information."""
    patterns = [
        r"(?:is\s+)?used\s+(?:in|to|for)\s+([^.]+)",
        r"(?:It\s+is\s+)?(?:widely\s+)?used\s+(?:in|to|for)\s+([^.]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            use = match.group(1).strip()
            use = re.sub(r"\s+", " ", use)
            if len(use) > 200:
                use = use[:200] + "..."
            return use

    return ""


def extract_fct_reference(text: str) -> str:
    """Extract FCT (Food and Chemical Toxicology) references."""
    matches = FCT_PATTERN.findall(text)

    refs = []
    for match in matches:
        year = match[0]
        vol = match[1]
        suppl = match[2]
        page = match[3].strip()

        if suppl:
            refs.append(f"FCT {year} ({vol}, suppl. {suppl}) p. {page}")
        else:
            refs.append(f"FCT {year} ({vol}) p. {page}")

    return "; ".join(refs) if refs else ""


def extract_trade_names(text: str) -> str:
    """Extract trade names with suppliers."""
    match = TRADE_NAMES_PATTERN.search(text)
    if match:
        names = match.group(1).strip()
        names = re.sub(r"\s+", " ", names)
        names = re.split(r"\s*FCT\s*|\s*\d+\.\d+\.\d+", names)[0]
        names = names.rstrip(".")
        if len(names) > 200:
            names = names[:200]
        return names
    return ""


def parse_entry(section_num: str, entry_text: str) -> dict:
    """Parse a single compound entry."""
    text = normalize_text(entry_text)

    name, cas_numbers, synonyms = parse_header_line(text)
    props = parse_physical_properties(text)

    compound = {
        "section": section_num,
        "name": name,
        "cas_numbers": "|".join(cas_numbers),
        "synonyms": "|".join(synonyms),
        "empirical_formula": props["empirical_formula"],
        "molecular_weight": props["molecular_weight"],
        "boiling_point": props["boiling_point"],
        "density": props["density"],
        "refractive_index": props["refractive_index"],
        "odor_description": extract_odor_description(text),
        "natural_occurrence": extract_natural_occurrence(text),
        "synthesis": extract_synthesis(text),
        "uses": extract_uses(text),
        "fct_reference": extract_fct_reference(text),
        "trade_names": extract_trade_names(text),
    }

    return compound


def extract_panten_data(input_pdf: Path, output_dir: Path) -> Path:
    """Extract compound data from the Panten PDF.

    Args:
        input_pdf: Path to the Panten PDF.
        output_dir: Directory to write output CSV.

    Returns:
        Path to the output CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "compounds.csv"

    if not input_pdf.exists():
        raise FileNotFoundError(f"PDF not found: {input_pdf}")

    print(f"Extracting text from: {input_pdf}")

    text = extract_pdf_text(input_pdf, start_page=7)
    print(f"Extracted {len(text):,} characters")

    entries = split_into_entries(text)
    print(f"Found {len(entries)} section entries")

    compounds = []
    for section_num, entry_text in entries:
        compound = parse_entry(section_num, entry_text)
        if compound["name"]:
            compounds.append(compound)

    print(f"Parsed {len(compounds)} compounds with names")

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(compounds)

    print(f"Saved: {output_csv}")

    # Print statistics
    print("\n--- Statistics ---")
    print(f"Total compounds: {len(compounds)}")
    with_cas = sum(1 for c in compounds if c["cas_numbers"])
    print(f"With CAS number: {with_cas}")
    with_formula = sum(1 for c in compounds if c["empirical_formula"])
    print(f"With formula: {with_formula}")
    with_odor = sum(1 for c in compounds if c["odor_description"])
    print(f"With odor description: {with_odor}")

    return output_csv


def fetch_panten(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    input_pdf: Path | None = None,
) -> Path:
    """Extract Panten compound data.

    Args:
        output_dir: Directory to write output CSV.
        input_pdf: Path to the Panten PDF. Defaults to
                   raw_data/Panten/panten_common_fragrance.pdf.

    Returns:
        Path to the output directory.
    """
    if input_pdf is None:
        input_pdf = output_dir / "panten_common_fragrance.pdf"

    extract_panten_data(input_pdf, output_dir)
    return output_dir
