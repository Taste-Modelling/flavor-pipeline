"""
Extract flavor substances from Fenaroli's Handbook of Flavor Ingredients PDF.

Outputs:
    raw_data/Fenaroli/flavor_substances.csv - Main flavor substances data
    raw_data/Fenaroli/reported_uses.csv - Flattened reported uses data
"""

import csv
import json
import re
from pathlib import Path

import fitz  # PyMuPDF

DEFAULT_OUTPUT_DIR = Path("raw_data/Fenaroli")
DEFAULT_INPUT_PDF = DEFAULT_OUTPUT_DIR / "fenaroli_handbook_flavor.pdf"


def extract_fenaroli_data(input_pdf: Path, output_dir: Path) -> tuple[Path, Path]:
    """Extract flavor substance data from the Fenaroli PDF.

    Args:
        input_pdf: Path to the Fenaroli handbook PDF.
        output_dir: Directory to write output CSV files.

    Returns:
        Tuple of (flavor_substances.csv path, reported_uses.csv path).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_substances = output_dir / "flavor_substances.csv"
    output_uses = output_dir / "reported_uses.csv"

    if not input_pdf.exists():
        raise FileNotFoundError(f"PDF not found: {input_pdf}")

    doc = fitz.open(input_pdf)
    full_text = ""
    for i in range(26, doc.page_count):
        full_text += doc[i].get_text() + "\n"

    cas_matches = list(re.finditer(r"CAS No\.:", full_text))

    entries = []
    for idx, cas_m in enumerate(cas_matches):
        cas_pos = cas_m.start()
        next_cas = cas_matches[idx + 1].start() if idx + 1 < len(cas_matches) else len(full_text)
        lookback = full_text[max(0, cas_pos - 3000) : cas_pos]
        entry_after = full_text[cas_pos:next_cas]
        block = "CAS No.:" + entry_after
        entry = {}
        lines = lookback.strip().split("\n")

        metadata_keywords = [
            "Synonym",
            "Botanical name:",
            "Botanical family:",
            "Other names:",
            "Foreign names:",
            "Genus/Species:",
        ]
        metadata_start = len(lines)
        for j in range(len(lines) - 1, -1, -1):
            line = lines[j].strip()
            if any(line.startswith(p) for p in metadata_keywords):
                metadata_start = j
            elif metadata_start < len(lines):
                break

        metadata_block = "\n".join(lines[metadata_start:])
        syn_m = re.search(r"Synonyms?:\s*(.+)", metadata_block, re.DOTALL)
        entry["Synonyms"] = syn_m.group(1).strip().replace("\n", " ") if syn_m else ""
        bn_m = re.search(
            r"Botanical name:\s*(.+?)(?=\n(?:Botanical family|Other names|Foreign names|CAS|Genus)|$)",
            metadata_block,
            re.DOTALL,
        )
        entry["Botanical_Name"] = bn_m.group(1).strip().replace("\n", " ") if bn_m else ""
        bf_m = re.search(r"Botanical family:\s*(.+?)(?=\n|$)", metadata_block)
        entry["Botanical_Family"] = bf_m.group(1).strip() if bf_m else ""
        on_m = re.search(
            r"Other names:\s*(.+?)(?=\n(?:Foreign names|CAS|Botanical)|$)",
            metadata_block,
            re.DOTALL,
        )
        entry["Other_Names"] = on_m.group(1).strip().replace("\n", " ") if on_m else ""
        gs_m = re.search(
            r"Genus/Species:\s*(.+?)(?=\n(?:Other names|Foreign names|CAS)|$)",
            metadata_block,
            re.DOTALL,
        )
        if gs_m:
            entry["Other_Names"] = (
                gs_m.group(1).strip().replace("\n", " ")
                + ("; " + entry["Other_Names"] if entry["Other_Names"] else "")
            )
        fn_m = re.search(r"Foreign names:\s*(.+?)(?=\nCAS|$)", metadata_block, re.DOTALL)
        entry["Foreign_Names"] = fn_m.group(1).strip().replace("\n", " ") if fn_m else ""

        # Name extraction
        name_candidates = []
        for j in range(metadata_start - 1, max(metadata_start - 4, -1), -1):
            if j < 0:
                break
            line = lines[j].strip()
            if not line or re.match(r"^\d+\s*$", line) or re.match(r"^[\d.,\s]+$", line):
                continue
            if any(
                x in line.lower()
                for x in [
                    "natural occurrence:",
                    "aroma threshold",
                    "taste threshold",
                    "synthesis:",
                    "reported uses",
                    "food category",
                    "note:",
                    "iofi:",
                    "padi:",
                    "specifications:",
                ]
            ):
                break
            if line.startswith(("Soluble", "Insoluble", "Miscible", "Slightly")):
                break
            if re.match(r"^[\d.]+[–\-][\d.]+", line):
                break
            name_candidates.insert(0, line)
            if len(name_candidates) >= 2:
                break

        raw_name = " ".join(name_candidates).strip()
        raw_name = re.sub(r"\*+$", "", raw_name).strip()

        if ". " in raw_name:
            candidate = raw_name.rsplit(". ", 1)[-1].strip()
            if candidate and (
                candidate[0].isupper()
                or candidate[0].isdigit()
                or candidate[0] in "αβγδεldt(+/"
            ):
                raw_name = candidate

        if raw_name and raw_name[0].islower():
            caps_m = re.search(r"([A-Zα-ω][A-Z\d\s,\-\(\)\+/ʹ\']+)$", raw_name)
            if caps_m:
                raw_name = caps_m.group(1).strip()

        # Handle botanical entries where name is above the metadata
        if not raw_name or raw_name.startswith("Botanical"):
            for j in range(metadata_start - 1, max(metadata_start - 3, -1), -1):
                if j < 0:
                    break
                line = lines[j].strip()
                if not line or re.match(r"^\d+\s*$", line):
                    continue
                if ". " in line:
                    raw_name = line.rsplit(". ", 1)[-1].strip()
                else:
                    raw_name = line
                break

        # If still problematic, try the running footer
        if len(raw_name) > 100 or (raw_name and raw_name[0].islower()):
            end_block = entry_after[-600:] if len(entry_after) > 600 else entry_after
            end_lines = [line.strip() for line in end_block.strip().split("\n") if line.strip()]
            for line in reversed(end_lines):
                if len(line) > 3 and not re.match(r"^\d+$", line) and line[0].isupper():
                    raw_name = line
                    break

        entry["Name"] = raw_name

        # IDs
        cas_val = re.search(
            r"CAS No\.:\s*\n?([\dn/a\-‑\u2011]+(?:[\s\n]*\([^)]+\)\s*\n?[\d\-‑\u2011n/a]*)*)",
            block,
        )
        entry["CAS_No"] = (
            cas_val.group(1).replace("‑", "-").replace("\u2011", "-").replace("\n", " ").strip()
            if cas_val
            else ""
        )
        for field, pat in [
            ("FL_No", r"FL No\.:\s*\n?([^\n]+)"),
            ("FEMA_No", r"FEMA No\.:\s*\n?([^\n]+)"),
            ("NAS_No", r"NAS No\.:\s*\n?([^\n]+)"),
            ("CoE_No", r"CoE No\.:\s*\n?([^\n]+)"),
            ("EINECS_No", r"EINECS No\.?:\s*\n?([^\n]+)"),
            ("JECFA_No", r"JECFA No\.:\s*\n?([^\n]+)"),
        ]:
            fm = re.search(pat, block)
            entry[field] = (
                fm.group(1).strip().replace("‑", "-").replace("\u2011", "-") if fm else ""
            )

        dm = re.search(
            r"Description:\s*(.+?)(?=\nConsumption:|Regulatory Status:|Reported uses|\nDerivatives:)",
            block,
            re.DOTALL,
        )
        entry["Description"] = dm.group(1).strip().replace("\n", " ") if dm else ""
        am = re.search(r"Annual:\s*([^\t\n]+)", block)
        entry["Consumption_Annual"] = am.group(1).strip() if am else ""
        im = re.search(r"Individual:\s*([^\t\n]+)", block)
        entry["Consumption_Individual"] = im.group(1).strip() if im else ""

        reg = re.search(
            r"Regulatory Status:\s*\n(.*?)(?=Trade association|Empirical Formula|Specifications:|Reported uses|$)",
            block,
            re.DOTALL,
        )
        rb = reg.group(1) if reg else ""
        rm = re.search(r"CoE:\s*(.+?)(?=\nFDA:)", rb, re.DOTALL)
        entry["Regulatory_CoE"] = rm.group(1).strip().replace("\n", " ") if rm else ""
        rm = re.search(r"(?<!\()FDA:\s*(.+?)(?=\nFDA \(other\):)", rb, re.DOTALL)
        entry["Regulatory_FDA"] = rm.group(1).strip().replace("\n", " ") if rm else ""
        rm = re.search(r"FDA \(other\):\s*(.+?)(?=\nJECFA:)", rb, re.DOTALL)
        entry["Regulatory_FDA_other"] = rm.group(1).strip().replace("\n", " ") if rm else ""
        rm = re.search(r"JECFA:\s*(.+?)$", rb, re.DOTALL)
        entry["Regulatory_JECFA"] = rm.group(1).strip().replace("\n", " ") if rm else ""

        pm = re.search(r"(?:FEMA )?PADI:\s*([\d.<]+\s*mg)", block)
        entry["FEMA_PADI"] = pm.group(1).strip() if pm else ""
        im2 = re.search(r"IOFI:\s*([^\n\t]+)", block)
        entry["IOFI"] = im2.group(1).strip() if im2 else ""

        ef = re.search(r"Empirical Formula/MW:\s*\n?(.+?)/([\d.]+)", block)
        entry["Empirical_Formula"] = ef.group(1).strip() if ef else ""
        entry["Molecular_Weight"] = ef.group(2).strip() if ef else ""

        sb_m = re.search(
            r"Specifications:.*?\n(.*?)(?=Reported uses|Synthesis:|Aroma threshold|Natural occurrence|Note:|Derivatives:|$)",
            block,
            re.DOTALL,
        )
        sb = sb_m.group(1) if sb_m else ""
        for sn, sk in [
            ("Appearance", "Spec_Appearance"),
            ("Assay", "Spec_Assay"),
            ("Specific gravity", "Spec_Specific_Gravity"),
            ("Refractive index", "Spec_Refractive_Index"),
            ("Boiling point", "Spec_Boiling_Point"),
            ("Melting point", "Spec_Melting_Point"),
            ("Solubility", "Spec_Solubility"),
            ("Acid value", "Spec_Acid_Value"),
        ]:
            sm = re.search(
                rf"{re.escape(sn)}\s*(?:\((?:min|max)\))?\s*\n(.+?)(?=\n[A-Z])",
                sb,
                re.IGNORECASE,
            )
            entry[sk] = sm.group(1).strip().replace("\n", " ") if sm else ""

        sm = re.search(r"Synthesis:\s*(.+?)(?=\nAroma threshold|$)", block, re.DOTALL)
        entry["Synthesis"] = sm.group(1).strip().replace("\n", " ") if sm else ""
        atm = re.search(
            r"Aroma threshold values?\s*(?:\(ppb\))?:?\s*(.+?)(?=\nTaste threshold|$)",
            block,
            re.DOTALL,
        )
        entry["Aroma_Threshold"] = atm.group(1).strip().replace("\n", " ") if atm else ""
        ttm = re.search(
            r"Taste threshold values?\s*(?:\(ppb\))?:?\s*(.+?)(?=\nNatural occurrence|$)",
            block,
            re.DOTALL,
        )
        entry["Taste_Threshold"] = ttm.group(1).strip().replace("\n", " ") if ttm else ""
        nm = re.search(
            r"Natural occurrence:\s*(.+?)(?=\n[A-Z][A-Z ,\-\d\(\)ʹ]+\n|Note:|$)",
            block,
            re.DOTALL,
        )
        entry["Natural_Occurrence"] = nm.group(1).strip().replace("\n", " ") if nm else ""

        um = re.search(
            r"Reported uses \(ppm\).*?\n(.*?)(?=Synthesis:|Aroma threshold|Specifications:|Natural occurrence|Note:|Derivatives:|$)",
            block,
            re.DOTALL,
        )
        if um:
            pairs = re.findall(r"([A-Za-z][A-Za-z /,.\'\-]+?)\s+([\d.]+)\s+([\d.]+)", um.group(1))
            ud = {}
            for cat, usual, mx in pairs:
                cat = cat.strip().rstrip(",.")
                if cat and "Food Category" not in cat and "Usual" not in cat:
                    ud[cat] = {"usual_ppm": usual, "max_ppm": mx}
            entry["Reported_Uses_JSON"] = json.dumps(ud) if ud else ""
        else:
            entry["Reported_Uses_JSON"] = ""

        entries.append(entry)

    print(f"Total entries extracted: {len(entries)}")
    good = sum(1 for e in entries if e["Name"] and len(e["Name"]) <= 80)
    print(f"Good names: {good}/{len(entries)}")

    # Write main substances CSV
    substance_columns = [
        "Name",
        "CAS_No",
        "FEMA_No",
        "FL_No",
        "NAS_No",
        "CoE_No",
        "EINECS_No",
        "JECFA_No",
        "Synonyms",
        "Other_Names",
        "Botanical_Name",
        "Botanical_Family",
        "Foreign_Names",
        "Description",
        "Empirical_Formula",
        "Molecular_Weight",
        "Consumption_Annual",
        "Consumption_Individual",
        "FEMA_PADI",
        "IOFI",
        "Regulatory_CoE",
        "Regulatory_FDA",
        "Regulatory_FDA_other",
        "Regulatory_JECFA",
        "Spec_Appearance",
        "Spec_Assay",
        "Spec_Specific_Gravity",
        "Spec_Refractive_Index",
        "Spec_Boiling_Point",
        "Spec_Melting_Point",
        "Spec_Solubility",
        "Spec_Acid_Value",
        "Synthesis",
        "Aroma_Threshold",
        "Taste_Threshold",
        "Natural_Occurrence",
        "Reported_Uses_JSON",
    ]

    with open(output_substances, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=substance_columns)
        writer.writeheader()
        writer.writerows(entries)

    print(f"Saved: {output_substances}")

    # Write flattened reported uses CSV
    uses_columns = ["Substance_Name", "CAS_No", "FEMA_No", "Food_Category", "Usual_PPM", "Max_PPM"]
    uses_rows = []

    for entry in entries:
        if entry["Reported_Uses_JSON"]:
            uses = json.loads(entry["Reported_Uses_JSON"])
            for cat, vals in uses.items():
                uses_rows.append({
                    "Substance_Name": entry["Name"],
                    "CAS_No": entry["CAS_No"],
                    "FEMA_No": entry["FEMA_No"],
                    "Food_Category": cat,
                    "Usual_PPM": vals["usual_ppm"],
                    "Max_PPM": vals["max_ppm"],
                })

    with open(output_uses, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=uses_columns)
        writer.writeheader()
        writer.writerows(uses_rows)

    print(f"Saved: {output_uses}")
    print(f"  - flavor_substances.csv: {len(entries)} rows")
    print(f"  - reported_uses.csv: {len(uses_rows)} rows")

    return output_substances, output_uses


def fetch_fenaroli(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    input_pdf: Path | None = None,
) -> Path:
    """Extract Fenaroli flavor substances data.

    Args:
        output_dir: Directory to write output CSV files.
        input_pdf: Path to the Fenaroli handbook PDF. Defaults to
                   raw_data/Fenaroli/fenaroli_handbook_flavor.pdf.

    Returns:
        Path to the output directory.
    """
    if input_pdf is None:
        input_pdf = output_dir / "fenaroli_handbook_flavor.pdf"

    extract_fenaroli_data(input_pdf, output_dir)
    return output_dir
