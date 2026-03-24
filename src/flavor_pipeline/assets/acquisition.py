"""Acquisition assets - fetch raw data from external sources.

These assets wrap the acquisition module functions and produce raw data files
that are consumed by the tier1 parsing assets.
"""

from pathlib import Path

from dagster import AssetExecutionContext, asset

from flavor_pipeline.acquisition import (
    fetch_bitterdb,
    fetch_fenaroli,
    fetch_flavordb2,
    fetch_fsbi,
    fetch_panten,
    fetch_vcf,
)

RAW_DATA_DIR = Path("raw_data")


@asset(
    group_name="acquisition",
    description="Fetch FlavorDB2 molecule data with PubChem enrichment (~25k molecules)",
)
def flavordb2_raw(context: AssetExecutionContext) -> Path:
    """Scrape FlavorDB2 molecules table and enrich with PubChem structural data.

    Outputs: raw_data/FlavorDB2/molecules.csv
    """
    output_dir = RAW_DATA_DIR / "FlavorDB2"

    # Check if data already exists
    output_file = output_dir / "molecules.csv"
    if output_file.exists():
        import pandas as pd
        df = pd.read_csv(output_file)
        context.log.info(f"Using existing data: {len(df)} molecules in {output_file}")
        return output_dir

    context.log.info("Fetching FlavorDB2 data (this may take several minutes)...")
    result_path = fetch_flavordb2(output_dir=output_dir)
    context.log.info(f"FlavorDB2 data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Download BitterDB 2024 compound data (12 CSV files, ~2.3k compounds)",
)
def bitterdb_raw(context: AssetExecutionContext) -> Path:
    """Download all BitterDB 2024 CSV files.

    Outputs: raw_data/BitterDB/*.csv
    """
    output_dir = RAW_DATA_DIR / "BitterDB"

    # Check if data already exists
    compounds_file = output_dir / "BitterCompoundsPropA_2024.csv"
    if compounds_file.exists():
        context.log.info(f"Using existing data in {output_dir}")
        return output_dir

    context.log.info("Downloading BitterDB 2024 data...")
    result_path = fetch_bitterdb(output_dir=output_dir)
    context.log.info(f"BitterDB data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Scrape FSBI-DB flavor compounds (~2.5k compounds with sensory data)",
)
def fsbi_raw(context: AssetExecutionContext) -> Path:
    """Scrape FSBI-DB compound detail pages.

    Outputs: raw_data/FSBI/compounds.csv
    """
    output_dir = RAW_DATA_DIR / "FSBI"

    # Check if data already exists
    output_file = output_dir / "compounds.csv"
    if output_file.exists():
        import pandas as pd
        df = pd.read_csv(output_file)
        context.log.info(f"Using existing data: {len(df)} compounds in {output_file}")
        return output_dir

    context.log.info("Scraping FSBI-DB data (this may take a while)...")
    result_path = fetch_fsbi(output_dir=output_dir)
    context.log.info(f"FSBI data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Scrape VCF EU-Flavis volatile compounds (~2.7k compounds)",
)
def vcf_raw(context: AssetExecutionContext) -> Path:
    """Scrape VCF Flavis compound tables.

    Outputs: raw_data/VCF/compounds.csv
    """
    output_dir = RAW_DATA_DIR / "VCF"

    # Check if data already exists
    output_file = output_dir / "compounds.csv"
    if output_file.exists():
        import pandas as pd
        df = pd.read_csv(output_file)
        context.log.info(f"Using existing data: {len(df)} compounds in {output_file}")
        return output_dir

    context.log.info("Scraping VCF EU-Flavis data...")
    result_path = fetch_vcf(output_dir=output_dir)
    context.log.info(f"VCF data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Extract Fenaroli handbook flavor substances from PDF",
)
def fenaroli_raw(context: AssetExecutionContext) -> Path:
    """Extract flavor substance data from Fenaroli's Handbook PDF.

    Requires: raw_data/Fenaroli/fenaroli_handbook_flavor.pdf
    Outputs: raw_data/Fenaroli/flavor_substances.csv
    """
    output_dir = RAW_DATA_DIR / "Fenaroli"
    input_pdf = output_dir / "fenaroli_handbook_flavor.pdf"

    # Check if output already exists
    output_file = output_dir / "flavor_substances.csv"
    if output_file.exists():
        import pandas as pd
        df = pd.read_csv(output_file)
        context.log.info(f"Using existing data: {len(df)} substances in {output_file}")
        return output_dir

    # Check if input PDF exists
    if not input_pdf.exists():
        context.log.warning(f"PDF not found: {input_pdf}. Skipping Fenaroli extraction.")
        return output_dir

    context.log.info("Extracting Fenaroli handbook data from PDF...")
    result_path = fetch_fenaroli(output_dir=output_dir)
    context.log.info(f"Fenaroli data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Extract Panten fragrance/flavor compounds from PDF (~350 compounds)",
)
def panten_raw(context: AssetExecutionContext) -> Path:
    """Extract compound data from Panten 'Common Fragrance and Flavor Materials' PDF.

    Requires: raw_data/Panten/*.pdf (the handbook PDF)
    Outputs: raw_data/Panten/compounds.csv
    """
    output_dir = RAW_DATA_DIR / "Panten"

    # Check if output already exists
    output_file = output_dir / "compounds.csv"
    if output_file.exists():
        import pandas as pd
        df = pd.read_csv(output_file)
        context.log.info(f"Using existing data: {len(df)} compounds in {output_file}")
        return output_dir

    # Check if any PDF exists in the directory
    pdfs = list(output_dir.glob("*.pdf")) if output_dir.exists() else []
    if not pdfs:
        context.log.warning(f"No PDF found in {output_dir}. Skipping Panten extraction.")
        return output_dir

    context.log.info("Extracting Panten handbook data from PDF...")
    result_path = fetch_panten(output_dir=output_dir)
    context.log.info(f"Panten data saved to {result_path}")

    return output_dir
