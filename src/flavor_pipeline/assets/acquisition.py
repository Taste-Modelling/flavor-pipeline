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
from flavor_pipeline.acquisition.culinarydb import fetch_culinarydb
from flavor_pipeline.acquisition.duke_phytochem import fetch_duke_phytochem
from flavor_pipeline.acquisition.fao_infoods import fetch_fao_infoods
from flavor_pipeline.acquisition.foodatlas import fetch_foodatlas
from flavor_pipeline.acquisition.foodb import fetch_foodb
from flavor_pipeline.acquisition.metabolights import fetch_metabolights
from flavor_pipeline.acquisition.sweetenersdb import fetch_sweetenersdb
from flavor_pipeline.acquisition.umamidb import fetch_umamidb
from flavor_pipeline.acquisition.winesensed import fetch_winesensed

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


@asset(
    group_name="acquisition",
    description="Download FooDB food compound data (~26k compounds with flavors)",
)
def foodb_raw(context: AssetExecutionContext) -> Path:
    """Download and extract FooDB CSV data archive.

    Outputs: raw_data/FooDB/foodb_2020_04_07_csv/*.csv
    """
    output_dir = RAW_DATA_DIR / "FooDB"
    csv_dir = output_dir / "foodb_2020_04_07_csv"

    # Check if data already exists
    compound_file = csv_dir / "Compound.csv"
    if compound_file.exists():
        context.log.info(f"Using existing data in {csv_dir}")
        return output_dir

    context.log.info("Downloading FooDB data...")
    result_path = fetch_foodb(output_dir=output_dir)
    context.log.info(f"FooDB data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Download FoodAtlas v3.2.0 knowledge graph (~1.4k foods, ~194k chemicals)",
)
def foodatlas_raw(context: AssetExecutionContext) -> Path:
    """Download and extract FoodAtlas food-chemical knowledge graph.

    Outputs: raw_data/Foodatlas/v3.2_20250211/*.tsv
    """
    output_dir = RAW_DATA_DIR / "Foodatlas"
    data_subdir = output_dir / "v3.2_20250211"

    # Check if data already exists
    entities_file = data_subdir / "entities.tsv"
    if entities_file.exists():
        context.log.info(f"Using existing data in {data_subdir}")
        return output_dir

    context.log.info("Downloading FoodAtlas data from Google Drive...")
    result_path = fetch_foodatlas(output_dir=output_dir)
    context.log.info(f"FoodAtlas data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Download CulinaryDB recipes (~46k recipes from 22 cuisines)",
)
def culinarydb_raw(context: AssetExecutionContext) -> Path:
    """Download and extract CulinaryDB recipe data.

    Outputs: raw_data/Culinarydb/*.csv
    """
    output_dir = RAW_DATA_DIR / "Culinarydb"

    # Check if data already exists
    recipe_file = output_dir / "01_Recipe_Details.csv"
    if recipe_file.exists():
        context.log.info(f"Using existing data in {output_dir}")
        return output_dir

    context.log.info("Downloading CulinaryDB data...")
    result_path = fetch_culinarydb(output_dir=output_dir)
    context.log.info(f"CulinaryDB data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Download WineSensed dataset (~350k wines, 824k reviews with flavor data)",
)
def winesensed_raw(context: AssetExecutionContext) -> Path:
    """Download WineSensed wine data from Hugging Face.

    Outputs: raw_data/WineSensed/*.csv
    """
    output_dir = RAW_DATA_DIR / "WineSensed"

    # Check if data already exists
    wines_file = output_dir / "images_reviews_attributes.csv"
    if wines_file.exists():
        context.log.info(f"Using existing data in {output_dir}")
        return output_dir

    context.log.info("Downloading WineSensed data from Hugging Face...")
    result_path = fetch_winesensed(output_dir=output_dir)
    context.log.info(f"WineSensed data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Download UmamiDB food amino acid/nucleotide data (~700 foods)",
)
def umamidb_raw(context: AssetExecutionContext) -> Path:
    """Download UmamiDB food data from API endpoint.

    Outputs: raw_data/Umamidb/foods.json
    """
    output_dir = RAW_DATA_DIR / "Umamidb"

    # Check if data already exists
    foods_file = output_dir / "foods.json"
    if foods_file.exists():
        context.log.info(f"Using existing data in {output_dir}")
        return output_dir

    context.log.info("Downloading UmamiDB data...")
    result_path = fetch_umamidb(output_dir=output_dir)
    context.log.info(f"UmamiDB data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Download SweetenersDB sweet compound data (~316 molecules)",
)
def sweetenersdb_raw(context: AssetExecutionContext) -> Path:
    """Download SweetenersDB from GitHub.

    Outputs: raw_data/Sweetenersdb/sweeteners.csv
    """
    output_dir = RAW_DATA_DIR / "Sweetenersdb"

    # Check if data already exists
    csv_file = output_dir / "sweeteners.csv"
    if csv_file.exists():
        context.log.info(f"Using existing data in {output_dir}")
        return output_dir

    context.log.info("Downloading SweetenersDB data from GitHub...")
    result_path = fetch_sweetenersdb(output_dir=output_dir)
    context.log.info(f"SweetenersDB data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Download FAO/INFOODS food composition databases (5 databases)",
)
def fao_infoods_raw(context: AssetExecutionContext) -> Path:
    """Download FAO/INFOODS food composition data.

    Downloads multiple databases:
    - AnFooD 2.0: Analytical Food Composition Database
    - uFiSh 1.0: Global fish/seafood nutrient database
    - uPulses 1.0: Global pulse nutrient database
    - BioFoodComp 4.0: Food biodiversity repository
    - Density Database v2.0: Food density values

    Outputs: raw_data/FAO_INFOODS/*.xlsx
    """
    output_dir = RAW_DATA_DIR / "FAO_INFOODS"

    # Check if data already exists (at least one main file)
    anfood_file = output_dir / "AnFooD2.0.xlsx"
    if anfood_file.exists():
        context.log.info(f"Using existing data in {output_dir}")
        return output_dir

    context.log.info("Downloading FAO/INFOODS databases...")
    result_path = fetch_fao_infoods(output_dir=output_dir)
    context.log.info(f"FAO/INFOODS data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Download Dr. Duke's Phytochemical database (~29k chemicals with activities)",
)
def duke_phytochem_raw(context: AssetExecutionContext) -> Path:
    """Download Dr. Duke's Phytochemical and Ethnobotanical database.

    Downloads bulk CSV archive containing:
    - ~29,000 phytochemicals with CAS numbers
    - ~2,400 biological activities
    - ~29,000 chemical-activity relationships
    - ~104,000 chemical-plant relationships

    Outputs: raw_data/DukePhytochem/*.csv
    """
    output_dir = RAW_DATA_DIR / "DukePhytochem"

    # Check if data already exists
    chemicals_file = output_dir / "CHEMICALS.csv"
    if chemicals_file.exists():
        context.log.info(f"Using existing data in {output_dir}")
        return output_dir

    context.log.info("Downloading Dr. Duke's Phytochemical database...")
    result_path = fetch_duke_phytochem(output_dir=output_dir)
    context.log.info(f"Duke Phytochem data saved to {result_path}")

    return output_dir


@asset(
    group_name="acquisition",
    description="Download MetaboLights compound data (~33k metabolites)",
)
def metabolights_raw(context: AssetExecutionContext) -> Path:
    """Download MetaboLights compound data via REST API.

    Downloads compound metadata including:
    - ~33,000 reference compounds
    - InChI, InChIKey, ChEBI IDs
    - Molecular formulas and names

    Note: Full download takes significant time due to API rate limits.

    Outputs: raw_data/MetaboLights/compounds.json
    """
    output_dir = RAW_DATA_DIR / "MetaboLights"

    # Check if data already exists
    compounds_file = output_dir / "compounds.json"
    if compounds_file.exists():
        import json
        with open(compounds_file) as f:
            compounds = json.load(f)
        context.log.info(f"Using existing data: {len(compounds)} compounds in {compounds_file}")
        return output_dir

    context.log.info("Downloading MetaboLights compound data (this may take a long time)...")
    result_path = fetch_metabolights(output_dir=output_dir)
    context.log.info(f"MetaboLights data saved to {result_path}")

    return output_dir
