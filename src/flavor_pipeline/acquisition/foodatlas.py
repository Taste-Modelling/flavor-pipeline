"""
Download FoodAtlas knowledge graph data from Google Drive.

Source: https://www.foodatlas.ai/food-composition-downloads

Downloads the FoodAtlas v3.2.0 data bundle containing food-chemical relationships
with evidence tracking from scientific literature.

Outputs:
    raw_data/Foodatlas/*.tsv (entities, relationships, and other data files)
"""

import sys
import zipfile
from pathlib import Path

import gdown

DEFAULT_OUTPUT_DIR = Path("raw_data/Foodatlas")

# FoodAtlas v3.2.0 Google Drive file ID (from the download page)
# URL: https://drive.google.com/file/d/1mjf4uNlFVdFnduzR6RTB3rQEtx8ukfiP
FOODATLAS_FILE_ID = "1mjf4uNlFVdFnduzR6RTB3rQEtx8ukfiP"
FOODATLAS_VERSION = "3.2.0"


def download_from_google_drive(file_id: str, output_path: Path) -> Path:
    """Download a file from Google Drive using gdown.

    Handles large file virus scan confirmation automatically.

    Args:
        file_id: Google Drive file ID.
        output_path: Path to save the downloaded file.

    Returns:
        Path to the downloaded file.
    """
    if output_path.exists():
        print(f"Archive already exists: {output_path}")
        return output_path

    print(f"Downloading FoodAtlas v{FOODATLAS_VERSION} from Google Drive...")
    print(f"File ID: {file_id}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use gdown for reliable Google Drive downloads
    url = f"https://drive.google.com/uc?id={file_id}"
    gdown.download(url, str(output_path), quiet=False, fuzzy=True)

    if not output_path.exists():
        raise RuntimeError(
            f"Download failed. Please download manually from:\n"
            f"https://drive.google.com/file/d/{file_id}/view\n"
            f"and save to: {output_path}"
        )

    print(f"Downloaded: {output_path} ({output_path.stat().st_size:,} bytes)")
    return output_path


def extract_archive(archive_path: Path, output_dir: Path) -> Path:
    """Extract the FoodAtlas ZIP archive.

    Args:
        archive_path: Path to the ZIP archive.
        output_dir: Directory to extract to.

    Returns:
        Path to the output directory containing extracted files.
    """
    # Check if already extracted by looking for key files
    expected_files = ["entities.tsv", "relationships.tsv"]
    all_exist = all((output_dir / f).exists() for f in expected_files)

    if all_exist:
        print(f"Archive already extracted: {output_dir}")
        return output_dir

    print(f"Extracting archive to {output_dir}...")

    with zipfile.ZipFile(archive_path, "r") as zf:
        # List contents
        print(f"Archive contains {len(zf.namelist())} files:")
        for name in zf.namelist()[:10]:  # Show first 10
            print(f"  {name}")
        if len(zf.namelist()) > 10:
            print(f"  ... and {len(zf.namelist()) - 10} more")

        # Extract all files
        zf.extractall(output_dir)

    print(f"Extracted to: {output_dir}")
    return output_dir


def fetch_foodatlas(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Download and extract FoodAtlas data.

    Args:
        output_dir: Directory to write output files.

    Returns:
        Path to the output directory containing TSV files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching FoodAtlas data...")
    print(f"Output directory: {output_dir}\n")

    # Download archive
    archive_path = output_dir / f"foodatlas_v{FOODATLAS_VERSION}.zip"
    download_from_google_drive(FOODATLAS_FILE_ID, archive_path)

    # Extract archive
    extract_archive(archive_path, output_dir)

    # List extracted files (check for nested directory structure)
    tsv_files = list(output_dir.glob("**/*.tsv"))
    csv_files = list(output_dir.glob("**/*.csv"))
    all_files = tsv_files + csv_files

    print(f"\n--- Extracted {len(all_files)} data files ---")

    total_bytes = 0
    for data_file in sorted(all_files):
        size = data_file.stat().st_size
        total_bytes += size
        rel_path = data_file.relative_to(output_dir)
        # Count lines for key files
        if data_file.name in ["entities.tsv", "relationships.tsv"]:
            with open(data_file, encoding="utf-8", errors="ignore") as f:
                line_count = sum(1 for _ in f)
            print(f"  {rel_path}: {line_count:,} lines ({size:,} bytes)")
        else:
            print(f"  {rel_path}: {size:,} bytes")

    print(f"\nTotal size: {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.2f} MB)")

    return output_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    fetch_foodatlas(output)
