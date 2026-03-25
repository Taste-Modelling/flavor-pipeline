"""
Download WineSensed dataset from Hugging Face.

Source: https://huggingface.co/datasets/Dakhoo/L2T-NeurIPS-2023

WineSensed is a multimodal wine dataset from NeurIPS 2023 containing:
- 350k+ wine vintages with metadata (grapes, region, alcohol, price, rating)
- 824k user reviews with flavor descriptions
- 5k+ pairwise flavor similarity rankings from tasting experiments

License: CC BY-NC-ND 4.0

Outputs:
    raw_data/WineSensed/images_reviews_attributes.csv
    raw_data/WineSensed/napping.csv
    raw_data/WineSensed/participants.csv
"""

import sys
from pathlib import Path

DEFAULT_OUTPUT_DIR = Path("raw_data/WineSensed")

REPO_ID = "Dakhoo/L2T-NeurIPS-2023"

# Files to download from the dataset repository
DATA_FILES = [
    "data/csv/images_reviews_attributes.csv",  # Main wine data (153 MB)
    "data/csv/napping.csv",  # Flavor similarity coordinates
    "data/csv/participants.csv",  # Tasting experiment participants
]


def fetch_winesensed(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Download WineSensed dataset from Hugging Face.

    Downloads CSV files containing wine metadata, reviews, and flavor
    similarity data from tasting experiments.

    Args:
        output_dir: Directory to write output files.

    Returns:
        Path to the output directory containing CSV files.
    """
    from huggingface_hub import hf_hub_download

    output_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching WineSensed dataset from Hugging Face...")
    print(f"Repository: {REPO_ID}")
    print(f"Output directory: {output_dir}\n")

    downloaded_files = []

    for file_path in DATA_FILES:
        filename = Path(file_path).name
        local_path = output_dir / filename

        if local_path.exists():
            print(f"Already exists: {local_path}")
            downloaded_files.append(local_path)
            continue

        print(f"Downloading {filename}...")
        try:
            downloaded = hf_hub_download(
                repo_id=REPO_ID,
                filename=file_path,
                repo_type="dataset",
                local_dir=output_dir,
                local_dir_use_symlinks=False,
            )
            # hf_hub_download preserves directory structure, move to output_dir root
            downloaded_path = Path(downloaded)
            if downloaded_path != local_path:
                downloaded_path.rename(local_path)
                # Clean up empty directories
                try:
                    (output_dir / "data" / "csv").rmdir()
                    (output_dir / "data").rmdir()
                except OSError:
                    pass

            print(f"Saved: {local_path} ({local_path.stat().st_size:,} bytes)")
            downloaded_files.append(local_path)
        except Exception as e:
            print(f"Error downloading {filename}: {e}")

    # Summary
    print("\n--- Downloaded Files ---")
    for f in sorted(output_dir.glob("*.csv")):
        print(f"  {f.name}: {f.stat().st_size:,} bytes")

    return output_dir


if __name__ == "__main__":
    output = DEFAULT_OUTPUT_DIR
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    fetch_winesensed(output)
