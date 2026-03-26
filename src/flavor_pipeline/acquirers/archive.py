"""Archive utilities for compressing and checksumming acquired data.

This module provides utilities for:
- Creating ZIP archives of raw data directories
- Computing and verifying SHA256 checksums
- Managing the archive manifest (archives/manifest.json)
- Extracting archives to restore raw data
"""

import hashlib
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict


class ArchiveEntry(TypedDict):
    """Type definition for an archive entry in the manifest."""

    checksum: str
    size_bytes: int
    created_at: str
    files: list[str]
    uncompressed_size: int


class Manifest(TypedDict):
    """Type definition for the archive manifest."""

    version: str
    updated_at: str
    archives: dict[str, ArchiveEntry]


# Default paths
ARCHIVES_DIR = Path("archives")
MANIFEST_PATH = ARCHIVES_DIR / "manifest.json"


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 checksum of a file.

    Args:
        file_path: Path to the file to checksum.

    Returns:
        SHA256 checksum as a hex string prefixed with 'sha256:'.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return f"sha256:{sha256.hexdigest()}"


def compute_dir_size(directory: Path) -> int:
    """Compute total size of all files in a directory.

    Args:
        directory: Path to the directory.

    Returns:
        Total size in bytes.
    """
    total = 0
    for path in directory.rglob("*"):
        if path.is_file():
            total += path.stat().st_size
    return total


def list_files_in_dir(directory: Path) -> list[str]:
    """List all files in a directory (relative paths).

    Args:
        directory: Path to the directory.

    Returns:
        List of relative file paths as strings.
    """
    files = []
    for path in directory.rglob("*"):
        if path.is_file():
            files.append(str(path.relative_to(directory)))
    return sorted(files)


def create_archive(
    source_dir: Path,
    archive_path: Path,
    compression: int = zipfile.ZIP_DEFLATED,
    compresslevel: int = 9,
) -> tuple[str, int, int, list[str]]:
    """Create a ZIP archive from a source directory.

    Args:
        source_dir: Directory containing files to archive.
        archive_path: Path where the ZIP archive will be created.
        compression: ZIP compression method (default: ZIP_DEFLATED).
        compresslevel: Compression level 0-9 (default: 9 for max compression).

    Returns:
        Tuple of (checksum, archive_size, uncompressed_size, files_list).

    Raises:
        FileNotFoundError: If source_dir does not exist.
        ValueError: If source_dir is empty.
    """
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    files = list_files_in_dir(source_dir)
    if not files:
        raise ValueError(f"Source directory is empty: {source_dir}")

    # Ensure parent directory exists
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    # Compute uncompressed size before archiving
    uncompressed_size = compute_dir_size(source_dir)

    # Create the archive
    with zipfile.ZipFile(
        archive_path, "w", compression=compression, compresslevel=compresslevel
    ) as zf:
        for file_path in files:
            full_path = source_dir / file_path
            zf.write(full_path, file_path)

    # Compute checksum and size
    checksum = compute_sha256(archive_path)
    archive_size = archive_path.stat().st_size

    return checksum, archive_size, uncompressed_size, files


def extract_archive(archive_path: Path, dest_dir: Path) -> list[str]:
    """Extract a ZIP archive to a destination directory.

    Args:
        archive_path: Path to the ZIP archive.
        dest_dir: Directory to extract files to.

    Returns:
        List of extracted file paths (relative to dest_dir).

    Raises:
        FileNotFoundError: If archive does not exist.
        zipfile.BadZipFile: If archive is corrupted.
    """
    if not archive_path.exists():
        raise FileNotFoundError(f"Archive not found: {archive_path}")

    # Ensure destination exists
    dest_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(dest_dir)
        return sorted(zf.namelist())


def verify_archive(archive_path: Path, expected_checksum: str) -> bool:
    """Verify an archive's integrity against an expected checksum.

    Args:
        archive_path: Path to the archive file.
        expected_checksum: Expected SHA256 checksum (with 'sha256:' prefix).

    Returns:
        True if checksum matches, False otherwise.
    """
    if not archive_path.exists():
        return False

    actual_checksum = compute_sha256(archive_path)
    return actual_checksum == expected_checksum


def load_manifest(manifest_path: Path | None = None) -> Manifest:
    """Load the archive manifest from disk.

    Args:
        manifest_path: Path to manifest file. Defaults to archives/manifest.json.

    Returns:
        Manifest dictionary. Returns empty manifest if file doesn't exist.
    """
    if manifest_path is None:
        manifest_path = MANIFEST_PATH

    if not manifest_path.exists():
        return {
            "version": "1.0",
            "updated_at": datetime.now(UTC).isoformat(),
            "archives": {},
        }

    with open(manifest_path) as f:
        return json.load(f)


def save_manifest(manifest: Manifest, manifest_path: Path | None = None) -> None:
    """Save the archive manifest to disk.

    Args:
        manifest: Manifest dictionary to save.
        manifest_path: Path to manifest file. Defaults to archives/manifest.json.
    """
    if manifest_path is None:
        manifest_path = MANIFEST_PATH

    # Ensure directory exists
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    # Update timestamp
    manifest["updated_at"] = datetime.now(UTC).isoformat()

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")  # Trailing newline


def update_manifest_entry(
    acquirer_name: str,
    checksum: str,
    size_bytes: int,
    uncompressed_size: int,
    files: list[str],
    manifest_path: Path | None = None,
) -> None:
    """Update or add an entry in the archive manifest.

    Args:
        acquirer_name: Name of the acquirer (e.g., 'flavordb2').
        checksum: SHA256 checksum of the archive.
        size_bytes: Size of the archive in bytes.
        uncompressed_size: Total uncompressed size of files.
        files: List of files in the archive.
        manifest_path: Path to manifest file. Defaults to archives/manifest.json.
    """
    manifest = load_manifest(manifest_path)
    manifest["archives"][acquirer_name] = {
        "checksum": checksum,
        "size_bytes": size_bytes,
        "created_at": datetime.now(UTC).isoformat(),
        "files": files,
        "uncompressed_size": uncompressed_size,
    }
    save_manifest(manifest, manifest_path)


def get_archive_entry(acquirer_name: str, manifest_path: Path | None = None) -> ArchiveEntry | None:
    """Get the manifest entry for an acquirer.

    Args:
        acquirer_name: Name of the acquirer.
        manifest_path: Path to manifest file. Defaults to archives/manifest.json.

    Returns:
        ArchiveEntry if found, None otherwise.
    """
    manifest = load_manifest(manifest_path)
    return manifest["archives"].get(acquirer_name)


def delete_manifest_entry(acquirer_name: str, manifest_path: Path | None = None) -> bool:
    """Delete an entry from the archive manifest.

    Args:
        acquirer_name: Name of the acquirer.
        manifest_path: Path to manifest file.

    Returns:
        True if entry was deleted, False if it didn't exist.
    """
    manifest = load_manifest(manifest_path)
    if acquirer_name in manifest["archives"]:
        del manifest["archives"][acquirer_name]
        save_manifest(manifest, manifest_path)
        return True
    return False
