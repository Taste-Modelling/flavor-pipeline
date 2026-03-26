"""Base acquirer class for data acquisition with Dagster integration."""

import stat
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass
class AcquisitionMetadata:
    """Metadata about an acquisition run."""

    acquired_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    source_url: str | None = None
    record_count: int | None = None
    file_sizes: dict[str, int] = field(default_factory=dict)
    archive_checksum: str | None = None


class BaseAcquirer(ABC):
    """Abstract base class for data acquirers.

    Subclasses implement the fetch() method to acquire data from external sources.
    The base class provides:
    - Caching/idempotency logic
    - Output directory management
    - Validation framework
    - Metadata generation for Dagster assets
    - Archive management (compression and checksumming)

    Example:
        class FlavorDB2Acquirer(BaseAcquirer):
            name = "flavordb2"
            description = "Fetch FlavorDB2 molecules with PubChem enrichment"

            @property
            def output_files(self) -> list[str]:
                return ["molecules.csv"]

            def fetch(self) -> Path:
                # Acquisition logic here
                ...
    """

    # Override these in subclasses
    name: str
    description: str
    group_name: str = "acquisition"

    def __init__(
        self,
        raw_data_base: Path | str = Path("raw_data"),
        archives_base: Path | str = Path("archives"),
    ):
        self._raw_data_base = Path(raw_data_base)
        self._archives_base = Path(archives_base)

    @property
    def output_dir(self) -> Path:
        """Directory where this acquirer writes data."""
        # Default: raw_data/{name} (lowercase, matching acquirer name)
        return self._raw_data_base / self.name

    @property
    @abstractmethod
    def output_files(self) -> list[str]:
        """List of files this acquirer produces (relative to output_dir).

        Used for cache checking and validation.
        """
        ...

    @abstractmethod
    def fetch(self) -> Path:
        """Fetch data from the external source.

        Returns:
            Path to the output directory containing fetched data.

        Raises:
            AcquisitionError: If fetching fails.
        """
        ...

    @property
    def archive_path(self) -> Path:
        """Path to this acquirer's archive file."""
        return self._archives_base / f"{self.name}.zip"

    def is_cached(self) -> bool:
        """Check if output already exists (idempotency check).

        Returns True if ALL expected output files exist.
        """
        for filename in self.output_files:
            if not (self.output_dir / filename).exists():
                return False
        return True

    def has_valid_archive(self) -> bool:
        """Check if a valid archive exists for this acquirer.

        Returns True if archive exists and matches the manifest checksum.
        """
        from flavor_pipeline.acquirers.archive import (
            get_archive_entry,
            verify_archive,
        )

        if not self.archive_path.exists():
            return False

        entry = get_archive_entry(self.name)
        if entry is None:
            return False

        return verify_archive(self.archive_path, entry["checksum"])

    def restore_from_archive(self) -> Path:
        """Restore raw data from the archive.

        Extracts the archive to the output directory.

        Returns:
            Path to the output directory.

        Raises:
            FileNotFoundError: If archive does not exist.
            ValueError: If archive checksum doesn't match manifest.
        """
        from flavor_pipeline.acquirers.archive import (
            extract_archive,
            get_archive_entry,
            verify_archive,
        )

        if not self.archive_path.exists():
            raise FileNotFoundError(f"Archive not found: {self.archive_path}")

        entry = get_archive_entry(self.name)
        if entry is None:
            raise ValueError(f"No manifest entry for {self.name}")

        if not verify_archive(self.archive_path, entry["checksum"]):
            raise ValueError(f"Archive checksum mismatch for {self.name}")

        extract_archive(self.archive_path, self.output_dir)
        self.make_readonly()
        return self.output_dir

    def make_readonly(self) -> None:
        """Make all files in output_dir read-only.

        This protects raw data from accidental modification after acquisition.
        Files are set to read-only for user/group/other (0o444).
        Directories are set to read+execute (0o555) to allow traversal.
        """
        if not self.output_dir.exists():
            return

        for path in self.output_dir.rglob("*"):
            if path.is_file():
                # Remove write permissions: r--r--r--
                path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            elif path.is_dir():
                # Directories need execute for traversal: r-xr-xr-x
                path.chmod(
                    stat.S_IRUSR | stat.S_IXUSR |
                    stat.S_IRGRP | stat.S_IXGRP |
                    stat.S_IROTH | stat.S_IXOTH
                )

        # Also set the output directory itself
        self.output_dir.chmod(
            stat.S_IRUSR | stat.S_IXUSR |
            stat.S_IRGRP | stat.S_IXGRP |
            stat.S_IROTH | stat.S_IXOTH
        )

    def make_writable(self) -> None:
        """Make all files in output_dir writable again.

        This is useful when re-acquiring data or updating existing files.
        Files are set to read+write for user (0o644).
        Directories are set to read+write+execute for user (0o755).
        """
        if not self.output_dir.exists():
            return

        # First make the directory writable so we can modify contents
        self.output_dir.chmod(
            stat.S_IRWXU |
            stat.S_IRGRP | stat.S_IXGRP |
            stat.S_IROTH | stat.S_IXOTH
        )

        for path in self.output_dir.rglob("*"):
            if path.is_dir():
                # Directories: rwxr-xr-x
                path.chmod(
                    stat.S_IRWXU |
                    stat.S_IRGRP | stat.S_IXGRP |
                    stat.S_IROTH | stat.S_IXOTH
                )
            elif path.is_file():
                # Files: rw-r--r--
                path.chmod(
                    stat.S_IRUSR | stat.S_IWUSR |
                    stat.S_IRGRP |
                    stat.S_IROTH
                )

    def create_archive_from_raw(self) -> tuple[str, int]:
        """Create an archive from the current raw data.

        Creates a ZIP archive and updates the manifest.

        Returns:
            Tuple of (checksum, archive_size_bytes).

        Raises:
            FileNotFoundError: If raw data directory does not exist.
            ValueError: If raw data directory is empty.
        """
        from flavor_pipeline.acquirers.archive import (
            create_archive,
            update_manifest_entry,
        )

        checksum, size_bytes, uncompressed_size, files = create_archive(
            self.output_dir, self.archive_path
        )

        update_manifest_entry(
            acquirer_name=self.name,
            checksum=checksum,
            size_bytes=size_bytes,
            uncompressed_size=uncompressed_size,
            files=files,
        )

        return checksum, size_bytes

    def validate(self) -> list[str]:
        """Validate the fetched data.

        Override in subclasses to add source-specific validation.

        Returns:
            List of validation error messages. Empty if valid.
        """
        errors = []
        for filename in self.output_files:
            filepath = self.output_dir / filename
            if not filepath.exists():
                errors.append(f"Missing output file: {filepath}")
            elif filepath.stat().st_size == 0:
                errors.append(f"Empty output file: {filepath}")
        return errors

    def get_metadata(self) -> AcquisitionMetadata:
        """Get metadata about the current acquisition state."""
        from flavor_pipeline.acquirers.archive import get_archive_entry

        file_sizes = {}
        for filename in self.output_files:
            filepath = self.output_dir / filename
            if filepath.exists():
                file_sizes[filename] = filepath.stat().st_size

        # Get archive checksum if available
        archive_checksum = None
        entry = get_archive_entry(self.name)
        if entry is not None:
            archive_checksum = entry["checksum"]

        return AcquisitionMetadata(
            source_url=getattr(self, "url", None),
            file_sizes=file_sizes,
            archive_checksum=archive_checksum,
        )

    def get_asset_metadata(self) -> dict[str, Any]:
        """Get metadata dict for Dagster asset materialization."""
        meta = self.get_metadata()
        result = {
            "output_dir": str(self.output_dir),
            "files": list(meta.file_sizes.keys()),
            "total_bytes": sum(meta.file_sizes.values()),
            "cached": self.is_cached(),
            "has_archive": self.has_valid_archive(),
        }
        if meta.archive_checksum:
            result["archive_checksum"] = meta.archive_checksum
        return result


class AcquisitionError(Exception):
    """Raised when data acquisition fails."""

    pass
