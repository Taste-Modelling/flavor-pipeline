"""Tests for archive utility functions."""

from pathlib import Path

import pytest

from flavor_pipeline.acquirers.archive import (
    compute_sha256,
    create_archive,
    extract_archive,
    get_archive_entry,
    load_manifest,
    save_manifest,
    update_manifest_entry,
    verify_archive,
)


class TestComputeSha256:
    """Tests for compute_sha256 function."""

    def test_computes_checksum_for_file(self, tmp_path: Path) -> None:
        """Should compute SHA256 checksum for a file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello world")

        checksum = compute_sha256(test_file)

        assert checksum.startswith("sha256:")
        assert len(checksum) == 71  # "sha256:" + 64 hex chars

    def test_same_content_same_checksum(self, tmp_path: Path) -> None:
        """Files with same content should have same checksum."""
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("identical content")
        file2.write_text("identical content")

        assert compute_sha256(file1) == compute_sha256(file2)

    def test_different_content_different_checksum(self, tmp_path: Path) -> None:
        """Files with different content should have different checksums."""
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("content A")
        file2.write_text("content B")

        assert compute_sha256(file1) != compute_sha256(file2)

    def test_raises_for_nonexistent_file(self, tmp_path: Path) -> None:
        """Should raise FileNotFoundError for nonexistent file."""
        with pytest.raises(FileNotFoundError):
            compute_sha256(tmp_path / "nonexistent.txt")


class TestCreateArchive:
    """Tests for create_archive function."""

    def test_creates_zip_archive(self, tmp_path: Path) -> None:
        """Should create a ZIP archive from a directory."""
        # Setup source directory
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "file1.txt").write_text("content 1")
        (source_dir / "file2.txt").write_text("content 2")

        archive_path = tmp_path / "archive.zip"

        checksum, size, uncompressed, files = create_archive(source_dir, archive_path)

        assert archive_path.exists()
        assert checksum.startswith("sha256:")
        assert size > 0
        assert uncompressed > 0
        assert sorted(files) == ["file1.txt", "file2.txt"]

    def test_creates_nested_archive(self, tmp_path: Path) -> None:
        """Should handle nested directories."""
        source_dir = tmp_path / "source"
        subdir = source_dir / "subdir"
        subdir.mkdir(parents=True)
        (source_dir / "root.txt").write_text("root")
        (subdir / "nested.txt").write_text("nested")

        archive_path = tmp_path / "archive.zip"

        _, _, _, files = create_archive(source_dir, archive_path)

        assert sorted(files) == ["root.txt", "subdir/nested.txt"]

    def test_raises_for_nonexistent_source(self, tmp_path: Path) -> None:
        """Should raise FileNotFoundError if source doesn't exist."""
        with pytest.raises(FileNotFoundError):
            create_archive(tmp_path / "nonexistent", tmp_path / "archive.zip")

    def test_raises_for_empty_source(self, tmp_path: Path) -> None:
        """Should raise ValueError if source is empty."""
        source_dir = tmp_path / "empty"
        source_dir.mkdir()

        with pytest.raises(ValueError, match="empty"):
            create_archive(source_dir, tmp_path / "archive.zip")

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        """Should create parent directories for archive path."""
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "file.txt").write_text("content")

        archive_path = tmp_path / "nested" / "path" / "archive.zip"

        create_archive(source_dir, archive_path)

        assert archive_path.exists()


class TestExtractArchive:
    """Tests for extract_archive function."""

    def test_extracts_archive(self, tmp_path: Path) -> None:
        """Should extract archive contents to destination."""
        # Create source and archive
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "file.txt").write_text("content")

        archive_path = tmp_path / "archive.zip"
        create_archive(source_dir, archive_path)

        # Extract to new location
        dest_dir = tmp_path / "dest"
        files = extract_archive(archive_path, dest_dir)

        assert (dest_dir / "file.txt").exists()
        assert (dest_dir / "file.txt").read_text() == "content"
        assert files == ["file.txt"]

    def test_preserves_nested_structure(self, tmp_path: Path) -> None:
        """Should preserve nested directory structure."""
        source_dir = tmp_path / "source"
        subdir = source_dir / "a" / "b"
        subdir.mkdir(parents=True)
        (subdir / "deep.txt").write_text("deep content")

        archive_path = tmp_path / "archive.zip"
        create_archive(source_dir, archive_path)

        dest_dir = tmp_path / "dest"
        extract_archive(archive_path, dest_dir)

        assert (dest_dir / "a" / "b" / "deep.txt").exists()

    def test_raises_for_nonexistent_archive(self, tmp_path: Path) -> None:
        """Should raise FileNotFoundError for nonexistent archive."""
        with pytest.raises(FileNotFoundError):
            extract_archive(tmp_path / "nonexistent.zip", tmp_path / "dest")


class TestVerifyArchive:
    """Tests for verify_archive function."""

    def test_returns_true_for_valid_checksum(self, tmp_path: Path) -> None:
        """Should return True when checksum matches."""
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "file.txt").write_text("content")

        archive_path = tmp_path / "archive.zip"
        checksum, _, _, _ = create_archive(source_dir, archive_path)

        assert verify_archive(archive_path, checksum) is True

    def test_returns_false_for_invalid_checksum(self, tmp_path: Path) -> None:
        """Should return False when checksum doesn't match."""
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "file.txt").write_text("content")

        archive_path = tmp_path / "archive.zip"
        create_archive(source_dir, archive_path)

        assert verify_archive(archive_path, "sha256:invalid") is False

    def test_returns_false_for_nonexistent_archive(self, tmp_path: Path) -> None:
        """Should return False for nonexistent archive."""
        assert verify_archive(tmp_path / "nonexistent.zip", "sha256:any") is False


class TestManifest:
    """Tests for manifest functions."""

    def test_load_manifest_returns_empty_if_not_exists(self, tmp_path: Path) -> None:
        """Should return empty manifest if file doesn't exist."""
        manifest = load_manifest(tmp_path / "manifest.json")

        assert manifest["version"] == "1.0"
        assert manifest["archives"] == {}

    def test_save_and_load_manifest(self, tmp_path: Path) -> None:
        """Should save and load manifest correctly."""
        manifest_path = tmp_path / "manifest.json"

        manifest = {
            "version": "1.0",
            "updated_at": "2025-01-01T00:00:00+00:00",
            "archives": {
                "test": {
                    "checksum": "sha256:abc123",
                    "size_bytes": 1000,
                    "created_at": "2025-01-01T00:00:00+00:00",
                    "files": ["file.txt"],
                    "uncompressed_size": 2000,
                }
            },
        }

        save_manifest(manifest, manifest_path)  # type: ignore[arg-type]
        loaded = load_manifest(manifest_path)

        assert loaded["archives"]["test"]["checksum"] == "sha256:abc123"

    def test_update_manifest_entry(self, tmp_path: Path) -> None:
        """Should add/update entry in manifest."""
        manifest_path = tmp_path / "manifest.json"

        update_manifest_entry(
            acquirer_name="test_acquirer",
            checksum="sha256:abc123",
            size_bytes=1000,
            uncompressed_size=2000,
            files=["file1.txt", "file2.txt"],
            manifest_path=manifest_path,
        )

        manifest = load_manifest(manifest_path)
        entry = manifest["archives"]["test_acquirer"]

        assert entry["checksum"] == "sha256:abc123"
        assert entry["size_bytes"] == 1000
        assert entry["files"] == ["file1.txt", "file2.txt"]

    def test_get_archive_entry(self, tmp_path: Path) -> None:
        """Should retrieve entry from manifest."""
        manifest_path = tmp_path / "manifest.json"

        update_manifest_entry(
            acquirer_name="test",
            checksum="sha256:abc",
            size_bytes=100,
            uncompressed_size=200,
            files=["f.txt"],
            manifest_path=manifest_path,
        )

        entry = get_archive_entry("test", manifest_path)
        assert entry is not None
        assert entry["checksum"] == "sha256:abc"

    def test_get_archive_entry_returns_none_if_not_found(self, tmp_path: Path) -> None:
        """Should return None for nonexistent entry."""
        manifest_path = tmp_path / "manifest.json"
        save_manifest({"version": "1.0", "updated_at": "", "archives": {}}, manifest_path)

        entry = get_archive_entry("nonexistent", manifest_path)
        assert entry is None


class TestArchiveRoundTrip:
    """Integration tests for create/extract round-trip."""

    def test_full_round_trip(self, tmp_path: Path) -> None:
        """Data should survive create -> extract round-trip."""
        # Create source with various file types
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "text.txt").write_text("hello world")
        (source_dir / "data.csv").write_text("a,b,c\n1,2,3\n")

        subdir = source_dir / "nested"
        subdir.mkdir()
        (subdir / "deep.json").write_text('{"key": "value"}')

        # Create archive
        archive_path = tmp_path / "archive.zip"
        checksum, _, _, _ = create_archive(source_dir, archive_path)

        # Extract to new location
        dest_dir = tmp_path / "dest"
        extract_archive(archive_path, dest_dir)

        # Verify contents
        assert (dest_dir / "text.txt").read_text() == "hello world"
        assert (dest_dir / "data.csv").read_text() == "a,b,c\n1,2,3\n"
        assert (dest_dir / "nested" / "deep.json").read_text() == '{"key": "value"}'

        # Verify checksum still valid
        assert verify_archive(archive_path, checksum)
