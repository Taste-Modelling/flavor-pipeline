"""Tests for BaseAcquirer class."""

import stat
from pathlib import Path

import pytest

from flavor_pipeline.acquirers.base import AcquisitionError, AcquisitionMetadata, BaseAcquirer


class ConcreteAcquirer(BaseAcquirer):
    """Concrete implementation for testing."""

    name = "test_source"
    description = "Test acquirer for unit tests"

    @property
    def output_files(self) -> list[str]:
        return ["data.csv"]

    def fetch(self) -> Path:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "data.csv").write_text("a,b,c\n1,2,3\n")
        return self.output_dir


class TestBaseAcquirerOutputDir:
    """Tests for output_dir property."""

    def test_default_output_dir_uses_lowercase_name(self, tmp_path: Path) -> None:
        """Output dir should use lowercase acquirer name."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)

        assert acquirer.output_dir == tmp_path / "test_source"

    def test_output_dir_respects_raw_data_base(self, tmp_path: Path) -> None:
        """Output dir should be under raw_data_base."""
        custom_base = tmp_path / "custom_raw"
        acquirer = ConcreteAcquirer(raw_data_base=custom_base)

        assert acquirer.output_dir == custom_base / "test_source"


class TestBaseAcquirerIsCached:
    """Tests for is_cached method."""

    def test_returns_false_when_files_missing(self, tmp_path: Path) -> None:
        """Should return False when expected files don't exist."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)

        assert acquirer.is_cached() is False

    def test_returns_true_when_all_files_exist(self, tmp_path: Path) -> None:
        """Should return True when all expected files exist."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)
        acquirer.output_dir.mkdir(parents=True)
        (acquirer.output_dir / "data.csv").write_text("content")

        assert acquirer.is_cached() is True


class TestBaseAcquirerMakeReadonly:
    """Tests for make_readonly method."""

    def test_makes_files_readonly(self, tmp_path: Path) -> None:
        """Should remove write permissions from files."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)
        acquirer.fetch()

        acquirer.make_readonly()

        file_path = acquirer.output_dir / "data.csv"
        mode = file_path.stat().st_mode
        # Check no write permissions
        assert not (mode & stat.S_IWUSR)
        assert not (mode & stat.S_IWGRP)
        assert not (mode & stat.S_IWOTH)

    def test_makes_directories_traversable(self, tmp_path: Path) -> None:
        """Directories should still be readable and executable."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)
        acquirer.fetch()

        acquirer.make_readonly()

        mode = acquirer.output_dir.stat().st_mode
        # Check read and execute permissions exist
        assert mode & stat.S_IRUSR
        assert mode & stat.S_IXUSR

    def test_handles_nonexistent_directory(self, tmp_path: Path) -> None:
        """Should not raise if directory doesn't exist."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)

        # Should not raise
        acquirer.make_readonly()


class TestBaseAcquirerMakeWritable:
    """Tests for make_writable method."""

    def test_restores_write_permissions(self, tmp_path: Path) -> None:
        """Should restore write permissions to files."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)
        acquirer.fetch()
        acquirer.make_readonly()

        acquirer.make_writable()

        file_path = acquirer.output_dir / "data.csv"
        mode = file_path.stat().st_mode
        # Check user write permission restored
        assert mode & stat.S_IWUSR

    def test_handles_nonexistent_directory(self, tmp_path: Path) -> None:
        """Should not raise if directory doesn't exist."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)

        # Should not raise
        acquirer.make_writable()


class TestBaseAcquirerValidate:
    """Tests for validate method."""

    def test_returns_empty_when_valid(self, tmp_path: Path) -> None:
        """Should return empty list when files exist and are non-empty."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)
        acquirer.fetch()

        errors = acquirer.validate()

        assert errors == []

    def test_returns_error_for_missing_file(self, tmp_path: Path) -> None:
        """Should return error when expected file is missing."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)

        errors = acquirer.validate()

        assert len(errors) == 1
        assert "Missing" in errors[0]

    def test_returns_error_for_empty_file(self, tmp_path: Path) -> None:
        """Should return error when file is empty."""
        acquirer = ConcreteAcquirer(raw_data_base=tmp_path)
        acquirer.output_dir.mkdir(parents=True)
        (acquirer.output_dir / "data.csv").write_text("")

        errors = acquirer.validate()

        assert len(errors) == 1
        assert "Empty" in errors[0]


class TestAcquisitionMetadata:
    """Tests for AcquisitionMetadata dataclass."""

    def test_default_values(self) -> None:
        """Should have sensible defaults."""
        meta = AcquisitionMetadata()

        assert meta.source_url is None
        assert meta.record_count is None
        assert meta.file_sizes == {}
        assert meta.archive_checksum is None
        assert meta.acquired_at is not None


class TestAcquisitionError:
    """Tests for AcquisitionError exception."""

    def test_can_be_raised(self) -> None:
        """Should be raisable with message."""
        with pytest.raises(AcquisitionError, match="Test error"):
            raise AcquisitionError("Test error")
