"""CLI commands for managing data archives.

Usage:
    python -m flavor_pipeline.cli.archive_commands create [acquirers...]
    python -m flavor_pipeline.cli.archive_commands verify [acquirers...]
    python -m flavor_pipeline.cli.archive_commands restore [acquirers...]
    python -m flavor_pipeline.cli.archive_commands list
"""

import argparse
import sys

from flavor_pipeline.acquirers import ACQUIRER_CLASSES, get_acquirers
from flavor_pipeline.acquirers.archive import (
    get_archive_entry,
    load_manifest,
    verify_archive,
)


def format_size(size_bytes: int) -> str:
    """Format byte size as human-readable string."""
    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def get_acquirer_names(args_acquirers: list[str] | None) -> list[str]:
    """Get list of acquirer names, defaulting to all if none specified."""
    if args_acquirers:
        # Validate acquirer names
        invalid = set(args_acquirers) - set(ACQUIRER_CLASSES.keys())
        if invalid:
            print(f"Error: Unknown acquirers: {', '.join(invalid)}", file=sys.stderr)
            print(f"Valid acquirers: {', '.join(ACQUIRER_CLASSES.keys())}", file=sys.stderr)
            sys.exit(1)
        return args_acquirers
    return list(ACQUIRER_CLASSES.keys())


def cmd_create(args: argparse.Namespace) -> None:
    """Create archives from raw data."""
    acquirer_names = get_acquirer_names(args.acquirers)
    acquirers = get_acquirers()

    created = 0
    skipped = 0
    errors = 0

    for name in acquirer_names:
        acquirer = acquirers[name]
        print(f"Processing {name}...", end=" ")

        if not acquirer.is_cached():
            print("SKIPPED (no raw data)")
            skipped += 1
            continue

        try:
            checksum, size_bytes = acquirer.create_archive_from_raw()
            print(f"OK ({format_size(size_bytes)}, {checksum[:20]}...)")
            created += 1
        except Exception as e:
            print(f"ERROR: {e}")
            errors += 1

    print(f"\nSummary: {created} created, {skipped} skipped, {errors} errors")


def cmd_verify(args: argparse.Namespace) -> None:
    """Verify archive checksums."""
    acquirer_names = get_acquirer_names(args.acquirers)
    acquirers = get_acquirers()

    valid = 0
    invalid = 0
    missing = 0

    for name in acquirer_names:
        acquirer = acquirers[name]
        print(f"Verifying {name}...", end=" ")

        entry = get_archive_entry(name)
        if entry is None:
            print("MISSING (not in manifest)")
            missing += 1
            continue

        if not acquirer.archive_path.exists():
            print("MISSING (archive file not found)")
            missing += 1
            continue

        if verify_archive(acquirer.archive_path, entry["checksum"]):
            print(f"VALID ({format_size(entry['size_bytes'])})")
            valid += 1
        else:
            print("INVALID (checksum mismatch)")
            invalid += 1

    print(f"\nSummary: {valid} valid, {invalid} invalid, {missing} missing")
    if invalid > 0:
        sys.exit(1)


def cmd_restore(args: argparse.Namespace) -> None:
    """Restore raw data from archives."""
    acquirer_names = get_acquirer_names(args.acquirers)
    acquirers = get_acquirers()

    restored = 0
    skipped = 0
    errors = 0

    for name in acquirer_names:
        acquirer = acquirers[name]
        print(f"Restoring {name}...", end=" ")

        # Skip if raw data already exists (unless --force)
        if acquirer.is_cached() and not args.force:
            print("SKIPPED (raw data exists, use --force to overwrite)")
            skipped += 1
            continue

        if not acquirer.has_valid_archive():
            print("SKIPPED (no valid archive)")
            skipped += 1
            continue

        try:
            acquirer.restore_from_archive()
            print("OK")
            restored += 1
        except Exception as e:
            print(f"ERROR: {e}")
            errors += 1

    print(f"\nSummary: {restored} restored, {skipped} skipped, {errors} errors")


def cmd_list(_args: argparse.Namespace) -> None:
    """List archive status for all acquirers."""
    manifest = load_manifest()
    acquirers = get_acquirers()

    print(f"{'Acquirer':<20} {'Archive':<10} {'Size':<12} {'Raw Data':<10} {'Checksum'}")
    print("-" * 80)

    for name in sorted(ACQUIRER_CLASSES.keys()):
        acquirer = acquirers[name]
        entry = manifest["archives"].get(name)

        archive_status = "Yes" if entry else "No"
        size = format_size(entry["size_bytes"]) if entry else "-"
        raw_status = "Yes" if acquirer.is_cached() else "No"
        checksum = entry["checksum"][:20] + "..." if entry else "-"

        print(f"{name:<20} {archive_status:<10} {size:<12} {raw_status:<10} {checksum}")

    # Summary
    total_archives = len(manifest["archives"])
    total_size = sum(e["size_bytes"] for e in manifest["archives"].values())
    print("-" * 80)
    print(f"Total: {total_archives} archives, {format_size(total_size)}")


def main() -> None:
    """Main entry point for archive CLI."""
    parser = argparse.ArgumentParser(
        description="Manage data archives for flavor pipeline",
        prog="flavor-archive",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # create command
    create_parser = subparsers.add_parser("create", help="Create archives from raw data")
    create_parser.add_argument(
        "acquirers",
        nargs="*",
        help="Acquirer names (default: all)",
    )
    create_parser.set_defaults(func=cmd_create)

    # verify command
    verify_parser = subparsers.add_parser("verify", help="Verify archive checksums")
    verify_parser.add_argument(
        "acquirers",
        nargs="*",
        help="Acquirer names (default: all)",
    )
    verify_parser.set_defaults(func=cmd_verify)

    # restore command
    restore_parser = subparsers.add_parser("restore", help="Restore raw data from archives")
    restore_parser.add_argument(
        "acquirers",
        nargs="*",
        help="Acquirer names (default: all)",
    )
    restore_parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Overwrite existing raw data",
    )
    restore_parser.set_defaults(func=cmd_restore)

    # list command
    list_parser = subparsers.add_parser("list", help="List archive status")
    list_parser.set_defaults(func=cmd_list)

    parsed_args = parser.parse_args()
    parsed_args.func(parsed_args)


if __name__ == "__main__":
    main()
