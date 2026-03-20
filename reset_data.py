#!/usr/bin/env python3
"""Admin content-data reset for X Feed Intel.

Clears posts/topics/votes/history/training/vector data while preserving
user credentials by default.

Usage:
    python3 -m x_feed_intel.reset_data --dry-run
    python3 -m x_feed_intel.reset_data --yes
    python3 -m x_feed_intel.reset_data --yes --clear-sessions --vacuum
"""
import argparse
import os
import sys


from .database import get_db


def _print_summary(summary: dict):
    mode = "DRY RUN" if summary.get("dry_run") else "RESET COMPLETE"
    print(f"\n=== {mode} ===")
    print(f"DB: {summary.get('db_path')}")
    print(f"Preserve sessions: {'yes' if summary.get('preserve_sessions') else 'no'}")
    print("")
    print("Rows before reset:")
    for table, count in summary.get("tables_before", {}).items():
        print(f"  {table:24s} {count}")

    v_before = summary.get("vector_rows_before")
    if v_before is not None:
        print(f"  {'topic_vectors (vec0)':24s} {v_before}")

    deleted_keys = summary.get("state_deleted_keys") or []
    print("")
    print("State keys to delete:" if summary.get("dry_run") else "State keys deleted:")
    if deleted_keys:
        for key in deleted_keys:
            print(f"  {key}")
    else:
        print("  (none)")

    preserved = ["users", "state.flask_secret_key", "state.taxonomy_version"]
    if summary.get("preserve_sessions"):
        preserved.append("sessions")
    print("")
    print("Preserved:")
    for item in preserved:
        print(f"  {item}")

    vec_err = summary.get("vector_reset_error")
    if vec_err:
        print("")
        print("Note:")
        print(f"  Vector table reset/count may be unavailable on this interpreter: {vec_err}")
        print("  Next fetch can still rebuild/sync vectors after topics are repopulated.")

    if not summary.get("dry_run"):
        print("")
        print(f"VACUUM: {'yes' if summary.get('vacuumed') else 'no'}")
    print("")


def main():
    parser = argparse.ArgumentParser(
        description="Clear X Feed Intel content data while preserving user credentials."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without making changes.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Actually perform the reset (required unless --dry-run).",
    )
    parser.add_argument(
        "--clear-sessions",
        action="store_true",
        help="Also clear login sessions (users/passwords are still preserved).",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run VACUUM after reset to shrink the database file.",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        parser.error("Refusing to reset without --yes (or use --dry-run).")

    db = get_db()
    db.init_db()

    summary = db.reset_content_data(
        preserve_sessions=not args.clear_sessions,
        dry_run=args.dry_run,
        vacuum=args.vacuum,
    )
    _print_summary(summary)


if __name__ == "__main__":
    main()
