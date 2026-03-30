"""Training data exporter — generates datasets from vote snapshots + impressions.

Usage (run from the repo root):
    python training/exporter.py summary
    python training/exporter.py snapshots
    python training/exporter.py impressions
    python training/exporter.py all
"""
import csv
import json
import os
import sys

# Setup path for standalone CLI execution (training/ is a subdirectory)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db
import config


def export_vote_snapshots(output_path: str = None):
    """Export all vote snapshots as JSONL."""
    if output_path is None:
        output_path = str(config.DATA_DIR / "training" / "vote_snapshots.jsonl")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    db = get_db()
    db.init_db()

    cur = db.conn.execute(
        "SELECT * FROM training_vote_snapshots ORDER BY snapshot_at"
    )

    count = 0
    with open(output_path, "w") as f:
        for row in cur.fetchall():
            record = dict(row)
            # Parse posts_json back to list for cleaner output
            if record.get("posts_json"):
                try:
                    record["posts"] = json.loads(record["posts_json"])
                except json.JSONDecodeError:
                    record["posts"] = []
            record.pop("posts_json", None)
            f.write(json.dumps(record) + "\n")
            count += 1

    print(f"Exported {count} vote snapshots to {output_path}")


def export_impressions(output_path: str = None):
    """Export impression logs as CSV."""
    if output_path is None:
        output_path = str(config.DATA_DIR / "training" / "impressions.csv")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    db = get_db()
    db.init_db()

    cur = db.conn.execute(
        "SELECT voter_name, topic_id, shown_at FROM training_impressions ORDER BY shown_at"
    )

    count = 0
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["voter_name", "topic_id", "shown_at"])
        for row in cur.fetchall():
            writer.writerow([row["voter_name"], row["topic_id"], row["shown_at"]])
            count += 1

    print(f"Exported {count} impressions to {output_path}")


def export_training_summary():
    """Print a summary of available training data."""
    db = get_db()
    db.init_db()

    snapshots = db.conn.execute(
        "SELECT COUNT(*) FROM training_vote_snapshots"
    ).fetchone()[0]
    impressions = db.conn.execute(
        "SELECT COUNT(*) FROM training_impressions"
    ).fetchone()[0]
    archived = db.conn.execute(
        "SELECT COUNT(*) FROM archived_posts"
    ).fetchone()[0]

    print("=== Training Data Summary ===")
    print(f"Vote snapshots:   {snapshots}")
    print(f"Impressions:      {impressions}")
    print(f"Archived posts:   {archived}")

    snap_voters = db.conn.execute(
        "SELECT voter_name, vote_type, COUNT(*) as c "
        "FROM training_vote_snapshots GROUP BY voter_name, vote_type "
        "ORDER BY voter_name, vote_type"
    ).fetchall()

    if snap_voters:
        print("\nSnapshots by voter/type:")
        for row in snap_voters:
            print(f"  {row['voter_name']}: {row['vote_type']} x{row['c']}")

    skip_reasons = db.conn.execute(
        "SELECT COALESCE(skip_reason, 'legacy_unspecified') as skip_reason, COUNT(*) as c "
        "FROM training_vote_snapshots WHERE vote_type = 'skip' "
        "GROUP BY COALESCE(skip_reason, 'legacy_unspecified') "
        "ORDER BY c DESC, skip_reason"
    ).fetchall()
    if skip_reasons:
        print("\nSkip snapshots by reason:")
        for row in skip_reasons:
            print(f"  {row['skip_reason']}: x{row['c']}")

    imp_voters = db.conn.execute(
        "SELECT voter_name, COUNT(DISTINCT topic_id) as topics, COUNT(*) as rows "
        "FROM training_impressions GROUP BY voter_name"
    ).fetchall()

    if imp_voters:
        print("\nImpressions by voter:")
        for row in imp_voters:
            print(f"  {row['voter_name']}: {row['topics']} unique topics, {row['rows']} total rows")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export training data")
    parser.add_argument(
        "command",
        choices=["snapshots", "impressions", "summary", "all"],
        default="summary",
        nargs="?",
    )
    args = parser.parse_args()

    if args.command == "snapshots":
        export_vote_snapshots()
    elif args.command == "impressions":
        export_impressions()
    elif args.command == "all":
        export_vote_snapshots()
        export_impressions()
        export_training_summary()
    else:
        export_training_summary()
