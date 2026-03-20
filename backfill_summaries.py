#!/usr/bin/env python3
"""Backfill summary bullets for topics that lack them."""
import argparse
import json
import logging
import sys
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import config
from .database import get_db
from .summary_generator import TopicSummaryGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("x_feed_intel.backfill")

# Thread-local DB connections for parallel mode
_thread_local = threading.local()


def _get_thread_db():
    """Get a per-thread DB connection (schema already initialized on main thread)."""
    if not hasattr(_thread_local, "db"):
        _thread_local.db = get_db()
    return _thread_local.db


def get_topics_needing_backfill(db, limit=None, force=False):
    """Get active topics needing summary backfill.

    Args:
        force: If True, return ALL active topics with posts (for re-generation
               in new 3-tier format), not just those missing summaries.
    """
    if force:
        query = """
            SELECT id, name, description, category, subcategory, post_count,
                   summary_bullets, summary_key_takeaways, summary_lifetime_posts_seen
            FROM topics
            WHERE is_active = 1
              AND post_count > 0
            ORDER BY post_count DESC
        """
    else:
        query = """
            SELECT id, name, description, category, subcategory, post_count,
                   summary_bullets, summary_key_takeaways, summary_lifetime_posts_seen
            FROM topics
            WHERE is_active = 1
              AND post_count > 0
              AND (summary_bullets IS NULL
                   OR summary_bullets = ''
                   OR summary_bullets = '[]'
                   OR summary_bullets = 'null'
                   OR summary_key_takeaways IS NULL
                   OR summary_key_takeaways = ''
                   OR summary_key_takeaways = '[]')
            ORDER BY post_count DESC
        """
    if limit:
        query += f" LIMIT {int(limit)}"
    cur = db.conn.execute(query)
    return [dict(row) for row in cur.fetchall()]


def _process_topic(topic, force, total):
    """Process a single topic (thread-safe with per-thread DB)."""
    db = _get_thread_db()
    generator = TopicSummaryGenerator()
    try:
        summary, usage = generator._generate_summary(db, topic)
        if summary:
            if force:
                description = summary.get("description", topic.get("description", ""))
            else:
                description = topic.get("description") or summary.get("description", "")

            db.update_topic_summary(
                topic_id=topic["id"],
                description=description,
                bullets=summary["bullets"],
                key_takeaways=summary.get("key_takeaways"),
                lifetime_seen=topic["post_count"],
            )

            cost = generator._calculate_cost(usage)
            db.record_api_usage(
                service="anthropic",
                operation="backfill_summary",
                input_tokens=usage["input_tokens"],
                output_tokens=usage["output_tokens"],
                cost_usd=cost,
                model=generator.model,
                batch_size=1,
            )

            kt_count = len(summary.get('key_takeaways', []))
            return ("ok", cost, kt_count, len(summary['bullets']))
        else:
            return ("skip", 0.0, 0, 0)
    except Exception as e:
        return ("error", 0.0, 0, 0, str(e))


def main():
    parser = argparse.ArgumentParser(description="Backfill topic summary bullets")
    parser.add_argument("--dry-run", action="store_true", help="Print topics without calling API")
    parser.add_argument("--force", action="store_true", help="Re-generate ALL topics in 3-tier format")
    parser.add_argument("--limit", type=int, default=None, help="Max topics to process")
    parser.add_argument("--parallel", "-p", type=int, default=1,
                        help="Number of parallel workers (default: 1)")
    args = parser.parse_args()

    db = get_db()
    db.init_db()

    topics = get_topics_needing_backfill(db, limit=args.limit, force=args.force)
    if not topics:
        print("No topics need summary backfill.")
        return

    mode = "FORCE (3-tier regeneration)" if args.force else "standard"
    print(f"Found {len(topics)} topics needing summary backfill [{mode}]", flush=True)

    if args.dry_run:
        for t in topics:
            has_kt = bool(t.get("summary_key_takeaways"))
            print(f"  [{t['id']}] {t['name']} ({t['post_count']} posts) kt={'Y' if has_kt else 'N'}")
        print(f"\nDry run complete. Use without --dry-run to process.")
        return

    workers = max(1, min(args.parallel, 8))
    success = 0
    errors = 0
    total_cost = 0.0
    total = len(topics)

    if workers == 1:
        # Sequential mode (original behavior)
        generator = TopicSummaryGenerator()
        for i, topic in enumerate(topics):
            print(f"[{i+1}/{total}] Processing: {topic['name'][:50]} ({topic['post_count']} posts)...", flush=True)
            try:
                summary, usage = generator._generate_summary(db, topic)
                if summary:
                    if args.force:
                        description = summary.get("description", topic.get("description", ""))
                    else:
                        description = topic.get("description") or summary.get("description", "")

                    db.update_topic_summary(
                        topic_id=topic["id"],
                        description=description,
                        bullets=summary["bullets"],
                        key_takeaways=summary.get("key_takeaways"),
                        lifetime_seen=topic["post_count"],
                    )

                    cost = generator._calculate_cost(usage)
                    total_cost += cost
                    db.record_api_usage(
                        service="anthropic",
                        operation="backfill_summary",
                        input_tokens=usage["input_tokens"],
                        output_tokens=usage["output_tokens"],
                        cost_usd=cost,
                        model=generator.model,
                        batch_size=1,
                    )

                    success += 1
                    kt_count = len(summary.get('key_takeaways', []))
                    print(f"  OK: {kt_count} takeaways, {len(summary['bullets'])} bullets (${cost:.4f})", flush=True)
                else:
                    errors += 1
                    print(f"  SKIP: no summary generated", flush=True)
            except Exception as e:
                errors += 1
                print(f"  ERROR: {e}", flush=True)
    else:
        # Parallel mode
        print(f"Running with {workers} parallel workers", flush=True)
        completed = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_process_topic, topic, args.force, total): topic
                for topic in topics
            }
            for future in as_completed(futures):
                topic = futures[future]
                completed += 1
                result = future.result()
                status = result[0]
                if status == "ok":
                    _, cost, kt_count, bullet_count = result
                    success += 1
                    total_cost += cost
                    print(
                        f"[{completed}/{total}] OK: {topic['name'][:45]} "
                        f"-- {kt_count} takeaways, {bullet_count} bullets (${cost:.4f})",
                        flush=True,
                    )
                elif status == "skip":
                    errors += 1
                    print(f"[{completed}/{total}] SKIP: {topic['name'][:45]}", flush=True)
                else:
                    errors += 1
                    err_msg = result[4] if len(result) > 4 else "unknown"
                    print(f"[{completed}/{total}] ERROR: {topic['name'][:45]} -- {err_msg}", flush=True)

    print(f"\nBackfill complete: {success} updated, {errors} errors, ${total_cost:.4f} total cost", flush=True)


if __name__ == "__main__":
    main()
