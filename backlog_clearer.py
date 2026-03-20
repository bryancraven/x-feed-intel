#!/usr/bin/env python3
"""X Feed Intel — Backlog-only processor (no X API fetch)."""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta

from .logging_config import setup_service_logging

logger = setup_service_logging("x_feed_intel")

from . import config
from .database import get_db
from .classifier import PostClassifier
from .topic_matcher import TopicMatcher


def _count_unclassified(db) -> int:
    row = db.conn.execute(
        "SELECT COUNT(*) AS c FROM posts WHERE classified_at IS NULL"
    ).fetchone()
    return int((row["c"] if row and row["c"] is not None else 0) or 0)


def _count_unlinked_relevant(db) -> int:
    row = db.conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM posts p
        LEFT JOIN post_topics pt ON p.tweet_id = pt.post_id
        WHERE p.is_relevant = 1
          AND p.classified_at IS NOT NULL
          AND pt.post_id IS NULL
        """
    ).fetchone()
    return int((row["c"] if row and row["c"] is not None else 0) or 0)


def _record_classification_usage(db, classify_usage: dict, batch_size: int):
    cls_input = int(classify_usage.get("input_tokens", 0) or 0)
    cls_output = int(classify_usage.get("output_tokens", 0) or 0)
    if not (cls_input or cls_output):
        return
    cls_cost = (
        (cls_input / 1_000_000) * config.HAIKU_INPUT_COST_PER_1M
        + (cls_output / 1_000_000) * config.HAIKU_OUTPUT_COST_PER_1M
    )
    db.record_api_usage(
        service="anthropic",
        operation="classify_backlog",
        input_tokens=cls_input,
        output_tokens=cls_output,
        cost_usd=cls_cost,
        model=config.HAIKU_MODEL,
        batch_size=batch_size,
    )
    logger.info(
        "Backlog classification tokens: %s in / %s out, cost $%.4f",
        cls_input,
        cls_output,
        cls_cost,
    )


def _record_topic_match_usage(db, topic_result: dict):
    tm_usage = topic_result.get("usage", {}) or {}
    tm_input = int(tm_usage.get("input_tokens", 0) or 0)
    tm_output = int(tm_usage.get("output_tokens", 0) or 0)
    if not (tm_input or tm_output):
        return
    tm_cost = (
        (tm_input / 1_000_000) * config.OPUS_INPUT_COST_PER_1M
        + (tm_output / 1_000_000) * config.OPUS_OUTPUT_COST_PER_1M
    )
    db.record_api_usage(
        service="anthropic",
        operation="topic_match_backlog",
        input_tokens=tm_input,
        output_tokens=tm_output,
        cost_usd=tm_cost,
        model=config.OPUS_MODEL,
        batch_size=int(topic_result.get("posts_processed", 0) or 0),
    )
    logger.info(
        "Backlog topic-match tokens: %s in / %s out, cost $%.4f",
        tm_input,
        tm_output,
        tm_cost,
    )


def _log_weekly_sections(db):
    try:
        weekly_roll = db.rollover_weekly_cycle_if_due(actor="system:backlog_clearer")
        cycle = weekly_roll.get("current_cycle") or db.get_current_weekly_cycle()
        since_value = (cycle or {}).get("starts_at")
        if not since_value:
            now_utc = datetime.utcnow()
            today = now_utc.date()
            days_since_thursday = (today.weekday() - 3) % 7
            if days_since_thursday == 0 and now_utc.hour < 20:
                days_since_thursday = 7
            last_thu = today - timedelta(days=days_since_thursday)
            since_value = last_thu.isoformat()
        weekly_sections = db.get_weekly_prep_sections(
            since_date=since_value,
            slide_target=getattr(config, "WEEKLY_PREP_TOPIC_LIMIT", 20),
            bullet_target=getattr(config, "WEEKLY_PREP_BULLET_TARGET", 30),
            week_id=(cycle or {}).get("id"),
        )
        ws = weekly_sections.get("summary", {})
        logger.info(
            "Weekly prep sections (backlog): slide=%s (target=%s), bullet=%s, candidate_radar=%s, candidate_fallback=%s, week_id=%s, rollovers=%s",
            ws.get("slide_count", 0),
            ws.get("slide_target", 0),
            ws.get("bullet_count", 0),
            ws.get("candidate_count", 0),
            ws.get("used_candidate_fallback", False),
            ws.get("week_id"),
            weekly_roll.get("rolled_over", 0),
        )
    except Exception as e:
        logger.warning("Weekly prep section logging failed (non-fatal): %s", e)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Process X Feed Intel backlog without fetching from X API."
    )
    p.add_argument(
        "--max-passes",
        type=int,
        default=12,
        help="Maximum backlog passes to run in one invocation (default: 12).",
    )
    p.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Pause between passes when more work remains (default: 1.0).",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_passes < 1:
        logger.error("Invalid --max-passes=%s (must be >= 1)", args.max_passes)
        return 1

    logger.info("=" * 60)
    logger.info("X Feed Intel — Starting backlog clear cycle")
    logger.info("=" * 60)

    db = None
    total_classified = 0
    total_relevant = 0
    total_topic_posts = 0
    total_topics_created = 0
    total_links_created = 0
    passes_run = 0

    try:
        db = get_db()
        db.init_db()
        config.validate()
        logger.info("Database ready at %s", config.DB_PATH)
        logger.info("Configuration validated")

        start_unclassified = _count_unclassified(db)
        start_unlinked = _count_unlinked_relevant(db)
        logger.info(
            "Backlog before run: %s unclassified, %s relevant-unlinked",
            start_unclassified,
            start_unlinked,
        )

        if start_unclassified == 0 and start_unlinked == 0:
            logger.info("No backlog to clear; exiting cleanly")
            logger.info("=" * 60)
            logger.info("X Feed Intel — Backlog clear cycle complete (no work)")
            logger.info("=" * 60)
            return 0

        for pass_num in range(1, args.max_passes + 1):
            passes_run = pass_num
            unclassified_before = _count_unclassified(db)
            unlinked_before = _count_unlinked_relevant(db)
            if unclassified_before == 0 and unlinked_before == 0:
                logger.info("Backlog fully cleared before pass %s", pass_num)
                break

            logger.info(
                "Backlog pass %s/%s — before: %s unclassified, %s relevant-unlinked",
                pass_num,
                args.max_passes,
                unclassified_before,
                unlinked_before,
            )

            classified_count = 0
            relevant_count = 0

            # Step A: Classify newest unclassified posts (up to DB query cap)
            unclassified = db.get_unclassified_posts()
            logger.info("Backlog pass %s: selected %s unclassified posts for classification", pass_num, len(unclassified))
            if unclassified:
                classifier = PostClassifier()
                results, classify_usage = classifier.classify_all(unclassified)

                for r in results:
                    if r.get("classified", True):
                        db.update_classification(
                            tweet_id=r["tweet_id"],
                            is_relevant=r["is_relevant"],
                            reasoning=r.get("reasoning", ""),
                            category=None,
                            subcategory=None,
                            secondary_categories=None,
                        )

                classified_count = sum(1 for r in results if r.get("classified", True))
                relevant_count = sum(1 for r in results if r.get("is_relevant"))
                total_classified += classified_count
                total_relevant += relevant_count
                logger.info(
                    "Backlog pass %s: classified %s/%s posts, %s relevant",
                    pass_num,
                    classified_count,
                    len(results),
                    relevant_count,
                )
                _record_classification_usage(db, classify_usage, len(unclassified))
            else:
                logger.info("Backlog pass %s: no unclassified posts to classify", pass_num)

            # Step B: Match/link relevant unlinked posts (topic matcher has its own cap)
            matcher = TopicMatcher()
            topic_result = matcher.match_all_unlinked()
            total_topic_posts += int(topic_result.get("posts_processed", 0) or 0)
            total_topics_created += int(topic_result.get("topics_created", 0) or 0)
            total_links_created += int(topic_result.get("links_created", 0) or 0)
            logger.info(
                "Backlog pass %s: topic matching processed=%s, topics_created=%s, links_created=%s",
                pass_num,
                topic_result.get("posts_processed", 0),
                topic_result.get("topics_created", 0),
                topic_result.get("links_created", 0),
            )
            _record_topic_match_usage(db, topic_result)

            # Step C: Backfill categories from matched topics
            try:
                updated, remaining = db.backfill_post_categories_from_topics()
                logger.info(
                    "Backlog pass %s: category backfill updated=%s remaining_uncategorized=%s",
                    pass_num,
                    updated,
                    remaining,
                )
            except Exception as e:
                logger.warning("Backlog pass %s: category backfill failed (non-fatal): %s", pass_num, e)

            # Step D: Topic promotion + pool stats
            try:
                promotion_result = db.promote_eligible_topics()
                promoted_count = int(promotion_result.get("promoted", 0) or 0)
                if promoted_count:
                    logger.info("Backlog pass %s: promoted %s candidate topics", pass_num, promoted_count)
                    for t in promotion_result.get("topics", [])[:10]:
                        logger.info(
                            "  Promoted topic id=%s week_posts=%s sources=%s score=%s reason=%s | %s",
                            t.get("id"),
                            t.get("week_posts"),
                            t.get("week_author_count"),
                            t.get("week_engagement_score"),
                            t.get("promotion_reason"),
                            t.get("name"),
                        )
                else:
                    logger.info("Backlog pass %s: no candidates met promotion thresholds", pass_num)
            except Exception as e:
                logger.warning("Backlog pass %s: topic promotion failed (non-fatal): %s", pass_num, e)

            try:
                ext_stats = db.get_stats_extended()
                logger.info(
                    "Backlog pass %s: topic pool status: %s promoted, %s candidates, %s total active",
                    pass_num,
                    ext_stats.get("promoted_topics", 0),
                    ext_stats.get("candidate_topics", 0),
                    ext_stats.get("total_topics", 0),
                )
            except Exception as e:
                logger.warning("Backlog pass %s: topic pool status logging failed (non-fatal): %s", pass_num, e)

            _log_weekly_sections(db)

            unclassified_after = _count_unclassified(db)
            unlinked_after = _count_unlinked_relevant(db)
            logger.info(
                "Backlog pass %s complete — after: %s unclassified, %s relevant-unlinked",
                pass_num,
                unclassified_after,
                unlinked_after,
            )

            progress = (
                classified_count
                + int(topic_result.get("posts_processed", 0) or 0)
            )
            if progress <= 0:
                logger.warning(
                    "Backlog pass %s made no progress; stopping to avoid a loop",
                    pass_num,
                )
                break

            if unclassified_after == 0 and unlinked_after == 0:
                logger.info("Backlog fully cleared in %s pass(es)", pass_num)
                break

            if pass_num < args.max_passes and args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

        # Archive once per backlog invocation
        try:
            archive_result = db.archive_old_posts(days=config.ARCHIVE_AFTER_DAYS)
            archived_count = int(archive_result.get("archived_posts", 0) or 0)
            if archived_count > 0:
                logger.info(
                    "Backlog run archived %s posts older than %s days",
                    archived_count,
                    config.ARCHIVE_AFTER_DAYS,
                )
            else:
                logger.info("Backlog run: no posts to archive")
        except Exception as e:
            logger.warning("Backlog run archive step failed (non-fatal): %s", e)

        final_unclassified = _count_unclassified(db)
        final_unlinked = _count_unlinked_relevant(db)
        stats = db.get_stats()
        logger.info(
            "Backlog clear summary: passes=%s classified=%s relevant=%s topic_posts=%s topics_created=%s links_created=%s",
            passes_run,
            total_classified,
            total_relevant,
            total_topic_posts,
            total_topics_created,
            total_links_created,
        )
        logger.info(
            "Backlog after run: %s unclassified, %s relevant-unlinked",
            final_unclassified,
            final_unlinked,
        )
        logger.info(
            "DB totals: %s posts, %s relevant, %s unclassified",
            stats.get("total_posts", 0),
            stats.get("relevant_posts", 0),
            stats.get("unclassified_posts", 0),
        )

        if final_unclassified > 0 or final_unlinked > 0:
            logger.info(
                "Backlog remains after %s pass(es); next run will continue from newest-first queues",
                passes_run,
            )

        logger.info("=" * 60)
        logger.info("X Feed Intel — Backlog clear cycle complete")
        logger.info("=" * 60)
        return 0

    except Exception as e:
        logger.error("Fatal error in backlog clear cycle: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
