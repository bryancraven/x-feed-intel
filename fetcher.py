#!/usr/bin/env python3
"""X Feed Intel -- Fetch and classify pipeline (cron entry point)."""
import os
import sys
import traceback
from datetime import datetime, timedelta

from logging_config import setup_service_logging

logger = setup_service_logging("x_feed_intel")

# Now import our modules (after logging is set up)
import config
from database import get_db
from x_client import XTimelineClient
from classifier import PostClassifier
from topic_matcher import TopicMatcher


def main() -> int:
    """Main fetch-classify-store pipeline. Returns 0 on success, 1 on error."""
    logger.info("=" * 60)
    logger.info("X Feed Intel -- Starting fetch cycle")
    logger.info("=" * 60)

    db = None
    fetch_id = None

    try:
        # Step 1: Initialize database
        db = get_db()
        db.init_db()
        logger.info(f"Database ready at {config.DB_PATH}")

        if not config.X_COLLECTION_ENABLED:
            logger.warning("X collection is paused (X_COLLECTION_ENABLED=0); skipping fetch cycle")
            db.record_fetch(
                status="success",
                tweets_fetched=0,
                tweets_new=0,
                tweets_relevant=0,
                error_message="collection paused",
            )
            logger.info("=" * 60)
            logger.info("X Feed Intel -- Fetch cycle skipped (collection paused)")
            logger.info("=" * 60)
            return 0

        # Step 2: Validate config
        config.validate()
        logger.info("Configuration validated")

        # Step 3: Get last since_id
        since_id = db.get_last_since_id()
        logger.info(f"Last since_id: {since_id or '(first run)'}")

        # Step 4: Fetch timeline
        client = XTimelineClient()
        tweets = client.fetch_timeline(since_id=since_id)
        x_req_stats = getattr(client, "last_request_stats", {}) or {}
        x_http_attempts = int(x_req_stats.get("http_requests_attempted", 0) or 0)
        x_http_success = int(x_req_stats.get("http_requests_succeeded", 0) or 0)
        x_pages = int(x_req_stats.get("timeline_pages_fetched", 0) or 0)
        logger.info(f"Fetched {len(tweets)} tweets from timeline")

        # Track variables for maintenance phase
        new_count = 0
        relevant_count = 0
        active_topic_ids = set()
        newest_id = None
        topic_result = {"posts_processed": 0, "topics_created": 0, "links_created": 0}
        promotion_result = {"promoted": 0}

        if not tweets:
            logger.info("No new tweets to process")
            db.record_api_usage(
                service="x_api",
                operation="fetch_timeline",
                input_tokens=0,
                output_tokens=0,
                cost_usd=0.0,
                model=None,
                batch_size=0,
            )
            db.record_api_usage(
                service="x_api",
                operation="fetch_timeline_http_requests",
                input_tokens=0,
                output_tokens=0,
                cost_usd=(
                    x_http_success * config.X_API_TIMELINE_COST_PER_SUCCESS_REQUEST_USD
                ),
                model=None,
                batch_size=x_http_attempts,
            )
            logger.info(
                "X API request usage: %s HTTP attempts, %s successes, %s pages fetched",
                x_http_attempts,
                x_http_success,
                x_pages,
            )
            logger.info(
                "X API estimated cost (timeline): request=$%.4f, posts=$%.4f, total=$%.4f",
                x_http_success * config.X_API_TIMELINE_COST_PER_SUCCESS_REQUEST_USD,
                0.0,
                x_http_success * config.X_API_TIMELINE_COST_PER_SUCCESS_REQUEST_USD,
            )
            db.record_fetch(
                status="success",
                tweets_fetched=0,
                tweets_new=0,
                tweets_relevant=0,
                since_id=since_id,
                pages_fetched=x_pages,
            )
            # Fall through to maintenance phase
        else:
            # Step 5: Store tweets (upsert -- new inserts + metrics refresh for dups)
            new_count, updated_count = db.insert_posts_batch(tweets)
            dup_count = len(tweets) - new_count - updated_count
            logger.info(
                f"Stored {new_count} new tweets ({updated_count} metrics-updated, "
                f"{dup_count} unchanged duplicates)"
            )

            # Step 6: Update since_id to newest tweet
            newest_id = max(tweets, key=lambda t: int(t["id"]))["id"]
            db.set_last_since_id(newest_id)
            logger.info(f"Updated since_id to {newest_id}")

            # Step 7: Classify unclassified posts
            unclassified = db.get_unclassified_posts()
            logger.info(f"Found {len(unclassified)} unclassified posts")

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

                relevant_count = sum(1 for r in results if r.get("is_relevant"))
                classified_count = sum(1 for r in results if r.get("classified", True))
                logger.info(
                    f"Classified {classified_count}/{len(results)} posts, "
                    f"{relevant_count} relevant"
                )

                cls_input = classify_usage.get("input_tokens", 0)
                cls_output = classify_usage.get("output_tokens", 0)
                cls_cost = (
                    (cls_input / 1_000_000) * config.HAIKU_INPUT_COST_PER_1M
                    + (cls_output / 1_000_000) * config.HAIKU_OUTPUT_COST_PER_1M
                )
                db.record_api_usage(
                    service="anthropic",
                    operation="classify",
                    input_tokens=cls_input,
                    output_tokens=cls_output,
                    cost_usd=cls_cost,
                    model=config.HAIKU_MODEL,
                    batch_size=len(unclassified),
                )
                logger.info(
                    f"Classification tokens: {cls_input} in / {cls_output} out, "
                    f"cost ${cls_cost:.4f}"
                )

            # Step 8: Topic matching (assign relevant posts to specific topics)
            matcher = TopicMatcher()
            topic_result = matcher.match_all_unlinked()
            logger.info(
                f"Topic matching: {topic_result['posts_processed']} posts processed, "
                f"{topic_result['topics_created']} new topics, "
                f"{topic_result['links_created']} links created"
            )

            # Step 8b: Backfill post categories from matched topics
            try:
                updated, remaining = db.backfill_post_categories_from_topics()
                logger.info(f"Category backfill: {updated} posts updated, {remaining} still uncategorized")
                if remaining and remaining > 0:
                    logger.warning(f"{remaining} relevant linked posts still lack category (topic-match gap)")
            except Exception as e:
                logger.warning(f"Category backfill step failed (non-fatal): {e}")

            # Step 8c: Promote eligible candidate topics to Weekly Prep
            try:
                promotion_result = db.promote_eligible_topics()
                promoted_count = promotion_result.get("promoted", 0)
                if promoted_count:
                    logger.info(f"Topic promotion: promoted {promoted_count} candidate topics")
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
                    logger.info("Topic promotion: no candidates met thresholds")
            except Exception as e:
                logger.warning(f"Topic promotion step failed (non-fatal): {e}")

            try:
                ext_stats = db.get_stats_extended()
                logger.info(
                    "Topic pool status: %s promoted, %s candidates, %s total active",
                    ext_stats.get("promoted_topics", 0),
                    ext_stats.get("candidate_topics", 0),
                    ext_stats.get("total_topics", 0),
                )
            except Exception as e:
                logger.warning(f"Topic pool status logging failed (non-fatal): {e}")

            try:
                weekly_roll = db.rollover_weekly_cycle_if_due(actor="system:fetcher")
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
                    "Weekly prep sections: slide=%s (target=%s), bullet=%s, candidate_radar=%s, candidate_fallback=%s, week_id=%s, rollovers=%s",
                    ws.get("slide_count", 0),
                    ws.get("slide_target", 0),
                    ws.get("bullet_count", 0),
                    ws.get("candidate_count", 0),
                    ws.get("used_candidate_fallback", False),
                    ws.get("week_id"),
                    weekly_roll.get("rolled_over", 0),
                )
            except Exception as e:
                logger.warning(f"Weekly prep section logging failed (non-fatal): {e}")

            # Record topic matching API usage (Opus 4.6)
            tm_usage = topic_result.get("usage", {})
            tm_input = tm_usage.get("input_tokens", 0)
            tm_output = tm_usage.get("output_tokens", 0)
            if tm_input or tm_output:
                tm_cost = (
                    (tm_input / 1_000_000) * config.OPUS_INPUT_COST_PER_1M
                    + (tm_output / 1_000_000) * config.OPUS_OUTPUT_COST_PER_1M
                )
                db.record_api_usage(
                    service="anthropic",
                    operation="topic_match",
                    input_tokens=tm_input,
                    output_tokens=tm_output,
                    cost_usd=tm_cost,
                    model=config.OPUS_MODEL,
                    batch_size=topic_result.get("posts_processed", 0),
                )
                logger.info(
                    f"Topic matching tokens: {tm_input} in / {tm_output} out, "
                    f"cost ${tm_cost:.4f}"
                )

            # Record X API usage (no token cost, just tracking call volume)
            x_timeline_post_cost = len(tweets) * config.X_API_TIMELINE_COST_PER_POST_USD
            x_timeline_request_cost = (
                x_http_success * config.X_API_TIMELINE_COST_PER_SUCCESS_REQUEST_USD
            )
            db.record_api_usage(
                service="x_api",
                operation="fetch_timeline",
                input_tokens=0,
                output_tokens=0,
                cost_usd=x_timeline_post_cost,
                model=None,
                batch_size=len(tweets),
            )
            db.record_api_usage(
                service="x_api",
                operation="fetch_timeline_http_requests",
                input_tokens=0,
                output_tokens=0,
                cost_usd=x_timeline_request_cost,
                model=None,
                batch_size=x_http_attempts,
            )
            logger.info(
                "X API request usage: %s HTTP attempts, %s successes, %s pages fetched",
                x_http_attempts,
                x_http_success,
                x_pages,
            )
            logger.info(
                "X API estimated cost (timeline): request=$%.4f, posts=$%.4f, total=$%.4f",
                x_timeline_request_cost,
                x_timeline_post_cost,
                x_timeline_request_cost + x_timeline_post_cost,
            )

            # Record fetch history
            db.record_fetch(
                status="success",
                tweets_fetched=len(tweets),
                tweets_new=new_count,
                tweets_relevant=relevant_count,
                since_id=since_id,
                newest_id=newest_id,
                topics_created=topic_result.get("topics_created", 0),
                topics_matched=topic_result.get("links_created", 0),
                topics_promoted=promotion_result.get("promoted", 0),
                pages_fetched=x_pages,
            )

            # Derive active topic IDs from newly inserted posts
            if new_count > 0:
                new_tweet_ids = [t["id"] for t in tweets[:new_count]]
                active_topic_ids = db.get_topic_ids_for_posts(new_tweet_ids)
                logger.info(f"Active topic IDs for metrics refresh: {len(active_topic_ids)} topics")

        # ===================================================
        # Maintenance phase (always runs, even on quiet cycles)
        # ===================================================

        # Step M0: Expire stale pending transcription topics
        if (
            getattr(config, "TRANSCRIPTION_INTEGRATION_ENABLED", True)
            and getattr(config, "TRANSCRIPTION_TIMEOUT_WATCHDOG_ENABLED", True)
        ):
            try:
                timed_out = db.mark_stale_transcription_topics(
                    timeout_minutes=getattr(config, "TRANSCRIPTION_PENDING_TIMEOUT_MINUTES", 45),
                    limit=getattr(config, "TRANSCRIPTION_TIMEOUT_WATCHDOG_SCAN_LIMIT", 200),
                )
                if timed_out:
                    logger.warning(
                        "Transcription watchdog: %s topic(s) moved pending -> failed_timeout",
                        len(timed_out),
                    )
            except Exception as e:
                logger.warning(f"Transcription watchdog step failed (non-fatal): {e}")

        # Step M1: Refresh engagement metrics (BEFORE summaries)
        if getattr(config, "METRICS_REFRESH_ENABLED", False):
            try:
                from metrics_refresher import MetricsRefresher
                refresher = MetricsRefresher()
                metrics_stats = refresher.refresh_metrics(db, active_topic_ids=active_topic_ids)
                if metrics_stats["refreshed"] > 0:
                    logger.info(
                        f"Metrics refresh: {metrics_stats['refreshed']} posts refreshed "
                        f"({metrics_stats['changed']} changed, {metrics_stats['stable']} stable)"
                    )
                    try:
                        post_metrics_promotion_result = db.promote_eligible_topics()
                        post_metrics_promoted_count = post_metrics_promotion_result.get("promoted", 0)
                        if post_metrics_promoted_count:
                            logger.info(
                                "Topic promotion (post-metrics): promoted %s candidate topics",
                                post_metrics_promoted_count,
                            )
                        else:
                            logger.info("Topic promotion (post-metrics): no candidates met thresholds")
                    except Exception as e:
                        logger.warning(f"Topic promotion post-metrics step failed (non-fatal): {e}")

                    try:
                        ext_stats = db.get_stats_extended()
                        logger.info(
                            "Topic pool status (post-metrics): %s promoted, %s candidates, %s total active",
                            ext_stats.get("promoted_topics", 0),
                            ext_stats.get("candidate_topics", 0),
                            ext_stats.get("total_topics", 0),
                        )
                    except Exception as e:
                        logger.warning(f"Topic pool status post-metrics logging failed (non-fatal): {e}")

                    try:
                        weekly_roll = db.rollover_weekly_cycle_if_due(actor="system:fetcher")
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
                            "Weekly prep sections (post-metrics): slide=%s (target=%s), bullet=%s, candidate_radar=%s, candidate_fallback=%s, week_id=%s, rollovers=%s",
                            ws.get("slide_count", 0),
                            ws.get("slide_target", 0),
                            ws.get("bullet_count", 0),
                            ws.get("candidate_count", 0),
                            ws.get("used_candidate_fallback", False),
                            ws.get("week_id"),
                            weekly_roll.get("rolled_over", 0),
                        )
                    except Exception as e:
                        logger.warning(f"Weekly prep section post-metrics logging failed (non-fatal): {e}")
            except Exception as e:
                logger.warning(f"Metrics refresh step failed (non-fatal): {e}")

        # Step M2: Refresh topic summaries for topics with enough new posts
        if getattr(config, "SUMMARY_REFRESH_ENABLED", False):
            try:
                from summary_generator import TopicSummaryGenerator
                summary_gen = TopicSummaryGenerator()
                summary_result = summary_gen.refresh_stale_summaries(db)
                if summary_result["refreshed"] > 0:
                    logger.info(
                        f"Summary refresh: {summary_result['refreshed']} topics updated, "
                        f"{summary_result['errors']} errors"
                    )
            except Exception as e:
                logger.warning(f"Summary refresh step failed (non-fatal): {e}")

        # Step M3: Archive old posts for sustainability
        try:
            archive_result = db.archive_old_posts(days=config.ARCHIVE_AFTER_DAYS)
            archived_count = archive_result.get("archived_posts", 0)
            if archived_count > 0:
                logger.info(f"Archived {archived_count} posts older than {config.ARCHIVE_AFTER_DAYS} days")
            else:
                logger.info("No posts to archive")
        except Exception as e:
            logger.warning(f"Archive step failed (non-fatal): {e}")

        # Step 12: Log summary
        stats = db.get_stats()
        logger.info(
            f"DB totals: {stats.get('total_posts', 0)} posts, "
            f"{stats.get('relevant_posts', 0)} relevant, "
            f"{stats.get('unclassified_posts', 0)} unclassified"
        )

        logger.info("=" * 60)
        logger.info("X Feed Intel -- Fetch cycle complete")
        logger.info("=" * 60)
        return 0

    except Exception as e:
        logger.error(f"Fatal error in fetch cycle: {e}", exc_info=True)
        try:
            if db:
                db.record_fetch(
                    status="error",
                    error_message=f"{type(e).__name__}: {e}",
                )
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
