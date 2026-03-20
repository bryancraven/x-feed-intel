"""Engagement metrics refresh — re-fetch public_metrics for recent posts."""
import json
import logging
from datetime import datetime

from . import config
from .x_client import XTimelineClient

logger = logging.getLogger("x_feed_intel")


class MetricsRefresher:
    """Refresh engagement metrics for recent posts using tiered age-based backoff."""

    AGE_BUCKET_LT12H = "lt12h"
    AGE_BUCKET_12_24H = "12_24h"
    AGE_BUCKET_24_48H = "24_48h"
    AGE_BUCKET_OLDER = "older"

    @staticmethod
    def _parse_iso(ts: str | None) -> datetime | None:
        """Parse an ISO-ish timestamp into a naive UTC datetime."""
        if not ts:
            return None
        raw = str(ts).strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt

    @classmethod
    def _age_bucket(cls, age_hours: float) -> str:
        if age_hours < 12:
            return cls.AGE_BUCKET_LT12H
        if age_hours < 24:
            return cls.AGE_BUCKET_12_24H
        if age_hours < 48:
            return cls.AGE_BUCKET_24_48H
        return cls.AGE_BUCKET_OLDER

    @classmethod
    def _priority_key(cls, post: dict) -> tuple:
        """Sort key for selection priority within an age bucket."""
        refreshed_dt = cls._parse_iso(post.get("metrics_refreshed_at"))
        return (
            0 if int(post.get("is_active_topic") or 0) else 1,
            0 if int(post.get("is_promoted_topic") or 0) else 1,
            0 if refreshed_dt is None else 1,
            refreshed_dt or datetime.min,
            post.get("_age_hours", 9999.0),
        )

    def _select_priority_posts(self, eligible: list[dict], target_posts: int) -> tuple[list[dict], dict]:
        """Select a recency-bucketed, priority-ranked subset for refresh."""
        now = datetime.utcnow()
        buckets = {
            self.AGE_BUCKET_LT12H: [],
            self.AGE_BUCKET_12_24H: [],
            self.AGE_BUCKET_24_48H: [],
            self.AGE_BUCKET_OLDER: [],
        }

        for row in eligible:
            created_dt = self._parse_iso(row.get("created_at"))
            if created_dt is None:
                continue
            age_hours = max((now - created_dt).total_seconds() / 3600.0, 0.0)
            bucket = self._age_bucket(age_hours)

            # Scope for now: keep refresh focused on posts <= 48h old.
            if bucket == self.AGE_BUCKET_OLDER:
                continue

            post = dict(row)
            post["_age_hours"] = age_hours
            post["_age_bucket"] = bucket

            if (
                bucket == self.AGE_BUCKET_24_48H
                and getattr(config, "METRICS_REFRESH_24_48H_PROMOTED_ONLY", True)
                and not int(post.get("is_active_topic") or 0)
                and not int(post.get("is_promoted_topic") or 0)
            ):
                continue

            buckets[bucket].append(post)

        for posts in buckets.values():
            posts.sort(key=self._priority_key)

        quotas = {
            self.AGE_BUCKET_LT12H: max(0, int(getattr(config, "METRICS_REFRESH_QUOTA_LT12H", 0))),
            self.AGE_BUCKET_12_24H: max(0, int(getattr(config, "METRICS_REFRESH_QUOTA_12_24H", 0))),
            self.AGE_BUCKET_24_48H: max(0, int(getattr(config, "METRICS_REFRESH_QUOTA_24_48H", 0))),
        }

        selected: list[dict] = []
        selected_ids: set[str] = set()
        selection_order = [
            self.AGE_BUCKET_LT12H,
            self.AGE_BUCKET_12_24H,
            self.AGE_BUCKET_24_48H,
        ]

        def take_from_bucket(bucket: str, cap: int) -> int:
            if cap <= 0:
                return 0
            taken = 0
            for post in buckets[bucket]:
                tid = post.get("tweet_id")
                if not tid or tid in selected_ids:
                    continue
                if len(selected) >= target_posts or taken >= cap:
                    break
                selected.append(post)
                selected_ids.add(tid)
                taken += 1
            return taken

        # First pass: honor per-bucket caps.
        for bucket in selection_order:
            take_from_bucket(bucket, quotas[bucket])

        # Second pass: spillover to most-recent buckets first.
        if len(selected) < target_posts:
            for bucket in selection_order:
                for post in buckets[bucket]:
                    tid = post.get("tweet_id")
                    if not tid or tid in selected_ids:
                        continue
                    if len(selected) >= target_posts:
                        break
                    selected.append(post)
                    selected_ids.add(tid)
                if len(selected) >= target_posts:
                    break

        selected_by_age = {
            self.AGE_BUCKET_LT12H: 0,
            self.AGE_BUCKET_12_24H: 0,
            self.AGE_BUCKET_24_48H: 0,
            self.AGE_BUCKET_OLDER: 0,
        }
        for post in selected:
            selected_by_age[post.get("_age_bucket", self.AGE_BUCKET_OLDER)] += 1

        meta = {
            "candidates_by_age": {k: len(v) for k, v in buckets.items()},
            "selected_by_age": selected_by_age,
            "selected_promoted": sum(1 for p in selected if int(p.get("is_promoted_topic") or 0)),
            "selected_active": sum(1 for p in selected if int(p.get("is_active_topic") or 0)),
        }
        return selected, meta

    def refresh_metrics(self, db, active_topic_ids: set = None) -> dict:
        """Run one metrics refresh cycle.

        Args:
            db: Database instance
            active_topic_ids: Topic IDs that received new posts this cycle
                (these posts skip backoff timers)

        Returns:
            Stats dict with refreshed/changed/stable/api_requests counts.
        """
        stats = {
            "refreshed": 0,
            "changed": 0,
            "stable": 0,
            "skipped": 0,
            "api_requests": 0,
            "candidates_scanned": 0,
            "selected": 0,
            "selected_by_age": {
                self.AGE_BUCKET_LT12H: 0,
                self.AGE_BUCKET_12_24H: 0,
                self.AGE_BUCKET_24_48H: 0,
                self.AGE_BUCKET_OLDER: 0,
            },
            "selected_promoted": 0,
            "selected_active": 0,
        }

        max_posts = max(0, int(config.METRICS_REFRESH_MAX_API_REQUESTS)) * 100
        target_posts = max(
            0,
            min(
                max_posts,
                int(getattr(config, "METRICS_REFRESH_TARGET_POSTS_PER_CYCLE", max_posts)),
            ),
        )
        candidate_scan_limit = max(
            target_posts,
            int(getattr(config, "METRICS_REFRESH_CANDIDATE_SCAN_LIMIT", target_posts)),
        )
        stable_threshold = config.METRICS_REFRESH_STABLE_THRESHOLD

        if target_posts <= 0:
            logger.info("Metrics refresh disabled by target_posts_per_cycle=0")
            return stats

        eligible = db.get_posts_needing_metrics_refresh(
            max_post_age_days=config.METRICS_REFRESH_MAX_POST_AGE_DAYS,
            limit=candidate_scan_limit,
            active_topic_ids=active_topic_ids,
            stable_threshold=stable_threshold,
        )
        stats["candidates_scanned"] = len(eligible)

        if not eligible:
            logger.info("Metrics refresh: no posts eligible")
            return stats

        selected, select_meta = self._select_priority_posts(
            eligible=eligible,
            target_posts=target_posts,
        )
        stats["selected"] = len(selected)
        stats["selected_by_age"] = select_meta["selected_by_age"]
        stats["selected_promoted"] = select_meta["selected_promoted"]
        stats["selected_active"] = select_meta["selected_active"]

        if not selected:
            logger.info(
                "Metrics refresh: %d candidates scanned but 0 selected by priority policy",
                stats["candidates_scanned"],
            )
            return stats

        logger.info(
            "Metrics refresh selection: scanned=%d selected=%d target=%d "
            "selected_by_age=%s candidate_by_age=%s selected_promoted=%d selected_active=%d",
            stats["candidates_scanned"],
            stats["selected"],
            target_posts,
            stats["selected_by_age"],
            select_meta["candidates_by_age"],
            stats["selected_promoted"],
            stats["selected_active"],
        )

        tweet_ids = [p["tweet_id"] for p in selected]

        client = XTimelineClient()
        fresh_metrics = client.fetch_tweets_batch(tweet_ids)
        stats["api_requests"] = client.last_request_stats.get(
            "metrics_refresh_api_requests", 0
        )

        for post in selected:
            tid = post["tweet_id"]
            new_pm = fresh_metrics.get(tid)

            if new_pm is None:
                stats["skipped"] += 1
                continue

            old_pm = None
            try:
                old_pm = json.loads(post["public_metrics_json"]) if post["public_metrics_json"] else None
            except (json.JSONDecodeError, TypeError):
                pass

            changed = (old_pm != new_pm)
            db.update_post_metrics(tid, new_pm, changed=changed)
            stats["refreshed"] += 1

            if changed:
                stats["changed"] += 1
                if not config.METRICS_REFRESH_AFFECTS_RANKING:
                    # Shadow mode: log delta for observation
                    logger.debug(
                        "Metrics delta [shadow] %s: old=%s new=%s",
                        tid, old_pm, new_pm,
                    )
            else:
                uc = (post.get("metrics_unchanged_count") or 0) + 1
                if uc >= stable_threshold:
                    stats["stable"] += 1

        db.conn.commit()

        # Record API usage (request + post components)
        request_cost = (
            stats["api_requests"]
            * getattr(config, "X_API_TWEET_LOOKUP_COST_PER_SUCCESS_REQUEST_USD", 0.0)
        )
        post_cost = (
            stats["refreshed"]
            * getattr(config, "X_API_TWEET_LOOKUP_COST_PER_POST_USD", 0.0)
        )
        x_cost = request_cost + post_cost
        db.record_api_usage(
            service="x_api",
            operation="metrics_refresh",
            input_tokens=0,
            output_tokens=0,
            cost_usd=x_cost,
            model=None,
            batch_size=stats["refreshed"],
        )

        logger.info(
            "Metrics refresh complete: %d refreshed (%d changed, %d stable, %d skipped), "
            "%d API requests, request=$%.4f post=$%.4f total=$%.4f",
            stats["refreshed"], stats["changed"], stats["stable"],
            stats["skipped"], stats["api_requests"], request_cost, post_cost, x_cost,
        )
        return stats
