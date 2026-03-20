#!/usr/bin/env python3
"""Demote weak legacy impression-promoted topics back to candidate status.

Dry-run by default. Use --apply to persist changes.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime


from . import config  # noqa: E402
from .database import Database  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Demote weak impression-auto-promoted topics back to candidate status."
    )
    parser.add_argument(
        "--db-path",
        default=str(config.DB_PATH),
        help="SQLite database path (default: config.DB_PATH)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (default is dry-run).",
    )
    parser.add_argument(
        "--actor",
        default="system:cleanup",
        help="Actor name recorded in topic edit history when applying.",
    )
    parser.add_argument(
        "--demotion-reason",
        default="cleanup_demote_weak_impression_auto_promo",
        help="Reason string to write when demoting topics.",
    )
    parser.add_argument(
        "--week-engagement-cutoff",
        type=int,
        default=300,
        help="Demote only if week_engagement_score is strictly below this value.",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=50,
        help="How many candidate rows to print in preview (default 50).",
    )
    return parser.parse_args()


def _vote_types_for_topic(votes_by_topic: dict, topic_id: int) -> set[str]:
    rows = votes_by_topic.get(topic_id, [])
    out = set()
    for row in rows:
        vt = str(row.get("vote_type") or "").strip().lower()
        if vt:
            out.add(vt)
    return out


def _load_week_context(db: Database) -> tuple[dict | None, str | None, int | None]:
    cycle = db.get_current_weekly_cycle(ensure=True)
    if not cycle:
        return None, None, None
    since_date = str(cycle.get("starts_at") or cycle.get("starts_local_date") or "")
    if not since_date:
        return cycle, None, None
    week_id = cycle.get("id")
    try:
        week_id = int(week_id) if week_id is not None else None
    except Exception:
        week_id = None
    return cycle, since_date, week_id


def _find_candidates(
    db: Database,
    *,
    since_date: str,
    week_id: int | None,
    week_engagement_cutoff: int,
) -> list[dict]:
    rows = db.get_weekly_topic_pool(since_date)
    if not rows:
        return []
    topic_ids = [int(r["id"]) for r in rows if r.get("id") is not None]
    votes_by_topic = db.get_votes_for_topics(topic_ids, week_id=week_id)
    auto_slide_min_content_score = float(
        getattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_CONTENT_SCORE", 140)
    )
    auto_slide_min_week_posts = int(getattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_WEEK_POSTS", 2))
    auto_slide_min_sources = int(getattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_SOURCES", 2))
    auto_slide_min_week_engagement = int(
        getattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_WEEK_ENGAGEMENT", 300)
    )
    auto_slide_min_reposts = int(getattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_REPOSTS", 15))
    auto_slide_min_agg_impressions = int(
        getattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_AGG_IMPRESSIONS", 1000)
    )

    candidates: list[dict] = []
    for row in rows:
        topic_id = int(row["id"])
        if int(row.get("is_promoted") or 0) != 1:
            continue

        topic = db.get_topic_by_id(topic_id) or {}
        promotion_reason = str(topic.get("promotion_reason") or "")
        if not promotion_reason.startswith("auto:team_impressions"):
            continue

        created_source = str(topic.get("created_source") or "").strip().lower()
        if created_source in {"manual", "user"}:
            continue

        override = str(topic.get("editorial_tier_override") or "").strip().lower()
        if override in {"slide", "bullet"}:
            continue

        vote_types = _vote_types_for_topic(votes_by_topic, topic_id)
        if "slide" in vote_types or "bullet" in vote_types:
            continue

        week_posts = int(row.get("week_post_count") or 0)
        source_count = int(row.get("source_count") or 0)
        week_engagement_score = int(row.get("week_engagement_score") or 0)
        if week_posts != 1:
            continue
        if source_count != 1:
            continue
        if week_engagement_score >= int(week_engagement_cutoff):
            continue

        agg = row.get("agg_metrics") or {}
        weekly_score_content = float(row.get("weekly_score_content") or 0)
        # Preserve strong single-post breakouts. Be more conservative than the
        # auto-slide gate: high reposts or aggregate impressions are enough to
        # skip demotion even if content_score narrowly misses the floor.
        passes_auto_slide_gate = False
        if int(agg.get("reposts") or 0) >= auto_slide_min_reposts:
            passes_auto_slide_gate = True
        elif int(agg.get("impressions") or 0) >= auto_slide_min_agg_impressions:
            passes_auto_slide_gate = True
        if weekly_score_content >= auto_slide_min_content_score:
            if week_posts >= auto_slide_min_week_posts:
                passes_auto_slide_gate = True
            elif source_count >= auto_slide_min_sources:
                passes_auto_slide_gate = True
            elif week_engagement_score >= auto_slide_min_week_engagement:
                passes_auto_slide_gate = True
        if passes_auto_slide_gate:
            continue

        candidates.append(
            {
                "id": topic_id,
                "name": str(row.get("name") or ""),
                "promotion_reason": promotion_reason,
                "created_source": created_source or "legacy",
                "week_posts": week_posts,
                "source_count": source_count,
                "week_engagement_score": week_engagement_score,
                "weekly_score": float(row.get("weekly_score") or 0),
                "weekly_score_content": weekly_score_content,
                "impression_viewers": int(row.get("impression_viewers") or 0),
                "impression_total": int(row.get("impression_total") or 0),
                "agg_reposts": int(agg.get("reposts") or 0),
                "agg_impressions": int(agg.get("impressions") or 0),
                "latest_activity": row.get("latest_activity"),
                "vote_types": sorted(vote_types),
                "editorial_tier_override": override or None,
            }
        )

    candidates.sort(
        key=lambda r: (
            r["week_posts"],
            r["source_count"],
            r["week_engagement_score"],
            r["weekly_score"],
            r["latest_activity"] or "",
            r["id"],
        )
    )
    return candidates


def _print_preview(candidates: list[dict], *, show: int) -> None:
    total = len(candidates)
    if total == 0:
        print("No matching weak impression-auto-promoted topics found.")
        return
    print(f"Found {total} topics matching demotion criteria (showing up to {show}).")
    for rec in candidates[: max(0, int(show))]:
        print(
            f"- id={rec['id']} wp={rec['week_posts']} src={rec['source_count']} "
            f"eng={rec['week_engagement_score']} score={rec['weekly_score']:.1f} "
            f"content={rec['weekly_score_content']:.1f} rep={rec['agg_reposts']} "
            f"imp={rec['agg_impressions']} viewers={rec['impression_viewers']} "
            f"imp_rows={rec['impression_total']} name={rec['name']}"
        )
    if total > show:
        print(f"... {total - show} more")


def main() -> int:
    args = _parse_args()
    db = Database(str(args.db_path))
    db.init_db()

    cycle, since_date, week_id = _load_week_context(db)
    if not since_date:
        print("Unable to resolve current weekly cycle window.", file=sys.stderr)
        return 2

    before_sections = db.get_weekly_prep_sections(
        since_date=since_date,
        slide_target=int(getattr(config, "WEEKLY_PREP_TOPIC_LIMIT", 20)),
        bullet_target=int(getattr(config, "WEEKLY_PREP_BULLET_TARGET", 30)),
        week_id=week_id,
    )

    candidates = _find_candidates(
        db,
        since_date=since_date,
        week_id=week_id,
        week_engagement_cutoff=int(args.week_engagement_cutoff),
    )

    print(
        f"Weekly cycle: id={week_id} starts_at={since_date} "
        f"(apply={'yes' if args.apply else 'no'})"
    )
    _print_preview(candidates, show=int(args.show))

    changed = 0
    failed: list[dict] = []
    if args.apply and candidates:
        for rec in candidates:
            ok = db.set_topic_promoted(
                topic_id=int(rec["id"]),
                promoted=False,
                actor=str(args.actor),
                reason=str(args.demotion_reason),
            )
            if ok:
                changed += 1
            else:
                failed.append({"id": rec["id"], "name": rec["name"], "error": "set_topic_promoted returned False"})

    after_sections = db.get_weekly_prep_sections(
        since_date=since_date,
        slide_target=int(getattr(config, "WEEKLY_PREP_TOPIC_LIMIT", 20)),
        bullet_target=int(getattr(config, "WEEKLY_PREP_BULLET_TARGET", 30)),
        week_id=week_id,
    )

    payload = {
        "ok": True,
        "ran_at_utc": datetime.utcnow().isoformat(),
        "db_path": str(args.db_path),
        "dry_run": (not args.apply),
        "week": {
            "id": week_id,
            "starts_at": since_date,
            "week_key": (cycle or {}).get("week_key"),
            "ends_at": (cycle or {}).get("ends_at"),
        },
        "criteria": {
            "promotion_reason_prefix": "auto:team_impressions",
            "exclude_created_sources": ["manual", "user"],
            "require_no_slide_or_bullet_vote_this_week": True,
            "require_no_slide_or_bullet_override": True,
            "week_posts_eq": 1,
            "source_count_eq": 1,
            "week_engagement_score_lt": int(args.week_engagement_cutoff),
            "preserve_if_passes_current_auto_slide_quality_gate": True,
            "preserve_if_high_reposts_or_agg_impressions": True,
        },
        "matched": len(candidates),
        "changed": changed,
        "failed": failed,
        "before_summary": before_sections.get("summary", {}),
        "after_summary": after_sections.get("summary", {}),
        "changed_topic_ids": [int(r["id"]) for r in candidates[:changed]] if args.apply else [],
    }
    print(json.dumps(payload, separators=(",", ":"), default=str))
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
