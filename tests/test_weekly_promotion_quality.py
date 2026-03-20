import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest


REPO_PARENT = Path(__file__).resolve().parents[1]
if str(REPO_PARENT) not in sys.path:
    sys.path.insert(0, str(REPO_PARENT))

import config  # noqa: E402
from database import Database  # noqa: E402


def _iso_now(offset_minutes: int = 0) -> str:
    return (datetime.utcnow() + timedelta(minutes=offset_minutes)).isoformat()


class _FixedFridayDateTime(datetime):
    """Freeze db-side utcnow() to Friday so prior-cycle window is exercised."""

    @classmethod
    def utcnow(cls):
        return cls(2026, 3, 6, 15, 0, 0)


def _add_topic(
    db: Database,
    name: str,
    *,
    is_promoted: bool,
    created_source: str = "legacy",
    promotion_reason: str | None = None,
    editorial_tier_override: str | None = None,
) -> int:
    now = _iso_now()
    cur = db.conn.execute(
        """
        INSERT INTO topics (
            name, name_norm, description, category, subcategory,
            post_count, first_seen_at, last_seen_at, is_active, is_promoted,
            promoted_at, promotion_reason, created_source, editorial_tier_override
        ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 1, ?, ?, ?, ?, ?)
        """,
        (
            name,
            db._normalize_topic_name(name),
            f"desc for {name}",
            "tools",
            "testing",
            now,
            now,
            1 if is_promoted else 0,
            now if is_promoted else None,
            promotion_reason,
            created_source,
            editorial_tier_override,
        ),
    )
    db.conn.commit()
    return int(cur.lastrowid)


def _add_post(
    db: Database,
    topic_id: int,
    *,
    tweet_id: str,
    author_username: str,
    created_at: str,
    metrics: dict | None = None,
) -> None:
    metrics_json = json.dumps(metrics) if metrics is not None else None
    db.conn.execute(
        """
        INSERT INTO posts (
            tweet_id, author_id, author_username, text, created_at, fetched_at, public_metrics_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tweet_id,
            f"author-{author_username}",
            author_username,
            f"post {tweet_id}",
            created_at,
            created_at,
            metrics_json,
        ),
    )
    db.conn.execute(
        "INSERT INTO post_topics (post_id, topic_id) VALUES (?, ?)",
        (tweet_id, topic_id),
    )
    db.conn.execute(
        """
        UPDATE topics
        SET post_count = COALESCE(post_count, 0) + 1,
            last_seen_at = CASE
                WHEN last_seen_at IS NULL OR last_seen_at < ? THEN ?
                ELSE last_seen_at
            END
        WHERE id = ?
        """,
        (created_at, created_at, topic_id),
    )
    db.conn.commit()


def _add_impressions(db: Database, topic_id: int, voter_name: str, count: int) -> None:
    now = _iso_now()
    for _ in range(count):
        db.conn.execute(
            "INSERT INTO training_impressions (voter_name, topic_id, shown_at) VALUES (?, ?, ?)",
            (voter_name, topic_id, now),
        )
    db.conn.commit()


@pytest.fixture
def db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    test_db = Database(str(db_path))
    test_db.init_db()

    monkeypatch.setattr(config, "TOPIC_AUTO_PROMOTION_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "TOPIC_PROMOTE_USE_IMPRESSIONS_FOR_AUTO_PROMOTION", False, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PREP_ALLOW_CANDIDATE_FALLBACK", False, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PREP_AUTO_SUGGESTED_MAX", 10, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_CONTENT_SCORE", 140, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_WEEK_POSTS", 2, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_SOURCES", 2, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_WEEK_ENGAGEMENT", 300, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_REPOSTS", 15, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PREP_AUTO_SUGGEST_MIN_AGG_IMPRESSIONS", 1000, raising=False)
    monkeypatch.setattr(config, "WEEKLY_SCORE_IMPRESSION_UNIQUE_VIEWER_WEIGHT", 20, raising=False)
    monkeypatch.setattr(config, "WEEKLY_SCORE_IMPRESSION_TOTAL_WEIGHT", 0, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_CUTOFF_UTC_OFFSET_HOURS", -7, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_CUTOFF_WEEKDAY", 3, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_CUTOFF_HOUR", 12, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_CUTOFF_MINUTE", 0, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MIN_PRE_POSTS", 3, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MIN_PRE_SOURCES", 2, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MIN_PRE_ENGAGEMENT", 400, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MAX_POST_CUTOFF_POSTS", 1, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MAX_POST_CUTOFF_SOURCES", 1, raising=False)
    monkeypatch.setattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MAX_POST_CUTOFF_ENGAGEMENT", 250, raising=False)

    try:
        yield test_db
    finally:
        test_db.conn.close()


def test_impression_only_signal_does_not_auto_promote(db: Database):
    tid = _add_topic(db, "Impression-only Candidate", is_promoted=False)
    _add_post(
        db,
        tid,
        tweet_id="p1",
        author_username="alice",
        created_at=_iso_now(),
        metrics={"like_count": 1, "retweet_count": 0, "reply_count": 0, "quote_count": 0, "impression_count": 10},
    )
    _add_impressions(db, tid, "User2", 20)
    _add_impressions(db, tid, "User1", 20)

    result = db.promote_eligible_topics()
    row = db.get_topic_by_id(tid)

    assert result["promoted"] == 0
    assert int(row["is_promoted"] or 0) == 0


def test_high_engagement_auto_promotion_requires_multiple_authors(db: Database):
    one_source = _add_topic(db, "High Engagement One Source", is_promoted=False)
    two_sources = _add_topic(db, "High Engagement Two Sources", is_promoted=False)
    now = _iso_now()

    for i in range(3):
        _add_post(
            db,
            one_source,
            tweet_id=f"os-{i}",
            author_username="alice",
            created_at=now,
            metrics={"retweet_count": 150, "like_count": 10, "reply_count": 5, "quote_count": 2, "impression_count": 5000},
        )
    for i, author in enumerate(["bob", "carol", "carol"]):
        _add_post(
            db,
            two_sources,
            tweet_id=f"ts-{i}",
            author_username=author,
            created_at=now,
            metrics={"retweet_count": 150, "like_count": 10, "reply_count": 5, "quote_count": 2, "impression_count": 5000},
        )

    result = db.promote_eligible_topics()
    promoted_ids = {t["id"] for t in result["topics"]}

    assert one_source not in promoted_ids
    assert two_sources in promoted_ids
    assert int(db.get_topic_by_id(one_source)["is_promoted"] or 0) == 0
    assert int(db.get_topic_by_id(two_sources)["is_promoted"] or 0) == 1


def test_weekly_auto_suggestions_are_not_backfilled_below_cap(db: Database):
    since = _iso_now(offset_minutes=-60)
    now = _iso_now()

    # 8 strong promoted topics (pass gate); weak promoted topics should not backfill to 10.
    for i in range(8):
        tid = _add_topic(db, f"Strong Promoted {i}", is_promoted=True, promotion_reason="manual_seed")
        _add_post(
            db,
            tid,
            tweet_id=f"sp-{i}-a",
            author_username=f"a{i}",
            created_at=now,
            metrics={"like_count": 5, "retweet_count": 1, "reply_count": 0, "quote_count": 0, "impression_count": 200},
        )
        _add_post(
            db,
            tid,
            tweet_id=f"sp-{i}-b",
            author_username=f"b{i}",
            created_at=now,
            metrics={"like_count": 4, "retweet_count": 1, "reply_count": 0, "quote_count": 0, "impression_count": 150},
        )

    weak_names = set()
    # Weak promoted topics should fail the quality gate and not backfill.
    for i in range(5):
        weak_name = f"Weak Promoted {i}"
        weak_names.add(weak_name)
        tid = _add_topic(
            db,
            weak_name,
            is_promoted=True,
            promotion_reason="auto:team_impressions>=6_viewers>=2",
        )
        _add_post(
            db,
            tid,
            tweet_id=f"wp-{i}",
            author_username=f"w{i}",
            created_at=now,
            metrics={"like_count": 0, "retweet_count": 0, "reply_count": 0, "quote_count": 0, "impression_count": 0},
        )

    sections = db.get_weekly_prep_sections(since_date=since, slide_target=20, bullet_target=30)
    slide_reasons = [t.get("selection_reason") for t in sections["slide_topics"]]
    slide_names = {t["name"] for t in sections["slide_topics"]}

    assert len(sections["slide_topics"]) == 8
    assert set(slide_reasons) == {"auto_promoted_rank"}
    assert sections["summary"]["auto_slide_suggested_count"] == 8
    assert sections["summary"]["auto_slide_suggested_cap"] == 10
    assert sections["summary"]["auto_slide_quality_reject_count"] >= 5
    assert slide_names.isdisjoint(weak_names)


def test_vote_forced_slide_can_exceed_auto_suggest_cap(db: Database, monkeypatch):
    monkeypatch.setattr(config, "WEEKLY_PREP_AUTO_SUGGESTED_MAX", 2, raising=False)
    since = _iso_now(offset_minutes=-60)
    now = _iso_now()

    forced_ids = []
    for i in range(3):
        tid = _add_topic(db, f"Forced {i}", is_promoted=True, promotion_reason="manual_seed")
        forced_ids.append(tid)
        _add_post(
            db,
            tid,
            tweet_id=f"f-{i}",
            author_username=f"forced{i}",
            created_at=now,
            metrics={"like_count": 2, "retweet_count": 1, "reply_count": 0, "quote_count": 0, "impression_count": 100},
        )
        db.upsert_vote(tid, "User2", "slide")

    for i in range(4):
        tid = _add_topic(db, f"Auto Eligible {i}", is_promoted=True, promotion_reason="manual_seed")
        _add_post(
            db,
            tid,
            tweet_id=f"ae-{i}-a",
            author_username=f"x{i}",
            created_at=now,
            metrics={"like_count": 2, "retweet_count": 1, "reply_count": 0, "quote_count": 0, "impression_count": 100},
        )
        _add_post(
            db,
            tid,
            tweet_id=f"ae-{i}-b",
            author_username=f"y{i}",
            created_at=now,
            metrics={"like_count": 2, "retweet_count": 1, "reply_count": 0, "quote_count": 0, "impression_count": 100},
        )

    sections = db.get_weekly_prep_sections(since_date=since, slide_target=20, bullet_target=30)
    reasons = [t.get("selection_reason") for t in sections["slide_topics"]]

    assert reasons.count("vote_slide") == 3
    assert reasons.count("auto_promoted_rank") == 2
    assert len(sections["slide_topics"]) == 5
    assert sections["summary"]["auto_slide_suggested_count"] == 2


def test_strong_singleton_can_pass_auto_slide_quality_gate(db: Database):
    since = _iso_now(offset_minutes=-60)
    now = _iso_now()

    strong_tid = _add_topic(db, "Strong Singleton", is_promoted=True, promotion_reason="manual_seed")
    _add_post(
        db,
        strong_tid,
        tweet_id="strong-singleton",
        author_username="solo",
        created_at=now,
        metrics={"like_count": 5, "retweet_count": 25, "reply_count": 2, "quote_count": 1, "impression_count": 900},
    )

    weak_tid = _add_topic(db, "Weak Singleton", is_promoted=True, promotion_reason="manual_seed")
    _add_post(
        db,
        weak_tid,
        tweet_id="weak-singleton",
        author_username="solo2",
        created_at=now,
        metrics={"like_count": 0, "retweet_count": 1, "reply_count": 0, "quote_count": 0, "impression_count": 5},
    )

    sections = db.get_weekly_prep_sections(since_date=since, slide_target=20, bullet_target=30)
    slide_ids = {int(t["id"]) for t in sections["slide_topics"]}

    assert strong_tid in slide_ids
    assert weak_tid not in slide_ids


def test_metrics_coverage_fields_report_partial_and_none(db: Database):
    since = _iso_now(offset_minutes=-60)
    now = _iso_now()

    full_tid = _add_topic(db, "Full Metrics", is_promoted=True)
    _add_post(
        db,
        full_tid,
        tweet_id="full-1",
        author_username="full",
        created_at=now,
        metrics={"like_count": 1, "retweet_count": 1, "reply_count": 0, "quote_count": 0, "impression_count": 10},
    )

    partial_tid = _add_topic(db, "Partial Metrics", is_promoted=True)
    _add_post(
        db,
        partial_tid,
        tweet_id="partial-1",
        author_username="p1",
        created_at=now,
        metrics={"like_count": 1, "retweet_count": 0, "reply_count": 0, "quote_count": 0, "impression_count": 50},
    )
    _add_post(
        db,
        partial_tid,
        tweet_id="partial-2",
        author_username="p2",
        created_at=now,
        metrics={"like_count": 2, "retweet_count": 0, "reply_count": 0, "quote_count": 0},
    )
    _add_post(
        db,
        partial_tid,
        tweet_id="partial-3",
        author_username="p3",
        created_at=now,
        metrics=None,
    )

    none_tid = _add_topic(db, "No Metrics", is_promoted=True)
    _add_post(
        db,
        none_tid,
        tweet_id="none-1",
        author_username="n1",
        created_at=now,
        metrics=None,
    )

    rows = db.get_weekly_topic_pool(since)
    by_id = {int(r["id"]): r for r in rows}

    assert by_id[full_tid]["metrics_coverage_state"] == "full"
    assert by_id[full_tid]["metric_posts_total_week"] == 1
    assert by_id[full_tid]["metric_posts_with_json"] == 1
    assert by_id[full_tid]["metric_posts_with_impression_count"] == 1

    assert by_id[partial_tid]["metrics_coverage_state"] == "partial"
    assert by_id[partial_tid]["metric_posts_total_week"] == 3
    assert by_id[partial_tid]["metric_posts_with_json"] == 2
    assert by_id[partial_tid]["metric_posts_with_impression_count"] == 1

    assert by_id[none_tid]["metrics_coverage_state"] == "none"
    assert by_id[none_tid]["metric_posts_total_week"] == 1
    assert by_id[none_tid]["metric_posts_with_json"] == 0
    assert by_id[none_tid]["metric_posts_with_impression_count"] == 0


def _seed_pre_reset_momentum_topic(
    db: Database,
    name: str,
    *,
    is_promoted: bool = False,
    post_cutoff_posts: int = 0,
    editorial_tier_override: str | None = None,
):
    """
    Seed activity for a fixed Friday context.

    - Current cycle starts at 2026-03-06T06:05:00Z.
    - Prior-cycle cutoff is Thursday noon MST = 2026-03-05T19:00:00Z.
    """
    tid = _add_topic(
        db,
        name,
        is_promoted=is_promoted,
        promotion_reason="manual_seed" if is_promoted else None,
        editorial_tier_override=editorial_tier_override,
    )

    pre_window = [
        ("2026-03-03T10:00:00", "alpha"),
        ("2026-03-04T11:00:00", "beta"),
        ("2026-03-05T18:30:00", "gamma"),
    ]
    for i, (created_at, author) in enumerate(pre_window):
        _add_post(
            db,
            tid,
            tweet_id=f"{name.lower().replace(' ', '-')}-pre-{i}",
            author_username=author,
            created_at=created_at,
            metrics={
                "like_count": 10,
                "retweet_count": 40,
                "reply_count": 1,
                "quote_count": 1,
                "impression_count": 800,
            },
        )

    for i in range(post_cutoff_posts):
        _add_post(
            db,
            tid,
            tweet_id=f"{name.lower().replace(' ', '-')}-post-{i}",
            author_username=f"carry{i}",
            created_at=f"2026-03-05T2{i}:10:00",
            metrics={
                "like_count": 2,
                "retweet_count": 2,
                "reply_count": 0,
                "quote_count": 0,
                "impression_count": 120,
            },
        )

    # Current-cycle post to ensure topic appears in weekly pool.
    _add_post(
        db,
        tid,
        tweet_id=f"{name.lower().replace(' ', '-')}-current",
        author_username="today",
        created_at="2026-03-06T08:10:00",
        metrics={
            "like_count": 1,
            "retweet_count": 0,
            "reply_count": 0,
            "quote_count": 0,
            "impression_count": 60,
        },
    )
    return tid


def test_pre_reset_momentum_is_deprioritized_and_applies_on_friday(db: Database, monkeypatch):
    monkeypatch.setattr("database.datetime", _FixedFridayDateTime, raising=False)
    since = "2026-03-06T06:05:00"
    depr_tid = _seed_pre_reset_momentum_topic(
        db,
        "Pre Reset Deprioritized",
        is_promoted=False,
        post_cutoff_posts=0,
    )

    rows = db.get_weekly_topic_pool(since)
    depr_row = next(r for r in rows if int(r["id"]) == depr_tid)
    assert depr_row["pre_reset_momentum_uses_previous_cycle"] is True
    assert depr_row["is_pre_reset_momentum_deprioritized"] is True
    assert int(depr_row["pre_reset_post_count"] or 0) >= 3
    assert int(depr_row["post_reset_post_count"] or 0) <= 1

    sections = db.get_weekly_prep_sections(since_date=since, slide_target=20, bullet_target=30)
    candidate_ids = {int(t["id"]) for t in sections["candidate_topics"]}
    depr = next((t for t in sections["deprioritized_topics"] if int(t["id"]) == depr_tid), None)

    assert depr_tid not in candidate_ids
    assert depr is not None
    assert depr.get("selection_reason") == "pre_reset_momentum_deprioritized"
    assert int(sections["summary"]["pre_reset_deprioritized_count"] or 0) >= 1


def test_post_cutoff_continuation_stays_candidate(db: Database, monkeypatch):
    monkeypatch.setattr("database.datetime", _FixedFridayDateTime, raising=False)
    since = "2026-03-06T06:05:00"
    cont_tid = _seed_pre_reset_momentum_topic(
        db,
        "Pre Reset Continued",
        is_promoted=False,
        post_cutoff_posts=2,
    )

    rows = db.get_weekly_topic_pool(since)
    cont_row = next(r for r in rows if int(r["id"]) == cont_tid)
    assert cont_row["is_pre_reset_momentum_deprioritized"] is False

    sections = db.get_weekly_prep_sections(since_date=since, slide_target=20, bullet_target=30)
    candidate_ids = {int(t["id"]) for t in sections["candidate_topics"]}
    deprioritized_ids = {int(t["id"]) for t in sections["deprioritized_topics"]}

    assert cont_tid in candidate_ids
    assert cont_tid not in deprioritized_ids


def test_vote_forced_slide_overrides_pre_reset_deprioritization(db: Database, monkeypatch):
    monkeypatch.setattr("database.datetime", _FixedFridayDateTime, raising=False)
    since = "2026-03-06T06:05:00"
    tid = _seed_pre_reset_momentum_topic(
        db,
        "Pre Reset Vote Override",
        is_promoted=False,
        post_cutoff_posts=0,
    )
    db.upsert_vote(tid, "User2", "slide")

    sections = db.get_weekly_prep_sections(since_date=since, slide_target=20, bullet_target=30)
    slide_topic = next((t for t in sections["slide_topics"] if int(t["id"]) == tid), None)
    deprioritized_ids = {int(t["id"]) for t in sections["deprioritized_topics"]}

    assert slide_topic is not None
    assert slide_topic.get("selection_reason") == "vote_slide"
    assert tid not in deprioritized_ids


def test_manual_override_slide_overrides_pre_reset_deprioritization(db: Database, monkeypatch):
    monkeypatch.setattr("database.datetime", _FixedFridayDateTime, raising=False)
    since = "2026-03-06T06:05:00"
    tid = _seed_pre_reset_momentum_topic(
        db,
        "Pre Reset Manual Override",
        is_promoted=False,
        post_cutoff_posts=0,
        editorial_tier_override="slide",
    )

    sections = db.get_weekly_prep_sections(since_date=since, slide_target=20, bullet_target=30)
    slide_topic = next((t for t in sections["slide_topics"] if int(t["id"]) == tid), None)
    deprioritized_ids = {int(t["id"]) for t in sections["deprioritized_topics"]}

    assert slide_topic is not None
    assert slide_topic.get("selection_reason") == "override_slide"
    assert tid not in deprioritized_ids
