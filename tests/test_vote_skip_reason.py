import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest


REPO_PARENT = Path(__file__).resolve().parents[1]
if str(REPO_PARENT) not in sys.path:
    sys.path.insert(0, str(REPO_PARENT))

from database import Database  # noqa: E402


def _iso_now(offset_minutes: int = 0) -> str:
    return (datetime.utcnow() + timedelta(minutes=offset_minutes)).isoformat()


def _add_topic(db: Database, name: str = "Skip Reason Topic") -> int:
    now = _iso_now()
    cur = db.conn.execute(
        """
        INSERT INTO topics (
            name, name_norm, description, category, subcategory,
            post_count, first_seen_at, last_seen_at, is_active, is_promoted, created_source
        ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 1, 1, 'test')
        """,
        (
            name,
            db._normalize_topic_name(name),
            "desc",
            "tools",
            "testing",
            now,
            now,
        ),
    )
    db.conn.commit()
    return int(cur.lastrowid)


def _add_week(db: Database) -> int:
    starts = _iso_now(offset_minutes=-60)
    ends = _iso_now(offset_minutes=60)
    cur = db.conn.execute(
        """
        INSERT INTO weekly_cycles (week_key, starts_at, ends_at, timezone, status)
        VALUES (?, ?, ?, ?, 'open')
        """,
        (f"test-week-{starts}", starts, ends, "America/Chicago"),
    )
    db.conn.commit()
    return int(cur.lastrowid)


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    test_db = Database(str(db_path))
    test_db.init_db()
    try:
        yield test_db
    finally:
        test_db.conn.close()


def test_skip_reason_round_trip_on_votes(db: Database):
    topic_id = _add_topic(db, "Covered Topic")

    db.upsert_vote(topic_id, "User2", "skip", skip_reason="already_covered")

    votes = db.get_votes_for_topics([topic_id])
    assert votes[topic_id][0]["vote_type"] == "skip"
    assert votes[topic_id][0]["skip_reason"] == "already_covered"


def test_skip_reason_defaults_and_clears_for_non_skip_votes(db: Database):
    topic_id = _add_topic(db, "Default Skip Topic")

    db.upsert_vote(topic_id, "User2", "skip")
    vote = db.get_votes_for_topics([topic_id])[topic_id][0]
    assert vote["skip_reason"] == "not_good_fit"

    db.upsert_vote(topic_id, "User2", "slide")
    vote_after = db.get_votes_for_topics([topic_id])[topic_id][0]
    assert vote_after["vote_type"] == "slide"
    assert vote_after["skip_reason"] is None

    recent = db.get_recent_activity(limit=5)
    latest_vote = next(item for item in recent if item.get("activity_type") == "vote")
    assert latest_vote["vote_type"] == "slide"
    assert latest_vote["previous_vote_type"] == "skip"
    assert latest_vote["previous_skip_reason"] == "not_good_fit"


def test_weekly_skip_reason_and_clear_event_preserve_reason_context(db: Database):
    topic_id = _add_topic(db, "Weekly Covered Topic")
    week_id = _add_week(db)

    db.upsert_vote(topic_id, "User1", "skip", week_id=week_id, skip_reason="already_covered")
    db.delete_vote(topic_id, "User1", week_id=week_id)

    weekly_row = db.conn.execute(
        "SELECT * FROM topic_week_votes WHERE week_id = ? AND topic_id = ? AND voter_name = ?",
        (week_id, topic_id, "User1"),
    ).fetchone()
    assert weekly_row is None

    clear_event = db.conn.execute(
        """
        SELECT action, vote_type, previous_vote_type, previous_skip_reason
        FROM topic_vote_events
        WHERE topic_id = ? AND voter_name = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (topic_id, "User1"),
    ).fetchone()
    assert clear_event["action"] == "clear"
    assert clear_event["vote_type"] is None
    assert clear_event["previous_vote_type"] == "skip"
    assert clear_event["previous_skip_reason"] == "already_covered"


def test_training_snapshot_persists_skip_reason(db: Database):
    topic_id = _add_topic(db, "Snapshot Covered Topic")

    db.save_vote_snapshot(
        voter_name="Admin",
        topic_id=topic_id,
        vote_type="skip",
        skip_reason="already_covered",
        topic_data={
            "name": "Snapshot Covered Topic",
            "description": "desc",
            "category": "tools",
            "subcategory": "testing",
            "post_count": 0,
        },
        posts_json="[]",
    )

    row = db.conn.execute(
        "SELECT vote_type, skip_reason FROM training_vote_snapshots ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row["vote_type"] == "skip"
    assert row["skip_reason"] == "already_covered"
