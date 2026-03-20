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


def _add_topic(db: Database, name: str, *, is_promoted: bool = False) -> int:
    now = _iso_now()
    cur = db.conn.execute(
        """
        INSERT INTO topics (
            name, name_norm, description, category, subcategory,
            post_count, first_seen_at, last_seen_at, is_active, is_promoted, created_source
        ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 1, ?, 'test')
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
        ),
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


def test_record_external_signal_is_idempotent_by_event_id(db: Database):
    tid = _add_topic(db, "Callback Topic", is_promoted=False)

    ok_first = db.record_external_signal(
        topic_id=tid,
        event_id="evt-123",
        workflow="transcribedeep",
        source_url="https://www.youtube.com/watch?v=abc123xyz99",
        source_type="youtube",
        video_title="Test Video",
        summary_bullets=["One", "Two"],
    )
    ok_second = db.record_external_signal(
        topic_id=tid,
        event_id="evt-123",
        workflow="transcribedeep",
        source_url="https://www.youtube.com/watch?v=abc123xyz99",
        source_type="youtube",
        video_title="Test Video",
        summary_bullets=["One", "Two"],
    )

    assert ok_first is True
    assert ok_second is False


def test_weekly_sections_include_external_only_candidate_topics(db: Database):
    tid = _add_topic(db, "External Signal Topic", is_promoted=False)
    db.update_topic_source_metadata(
        tid,
        source_url="https://www.youtube.com/watch?v=abc123xyz99",
        source_type="youtube",
        transcription_status="completed",
        transcription_workflow="transcribedeep",
        transcription_event_id="evt-weekly-1",
    )
    inserted = db.record_external_signal(
        topic_id=tid,
        event_id="evt-weekly-1",
        workflow="transcribedeep",
        source_url="https://www.youtube.com/watch?v=abc123xyz99",
        source_type="youtube",
        video_title="External Weekly Video",
        summary_bullets=["Signal bullet one", "Signal bullet two"],
        completed_at=_iso_now(-5),
    )
    assert inserted is True

    since = _iso_now(-60)
    sections = db.get_weekly_prep_sections(since_date=since, slide_target=20, bullet_target=30)
    candidate_ids = {int(t["id"]) for t in sections["candidate_topics"]}
    ext_topic = next((t for t in sections["candidate_topics"] if int(t["id"]) == tid), None)

    assert tid in candidate_ids
    assert ext_topic is not None
    assert int(ext_topic.get("week_post_count") or 0) == 0
    assert int(ext_topic.get("external_signal_count") or 0) >= 1
    assert ext_topic.get("selection_reason") == "external_transcription_signal"

