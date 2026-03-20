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
from metrics_refresher import MetricsRefresher  # noqa: E402


def _iso_hours_ago(hours: float) -> str:
    return (datetime.utcnow() - timedelta(hours=hours)).isoformat()


def _add_topic(db: Database, name: str, *, is_promoted: bool) -> int:
    now = datetime.utcnow().isoformat()
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


def _add_post(
    db: Database,
    topic_id: int,
    *,
    tweet_id: str,
    hours_ago: float,
    metrics: dict | None = None,
    metrics_refreshed_at: str | None = None,
    metrics_unchanged_count: int = 0,
) -> None:
    created_at = _iso_hours_ago(hours_ago)
    metrics_json = json.dumps(metrics) if metrics is not None else None
    db.conn.execute(
        """
        INSERT INTO posts (
            tweet_id, author_id, author_username, text,
            created_at, fetched_at, is_relevant,
            public_metrics_json, metrics_refreshed_at, metrics_unchanged_count
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
        """,
        (
            tweet_id,
            f"author-{tweet_id}",
            f"user-{tweet_id}",
            f"post {tweet_id}",
            created_at,
            created_at,
            metrics_json,
            metrics_refreshed_at,
            metrics_unchanged_count,
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


class FakeTimelineClient:
    requested_ids: list[str] = []

    def __init__(self):
        self.last_request_stats = {"metrics_refresh_api_requests": 0}

    def fetch_tweets_batch(self, tweet_ids: list[str]) -> dict:
        type(self).requested_ids = list(tweet_ids)
        self.last_request_stats["metrics_refresh_api_requests"] = (len(tweet_ids) + 99) // 100
        return {
            tid: {"like_count": 999, "retweet_count": 10, "reply_count": 1, "quote_count": 0}
            for tid in tweet_ids
        }


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    test_db = Database(str(db_path))
    test_db.init_db()
    try:
        yield test_db
    finally:
        test_db.conn.close()


def test_metrics_refresh_records_request_and_post_cost(db: Database, monkeypatch):
    topic_id = _add_topic(db, "Promoted Topic", is_promoted=True)
    _add_post(
        db,
        topic_id,
        tweet_id="cost-1",
        hours_ago=1,
        metrics={"like_count": 1, "retweet_count": 0},
    )
    db.conn.commit()

    monkeypatch.setattr(config, "METRICS_REFRESH_MAX_API_REQUESTS", 3, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_TARGET_POSTS_PER_CYCLE", 200, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_CANDIDATE_SCAN_LIMIT", 500, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_LT12H", 140, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_12_24H", 40, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_24_48H", 20, raising=False)
    monkeypatch.setattr(config, "X_API_TWEET_LOOKUP_COST_PER_SUCCESS_REQUEST_USD", 0.2, raising=False)
    monkeypatch.setattr(config, "X_API_TWEET_LOOKUP_COST_PER_POST_USD", 0.005, raising=False)
    monkeypatch.setattr("metrics_refresher.XTimelineClient", FakeTimelineClient)

    refresher = MetricsRefresher()
    stats = refresher.refresh_metrics(db, active_topic_ids=set())

    assert stats["selected"] == 1
    assert stats["refreshed"] == 1
    assert stats["api_requests"] == 1

    row = db.conn.execute(
        """
        SELECT cost_usd, batch_size
        FROM api_usage
        WHERE operation = 'metrics_refresh'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    assert pytest.approx(row["cost_usd"], rel=1e-6) == 0.205
    assert int(row["batch_size"]) == 1


def test_metrics_refresh_selection_respects_cap_quotas_and_24_48_policy(db: Database, monkeypatch):
    promoted_tid = _add_topic(db, "Promoted", is_promoted=True)
    active_candidate_tid = _add_topic(db, "Active Candidate", is_promoted=False)
    passive_candidate_tid = _add_topic(db, "Passive Candidate", is_promoted=False)

    for i in range(40):
        _add_post(
            db,
            promoted_tid,
            tweet_id=f"p12-{i}",
            hours_ago=2,
            metrics={"like_count": i},
        )
    for i in range(20):
        _add_post(
            db,
            promoted_tid,
            tweet_id=f"p18-{i}",
            hours_ago=18,
            metrics={"like_count": i},
        )
    for i in range(20):
        _add_post(
            db,
            promoted_tid,
            tweet_id=f"p30-{i}",
            hours_ago=30,
            metrics={"like_count": i},
        )

    for i in range(15):
        _add_post(
            db,
            passive_candidate_tid,
            tweet_id=f"c30-{i}",
            hours_ago=30,
            metrics={"like_count": i},
        )
    for i in range(5):
        _add_post(
            db,
            active_candidate_tid,
            tweet_id=f"a30-{i}",
            hours_ago=30,
            metrics={"like_count": i},
        )
    db.conn.commit()

    monkeypatch.setattr(config, "METRICS_REFRESH_MAX_API_REQUESTS", 3, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_TARGET_POSTS_PER_CYCLE", 60, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_CANDIDATE_SCAN_LIMIT", 300, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_LT12H", 30, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_12_24H", 20, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_24_48H", 10, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_24_48H_PROMOTED_ONLY", True, raising=False)
    monkeypatch.setattr("metrics_refresher.XTimelineClient", FakeTimelineClient)

    refresher = MetricsRefresher()
    stats = refresher.refresh_metrics(db, active_topic_ids={active_candidate_tid})

    assert stats["selected"] == 60
    assert stats["selected_by_age"]["lt12h"] == 30
    assert stats["selected_by_age"]["12_24h"] == 20
    assert stats["selected_by_age"]["24_48h"] == 10
    assert stats["selected_active"] >= 1
    assert all(not tid.startswith("c30-") for tid in FakeTimelineClient.requested_ids)
    assert any(tid.startswith("a30-") for tid in FakeTimelineClient.requested_ids)


def test_metrics_refresh_prioritizes_active_then_promoted(db: Database, monkeypatch):
    promoted_tid = _add_topic(db, "Promoted", is_promoted=True)
    active_candidate_tid = _add_topic(db, "Active Candidate", is_promoted=False)
    passive_candidate_tid = _add_topic(db, "Passive Candidate", is_promoted=False)

    _add_post(
        db,
        active_candidate_tid,
        tweet_id="active-1",
        hours_ago=1,
        metrics={"like_count": 1},
    )
    _add_post(
        db,
        promoted_tid,
        tweet_id="promoted-1",
        hours_ago=1.1,
        metrics={"like_count": 1},
    )
    _add_post(
        db,
        promoted_tid,
        tweet_id="promoted-2",
        hours_ago=1.2,
        metrics={"like_count": 1},
    )
    _add_post(
        db,
        passive_candidate_tid,
        tweet_id="candidate-1",
        hours_ago=1.3,
        metrics={"like_count": 1},
    )
    db.conn.commit()

    monkeypatch.setattr(config, "METRICS_REFRESH_MAX_API_REQUESTS", 3, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_TARGET_POSTS_PER_CYCLE", 2, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_CANDIDATE_SCAN_LIMIT", 50, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_LT12H", 2, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_12_24H", 0, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_24_48H", 0, raising=False)
    monkeypatch.setattr("metrics_refresher.XTimelineClient", FakeTimelineClient)

    refresher = MetricsRefresher()
    refresher.refresh_metrics(db, active_topic_ids={active_candidate_tid})

    selected = set(FakeTimelineClient.requested_ids)
    assert "active-1" in selected
    assert "candidate-1" not in selected
    assert "promoted-1" in selected or "promoted-2" in selected


def test_metrics_refresh_quota_spillover_fills_from_next_bucket(db: Database, monkeypatch):
    promoted_tid = _add_topic(db, "Promoted", is_promoted=True)

    for i in range(8):
        _add_post(
            db,
            promoted_tid,
            tweet_id=f"lt12-{i}",
            hours_ago=2,
            metrics={"like_count": i},
        )
    for i in range(20):
        _add_post(
            db,
            promoted_tid,
            tweet_id=f"h18-{i}",
            hours_ago=18,
            metrics={"like_count": i},
        )
    for i in range(20):
        _add_post(
            db,
            promoted_tid,
            tweet_id=f"h30-{i}",
            hours_ago=30,
            metrics={"like_count": i},
        )
    db.conn.commit()

    monkeypatch.setattr(config, "METRICS_REFRESH_MAX_API_REQUESTS", 3, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_TARGET_POSTS_PER_CYCLE", 25, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_CANDIDATE_SCAN_LIMIT", 300, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_LT12H", 10, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_12_24H", 10, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_QUOTA_24_48H", 10, raising=False)
    monkeypatch.setattr(config, "METRICS_REFRESH_24_48H_PROMOTED_ONLY", True, raising=False)
    monkeypatch.setattr("metrics_refresher.XTimelineClient", FakeTimelineClient)

    refresher = MetricsRefresher()
    stats = refresher.refresh_metrics(db, active_topic_ids=set())

    assert stats["selected"] == 25
    assert stats["selected_by_age"]["lt12h"] == 8
    assert stats["selected_by_age"]["12_24h"] == 10
    assert stats["selected_by_age"]["24_48h"] == 7
