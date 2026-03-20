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


def _add_topic(
    db: Database,
    name: str,
    *,
    description: str,
    category: str = "tools",
    subcategory: str = "testing",
    post_count: int = 0,
    is_promoted: bool = True,
    offset_minutes: int = 0,
) -> int:
    stamp = _iso_now(offset_minutes)
    cur = db.conn.execute(
        """
        INSERT INTO topics (
            name, name_norm, description, category, subcategory,
            post_count, first_seen_at, last_seen_at, is_active, is_promoted, created_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 'test')
        """,
        (
            name,
            db._normalize_topic_name(name),
            description,
            category,
            subcategory,
            post_count,
            stamp,
            stamp,
            1 if is_promoted else 0,
        ),
    )
    db.conn.commit()
    return int(cur.lastrowid)


@pytest.fixture
def db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    test_db = Database(str(db_path))
    test_db.init_db()

    monkeypatch.setattr(config, "TOPIC_SEARCH_SEMANTIC_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "TOPIC_SEARCH_HYBRID_ALPHA", 0.65, raising=False)
    monkeypatch.setattr(config, "TOPIC_SEARCH_SEMANTIC_K_DEFAULT", 40, raising=False)
    monkeypatch.setattr(config, "TOPIC_SEARCH_SEMANTIC_K_MAX", 120, raising=False)
    monkeypatch.setattr(config, "TOPIC_SEARCH_SEMANTIC_MIN_QUERY_LEN", 2, raising=False)

    try:
        yield test_db
    finally:
        test_db.conn.close()


def test_search_topics_wrapper_is_lexical_only(db: Database, monkeypatch):
    gpu_tid = _add_topic(
        db,
        "GPU Roadmap",
        description="chip launch timeline",
        post_count=12,
        is_promoted=True,
    )
    _add_topic(
        db,
        "Hiring Trends",
        description="team growth",
        post_count=8,
        is_promoted=False,
    )

    def _should_not_be_called(_self, _query, _k):
        raise AssertionError("semantic path should not run for search_topics()")

    monkeypatch.setattr(
        Database,
        "_search_topics_semantic_distances",
        _should_not_be_called,
        raising=True,
    )

    rows = db.search_topics("gpu", limit=5)

    assert rows
    assert int(rows[0]["id"]) == gpu_tid


def test_hybrid_returns_semantic_candidates_when_lexical_misses(db: Database, monkeypatch):
    semantic_tid = _add_topic(
        db,
        "Accelerator Hardware",
        description="Data center GPUs and inference chips",
        post_count=5,
        is_promoted=False,
    )
    _add_topic(
        db,
        "CPU Pricing",
        description="processor discount updates",
        post_count=9,
        is_promoted=True,
    )

    def _fake_semantic(_self, _query, _k):
        return {semantic_tid: 0.11}

    monkeypatch.setattr(Database, "_search_topics_semantic_distances", _fake_semantic, raising=True)

    rows = db.search_topics_hybrid("graphics processors", limit=5, semantic=True)

    assert rows
    assert int(rows[0]["id"]) == semantic_tid


def test_hybrid_preserves_exact_lexical_priority(db: Database, monkeypatch):
    exact_tid = _add_topic(
        db,
        "Nvidia Earnings",
        description="Quarterly report and guidance",
        post_count=20,
        is_promoted=True,
        offset_minutes=-10,
    )
    semantic_tid = _add_topic(
        db,
        "GPU Demand Trends",
        description="Hyperscaler accelerator demand",
        post_count=15,
        is_promoted=True,
        offset_minutes=-5,
    )

    def _fake_semantic(_self, _query, _k):
        return {
            semantic_tid: 0.01,
            exact_tid: 0.20,
        }

    monkeypatch.setattr(Database, "_search_topics_semantic_distances", _fake_semantic, raising=True)

    rows = db.search_topics_hybrid("Nvidia Earnings", limit=5, semantic=True, alpha=0.65)

    assert rows
    assert int(rows[0]["id"]) == exact_tid


def test_hybrid_falls_back_to_lexical_when_semantic_errors(db: Database, monkeypatch):
    lexical_tid = _add_topic(
        db,
        "AI Regulation",
        description="policy and compliance",
        post_count=11,
        is_promoted=True,
    )

    def _raise_semantic(_self, _query, _k):
        raise RuntimeError("vec unavailable")

    monkeypatch.setattr(Database, "_search_topics_semantic_distances", _raise_semantic, raising=True)

    rows = db.search_topics_hybrid("AI Regulation", limit=5, semantic=True)

    assert rows
    assert int(rows[0]["id"]) == lexical_tid


def test_hybrid_respects_semantic_disable_flag(db: Database, monkeypatch):
    semantic_tid = _add_topic(
        db,
        "Vision Models",
        description="multimodal perception",
        post_count=4,
        is_promoted=False,
    )

    def _fake_semantic(_self, _query, _k):
        return {semantic_tid: 0.1}

    monkeypatch.setattr(Database, "_search_topics_semantic_distances", _fake_semantic, raising=True)

    rows = db.search_topics_hybrid("image understanding", limit=5, semantic=False)

    assert rows == []
