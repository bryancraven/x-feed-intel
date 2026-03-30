"""SQLite storage for X Feed Intel — schema, CRUD, deduplication."""
import json
import logging
import re
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone, time as dt_time
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo

from werkzeug.security import generate_password_hash

logger = logging.getLogger("x_feed_intel")

import config

# ---------------------------------------------------------------------------
# Singleton connection
# ---------------------------------------------------------------------------
_db: Optional["Database"] = None


def get_db() -> "Database":
    """Return a request-scoped DB in Flask requests, else process-global fallback."""
    try:
        from flask import g, has_request_context
        if has_request_context():
            db = getattr(g, "_xfi_db", None)
            if db is None:
                db = Database(str(config.DB_PATH))
                g._xfi_db = db
            return db
    except Exception:
        # Flask may be unavailable in non-web scripts or import contexts.
        pass

    global _db
    if _db is None:
        _db = Database(str(config.DB_PATH))
    return _db


class Database:
    """SQLite wrapper with WAL mode, schema init, and CRUD helpers."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def init_db(self):
        """Create tables and indexes if they don't exist."""
        cur = self.conn.cursor()

        cur.executescript("""
            CREATE TABLE IF NOT EXISTS posts (
                tweet_id            TEXT PRIMARY KEY,
                author_id           TEXT NOT NULL,
                author_username     TEXT,
                author_name         TEXT,
                text                TEXT NOT NULL,
                full_text           TEXT,
                created_at          TEXT NOT NULL,
                is_relevant         INTEGER,
                relevance_reasoning TEXT,
                category            TEXT,
                subcategories       TEXT,
                public_metrics_json TEXT,
                entities_json       TEXT,
                referenced_tweets_json TEXT,
                fetched_at          TEXT NOT NULL,
                classified_at       TEXT,
                classification_model TEXT,
                raw_json            TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_posts_relevant
                ON posts(is_relevant, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_posts_category
                ON posts(category, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_posts_created
                ON posts(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_posts_classified
                ON posts(classified_at);

            CREATE TABLE IF NOT EXISTS fetch_history (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at          TEXT NOT NULL,
                completed_at        TEXT,
                status              TEXT NOT NULL DEFAULT 'running',
                tweets_fetched      INTEGER DEFAULT 0,
                tweets_new          INTEGER DEFAULT 0,
                tweets_relevant     INTEGER DEFAULT 0,
                since_id            TEXT,
                newest_id           TEXT,
                pages_fetched       INTEGER DEFAULT 0,
                error_message       TEXT
            );

            CREATE TABLE IF NOT EXISTS state (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL,
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS topics (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT NOT NULL UNIQUE,
                name_norm       TEXT,
                description     TEXT,
                category        TEXT,
                subcategory     TEXT,
                post_count      INTEGER DEFAULT 0,
                first_seen_at   TEXT NOT NULL DEFAULT (datetime('now')),
                last_seen_at    TEXT,
                is_active       INTEGER DEFAULT 1,
                is_promoted     INTEGER DEFAULT 1,
                promoted_at     TEXT,
                promotion_reason TEXT,
                created_source  TEXT DEFAULT 'legacy',
                primary_source_url TEXT,
                primary_source_type TEXT,
                transcription_status TEXT DEFAULT 'none',
                transcription_status_updated_at TEXT DEFAULT (datetime('now')),
                transcription_workflow TEXT,
                transcription_event_id TEXT,
                editorial_tier_override TEXT,
                editorial_tier_set_by TEXT,
                editorial_tier_set_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_topics_active
                ON topics(is_active, post_count DESC);
            CREATE INDEX IF NOT EXISTS idx_topics_category
                ON topics(category);

            CREATE TABLE IF NOT EXISTS post_topics (
                post_id     TEXT NOT NULL REFERENCES posts(tweet_id),
                topic_id    INTEGER NOT NULL REFERENCES topics(id),
                PRIMARY KEY (post_id, topic_id)
            );

            CREATE INDEX IF NOT EXISTS idx_post_topics_topic
                ON post_topics(topic_id);

            CREATE TABLE IF NOT EXISTS api_usage (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       TEXT NOT NULL DEFAULT (datetime('now')),
                service         TEXT NOT NULL,
                operation       TEXT NOT NULL,
                input_tokens    INTEGER DEFAULT 0,
                output_tokens   INTEGER DEFAULT 0,
                cost_usd        REAL DEFAULT 0.0,
                model           TEXT,
                batch_size      INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_api_usage_ts
                ON api_usage(timestamp DESC);

            CREATE TABLE IF NOT EXISTS topic_votes (
                topic_id    INTEGER NOT NULL REFERENCES topics(id),
                voter_name  TEXT NOT NULL,
                vote_type   TEXT NOT NULL,
                skip_reason TEXT,
                voted_at    TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (topic_id, voter_name)
            );
            CREATE INDEX IF NOT EXISTS idx_topic_votes_topic
                ON topic_votes(topic_id);

            CREATE TABLE IF NOT EXISTS topic_vote_events (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id            INTEGER NOT NULL REFERENCES topics(id),
                voter_name          TEXT NOT NULL,
                action              TEXT NOT NULL, -- set | clear
                vote_type           TEXT,
                skip_reason         TEXT,
                previous_vote_type  TEXT,
                previous_skip_reason TEXT,
                created_at          TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_topic_vote_events_created
                ON topic_vote_events(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_topic_vote_events_topic
                ON topic_vote_events(topic_id);

            CREATE TABLE IF NOT EXISTS weekly_cycles (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                week_key    TEXT NOT NULL UNIQUE,
                starts_at   TEXT NOT NULL,
                ends_at     TEXT NOT NULL,
                timezone    TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'open',
                closed_at   TEXT,
                created_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_weekly_cycles_status
                ON weekly_cycles(status, starts_at DESC);

            CREATE TABLE IF NOT EXISTS topic_week_votes (
                week_id      INTEGER NOT NULL REFERENCES weekly_cycles(id),
                topic_id     INTEGER NOT NULL REFERENCES topics(id),
                voter_name   TEXT NOT NULL,
                vote_type    TEXT NOT NULL,
                skip_reason  TEXT,
                voted_at     TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (week_id, topic_id, voter_name)
            );
            CREATE INDEX IF NOT EXISTS idx_topic_week_votes_topic
                ON topic_week_votes(topic_id, week_id);
            CREATE INDEX IF NOT EXISTS idx_topic_week_votes_week
                ON topic_week_votes(week_id, topic_id);

            CREATE TABLE IF NOT EXISTS topic_week_outcomes (
                week_id       INTEGER NOT NULL REFERENCES weekly_cycles(id),
                topic_id      INTEGER NOT NULL REFERENCES topics(id),
                outcome       TEXT NOT NULL,
                resolved_by   TEXT,
                resolved_at   TEXT NOT NULL DEFAULT (datetime('now')),
                notes         TEXT,
                PRIMARY KEY (week_id, topic_id)
            );
            CREATE INDEX IF NOT EXISTS idx_topic_week_outcomes_week
                ON topic_week_outcomes(week_id, outcome);
        """)

        # Add new columns for taxonomy overhaul (idempotent)
        for col_stmt in [
            "ALTER TABLE posts ADD COLUMN subcategory TEXT",
            "ALTER TABLE topics ADD COLUMN subcategory TEXT",
            "ALTER TABLE topics ADD COLUMN name_norm TEXT",
            "ALTER TABLE topics ADD COLUMN is_promoted INTEGER DEFAULT 1",
            "ALTER TABLE topics ADD COLUMN promoted_at TEXT",
            "ALTER TABLE topics ADD COLUMN promotion_reason TEXT",
            "ALTER TABLE topics ADD COLUMN created_source TEXT DEFAULT 'legacy'",
            "ALTER TABLE topics ADD COLUMN editorial_tier_override TEXT",
            "ALTER TABLE topics ADD COLUMN editorial_tier_set_by TEXT",
            "ALTER TABLE topics ADD COLUMN editorial_tier_set_at TEXT",
            "ALTER TABLE topics ADD COLUMN last_covered_week_id INTEGER",
            "ALTER TABLE topics ADD COLUMN last_covered_at TEXT",
            "ALTER TABLE topics ADD COLUMN coverage_cooldown_until TEXT",
            "ALTER TABLE topics ADD COLUMN last_covered_total_post_count INTEGER",
            "ALTER TABLE topics ADD COLUMN last_covered_latest_activity TEXT",
            # Summary bullets (evolving topic summaries)
            "ALTER TABLE topics ADD COLUMN summary_bullets TEXT",
            "ALTER TABLE topics ADD COLUMN summary_updated_at TEXT",
            "ALTER TABLE topics ADD COLUMN summary_lifetime_posts_seen INTEGER DEFAULT 0",
            # 3-tier summaries: key takeaways column
            "ALTER TABLE topics ADD COLUMN summary_key_takeaways TEXT",
            # External video/transcription integration metadata
            "ALTER TABLE topics ADD COLUMN primary_source_url TEXT",
            "ALTER TABLE topics ADD COLUMN primary_source_type TEXT",
            "ALTER TABLE topics ADD COLUMN transcription_status TEXT DEFAULT 'none'",
            "ALTER TABLE topics ADD COLUMN transcription_status_updated_at TEXT",
            "ALTER TABLE topics ADD COLUMN transcription_workflow TEXT",
            "ALTER TABLE topics ADD COLUMN transcription_event_id TEXT",
            # Engagement metrics refresh tracking
            "ALTER TABLE posts ADD COLUMN metrics_refreshed_at TEXT",
            "ALTER TABLE posts ADD COLUMN metrics_unchanged_count INTEGER DEFAULT 0",
            "ALTER TABLE posts ADD COLUMN metrics_last_changed_at TEXT",
            "ALTER TABLE topic_votes ADD COLUMN skip_reason TEXT",
            "ALTER TABLE topic_week_votes ADD COLUMN skip_reason TEXT",
            "ALTER TABLE topic_vote_events ADD COLUMN skip_reason TEXT",
            "ALTER TABLE topic_vote_events ADD COLUMN previous_skip_reason TEXT",
            "ALTER TABLE training_vote_snapshots ADD COLUMN skip_reason TEXT",
            # Topic stats per fetch cycle (history tab)
            "ALTER TABLE fetch_history ADD COLUMN topics_created INTEGER DEFAULT 0",
            "ALTER TABLE fetch_history ADD COLUMN topics_matched INTEGER DEFAULT 0",
            "ALTER TABLE fetch_history ADD COLUMN topics_promoted INTEGER DEFAULT 0",
        ]:
            try:
                cur.execute(col_stmt)
            except sqlite3.OperationalError:
                pass  # Column already exists

        cur.executescript("""
            CREATE INDEX IF NOT EXISTS idx_posts_subcategory
                ON posts(subcategory, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_topics_subcategory
                ON topics(subcategory);
            CREATE INDEX IF NOT EXISTS idx_topics_promoted
                ON topics(is_active, is_promoted, last_seen_at DESC);
            CREATE INDEX IF NOT EXISTS idx_topics_editorial_tier
                ON topics(editorial_tier_override, is_active);
            CREATE INDEX IF NOT EXISTS idx_topics_primary_source_url
                ON topics(primary_source_url);
            CREATE INDEX IF NOT EXISTS idx_topics_transcription_status
                ON topics(transcription_status, is_active);
            CREATE INDEX IF NOT EXISTS idx_topics_transcription_pending_updated
                ON topics(transcription_status, transcription_status_updated_at, is_active);

            CREATE INDEX IF NOT EXISTS idx_posts_metrics_refresh
                ON posts(metrics_refreshed_at, metrics_unchanged_count, created_at DESC);

            CREATE TABLE IF NOT EXISTS archived_posts (
                tweet_id            TEXT PRIMARY KEY,
                author_username     TEXT,
                full_text           TEXT,
                created_at          TEXT NOT NULL,
                category            TEXT,
                subcategory         TEXT,
                relevance_reasoning TEXT,
                public_metrics_json TEXT,
                archived_at         TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_archived_posts_created
                ON archived_posts(created_at DESC);

            CREATE TABLE IF NOT EXISTS training_impressions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                voter_name  TEXT NOT NULL,
                topic_id    INTEGER NOT NULL,
                shown_at    TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_training_impressions_voter
                ON training_impressions(voter_name, shown_at DESC);

            CREATE TABLE IF NOT EXISTS training_vote_snapshots (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                voter_name        TEXT NOT NULL,
                topic_id          INTEGER NOT NULL,
                vote_type         TEXT NOT NULL,
                skip_reason       TEXT,
                snapshot_at       TEXT NOT NULL DEFAULT (datetime('now')),
                topic_name        TEXT,
                topic_description TEXT,
                topic_category    TEXT,
                topic_subcategory TEXT,
                topic_post_count  INTEGER,
                posts_json        TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_training_snapshots_voter
                ON training_vote_snapshots(voter_name, snapshot_at DESC);

            -- Authentication tables
            CREATE TABLE IF NOT EXISTS users (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                username     TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                is_admin     INTEGER DEFAULT 0,
                created_at   TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token       TEXT PRIMARY KEY,
                user_id     INTEGER NOT NULL REFERENCES users(id),
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                expires_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_expires
                ON sessions(expires_at);

            -- User-created topics (creation pipeline tracking)
            CREATE TABLE IF NOT EXISTS user_topics (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT NOT NULL,
                description  TEXT,
                category     TEXT,
                subcategory  TEXT,
                created_by   TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                error_message TEXT,
                topic_id     INTEGER REFERENCES topics(id),
                created_at   TEXT NOT NULL DEFAULT (datetime('now')),
                activated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_user_topics_status
                ON user_topics(status);

            -- Topic edit history
            CREATE TABLE IF NOT EXISTS topic_edits (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id  INTEGER NOT NULL REFERENCES topics(id),
                edited_by TEXT NOT NULL,
                field     TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                edited_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_topic_edits_topic
                ON topic_edits(topic_id);

            -- External integration events (e.g., transcription workflow callbacks)
            CREATE TABLE IF NOT EXISTS topic_external_signals (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id             INTEGER NOT NULL REFERENCES topics(id),
                event_id             TEXT NOT NULL UNIQUE,
                workflow             TEXT NOT NULL,
                source_url           TEXT NOT NULL,
                source_type          TEXT,
                video_title          TEXT,
                summary_bullets_json TEXT,
                summary_html         TEXT,
                sender_email         TEXT,
                completed_at         TEXT,
                created_at           TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_topic_external_signals_topic
                ON topic_external_signals(topic_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_topic_external_signals_source
                ON topic_external_signals(source_url, created_at DESC);
        """)

        self.conn.commit()

        # Backfill topic lifecycle columns for existing rows (idempotent).
        cur.execute("""
            UPDATE topics
            SET is_promoted = COALESCE(is_promoted, 1)
            WHERE is_promoted IS NULL
        """)
        cur.execute("""
            UPDATE topics
            SET created_source = COALESCE(created_source, 'legacy')
            WHERE created_source IS NULL OR created_source = ''
        """)
        cur.execute("""
            UPDATE topics
            SET transcription_status = COALESCE(NULLIF(transcription_status, ''), 'none')
            WHERE transcription_status IS NULL OR transcription_status = ''
        """)
        cur.execute("""
            UPDATE topics
            SET transcription_status_updated_at = COALESCE(
                NULLIF(transcription_status_updated_at, ''),
                CASE
                    WHEN lower(COALESCE(transcription_status, 'none')) = 'pending'
                        THEN COALESCE(NULLIF(last_seen_at, ''), NULLIF(first_seen_at, ''))
                    ELSE datetime('now')
                END,
                datetime('now')
            )
            WHERE transcription_status_updated_at IS NULL OR transcription_status_updated_at = ''
        """)
        self.conn.commit()

        self._backfill_topic_name_norms()

        # Run one-time taxonomy migration
        self._migrate_taxonomy()

    def _migrate_taxonomy(self):
        """One-time migration: map old 12 categories to new 14-category taxonomy."""
        cur = self.conn.execute(
            "SELECT value FROM state WHERE key = 'taxonomy_version'"
        )
        row = cur.fetchone()
        if row and row["value"] >= "2":
            return  # Already migrated

        for old_cat, new_cat in config.OLD_CATEGORY_MAP.items():
            self.conn.execute(
                "UPDATE posts SET category = ? WHERE category = ?",
                (new_cat, old_cat),
            )
            self.conn.execute(
                "UPDATE topics SET category = ? WHERE category = ?",
                (new_cat, old_cat),
            )

        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            INSERT INTO state (key, value, updated_at)
            VALUES ('taxonomy_version', '2', ?)
            ON CONFLICT(key) DO UPDATE SET value = '2', updated_at = ?
        """, (now, now))
        self.conn.commit()

    # ------------------------------------------------------------------
    # Posts
    # ------------------------------------------------------------------
    def insert_posts_batch(self, tweets: list[dict]) -> tuple[int, int]:
        """
        Insert tweets, updating metrics for duplicates via upsert.
        Returns (inserted_count, updated_count).
        """
        if not tweets:
            return (0, 0)

        now = datetime.utcnow().isoformat()
        cur = self.conn.cursor()
        inserted = 0
        updated = 0

        # Build set of existing tweet_ids for insert/update tracking
        tweet_ids = [t["id"] for t in tweets]
        placeholders = ",".join("?" for _ in tweet_ids)
        cur.execute(
            f"SELECT tweet_id FROM posts WHERE tweet_id IN ({placeholders})",
            tweet_ids,
        )
        existing_ids = {row[0] for row in cur.fetchall()}

        for t in tweets:
            try:
                metrics_json = (
                    json.dumps(t.get("public_metrics"))
                    if t.get("public_metrics") else None
                )
                is_existing = t["id"] in existing_ids
                cur.execute("""
                    INSERT INTO posts
                        (tweet_id, author_id, author_username, author_name,
                         text, full_text, created_at,
                         public_metrics_json, entities_json,
                         referenced_tweets_json, fetched_at, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tweet_id) DO UPDATE SET
                        public_metrics_json = CASE
                            WHEN excluded.public_metrics_json IS NOT NULL
                            THEN excluded.public_metrics_json
                            ELSE posts.public_metrics_json
                        END,
                        metrics_refreshed_at = excluded.fetched_at,
                        metrics_unchanged_count = 0
                    WHERE excluded.public_metrics_json IS NOT NULL
                """, (
                    t["id"],
                    t.get("author_id", ""),
                    t.get("author_username", ""),
                    t.get("author_name", ""),
                    t.get("text", ""),
                    t.get("full_text"),
                    t.get("created_at", now),
                    metrics_json,
                    json.dumps(t.get("entities")) if t.get("entities") else None,
                    json.dumps(t.get("referenced_tweets")) if t.get("referenced_tweets") else None,
                    now,
                    json.dumps(t),
                ))
                if cur.rowcount > 0:
                    if is_existing:
                        updated += 1
                    else:
                        inserted += 1
            except sqlite3.Error:
                continue

        self.conn.commit()
        return (inserted, updated)

    def get_unclassified_posts(self, limit: int = 200) -> list[dict]:
        """Return posts that have not been classified yet."""
        cur = self.conn.execute("""
            SELECT tweet_id, author_username, author_name, text, full_text, created_at
            FROM posts
            WHERE classified_at IS NULL
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]

    def update_classification(
        self,
        tweet_id: str,
        is_relevant: bool,
        reasoning: str,
        category: Optional[str],
        subcategory: Optional[str] = None,
        secondary_categories: Optional[list] = None,
        model: Optional[str] = None,
    ):
        """Update a post's classification result."""
        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            UPDATE posts
            SET is_relevant = ?,
                relevance_reasoning = ?,
                category = ?,
                subcategory = ?,
                subcategories = ?,
                classified_at = ?,
                classification_model = ?
            WHERE tweet_id = ?
        """, (
            1 if is_relevant else 0,
            reasoning,
            category,
            subcategory,
            json.dumps(secondary_categories) if secondary_categories else None,
            now,
            model or config.HAIKU_MODEL,
            tweet_id,
        ))
        self.conn.commit()

    def get_relevant_posts(
        self,
        limit: int = 50,
        offset: int = 0,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        search: Optional[str] = None,
    ) -> list[dict]:
        """
        Return relevant posts with optional filters for the dashboard.
        """
        clauses = ["is_relevant = 1"]
        params: list = []

        if category:
            clauses.append("category = ?")
            params.append(category)
        if subcategory:
            clauses.append("subcategory = ?")
            params.append(subcategory)
        if date_from:
            clauses.append("created_at >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("created_at <= ?")
            params.append(date_to + "T23:59:59")
        if search:
            clauses.append("(text LIKE ? OR author_username LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = " AND ".join(clauses)
        params.extend([limit, offset])

        cur = self.conn.execute(f"""
            SELECT tweet_id, author_id, author_username, author_name,
                   text, full_text, created_at, category, subcategory,
                   subcategories, relevance_reasoning, public_metrics_json,
                   referenced_tweets_json
            FROM posts
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """, params)
        return [dict(row) for row in cur.fetchall()]

    def count_relevant_posts(
        self,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        search: Optional[str] = None,
    ) -> int:
        """Count relevant posts matching filters."""
        clauses = ["is_relevant = 1"]
        params: list = []

        if category:
            clauses.append("category = ?")
            params.append(category)
        if subcategory:
            clauses.append("subcategory = ?")
            params.append(subcategory)
        if date_from:
            clauses.append("created_at >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("created_at <= ?")
            params.append(date_to + "T23:59:59")
        if search:
            clauses.append("(text LIKE ? OR author_username LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = " AND ".join(clauses)
        cur = self.conn.execute(
            f"SELECT COUNT(*) FROM posts WHERE {where}", params
        )
        return cur.fetchone()[0]

    def get_topic_summary(self) -> list[dict]:
        """Aggregate relevant post counts by category."""
        cur = self.conn.execute("""
            SELECT category, COUNT(*) as post_count,
                   MAX(created_at) as last_seen_at
            FROM posts
            WHERE is_relevant = 1 AND category IS NOT NULL
            GROUP BY category
            ORDER BY post_count DESC
        """)
        return [dict(row) for row in cur.fetchall()]

    def get_stats(self) -> dict:
        """Summary statistics for the dashboard header."""
        cur = self.conn.execute("""
            SELECT
                COUNT(*) as total_posts,
                SUM(CASE WHEN is_relevant = 1 THEN 1 ELSE 0 END) as relevant_posts,
                SUM(CASE WHEN classified_at IS NULL THEN 1 ELSE 0 END) as unclassified_posts,
                MAX(fetched_at) as last_fetch_time
            FROM posts
        """)
        row = cur.fetchone()
        return dict(row) if row else {
            "total_posts": 0,
            "relevant_posts": 0,
            "unclassified_posts": 0,
            "last_fetch_time": None,
        }

    # ------------------------------------------------------------------
    # Topics (granular subject tracking)
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_topic_name(name: str) -> str:
        """Normalize topic names for near-duplicate detection."""
        if not name:
            return ""
        norm = name.lower().strip()
        norm = re.sub(r"[\"'`]+", "", norm)
        norm = re.sub(r"[^a-z0-9]+", " ", norm)
        norm = re.sub(r"\s+", " ", norm).strip()
        return norm

    @staticmethod
    def normalize_source_url(url: Optional[str]) -> str:
        """Normalize source URLs for stable deduplication and matching."""
        raw = (url or "").strip()
        if not raw:
            return ""
        try:
            parsed = urlparse(raw)
            scheme = (parsed.scheme or "https").lower()
            netloc = (parsed.netloc or "").lower().strip()
            path = re.sub(r"/+", "/", (parsed.path or "").strip())
            if path.endswith("/") and path != "/":
                path = path[:-1]
            query_pairs = []
            for key, value in parse_qsl(parsed.query or "", keep_blank_values=False):
                k = (key or "").strip().lower()
                if not k:
                    continue
                # Keep only identity-bearing params.
                if k in {"v", "list", "index", "t"}:
                    query_pairs.append((k, (value or "").strip()))
            query = urlencode(query_pairs, doseq=True)
            return urlunparse((scheme, netloc, path, "", query, ""))
        except Exception:
            return raw

    @staticmethod
    def infer_source_type(source_url: Optional[str]) -> Optional[str]:
        """Infer source type from URL."""
        raw = (source_url or "").strip().lower()
        if not raw:
            return None
        if "youtube.com" in raw or "youtu.be" in raw:
            return "youtube"
        if "x.com/" in raw or "twitter.com/" in raw:
            return "x_video"
        return "external"

    def _backfill_topic_name_norms(self):
        """Populate name_norm for existing rows (idempotent)."""
        rows = self.conn.execute(
            "SELECT id, name FROM topics WHERE name_norm IS NULL OR name_norm = ''"
        ).fetchall()
        if not rows:
            return
        for row in rows:
            self.conn.execute(
                "UPDATE topics SET name_norm = ? WHERE id = ?",
                (self._normalize_topic_name(row["name"]), row["id"]),
            )
        self.conn.commit()

    @staticmethod
    def _engagement_score_from_metrics(metrics_json: Optional[str]) -> int:
        """Weighted engagement score from X public metrics JSON or parsed dict."""
        if not metrics_json:
            return 0
        try:
            if isinstance(metrics_json, dict):
                m = metrics_json
            else:
                m = json.loads(metrics_json)
                if not isinstance(m, dict):
                    return 0
        except Exception:
            return 0

        like_count = int(m.get("like_count", 0) or 0)
        repost_count = int(m.get("retweet_count", 0) or m.get("repost_count", 0) or 0)
        reply_count = int(m.get("reply_count", 0) or 0)
        quote_count = int(m.get("quote_count", 0) or 0)
        bookmark_count = int(m.get("bookmark_count", 0) or 0)
        impression_count = int(m.get("impression_count", 0) or 0)

        return (
            repost_count * 4
            + quote_count * 3
            + reply_count * 2
            + like_count
            + bookmark_count
            + (impression_count // 100)
        )

    @staticmethod
    def _aggregate_metrics_from_rows(rows: list) -> dict:
        """Sum raw engagement counts from public_metrics_json across rows."""
        agg = {"likes": 0, "reposts": 0, "replies": 0, "quotes": 0, "bookmarks": 0, "impressions": 0}
        for row in rows:
            raw = row["public_metrics_json"] if isinstance(row, dict) else row.get("public_metrics_json") if hasattr(row, "get") else getattr(row, "public_metrics_json", None)
            if not raw:
                continue
            try:
                m = json.loads(raw) if isinstance(raw, str) else raw
                if not isinstance(m, dict):
                    continue
            except Exception:
                continue
            agg["likes"] += int(m.get("like_count", 0) or 0)
            agg["reposts"] += int(m.get("retweet_count", 0) or m.get("repost_count", 0) or 0)
            agg["replies"] += int(m.get("reply_count", 0) or 0)
            agg["quotes"] += int(m.get("quote_count", 0) or 0)
            agg["bookmarks"] += int(m.get("bookmark_count", 0) or 0)
            agg["impressions"] += int(m.get("impression_count", 0) or 0)
        return agg

    @staticmethod
    def _iso_to_utc_naive(value: Optional[str]) -> Optional[datetime]:
        """Parse an ISO timestamp and normalize to naive UTC."""
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    def find_topic_by_source_url(self, source_url: str) -> Optional[dict]:
        """Find the most likely active topic for a source URL."""
        norm = self.normalize_source_url(source_url)
        if not norm:
            return None
        row = self.conn.execute(
            """
            SELECT t.*
            FROM topics t
            WHERE t.is_active = 1
              AND lower(COALESCE(t.primary_source_url, '')) = lower(?)
            ORDER BY COALESCE(t.last_seen_at, t.first_seen_at) DESC, t.id DESC
            LIMIT 1
            """,
            (norm,),
        ).fetchone()
        return dict(row) if row else None

    def update_topic_source_metadata(
        self,
        topic_id: int,
        *,
        source_url: Optional[str] = None,
        source_type: Optional[str] = None,
        transcription_status: Optional[str] = None,
        transcription_workflow: Optional[str] = None,
        transcription_event_id: Optional[str] = None,
    ) -> None:
        """Update source/transcription metadata for a topic."""
        updates: list[str] = []
        params: list = []

        if source_url is not None:
            updates.append("primary_source_url = ?")
            params.append(self.normalize_source_url(source_url))
        if source_type is not None:
            updates.append("primary_source_type = ?")
            params.append((source_type or "").strip() or None)
        if transcription_status is not None:
            updates.append("transcription_status = ?")
            params.append((transcription_status or "").strip().lower() or "none")
            updates.append("transcription_status_updated_at = ?")
            params.append(datetime.utcnow().isoformat())
        if transcription_workflow is not None:
            updates.append("transcription_workflow = ?")
            params.append((transcription_workflow or "").strip() or None)
        if transcription_event_id is not None:
            updates.append("transcription_event_id = ?")
            params.append((transcription_event_id or "").strip() or None)

        if not updates:
            return
        params.append(topic_id)
        self.conn.execute(f"UPDATE topics SET {', '.join(updates)} WHERE id = ?", params)
        self.conn.commit()

    def mark_stale_transcription_topics(
        self,
        *,
        timeout_minutes: int = 45,
        limit: int = 200,
    ) -> list[dict]:
        """
        Mark pending transcription topics as failed_timeout when stale.
        Returns timed-out topic records.
        """
        try:
            timeout_minutes = int(timeout_minutes or 0)
        except Exception:
            timeout_minutes = 0
        if timeout_minutes <= 0:
            return []
        try:
            limit = max(1, int(limit or 200))
        except Exception:
            limit = 200

        rows = self.conn.execute(
            """
            SELECT id, name, transcription_workflow, transcription_status_updated_at, first_seen_at
            FROM topics
            WHERE is_active = 1
              AND lower(COALESCE(transcription_status, 'none')) = 'pending'
            ORDER BY COALESCE(transcription_status_updated_at, first_seen_at, datetime('now')) ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        if not rows:
            return []

        cutoff = datetime.utcnow() - timedelta(minutes=timeout_minutes)
        stale_rows: list[dict] = []
        for row in rows:
            rec = dict(row)
            status_at = self._iso_to_utc_naive(rec.get("transcription_status_updated_at"))
            if status_at is None:
                status_at = self._iso_to_utc_naive(rec.get("first_seen_at"))
            if status_at is not None and status_at <= cutoff:
                stale_rows.append(rec)
        if not stale_rows:
            return []

        now = datetime.utcnow().isoformat()
        timed_out: list[dict] = []
        for rec in stale_rows:
            topic_id = int(rec["id"])
            cur = self.conn.execute(
                """
                UPDATE topics
                SET transcription_status = 'failed_timeout',
                    transcription_status_updated_at = ?
                WHERE id = ?
                  AND lower(COALESCE(transcription_status, 'none')) = 'pending'
                """,
                (now, topic_id),
            )
            if cur.rowcount > 0:
                timed_out.append({
                    "id": topic_id,
                    "name": rec.get("name"),
                    "transcription_workflow": rec.get("transcription_workflow"),
                    "stale_since": rec.get("transcription_status_updated_at") or rec.get("first_seen_at"),
                })

        if timed_out:
            self.conn.commit()
        return timed_out

    def record_external_signal(
        self,
        *,
        topic_id: int,
        event_id: str,
        workflow: str,
        source_url: str,
        source_type: Optional[str] = None,
        video_title: Optional[str] = None,
        summary_bullets: Optional[list] = None,
        summary_html: Optional[str] = None,
        sender_email: Optional[str] = None,
        completed_at: Optional[str] = None,
    ) -> bool:
        """
        Record an external integration signal.
        Returns True when inserted, False when duplicate event_id.
        """
        if not event_id:
            raise ValueError("event_id is required")
        stamp = datetime.utcnow().isoformat()
        bullets_json = json.dumps(summary_bullets or [])
        norm_source = self.normalize_source_url(source_url)
        try:
            self.conn.execute(
                """
                INSERT INTO topic_external_signals (
                    topic_id, event_id, workflow, source_url, source_type, video_title,
                    summary_bullets_json, summary_html, sender_email, completed_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    topic_id,
                    event_id,
                    (workflow or "").strip(),
                    norm_source,
                    (source_type or "").strip() or self.infer_source_type(norm_source),
                    (video_title or "").strip() or None,
                    bullets_json,
                    summary_html,
                    (sender_email or "").strip() or None,
                    completed_at,
                    stamp,
                ),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def get_external_signal_event(self, event_id: str) -> Optional[dict]:
        """Return a single external signal event by idempotency key."""
        row = self.conn.execute(
            "SELECT * FROM topic_external_signals WHERE event_id = ? LIMIT 1",
            ((event_id or "").strip(),),
        ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _topic_status_filter_clause(status: Optional[str]) -> tuple[Optional[str], list]:
        """SQL predicate for topic status filters used by browse queries."""
        status = (status or "all").strip().lower()
        if status == "promoted":
            return "COALESCE(t.is_promoted, 1) = 1", []
        if status == "candidate":
            return "COALESCE(t.is_promoted, 1) = 0", []
        return None, []

    def promote_eligible_topics(self) -> dict:
        """Promote candidate topics to Weekly Prep when they meet thresholds."""
        if not getattr(config, "TOPIC_AUTO_PROMOTION_ENABLED", True):
            return {"promoted": 0, "topics": []}

        rows = self.conn.execute("""
            SELECT
                t.id,
                t.name,
                t.post_count,
                t.created_source,
                p.created_at,
                p.author_username,
                p.public_metrics_json
            FROM topics t
            LEFT JOIN post_topics pt ON pt.topic_id = t.id
            LEFT JOIN posts p ON p.tweet_id = pt.post_id
            WHERE t.is_active = 1
              AND COALESCE(t.is_promoted, 1) = 0
        """).fetchall()

        if not rows:
            return {"promoted": 0, "topics": []}

        now = datetime.utcnow()
        week_cutoff = now - timedelta(days=7)
        by_topic = {}

        for row in rows:
            tid = row["id"]
            rec = by_topic.setdefault(tid, {
                "id": tid,
                "name": row["name"],
                "total_posts": int(row["post_count"] or 0),
                "created_source": row["created_source"] or "legacy",
                "week_posts": 0,
                "week_authors": set(),
                "week_engagement_score": 0,
            })

            created_at = row["created_at"]
            if not created_at:
                continue
            dt = self._iso_to_utc_naive(created_at)
            if dt is None:
                continue
            if dt < week_cutoff:
                continue

            rec["week_posts"] += 1
            author = (row["author_username"] or "").strip().lower()
            if author:
                rec["week_authors"].add(author)
            rec["week_engagement_score"] += self._engagement_score_from_metrics(
                row["public_metrics_json"]
            )

        # Impression data for candidate topics (team interest signal / legacy).
        use_impression_auto_promo = bool(
            getattr(config, "TOPIC_PROMOTE_USE_IMPRESSIONS_FOR_AUTO_PROMOTION", False)
        )
        candidate_ids = list(by_topic.keys())
        impression_promo_map: dict[int, dict] = {}
        if candidate_ids and use_impression_auto_promo:
            ph = ",".join("?" for _ in candidate_ids)
            imp_rows = self.conn.execute(f"""
                SELECT topic_id,
                       COUNT(*) as total_impressions,
                       COUNT(DISTINCT voter_name) as unique_viewers
                FROM training_impressions
                WHERE topic_id IN ({ph})
                  AND shown_at >= ?
                GROUP BY topic_id
            """, [*candidate_ids, week_cutoff.isoformat()]).fetchall()
            impression_promo_map = {
                int(r["topic_id"]): {
                    "total": int(r["total_impressions"]),
                    "viewers": int(r["unique_viewers"]),
                }
                for r in imp_rows
            }

        to_promote = []
        for rec in by_topic.values():
            week_authors = len(rec["week_authors"])
            reason = None
            engagement_min_authors = int(
                getattr(config, "TOPIC_PROMOTE_ENGAGEMENT_MIN_WEEK_AUTHORS", 2)
            )

            imp_data = impression_promo_map.get(rec["id"], {"total": 0, "viewers": 0})
            imp_total = imp_data["total"]
            imp_viewers = imp_data["viewers"]

            if rec["total_posts"] >= config.TOPIC_PROMOTE_MIN_TOTAL_POSTS:
                reason = f"auto:lifetime_posts>={config.TOPIC_PROMOTE_MIN_TOTAL_POSTS}"
            elif (
                rec["week_posts"] >= config.TOPIC_PROMOTE_MIN_WEEK_POSTS
                and week_authors >= config.TOPIC_PROMOTE_MIN_WEEK_AUTHORS
            ):
                reason = (
                    f"auto:week_posts>={config.TOPIC_PROMOTE_MIN_WEEK_POSTS}"
                    f"_week_authors>={config.TOPIC_PROMOTE_MIN_WEEK_AUTHORS}"
                )
            elif (
                rec["week_posts"] >= config.TOPIC_PROMOTE_ENGAGEMENT_MIN_WEEK_POSTS
                and week_authors >= engagement_min_authors
                and rec["week_engagement_score"] >= config.TOPIC_PROMOTE_MIN_WEEK_ENGAGEMENT_SCORE
            ):
                reason = (
                    "auto:high_engagement"
                    f"_posts>={config.TOPIC_PROMOTE_ENGAGEMENT_MIN_WEEK_POSTS}"
                    f"_authors>={engagement_min_authors}"
                    f"_score>={config.TOPIC_PROMOTE_MIN_WEEK_ENGAGEMENT_SCORE}"
                )
            elif (
                use_impression_auto_promo
                and
                imp_total >= getattr(config, "TOPIC_PROMOTE_MIN_IMPRESSIONS", 6)
                and imp_viewers >= getattr(config, "TOPIC_PROMOTE_MIN_IMPRESSION_VIEWERS", 2)
            ):
                reason = (
                    f"auto:team_impressions>={getattr(config, 'TOPIC_PROMOTE_MIN_IMPRESSIONS', 6)}"
                    f"_viewers>={getattr(config, 'TOPIC_PROMOTE_MIN_IMPRESSION_VIEWERS', 2)}"
                )

            if reason:
                rec["week_author_count"] = week_authors
                rec["promotion_reason"] = reason
                to_promote.append(rec)

        if not to_promote:
            return {"promoted": 0, "topics": []}

        stamp = now.isoformat()
        for rec in to_promote:
            self.conn.execute("""
                UPDATE topics
                SET is_promoted = 1,
                    promoted_at = COALESCE(promoted_at, ?),
                    promotion_reason = ?
                WHERE id = ?
            """, (stamp, rec["promotion_reason"], rec["id"]))
        self.conn.commit()

        # Keep payload small + JSON-safe
        out = []
        for rec in sorted(to_promote, key=lambda r: (r["week_posts"], r["total_posts"]), reverse=True):
            out.append({
                "id": rec["id"],
                "name": rec["name"],
                "total_posts": rec["total_posts"],
                "week_posts": rec["week_posts"],
                "week_author_count": rec.get("week_author_count", len(rec["week_authors"])),
                "week_engagement_score": rec["week_engagement_score"],
                "promotion_reason": rec["promotion_reason"],
            })

        return {"promoted": len(out), "topics": out}

    def get_active_topics(self, limit: int = 200) -> list[dict]:
        """Return active topics with their descriptions for Haiku context."""
        cur = self.conn.execute("""
            SELECT id, name, description, category, subcategory,
                   post_count, last_seen_at, is_promoted
            FROM topics
            WHERE is_active = 1
            ORDER BY last_seen_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]

    def get_or_create_topic_status(
        self,
        name: str,
        description: str,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        promote: bool = True,
        created_source: str = "manual",
    ) -> tuple[int, bool]:
        """
        Get existing topic ID by name, or create it.
        Returns (topic_id, was_created).
        """
        name = (name or "").strip()
        if not name:
            raise ValueError("Topic name is required")
        name_norm = self._normalize_topic_name(name)

        cur = self.conn.execute(
            "SELECT id, is_promoted FROM topics WHERE name = ?", (name,)
        )
        row = cur.fetchone()
        if row:
            topic_id = row["id"]
            if promote and not int(row["is_promoted"] or 0):
                now = datetime.utcnow().isoformat()
                self.conn.execute("""
                    UPDATE topics
                    SET is_promoted = 1,
                        promoted_at = COALESCE(promoted_at, ?),
                        promotion_reason = COALESCE(NULLIF(promotion_reason, ''), 'manual_promote_on_reuse')
                    WHERE id = ?
                """, (now, topic_id))
                self.conn.commit()
            return topic_id, False

        if name_norm:
            cur = self.conn.execute("""
                SELECT id, is_promoted
                FROM topics
                WHERE name_norm = ? AND is_active = 1
                ORDER BY is_promoted DESC, post_count DESC, id ASC
                LIMIT 1
            """, (name_norm,))
            row = cur.fetchone()
            if row:
                topic_id = row["id"]
                if promote and not int(row["is_promoted"] or 0):
                    now = datetime.utcnow().isoformat()
                    self.conn.execute("""
                        UPDATE topics
                        SET is_promoted = 1,
                            promoted_at = COALESCE(promoted_at, ?),
                            promotion_reason = COALESCE(NULLIF(promotion_reason, ''), 'manual_promote_normalized_match')
                        WHERE id = ?
                    """, (now, topic_id))
                    self.conn.commit()
                return topic_id, False

        now = datetime.utcnow().isoformat()
        promoted_at = now if promote else None
        promotion_reason = "manual_create" if promote else "candidate_new_topic"
        cur = self.conn.execute("""
            INSERT INTO topics (
                name, name_norm, description, category, subcategory,
                first_seen_at, last_seen_at, is_promoted, promoted_at,
                promotion_reason, created_source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name, name_norm, description, category, subcategory,
            now, now, 1 if promote else 0, promoted_at, promotion_reason, created_source
        ))
        self.conn.commit()
        return cur.lastrowid, True

    def get_or_create_topic(
        self,
        name: str,
        description: str,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
    ) -> int:
        """Compatibility wrapper returning only topic_id."""
        topic_id, _ = self.get_or_create_topic_status(
            name, description, category, subcategory
        )
        return topic_id

    @staticmethod
    def _topic_search_min_query_len() -> int:
        try:
            min_len = int(getattr(config, "TOPIC_SEARCH_SEMANTIC_MIN_QUERY_LEN", 2) or 2)
        except Exception:
            min_len = 2
        return max(2, min_len)

    @staticmethod
    def _topic_search_alpha(alpha: Optional[float]) -> float:
        try:
            default_alpha = float(getattr(config, "TOPIC_SEARCH_HYBRID_ALPHA", 0.65))
        except Exception:
            default_alpha = 0.65
        if alpha is None:
            return min(1.0, max(0.0, default_alpha))
        try:
            return min(1.0, max(0.0, float(alpha)))
        except Exception:
            return min(1.0, max(0.0, default_alpha))

    @staticmethod
    def _topic_search_semantic_k(semantic_k: Optional[int], limit: int) -> int:
        default_k = max(
            int(getattr(config, "TOPIC_SEARCH_SEMANTIC_K_DEFAULT", 40) or 40),
            max(40, limit * 4),
        )
        max_k = max(
            1,
            int(getattr(config, "TOPIC_SEARCH_SEMANTIC_K_MAX", 120) or 120),
        )
        try:
            k_val = int(semantic_k) if semantic_k is not None else default_k
        except Exception:
            k_val = default_k
        k_val = max(limit, max(1, k_val))
        return min(k_val, max_k)

    @staticmethod
    def _escape_like(val: str) -> str:
        return val.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def _search_topics_lexical(self, query: str, limit: int) -> list[dict]:
        q_lower = query.lower()
        q_like = f"%{self._escape_like(q_lower)}%"
        q_prefix = f"{self._escape_like(q_lower)}%"

        tokens = [t for t in re.findall(r"[a-z0-9]+", q_lower) if len(t) >= 2][:6]
        token_clauses = []
        params: list = [
            q_lower,          # exact name match
            q_prefix,         # prefix match in name
            q_like,           # contains match in name
            q_like,           # contains match in description
            q_lower,          # exact category
            q_lower,          # exact subcategory
            q_like,           # name/desc/category/subcategory filter (name)
            q_like,           # desc
            q_like,           # category
            q_like,           # subcategory
        ]

        for tok in tokens:
            tok_like = f"%{self._escape_like(tok)}%"
            token_clauses.append(
                """(
                    lower(t.name) LIKE ? ESCAPE '\\'
                    OR lower(COALESCE(t.description, '')) LIKE ? ESCAPE '\\'
                )"""
            )
            params.extend([tok_like, tok_like])

        token_sql = (" AND " + " AND ".join(token_clauses)) if token_clauses else ""
        params.extend([limit])

        cur = self.conn.execute(f"""
            SELECT
                t.id,
                t.name,
                t.description,
                t.category,
                t.subcategory,
                t.post_count,
                t.first_seen_at,
                t.last_seen_at,
                t.is_active,
                COALESCE(t.is_promoted, 1) as is_promoted,
                t.editorial_tier_override,
                CASE
                    WHEN lower(t.name) = ? THEN 0
                    WHEN lower(t.name) LIKE ? ESCAPE '\\' THEN 1
                    WHEN lower(t.name) LIKE ? ESCAPE '\\' THEN 2
                    WHEN lower(COALESCE(t.description, '')) LIKE ? ESCAPE '\\' THEN 3
                    WHEN lower(COALESCE(t.category, '')) = ? THEN 4
                    WHEN lower(COALESCE(t.subcategory, '')) = ? THEN 5
                    ELSE 6
                END AS match_rank
            FROM topics t
            WHERE t.is_active = 1 AND (
                lower(t.name) LIKE ? ESCAPE '\\'
                OR lower(COALESCE(t.description, '')) LIKE ? ESCAPE '\\'
                OR lower(COALESCE(t.category, '')) LIKE ? ESCAPE '\\'
                OR lower(COALESCE(t.subcategory, '')) LIKE ? ESCAPE '\\'
            )
            {token_sql}
            ORDER BY
                match_rank ASC,
                COALESCE(t.is_promoted, 1) DESC,
                t.post_count DESC,
                COALESCE(t.last_seen_at, '') DESC,
                t.name COLLATE NOCASE ASC
            LIMIT ?
        """, params)
        return [dict(row) for row in cur.fetchall()]

    def _get_active_topics_by_ids(self, topic_ids: list[int]) -> list[dict]:
        ids = [int(tid) for tid in topic_ids if tid is not None]
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        cur = self.conn.execute(f"""
            SELECT
                t.id,
                t.name,
                t.description,
                t.category,
                t.subcategory,
                t.post_count,
                t.first_seen_at,
                t.last_seen_at,
                t.is_active,
                COALESCE(t.is_promoted, 1) as is_promoted,
                t.editorial_tier_override
            FROM topics t
            WHERE t.is_active = 1
              AND t.id IN ({placeholders})
        """, ids)
        return [dict(row) for row in cur.fetchall()]

    def _search_topics_semantic_distances(self, query: str, k: int) -> dict[int, float]:
        from vector_search import TopicVectorIndex

        idx = TopicVectorIndex.get_instance(self.conn)
        rows = idx.search_topic_ids_by_text(query, k=k)
        out: dict[int, float] = {}
        for tid, dist in rows:
            tid = int(tid)
            dist = float(dist)
            if tid not in out or dist < out[tid]:
                out[tid] = dist
        return out

    def search_topics(self, query: str, limit: int = 50) -> list[dict]:
        """Compatibility wrapper: lexical topic search only."""
        return self.search_topics_hybrid(query, limit=limit, semantic=False)

    def search_topics_hybrid(
        self,
        query: str,
        limit: int = 50,
        semantic: bool = True,
        alpha: Optional[float] = None,
        semantic_k: Optional[int] = None,
    ) -> list[dict]:
        """Search topics with lexical ranking and optional semantic blending."""
        q = (query or "").strip()
        if len(q) < self._topic_search_min_query_len():
            return []

        try:
            safe_limit = max(1, int(limit))
        except Exception:
            safe_limit = 50

        lexical_pool_limit = min(200, max(safe_limit, max(40, safe_limit * 4)))
        lexical_rows = self._search_topics_lexical(q, limit=lexical_pool_limit)

        semantic_enabled = bool(semantic) and bool(
            getattr(config, "TOPIC_SEARCH_SEMANTIC_ENABLED", True)
        )
        if not semantic_enabled:
            return lexical_rows[:safe_limit]

        blend_alpha = self._topic_search_alpha(alpha)
        k_value = self._topic_search_semantic_k(semantic_k, safe_limit)

        try:
            semantic_distances = self._search_topics_semantic_distances(q, k_value)
        except Exception as e:
            logger.warning("Topic semantic search unavailable, using lexical only: %s", e)
            return lexical_rows[:safe_limit]

        if not semantic_distances:
            return lexical_rows[:safe_limit]

        semantic_rows = self._get_active_topics_by_ids(list(semantic_distances.keys()))
        if not semantic_rows:
            return lexical_rows[:safe_limit]

        combined: dict[int, dict] = {}
        for row in lexical_rows:
            tid = int(row["id"])
            rec = dict(row)
            combined[tid] = rec

        for row in semantic_rows:
            tid = int(row["id"])
            if tid not in combined:
                rec = dict(row)
                rec["match_rank"] = None
                combined[tid] = rec

        dist_values = list(semantic_distances.values())
        dist_min = min(dist_values)
        dist_max = max(dist_values)
        dist_span = dist_max - dist_min

        for rec in combined.values():
            tid = int(rec["id"])
            rank_val = rec.get("match_rank")
            if rank_val is None:
                lex_score = 0.0
            else:
                try:
                    rank = max(0, min(6, int(rank_val)))
                except Exception:
                    rank = 6
                lex_score = 1.0 - (rank / 6.0)

            dist_val = semantic_distances.get(tid)
            if dist_val is None:
                sem_score = 0.0
            elif dist_span <= 1e-12:
                sem_score = 1.0
            else:
                sem_score = 1.0 - ((dist_val - dist_min) / dist_span)

            rec["_final_score"] = (blend_alpha * lex_score) + ((1.0 - blend_alpha) * sem_score)

        ranked = list(combined.values())
        # Stable multi-pass sort to keep name ascending as final tiebreaker.
        ranked.sort(key=lambda r: (r.get("name") or "").lower())
        ranked.sort(key=lambda r: r.get("last_seen_at") or "", reverse=True)
        ranked.sort(key=lambda r: int(r.get("post_count") or 0), reverse=True)
        ranked.sort(key=lambda r: int(r.get("is_promoted", 1) or 0), reverse=True)
        ranked.sort(key=lambda r: float(r.get("_final_score", 0.0)), reverse=True)

        out = []
        for rec in ranked[:safe_limit]:
            rec.pop("_final_score", None)
            out.append(rec)
        return out

    def link_post_to_topic(self, tweet_id: str, topic_id: int):
        """Link a post to a topic (many-to-many)."""
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO post_topics (post_id, topic_id)
                VALUES (?, ?)
            """, (tweet_id, topic_id))
            # Update topic stats
            self.conn.execute("""
                UPDATE topics SET
                    post_count = (SELECT COUNT(*) FROM post_topics WHERE topic_id = ?),
                    last_seen_at = (SELECT MAX(p.created_at) FROM posts p
                                    JOIN post_topics pt ON p.tweet_id = pt.post_id
                                    WHERE pt.topic_id = ?)
                WHERE id = ?
            """, (topic_id, topic_id, topic_id))
            self.conn.commit()
        except sqlite3.Error:
            pass

    def backfill_post_categories_from_topics(self) -> tuple[int, int]:
        """Copy category/subcategory from the best-matched topic to posts missing them.

        Picks one topic per post (highest post_count, lowest id for tie-break)
        and copies BOTH category and subcategory from that same topic row.

        Returns (updated_count, remaining_null_count).
        """
        try:
            cur = self.conn.execute("""
                UPDATE posts
                SET category = (
                        SELECT t.category
                        FROM post_topics pt
                        JOIN topics t ON t.id = pt.topic_id
                        WHERE pt.post_id = posts.tweet_id
                          AND t.category IS NOT NULL
                        ORDER BY t.post_count DESC, t.id ASC
                        LIMIT 1
                    ),
                    subcategory = (
                        SELECT t.subcategory
                        FROM post_topics pt
                        JOIN topics t ON t.id = pt.topic_id
                        WHERE pt.post_id = posts.tweet_id
                          AND t.category IS NOT NULL
                        ORDER BY t.post_count DESC, t.id ASC
                        LIMIT 1
                    )
                WHERE is_relevant = 1
                  AND (category IS NULL OR subcategory IS NULL)
                  AND EXISTS (
                      SELECT 1 FROM post_topics pt
                      JOIN topics t ON t.id = pt.topic_id
                      WHERE pt.post_id = posts.tweet_id
                        AND t.category IS NOT NULL
                  )
            """)
            updated = cur.rowcount
            self.conn.commit()

            # Count remaining nulls among linked relevant posts
            row = self.conn.execute("""
                SELECT COUNT(*) FROM posts p
                JOIN post_topics pt ON p.tweet_id = pt.post_id
                WHERE p.is_relevant = 1
                  AND p.category IS NULL
            """).fetchone()
            remaining = row[0] if row else 0

            return updated, remaining
        except sqlite3.Error as e:
            logger.warning(f"Category backfill failed (non-fatal): {e}")
            return 0, -1

    def get_posts_unlinked_to_topics(self, limit: int = 100) -> list[dict]:
        """Return relevant, classified posts that haven't been linked to any topic yet."""
        cur = self.conn.execute("""
            SELECT p.tweet_id, p.author_username, p.author_name,
                   p.text, p.full_text, p.created_at, p.category,
                   p.relevance_reasoning
            FROM posts p
            LEFT JOIN post_topics pt ON p.tweet_id = pt.post_id
            WHERE p.is_relevant = 1
              AND p.classified_at IS NOT NULL
              AND pt.post_id IS NULL
            ORDER BY p.created_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]

    def get_topics_detail(self, limit: int = 50) -> list[dict]:
        """Return topics with details for the dashboard."""
        cur = self.conn.execute("""
            SELECT t.id, t.name, t.description, t.category,
                   t.subcategory, t.post_count, t.first_seen_at, t.last_seen_at,
                   COALESCE(t.is_promoted, 1) as is_promoted,
                   t.editorial_tier_override, t.editorial_tier_set_by, t.editorial_tier_set_at
            FROM topics t
            WHERE t.is_active = 1
            ORDER BY COALESCE(t.is_promoted, 1) DESC, t.last_seen_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]

    def get_posts_for_topic(self, topic_id: int, limit: int = 50) -> list[dict]:
        """Return posts linked to a specific topic."""
        cur = self.conn.execute("""
            SELECT p.tweet_id, p.author_username, p.author_name,
                   p.text, p.full_text, p.created_at, p.category,
                   p.relevance_reasoning, p.referenced_tweets_json,
                   p.public_metrics_json
            FROM posts p
            JOIN post_topics pt ON p.tweet_id = pt.post_id
            WHERE pt.topic_id = ?
            ORDER BY p.created_at DESC
            LIMIT ?
        """, (topic_id, limit))
        return [dict(row) for row in cur.fetchall()]

    def get_topics_this_week(self) -> list[dict]:
        """Return topics that have received posts this week (Mon-Sun)."""
        cur = self.conn.execute("""
            SELECT t.id, t.name, t.description, t.category,
                   t.subcategory, t.post_count, t.first_seen_at, t.last_seen_at,
                   COALESCE(t.is_promoted, 1) as is_promoted
            FROM topics t
            WHERE t.is_active = 1
              AND t.last_seen_at >= date('now', 'weekday 1', '-7 days')
            ORDER BY COALESCE(t.is_promoted, 1) DESC, t.post_count DESC
        """)
        return [dict(row) for row in cur.fetchall()]

    def get_topics_recently_created(self, days: int = 3) -> list[dict]:
        """Return topics created in the last N days."""
        cur = self.conn.execute("""
            SELECT t.id, t.name, t.description, t.category,
                   t.subcategory, t.post_count, t.first_seen_at, t.last_seen_at,
                   COALESCE(t.is_promoted, 1) as is_promoted
            FROM topics t
            WHERE t.is_active = 1
              AND t.first_seen_at >= datetime('now', ?)
            ORDER BY COALESCE(t.is_promoted, 1) DESC, t.first_seen_at DESC
        """, (f"-{days} days",))
        return [dict(row) for row in cur.fetchall()]

    def get_topics_by_category(self, category: str) -> list[dict]:
        """Return all active topics in a given broad category."""
        cur = self.conn.execute("""
            SELECT t.id, t.name, t.description, t.category,
                   t.subcategory, t.post_count, t.first_seen_at, t.last_seen_at,
                   COALESCE(t.is_promoted, 1) as is_promoted
            FROM topics t
            WHERE t.is_active = 1
              AND t.category = ?
            ORDER BY COALESCE(t.is_promoted, 1) DESC, t.post_count DESC
        """, (category,))
        return [dict(row) for row in cur.fetchall()]

    def get_category_topic_counts(self, status: str = "all") -> list[dict]:
        """Return count of granular topics per broad category."""
        where = ["is_active = 1", "category IS NOT NULL"]
        if (status or "").lower() == "promoted":
            where.append("COALESCE(is_promoted, 1) = 1")
        elif (status or "").lower() == "candidate":
            where.append("COALESCE(is_promoted, 1) = 0")
        cur = self.conn.execute(f"""
            SELECT category, COUNT(*) as topic_count,
                   SUM(post_count) as total_posts
            FROM topics
            WHERE {' AND '.join(where)}
            GROUP BY category
            ORDER BY total_posts DESC
        """)
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Weekly cycles / week-scoped voting
    # ------------------------------------------------------------------
    def _weekly_cycle_tz(self, tz_name: Optional[str] = None) -> tuple[str, ZoneInfo]:
        configured = (tz_name or getattr(config, "WEEKLY_CYCLE_TIMEZONE", "UTC") or "UTC").strip()
        try:
            return configured, ZoneInfo(configured)
        except Exception:
            logger.warning("Invalid WEEKLY_CYCLE_TIMEZONE=%r; falling back to UTC", configured)
            return "UTC", ZoneInfo("UTC")

    def _weekly_cycle_bounds_for(self, now_utc: Optional[datetime] = None, tz_name: Optional[str] = None) -> dict:
        tz_label, tz = self._weekly_cycle_tz(tz_name)
        now_utc = now_utc or datetime.utcnow()
        now_aware = now_utc.replace(tzinfo=timezone.utc) if now_utc.tzinfo is None else now_utc.astimezone(timezone.utc)
        local_now = now_aware.astimezone(tz)

        start_weekday = int(getattr(config, "WEEKLY_CYCLE_START_WEEKDAY", 4))
        start_hour = int(getattr(config, "WEEKLY_CYCLE_START_HOUR", 0))
        start_minute = int(getattr(config, "WEEKLY_CYCLE_START_MINUTE", 5))

        days_since_start = (local_now.weekday() - start_weekday) % 7
        start_date = local_now.date() - timedelta(days=days_since_start)
        start_local = datetime.combine(start_date, dt_time(start_hour, start_minute), tzinfo=tz)
        if local_now < start_local:
            start_local -= timedelta(days=7)
        end_local = start_local + timedelta(days=7)

        start_utc = start_local.astimezone(timezone.utc).replace(tzinfo=None)
        end_utc = end_local.astimezone(timezone.utc).replace(tzinfo=None)
        return {
            "week_key": start_local.date().isoformat(),
            "starts_at": start_utc.isoformat(),
            "ends_at": end_utc.isoformat(),
            "timezone": tz_label,
            "starts_local_date": start_local.date().isoformat(),
            "ends_local_date": end_local.date().isoformat(),
        }

    def _pre_reset_cutoff_utc_for_cycle_start(self, since_date: str) -> Optional[datetime]:
        """Return Thursday-noon fixed-MST (UTC-7) cutoff aligned to a cycle start."""
        cycle_start = self._iso_to_utc_naive(since_date)
        if cycle_start is None:
            return None

        offset_hours = int(getattr(config, "WEEKLY_PRE_RESET_CUTOFF_UTC_OFFSET_HOURS", -7))
        cutoff_weekday = int(getattr(config, "WEEKLY_PRE_RESET_CUTOFF_WEEKDAY", 3))
        cutoff_hour = int(getattr(config, "WEEKLY_PRE_RESET_CUTOFF_HOUR", 12))
        cutoff_minute = int(getattr(config, "WEEKLY_PRE_RESET_CUTOFF_MINUTE", 0))

        fixed_tz = timezone(timedelta(hours=offset_hours))
        cycle_start_local = cycle_start.replace(tzinfo=timezone.utc).astimezone(fixed_tz)

        days_until_cutoff = (cutoff_weekday - cycle_start_local.weekday()) % 7
        cutoff_date = (cycle_start_local + timedelta(days=days_until_cutoff)).date()
        cutoff_local = datetime.combine(cutoff_date, dt_time(cutoff_hour, cutoff_minute), tzinfo=fixed_tz)
        if cutoff_local <= cycle_start_local:
            cutoff_local += timedelta(days=7)
        return cutoff_local.astimezone(timezone.utc).replace(tzinfo=None)

    def _pre_reset_momentum_window_for_cycle_start(self, since_date: str) -> Optional[dict]:
        """
        Resolve pre/post-cutoff windows used for momentum de-prioritization.

        If we are early in a fresh cycle (before this cycle's cutoff), evaluate
        the previous cycle window so the rule applies immediately on Friday.
        """
        cycle_start = self._iso_to_utc_naive(since_date)
        if cycle_start is None:
            return None
        cutoff_current = self._pre_reset_cutoff_utc_for_cycle_start(since_date)
        if cutoff_current is None:
            return None

        now_utc = datetime.utcnow()
        use_previous_cycle = now_utc < cutoff_current
        anchor_start = cycle_start - timedelta(days=7) if use_previous_cycle else cycle_start
        anchor_cutoff = cutoff_current - timedelta(days=7) if use_previous_cycle else cutoff_current
        anchor_end = cycle_start if use_previous_cycle else now_utc
        if anchor_end <= anchor_cutoff:
            anchor_end = anchor_cutoff + timedelta(seconds=1)

        return {
            "use_previous_cycle": use_previous_cycle,
            "window_start": anchor_start,
            "window_cutoff": anchor_cutoff,
            "window_end": anchor_end,
            "window_start_iso": anchor_start.isoformat(),
            "window_cutoff_iso": anchor_cutoff.isoformat(),
            "window_end_iso": anchor_end.isoformat(),
        }

    def _decorate_weekly_cycle(self, row) -> Optional[dict]:
        if not row:
            return None
        rec = dict(row)
        tz_label, tz = self._weekly_cycle_tz(rec.get("timezone"))
        rec["timezone"] = tz_label
        start_dt = self._iso_to_utc_naive(rec.get("starts_at"))
        end_dt = self._iso_to_utc_naive(rec.get("ends_at"))
        if start_dt:
            start_local = start_dt.replace(tzinfo=timezone.utc).astimezone(tz)
            rec["starts_at_local"] = start_local.isoformat()
            rec["starts_local_date"] = start_local.date().isoformat()
        if end_dt:
            end_local = end_dt.replace(tzinfo=timezone.utc).astimezone(tz)
            rec["ends_at_local"] = end_local.isoformat()
            rec["ends_local_date"] = end_local.date().isoformat()
        return rec

    def _get_open_weekly_cycle_row(self):
        return self.conn.execute("""
            SELECT *
            FROM weekly_cycles
            WHERE status = 'open'
            ORDER BY starts_at DESC, id DESC
            LIMIT 1
        """).fetchone()

    def _create_weekly_cycle(self, bounds: dict) -> dict:
        self.conn.execute("""
            INSERT INTO weekly_cycles (week_key, starts_at, ends_at, timezone, status)
            VALUES (?, ?, ?, ?, 'open')
            ON CONFLICT(week_key) DO NOTHING
        """, (bounds["week_key"], bounds["starts_at"], bounds["ends_at"], bounds["timezone"]))
        self.conn.commit()
        row = self.conn.execute("SELECT * FROM weekly_cycles WHERE week_key = ? LIMIT 1", (bounds["week_key"],)).fetchone()
        return self._decorate_weekly_cycle(row) or dict(bounds)

    def _close_weekly_cycle(self, cycle_id: int, closed_at: Optional[str] = None):
        stamp = closed_at or datetime.utcnow().isoformat()
        self.conn.execute("""
            UPDATE weekly_cycles
            SET status = 'closed', closed_at = ?
            WHERE id = ? AND status != 'closed'
        """, (stamp, cycle_id))

    def _upsert_topic_week_outcome(
        self,
        week_id: int,
        topic_id: int,
        outcome: str,
        resolved_by: str = "system",
        notes: Optional[str] = None,
        resolved_at: Optional[str] = None,
    ):
        stamp = resolved_at or datetime.utcnow().isoformat()
        self.conn.execute("""
            INSERT INTO topic_week_outcomes (week_id, topic_id, outcome, resolved_by, resolved_at, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(week_id, topic_id)
            DO UPDATE SET
                outcome = excluded.outcome,
                resolved_by = excluded.resolved_by,
                resolved_at = excluded.resolved_at,
                notes = excluded.notes
        """, (week_id, topic_id, outcome, resolved_by, stamp, notes))

    def _mark_topic_covered(self, topic_id: int, week_id: int, covered_at: Optional[str] = None):
        stamp = covered_at or datetime.utcnow().isoformat()
        topic = self.conn.execute(
            "SELECT post_count, last_seen_at FROM topics WHERE id = ?",
            (topic_id,),
        ).fetchone()
        self.conn.execute("""
            UPDATE topics
            SET last_covered_week_id = ?,
                last_covered_at = ?,
                last_covered_total_post_count = ?,
                last_covered_latest_activity = ?
            WHERE id = ?
        """, (
            week_id,
            stamp,
            int(topic["post_count"] or 0) if topic else None,
            topic["last_seen_at"] if topic else None,
            topic_id,
        ))

    def _finalize_weekly_cycle(self, cycle: dict, actor: str = "system"):
        if not cycle or not cycle.get("id"):
            return
        week_id = int(cycle["id"])
        since_value = str(cycle.get("starts_at") or cycle.get("starts_local_date") or "")
        sections = self.get_weekly_prep_sections(
            since_date=since_value,
            slide_target=getattr(config, "WEEKLY_PREP_TOPIC_LIMIT", 20),
            bullet_target=getattr(config, "WEEKLY_PREP_BULLET_TARGET", 30),
            week_id=week_id,
        )
        stamp = datetime.utcnow().isoformat()
        for t in sections.get("slide_topics", []):
            tid = int(t["id"])
            self._upsert_topic_week_outcome(week_id, tid, "slide", resolved_by=actor, resolved_at=stamp)
            self._mark_topic_covered(tid, week_id, covered_at=stamp)
        for t in sections.get("bullet_topics", []):
            tid = int(t["id"])
            self._upsert_topic_week_outcome(week_id, tid, "bullet", resolved_by=actor, resolved_at=stamp)
            self._mark_topic_covered(tid, week_id, covered_at=stamp)
        self._close_weekly_cycle(week_id, closed_at=stamp)
        self.conn.commit()

    def rollover_weekly_cycle_if_due(self, actor: str = "system") -> dict:
        """Close/open weekly cycles as needed based on configured rollover boundary."""
        now = datetime.utcnow()
        rolled = 0
        for _ in range(12):
            open_row = self._get_open_weekly_cycle_row()
            if not open_row:
                current = self._create_weekly_cycle(self._weekly_cycle_bounds_for(now))
                return {"rolled_over": rolled, "current_cycle": current}

            cycle = self._decorate_weekly_cycle(open_row)
            ends_at = self._iso_to_utc_naive(cycle.get("ends_at"))
            if not ends_at or now < ends_at:
                return {"rolled_over": rolled, "current_cycle": cycle}

            self._finalize_weekly_cycle(cycle, actor=actor)
            rolled += 1
            self._create_weekly_cycle(self._weekly_cycle_bounds_for(ends_at + timedelta(seconds=1)))

        return {"rolled_over": rolled, "current_cycle": self.get_current_weekly_cycle(ensure=False)}

    def ensure_current_weekly_cycle(self, actor: str = "system") -> dict:
        self.rollover_weekly_cycle_if_due(actor=actor)
        row = self._get_open_weekly_cycle_row()
        if row:
            return self._decorate_weekly_cycle(row)
        return self._create_weekly_cycle(self._weekly_cycle_bounds_for())

    def get_current_weekly_cycle(self, ensure: bool = True) -> Optional[dict]:
        if ensure:
            return self.ensure_current_weekly_cycle(actor="system")
        return self._decorate_weekly_cycle(self._get_open_weekly_cycle_row())

    # ------------------------------------------------------------------
    # Weekly prep queries (topics ranked by activity within time windows)
    # ------------------------------------------------------------------
    def get_weekly_topic_pool(self, since_date: str) -> list[dict]:
        """All topics with activity since `since_date`, annotated with ranking fields."""
        cur = self.conn.execute("""
            SELECT t.id, t.name, t.description, t.category, t.subcategory,
                   t.summary_bullets, t.summary_key_takeaways,
                   t.post_count as total_post_count,
                   COALESCE(t.is_promoted, 1) as is_promoted,
                   t.editorial_tier_override,
                   t.editorial_tier_set_by,
                   t.editorial_tier_set_at,
                   t.last_covered_week_id,
                   t.last_covered_at,
                   t.coverage_cooldown_until,
                   t.last_covered_total_post_count,
                   t.last_covered_latest_activity,
                   COUNT(pt.post_id) as week_post_count,
                   COUNT(DISTINCT p.author_username) as source_count,
                   MAX(p.created_at) as latest_activity
            FROM topics t
            JOIN post_topics pt ON t.id = pt.topic_id
            JOIN posts p ON pt.post_id = p.tweet_id
            WHERE t.is_active = 1
              AND p.created_at >= ?
            GROUP BY t.id
        """, (since_date,))
        rows = [dict(row) for row in cur.fetchall()]

        by_topic_id: dict[int, dict] = {}
        for row in rows:
            try:
                tid = int(row["id"])
            except Exception:
                continue
            by_topic_id[tid] = row

        # Merge in external integration signals so transcription-derived topics
        # can appear in weekly prep even without linked X posts.
        ext_rows = self.conn.execute(
            """
            SELECT
                s.topic_id,
                COUNT(*) AS external_signal_count,
                MAX(COALESCE(s.completed_at, s.created_at)) AS latest_external_signal_at
            FROM topic_external_signals s
            JOIN topics t ON t.id = s.topic_id
            WHERE t.is_active = 1
              AND COALESCE(s.completed_at, s.created_at) >= ?
            GROUP BY s.topic_id
            """,
            (since_date,),
        ).fetchall()
        external_map = {int(r["topic_id"]): dict(r) for r in ext_rows}

        missing_external_ids = [tid for tid in external_map.keys() if tid not in by_topic_id]
        if missing_external_ids:
            placeholders = ",".join("?" for _ in missing_external_ids)
            topic_meta_rows = self.conn.execute(
                f"""
                SELECT
                    t.id, t.name, t.description, t.category, t.subcategory,
                    t.summary_bullets, t.summary_key_takeaways,
                    t.post_count as total_post_count,
                    COALESCE(t.is_promoted, 1) as is_promoted,
                    t.editorial_tier_override,
                    t.editorial_tier_set_by,
                    t.editorial_tier_set_at,
                    t.last_covered_week_id,
                    t.last_covered_at,
                    t.coverage_cooldown_until,
                    t.last_covered_total_post_count,
                    t.last_covered_latest_activity
                FROM topics t
                WHERE t.is_active = 1
                  AND t.id IN ({placeholders})
                """,
                missing_external_ids,
            ).fetchall()
            for meta in topic_meta_rows:
                rec = dict(meta)
                tid = int(rec["id"])
                sig = external_map.get(tid, {})
                rec["week_post_count"] = 0
                rec["source_count"] = 0
                rec["latest_activity"] = sig.get("latest_external_signal_at")
                by_topic_id[tid] = rec

        rows = list(by_topic_id.values())
        if not rows:
            return rows

        momentum_window = self._pre_reset_momentum_window_for_cycle_start(since_date)
        momentum_start_iso = momentum_window.get("window_start_iso") if momentum_window else None
        momentum_cutoff_iso = momentum_window.get("window_cutoff_iso") if momentum_window else None
        momentum_end_iso = momentum_window.get("window_end_iso") if momentum_window else None

        # Attach a few recent distinct author usernames per topic so cards can
        # show quick "who is talking about this" context without opening detail.
        topic_ids = [int(r["id"]) for r in rows if r.get("id") is not None]
        if topic_ids:
            placeholders = ",".join("?" for _ in topic_ids)
            author_rows = self.conn.execute(f"""
                SELECT
                    pt.topic_id,
                    p.author_username,
                    MAX(p.created_at) AS latest_seen
                FROM post_topics pt
                JOIN posts p ON pt.post_id = p.tweet_id
                WHERE pt.topic_id IN ({placeholders})
                  AND p.created_at >= ?
                  AND p.author_username IS NOT NULL
                  AND p.author_username != ''
                GROUP BY pt.topic_id, p.author_username
                ORDER BY pt.topic_id ASC, latest_seen DESC
            """, [*topic_ids, since_date]).fetchall()
            author_map: dict[int, list[str]] = {}
            for ar in author_rows:
                tid = int(ar["topic_id"])
                arr = author_map.setdefault(tid, [])
                uname = str(ar["author_username"] or "").strip()
                if not uname or uname in arr:
                    continue
                if len(arr) < 3:
                    arr.append(uname)

            momentum_counts_map: dict[int, dict] = {}
            if momentum_start_iso and momentum_cutoff_iso and momentum_end_iso:
                momentum_rows = self.conn.execute(f"""
                    SELECT
                        pt.topic_id,
                        SUM(CASE WHEN p.created_at < ? THEN 1 ELSE 0 END) AS pre_reset_post_count,
                        COUNT(DISTINCT CASE
                            WHEN p.created_at < ?
                             AND p.author_username IS NOT NULL
                             AND p.author_username != ''
                            THEN p.author_username
                        END) AS pre_reset_source_count,
                        SUM(CASE WHEN p.created_at >= ? THEN 1 ELSE 0 END) AS post_reset_post_count,
                        COUNT(DISTINCT CASE
                            WHEN p.created_at >= ?
                             AND p.author_username IS NOT NULL
                             AND p.author_username != ''
                            THEN p.author_username
                        END) AS post_reset_source_count
                    FROM post_topics pt
                    JOIN posts p ON pt.post_id = p.tweet_id
                    WHERE pt.topic_id IN ({placeholders})
                      AND p.created_at >= ?
                      AND p.created_at < ?
                    GROUP BY pt.topic_id
                """, [
                    momentum_cutoff_iso,
                    momentum_cutoff_iso,
                    momentum_cutoff_iso,
                    momentum_cutoff_iso,
                    *topic_ids,
                    momentum_start_iso,
                    momentum_end_iso,
                ]).fetchall()
                momentum_counts_map = {
                    int(r["topic_id"]): {
                        "pre_reset_post_count": int(r["pre_reset_post_count"] or 0),
                        "pre_reset_source_count": int(r["pre_reset_source_count"] or 0),
                        "post_reset_post_count": int(r["post_reset_post_count"] or 0),
                        "post_reset_source_count": int(r["post_reset_source_count"] or 0),
                    }
                    for r in momentum_rows
                }

            # Weighted engagement this week (used for material-change re-entry).
            engagement_rows_week = self.conn.execute(f"""
                SELECT pt.topic_id, p.public_metrics_json
                FROM post_topics pt
                JOIN posts p ON pt.post_id = p.tweet_id
                WHERE pt.topic_id IN ({placeholders})
                  AND p.created_at >= ?
            """, [*topic_ids, since_date]).fetchall()
            week_engagement_map: dict[int, int] = {}
            pre_reset_engagement_map: dict[int, int] = {}
            post_reset_engagement_map: dict[int, int] = {}
            topic_engagement_rows_map: dict[int, list] = {}
            topic_metric_coverage_map: dict[int, dict] = {}
            for er in engagement_rows_week:
                tid = int(er["topic_id"])
                cov = topic_metric_coverage_map.setdefault(tid, {
                    "total": 0,
                    "with_json": 0,
                    "with_impression_count": 0,
                })
                cov["total"] += 1
                raw = er["public_metrics_json"]
                if not raw:
                    continue
                try:
                    m = json.loads(raw) if isinstance(raw, str) else raw
                except Exception:
                    continue
                if not isinstance(m, dict):
                    continue
                cov["with_json"] += 1
                if "impression_count" in m and m.get("impression_count") is not None:
                    cov["with_impression_count"] += 1
                score = self._engagement_score_from_metrics(m)
                week_engagement_map[tid] = week_engagement_map.get(tid, 0) + score
                topic_engagement_rows_map.setdefault(tid, []).append({"public_metrics_json": m})

            if momentum_start_iso and momentum_cutoff_iso and momentum_end_iso:
                momentum_engagement_rows = self.conn.execute(f"""
                    SELECT pt.topic_id, p.created_at, p.public_metrics_json
                    FROM post_topics pt
                    JOIN posts p ON pt.post_id = p.tweet_id
                    WHERE pt.topic_id IN ({placeholders})
                      AND p.created_at >= ?
                      AND p.created_at < ?
                """, [*topic_ids, momentum_start_iso, momentum_end_iso]).fetchall()
                for er in momentum_engagement_rows:
                    raw = er["public_metrics_json"]
                    if not raw:
                        continue
                    score = self._engagement_score_from_metrics(raw)
                    if score <= 0:
                        continue
                    tid = int(er["topic_id"])
                    if str(er["created_at"] or "") < momentum_cutoff_iso:
                        pre_reset_engagement_map[tid] = pre_reset_engagement_map.get(tid, 0) + score
                    else:
                        post_reset_engagement_map[tid] = post_reset_engagement_map.get(tid, 0) + score

            # Engagement delta since topic was last covered (if ever).
            engagement_rows_delta = self.conn.execute(f"""
                SELECT pt.topic_id, p.public_metrics_json
                FROM post_topics pt
                JOIN posts p ON pt.post_id = p.tweet_id
                JOIN topics t ON t.id = pt.topic_id
                WHERE pt.topic_id IN ({placeholders})
                  AND t.last_covered_at IS NOT NULL
                  AND p.created_at > t.last_covered_at
            """, topic_ids).fetchall()
            engagement_delta_map: dict[int, int] = {}
            for er in engagement_rows_delta:
                tid = int(er["topic_id"])
                engagement_delta_map[tid] = engagement_delta_map.get(tid, 0) + self._engagement_score_from_metrics(
                    er["public_metrics_json"]
                )

            # Team impression counts this week (from training_impressions)
            impression_rows = self.conn.execute(f"""
                SELECT topic_id,
                       COUNT(*) as total_impressions,
                       COUNT(DISTINCT voter_name) as unique_viewers
                FROM training_impressions
                WHERE topic_id IN ({placeholders})
                  AND shown_at >= ?
                GROUP BY topic_id
            """, [*topic_ids, since_date]).fetchall()
            impression_map = {
                int(r["topic_id"]): {
                    "total": int(r["total_impressions"]),
                    "viewers": int(r["unique_viewers"]),
                }
                for r in impression_rows
            }
        else:
            author_map = {}
            momentum_counts_map = {}
            week_engagement_map = {}
            pre_reset_engagement_map = {}
            post_reset_engagement_map = {}
            engagement_delta_map = {}
            topic_engagement_rows_map = {}
            topic_metric_coverage_map = {}
            impression_map = {}

        engagement_cap = int(getattr(config, "WEEKLY_SCORE_ENGAGEMENT_CAP", 5000))
        engagement_weight = float(getattr(config, "WEEKLY_SCORE_ENGAGEMENT_WEIGHT", 0.01))

        now = datetime.utcnow()
        for row in rows:
            tid = int(row["id"])
            ext = external_map.get(tid, {})
            ext_count = int(ext.get("external_signal_count") or 0)
            latest_external = ext.get("latest_external_signal_at")

            latest = row.get("latest_activity")
            if latest_external and (not latest or str(latest_external) > str(latest)):
                latest = latest_external
                row["latest_activity"] = latest_external
            recency_bonus = 0.0
            if latest:
                dt = self._iso_to_utc_naive(latest)
                if dt is not None:
                    hours_ago = max(0.0, (now - dt).total_seconds() / 3600.0)
                    recency_bonus = max(0.0, 24.0 - min(hours_ago, 24.0))
            week_eng = int(week_engagement_map.get(tid, 0)) if topic_ids else 0
            engagement_bonus = min(week_eng, engagement_cap) * engagement_weight
            imp = impression_map.get(tid, {"total": 0, "viewers": 0})
            impression_bonus = (
                imp["viewers"] * int(getattr(config, "WEEKLY_SCORE_IMPRESSION_UNIQUE_VIEWER_WEIGHT", 40))
                + min(imp["total"], int(getattr(config, "WEEKLY_SCORE_IMPRESSION_TOTAL_CAP", 30)))
                  * int(getattr(config, "WEEKLY_SCORE_IMPRESSION_TOTAL_WEIGHT", 5))
            )
            weekly_score_content = (
                int(row.get("week_post_count") or 0) * 100
                + int(row.get("source_count") or 0) * 35
                + min(int(row.get("total_post_count") or 0), 50)
                + recency_bonus
                + engagement_bonus
            )
            row["weekly_score_content"] = weekly_score_content
            row["weekly_score_impression_bonus"] = impression_bonus
            row["weekly_score"] = weekly_score_content + impression_bonus
            row["topic_state"] = "active" if int(row.get("is_promoted") or 0) else "candidate"
            row["editorial_tier_override"] = (row.get("editorial_tier_override") or "").strip().lower() or None
            row["sample_authors"] = author_map.get(tid, []) if topic_ids else []
            row["external_signal_count"] = ext_count
            row["latest_external_signal_at"] = latest_external
            row["has_external_signal"] = bool(ext_count > 0)
            row["week_engagement_score"] = week_eng
            row["engagement_delta_since_covered"] = int(engagement_delta_map.get(tid, 0)) if topic_ids else 0
            row["impression_total"] = imp["total"]
            row["impression_viewers"] = imp["viewers"]
            momentum_counts = momentum_counts_map.get(tid, {}) if topic_ids else {}
            pre_reset_post_count = int(momentum_counts.get("pre_reset_post_count") or 0)
            pre_reset_source_count = int(momentum_counts.get("pre_reset_source_count") or 0)
            post_reset_post_count = int(momentum_counts.get("post_reset_post_count") or 0)
            post_reset_source_count = int(momentum_counts.get("post_reset_source_count") or 0)
            pre_reset_engagement_score = int(pre_reset_engagement_map.get(tid, 0)) if topic_ids else 0
            post_reset_engagement_score = int(post_reset_engagement_map.get(tid, 0)) if topic_ids else 0

            pre_reset_min_posts = int(getattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MIN_PRE_POSTS", 5))
            pre_reset_min_sources = int(getattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MIN_PRE_SOURCES", 3))
            pre_reset_min_engagement = int(getattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MIN_PRE_ENGAGEMENT", 900))
            post_reset_max_posts = int(getattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MAX_POST_CUTOFF_POSTS", 1))
            post_reset_max_sources = int(getattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MAX_POST_CUTOFF_SOURCES", 1))
            post_reset_max_engagement = int(getattr(config, "WEEKLY_PRE_RESET_DEPRIOR_MAX_POST_CUTOFF_ENGAGEMENT", 250))

            pre_reset_strong = bool(
                pre_reset_post_count >= pre_reset_min_posts
                or pre_reset_source_count >= pre_reset_min_sources
                or pre_reset_engagement_score >= pre_reset_min_engagement
            )
            post_reset_weak = bool(
                post_reset_post_count <= post_reset_max_posts
                and post_reset_source_count <= post_reset_max_sources
                and post_reset_engagement_score <= post_reset_max_engagement
            )
            pre_reset_dominant = bool(
                pre_reset_post_count > post_reset_post_count
                or pre_reset_source_count > post_reset_source_count
                or pre_reset_engagement_score > post_reset_engagement_score
            )
            is_pre_reset_momentum_deprioritized = bool(
                momentum_window
                and pre_reset_strong
                and post_reset_weak
                and pre_reset_dominant
            )

            row["pre_reset_momentum_window_start"] = momentum_start_iso
            row["pre_reset_momentum_cutoff"] = momentum_cutoff_iso
            row["pre_reset_momentum_window_end"] = momentum_end_iso
            row["pre_reset_momentum_uses_previous_cycle"] = bool(
                momentum_window and momentum_window.get("use_previous_cycle")
            )
            row["pre_reset_post_count"] = pre_reset_post_count
            row["pre_reset_source_count"] = pre_reset_source_count
            row["pre_reset_engagement_score"] = pre_reset_engagement_score
            row["post_reset_post_count"] = post_reset_post_count
            row["post_reset_source_count"] = post_reset_source_count
            row["post_reset_engagement_score"] = post_reset_engagement_score
            row["is_pre_reset_momentum_deprioritized"] = is_pre_reset_momentum_deprioritized
            row["pre_reset_deprior_reason"] = (
                "pre_reset_momentum_deprioritized" if is_pre_reset_momentum_deprioritized else None
            )
            cov = topic_metric_coverage_map.get(tid, {"total": 0, "with_json": 0, "with_impression_count": 0}) if topic_ids else {"total": 0, "with_json": 0, "with_impression_count": 0}
            metric_total = int(cov.get("total") or 0) or int(row.get("week_post_count") or 0)
            metric_with_json = int(cov.get("with_json") or 0)
            metric_with_impressions = int(cov.get("with_impression_count") or 0)
            if metric_total <= 0 or metric_with_json <= 0:
                metrics_coverage_state = "none"
            elif metric_with_json >= metric_total and metric_with_impressions >= metric_total:
                metrics_coverage_state = "full"
            else:
                metrics_coverage_state = "partial"
            row["metric_posts_total_week"] = metric_total
            row["metric_posts_with_json"] = metric_with_json
            row["metric_posts_with_impression_count"] = metric_with_impressions
            row["metrics_coverage_state"] = metrics_coverage_state
            row["agg_metrics"] = self._aggregate_metrics_from_rows(
                topic_engagement_rows_map.get(tid, [])
            ) if topic_ids else {"likes": 0, "reposts": 0, "replies": 0, "quotes": 0, "bookmarks": 0, "impressions": 0}

        rows.sort(
            key=lambda r: (
                int(r.get("is_promoted") or 0),
                float(r.get("weekly_score") or 0),
                int(r.get("week_post_count") or 0),
                int(r.get("source_count") or 0),
                str(r.get("latest_activity") or ""),
            ),
            reverse=True,
        )
        return rows

    def get_weekly_prep_sections(
        self,
        since_date: str,
        slide_target: int = 20,
        bullet_target: int = 30,
        allow_candidate_fallback: Optional[bool] = None,
        week_id: Optional[int] = None,
    ) -> dict:
        """
        Build Weekly Prep editorial funnel sections.

        Slide and bullet votes force inclusion. Manual `editorial_tier_override`
        can set slide/bullet/hold/none. Auto-fill prefers promoted topics.
        Candidate fallback is optional and disabled by default (sparse weekly).
        """
        if allow_candidate_fallback is None:
            allow_candidate_fallback = bool(getattr(config, "WEEKLY_PREP_ALLOW_CANDIDATE_FALLBACK", False))

        rows = self.get_weekly_topic_pool(since_date)
        if not rows:
            return {
                "slide_topics": [],
                "bullet_topics": [],
                "unsure_topics": [],
                "candidate_topics": [],
                "deprioritized_topics": [],
                "overflow_promoted_topics": [],
                "all_topics_ranked": [],
                "votes": {},
                "summary": {
                    "slide_count": 0,
                    "bullet_count": 0,
                    "unsure_count": 0,
                    "candidate_count": 0,
                    "deprioritized_count": 0,
                    "promoted_pool_count": 0,
                    "candidate_pool_count": 0,
                    "pre_reset_deprioritized_count": 0,
                    "slide_target": slide_target,
                    "bullet_target": bullet_target,
                    "week_id": week_id,
                    "candidate_fallback_enabled": bool(
                        getattr(config, "WEEKLY_PREP_ALLOW_CANDIDATE_FALLBACK", False)
                    ),
                    "used_candidate_fallback": False,
                },
            }

        topic_ids = [r["id"] for r in rows]
        votes = self.get_votes_for_topics(topic_ids, week_id=week_id)

        def _vote_types(topic_id: int) -> set[str]:
            tv = votes.get(topic_id, [])
            return {str(v.get("vote_type") or "").strip().lower() for v in tv if v.get("vote_type")}

        def _vote_tier(topic_id: int) -> Optional[str]:
            types = _vote_types(topic_id)
            if "slide" in types:
                return "slide"
            if "bullet" in types:
                return "bullet"
            return None

        ranked_rows = []
        for r in rows:
            rid = r["id"]
            vote_types = _vote_types(rid)
            vote_tier = _vote_tier(rid)
            override = (r.get("editorial_tier_override") or "").strip().lower() or None
            if override not in {"slide", "bullet", "hold", "none"}:
                override = None

            effective_tier = None
            tier_source = None
            if vote_tier:
                effective_tier = vote_tier
                tier_source = "vote"
            elif override in {"slide", "bullet"}:
                effective_tier = override
                tier_source = "override"

            suppressed_by_hold = (override == "hold" and vote_tier is None)
            has_skip_vote = "skip" in vote_types
            # Deprioritize only when the topic has skip votes and no competing
            # editorial signal (slide/bullet/unsure). Flag-only co-votes don't
            # count as a positive/neutral keep signal.
            skip_only_votes = has_skip_vote and vote_types.issubset({"skip", "flag"})
            suppressed_by_skip = bool(skip_only_votes and vote_tier is None and override not in {"slide", "bullet"})
            current_week_id = int(week_id) if week_id is not None else None
            last_covered_week_id = r.get("last_covered_week_id")
            try:
                last_covered_week_id = int(last_covered_week_id) if last_covered_week_id is not None else None
            except Exception:
                last_covered_week_id = None
            covered_in_prior_week = bool(
                current_week_id is not None
                and last_covered_week_id is not None
                and last_covered_week_id != current_week_id
            )
            last_covered_at_dt = self._iso_to_utc_naive(r.get("last_covered_at"))
            editorial_tier_set_at_dt = self._iso_to_utc_naive(r.get("editorial_tier_set_at"))
            manual_unhold_signal = bool(
                covered_in_prior_week
                and override is None
                and last_covered_at_dt is not None
                and editorial_tier_set_at_dt is not None
                and editorial_tier_set_at_dt > last_covered_at_dt
            )
            engagement_delta_reentry = bool(
                int(r.get("engagement_delta_since_covered") or 0)
                >= int(getattr(config, "WEEKLY_COVERED_REENTRY_MIN_ENGAGEMENT_DELTA", 1200))
            )
            material_change_reentry = bool(
                (
                    int(r.get("week_post_count") or 0) >= int(getattr(config, "WEEKLY_COVERED_REENTRY_MIN_POSTS", 3))
                    and int(r.get("source_count") or 0) >= int(getattr(config, "WEEKLY_COVERED_REENTRY_MIN_AUTHORS", 2))
                )
                or engagement_delta_reentry
            )
            suppressed_by_covered = bool(
                covered_in_prior_week
                and not material_change_reentry
                and not manual_unhold_signal
                and vote_tier is None
                and override not in {"slide", "bullet"}
            )
            suppressed_by_pre_reset_momentum = bool(
                r.get("is_pre_reset_momentum_deprioritized")
                and vote_tier is None
                and override not in {"slide", "bullet"}
            )
            rec = dict(r)
            rec["vote_types"] = sorted(vote_types)
            rec["vote_tier"] = vote_tier
            rec["effective_tier"] = effective_tier
            rec["effective_tier_source"] = tier_source
            rec["is_vote_forced"] = bool(vote_tier)
            rec["is_override_forced"] = (tier_source == "override")
            rec["is_held"] = suppressed_by_hold
            rec["has_skip_vote"] = has_skip_vote
            rec["is_skip_deprioritized"] = suppressed_by_skip
            rec["was_previously_covered"] = covered_in_prior_week
            rec["manual_unhold_signal"] = manual_unhold_signal
            rec["engagement_delta_reentry"] = engagement_delta_reentry
            rec["material_change_eligible"] = material_change_reentry
            rec["is_covered_deprioritized"] = suppressed_by_covered
            rec["is_pre_reset_momentum_deprioritized"] = suppressed_by_pre_reset_momentum
            has_unsure_vote = "unsure" in vote_types
            rec["has_unsure_vote"] = has_unsure_vote
            rec["is_unsure_needs_discussion"] = bool(
                has_unsure_vote
                and vote_tier is None
                and override not in {"slide", "bullet"}
                and not suppressed_by_hold
            )
            rec["selection_reason"] = rec.get("selection_reason")
            ranked_rows.append(rec)

        def _rank_key(r: dict):
            return (
                float(r.get("weekly_score") or 0),
                int(r.get("week_post_count") or 0),
                int(r.get("source_count") or 0),
                str(r.get("latest_activity") or ""),
            )

        auto_slide_suggested_cap = max(0, int(getattr(config, "WEEKLY_PREP_AUTO_SUGGESTED_MAX", 10)))
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

        def _passes_auto_slide_quality_gate(r: dict) -> bool:
            # Use score excluding page-impression bonus so exposure logging does not
            # force weak topics into the slide suggestions.
            content_score = float(
                r.get("weekly_score_content")
                if r.get("weekly_score_content") is not None
                else float(r.get("weekly_score") or 0) - float(r.get("weekly_score_impression_bonus") or 0)
            )
            if content_score < auto_slide_min_content_score:
                return False
            if int(r.get("week_post_count") or 0) >= auto_slide_min_week_posts:
                return True
            if int(r.get("source_count") or 0) >= auto_slide_min_sources:
                return True
            if int(r.get("week_engagement_score") or 0) >= auto_slide_min_week_engagement:
                return True
            agg = r.get("agg_metrics") or {}
            if int(agg.get("reposts") or 0) >= auto_slide_min_reposts:
                return True
            if int(agg.get("impressions") or 0) >= auto_slide_min_agg_impressions:
                return True
            return False

        slide_topics: list[dict] = []
        bullet_topics: list[dict] = []
        surfaced_ids: set[int] = set()
        auto_slide_suggested_count = 0
        auto_slide_quality_reject_count = 0

        def _append_unique(lst: list[dict], item: dict):
            if item["id"] in surfaced_ids:
                return False
            lst.append(item)
            surfaced_ids.add(item["id"])
            return True

        # Forced slide first (votes, then overrides), preserving rank order.
        forced_slide = [r for r in ranked_rows if r.get("effective_tier") == "slide"]
        forced_slide.sort(key=_rank_key, reverse=True)
        for r in forced_slide:
            r["selection_reason"] = r["selection_reason"] or ("vote_slide" if r["is_vote_forced"] else "override_slide")
            _append_unique(slide_topics, r)

        # Auto-fill slide target from promoted. Candidate fallback is opt-in.
        used_candidate_fallback = False
        promoted_auto = [
            r for r in ranked_rows
            if not r["is_held"]
            and not r.get("is_skip_deprioritized")
            and not r.get("is_covered_deprioritized")
            and not r.get("is_pre_reset_momentum_deprioritized")
            and r["id"] not in surfaced_ids
            and int(r.get("is_promoted") or 0) == 1
            and r.get("effective_tier") != "bullet"
        ]
        promoted_auto.sort(key=_rank_key, reverse=True)
        for r in promoted_auto:
            if len(slide_topics) >= slide_target:
                break
            if auto_slide_suggested_count >= auto_slide_suggested_cap:
                break
            if not _passes_auto_slide_quality_gate(r):
                auto_slide_quality_reject_count += 1
                continue
            r["selection_reason"] = r["selection_reason"] or "auto_promoted_rank"
            if _append_unique(slide_topics, r):
                auto_slide_suggested_count += 1

        if allow_candidate_fallback and len(slide_topics) < slide_target:
            candidate_auto = [
                r for r in ranked_rows
                if not r["is_held"]
                and not r.get("is_skip_deprioritized")
                and not r.get("is_covered_deprioritized")
                and not r.get("is_pre_reset_momentum_deprioritized")
                and r["id"] not in surfaced_ids
                and int(r.get("is_promoted") or 0) == 0
                and r.get("effective_tier") != "bullet"
            ]
            candidate_auto.sort(key=_rank_key, reverse=True)
            for r in candidate_auto:
                if len(slide_topics) >= slide_target:
                    break
                if auto_slide_suggested_count >= auto_slide_suggested_cap:
                    break
                if not _passes_auto_slide_quality_gate(r):
                    continue
                used_candidate_fallback = True
                r["selection_reason"] = r["selection_reason"] or "candidate_fallback"
                if _append_unique(slide_topics, r):
                    auto_slide_suggested_count += 1

        # Forced bullet (votes or overrides), excluding anything already in slide.
        forced_bullet = [r for r in ranked_rows if r.get("effective_tier") == "bullet"]
        forced_bullet.sort(key=_rank_key, reverse=True)
        for r in forced_bullet:
            r["selection_reason"] = r["selection_reason"] or ("vote_bullet" if r["is_vote_forced"] else "override_bullet")
            _append_unique(bullet_topics, r)

        # Auto-fill bullet tier from remaining promoted topics.
        bullet_auto = [
            r for r in ranked_rows
            if not r["is_held"]
            and not r.get("is_skip_deprioritized")
            and not r.get("is_covered_deprioritized")
            and r["id"] not in surfaced_ids
            and int(r.get("is_promoted") or 0) == 1
        ]
        bullet_auto.sort(key=_rank_key, reverse=True)
        for r in bullet_auto:
            if len(bullet_topics) >= bullet_target:
                break
            r["selection_reason"] = r["selection_reason"] or "auto_bullet_overflow"
            _append_unique(bullet_topics, r)

        # Needs Discussion: unsure-voted topics not forced to slide/bullet
        unsure_topics = [
            r for r in ranked_rows
            if r.get("is_unsure_needs_discussion")
            and r["id"] not in surfaced_ids
        ]
        unsure_topics.sort(key=_rank_key, reverse=True)
        for r in unsure_topics:
            r["selection_reason"] = r["selection_reason"] or "unsure_needs_discussion"
            surfaced_ids.add(r["id"])

        candidate_topics = [
            r for r in ranked_rows
            if not r["is_held"]
            and not r.get("is_skip_deprioritized")
            and not r.get("is_covered_deprioritized")
            and not r.get("is_pre_reset_momentum_deprioritized")
            and r["id"] not in surfaced_ids
            and int(r.get("is_promoted") or 0) == 0
        ]
        candidate_topics.sort(key=_rank_key, reverse=True)
        for r in candidate_topics:
            if r["selection_reason"]:
                continue
            if int(r.get("week_post_count") or 0) == 0 and int(r.get("external_signal_count") or 0) > 0:
                r["selection_reason"] = "external_transcription_signal"
            else:
                r["selection_reason"] = "candidate_radar"

        deprioritized_topics = [
            r for r in ranked_rows
            if not r["is_held"]
            and (
                r.get("is_skip_deprioritized")
                or r.get("is_covered_deprioritized")
                or r.get("is_pre_reset_momentum_deprioritized")
            )
            and r["id"] not in surfaced_ids
        ]
        deprioritized_topics.sort(key=_rank_key, reverse=True)
        for r in deprioritized_topics:
            if r.get("is_covered_deprioritized"):
                r["selection_reason"] = r["selection_reason"] or "recently_covered"
            elif r.get("is_pre_reset_momentum_deprioritized"):
                r["selection_reason"] = r["selection_reason"] or "pre_reset_momentum_deprioritized"
            else:
                r["selection_reason"] = r["selection_reason"] or "skip_deprioritized"

        overflow_promoted = [
            r for r in ranked_rows
            if not r["is_held"]
            and not r.get("is_skip_deprioritized")
            and not r.get("is_covered_deprioritized")
            and not r.get("is_pre_reset_momentum_deprioritized")
            and r["id"] not in surfaced_ids
            and int(r.get("is_promoted") or 0) == 1
        ]
        overflow_promoted.sort(key=_rank_key, reverse=True)

        return {
            "slide_topics": slide_topics,
            "bullet_topics": bullet_topics,
            "unsure_topics": unsure_topics,
            "candidate_topics": candidate_topics,
            "deprioritized_topics": deprioritized_topics,
            "overflow_promoted_topics": overflow_promoted,
            "all_topics_ranked": ranked_rows,
            "votes": votes,
            "summary": {
                "slide_count": len(slide_topics),
                "bullet_count": len(bullet_topics),
                "unsure_count": len(unsure_topics),
                "candidate_count": len(candidate_topics),
                "deprioritized_count": len(deprioritized_topics),
                "promoted_pool_count": sum(1 for r in ranked_rows if int(r.get("is_promoted") or 0) == 1),
                "candidate_pool_count": sum(1 for r in ranked_rows if int(r.get("is_promoted") or 0) == 0),
                "slide_target": slide_target,
                "bullet_target": bullet_target,
                "week_id": week_id,
                "covered_deprioritized_count": sum(1 for r in ranked_rows if r.get("is_covered_deprioritized")),
                "pre_reset_deprioritized_count": sum(
                    1 for r in ranked_rows if r.get("is_pre_reset_momentum_deprioritized")
                ),
                "candidate_fallback_enabled": bool(allow_candidate_fallback),
                "used_candidate_fallback": used_candidate_fallback,
                "auto_slide_suggested_cap": auto_slide_suggested_cap,
                "auto_slide_suggested_count": auto_slide_suggested_count,
                "auto_slide_quality_reject_count": auto_slide_quality_reject_count,
            },
        }

    def get_weekly_topics(
        self,
        since_date: str,
        limit: int = 50,
        allow_candidate_fallback: Optional[bool] = None,
    ) -> list[dict]:
        """
        Backward-compatible weekly topic list for legacy UI/export paths.

        Uses the ranked pool and returns promoted-first topics. Candidate fallback
        is optional and defaults to the same sparse-weekly config used by the UI.
        """
        if allow_candidate_fallback is None:
            allow_candidate_fallback = bool(getattr(config, "WEEKLY_PREP_ALLOW_CANDIDATE_FALLBACK", False))

        rows = self.get_weekly_topic_pool(since_date)
        if not rows:
            return []
        promoted = [r for r in rows if int(r.get("is_promoted") or 0) == 1]
        candidates = [r for r in rows if int(r.get("is_promoted") or 0) == 0]

        sort_key = lambda r: (
            float(r.get("weekly_score") or 0),
            int(r.get("week_post_count") or 0),
            int(r.get("source_count") or 0),
            str(r.get("latest_activity") or ""),
        )
        promoted.sort(key=sort_key, reverse=True)
        candidates.sort(key=sort_key, reverse=True)

        selected = promoted[:limit]
        if allow_candidate_fallback and len(selected) < limit:
            need = limit - len(selected)
            for c in candidates[:need]:
                c["selection_reason"] = "candidate_fallback"
            selected.extend(candidates[:need])

        return selected

    def get_trending_today(self, limit: int = 20) -> list[dict]:
        """Topics with activity today, ordered by recency."""
        cur = self.conn.execute("""
            SELECT t.id, t.name, t.description, t.category, t.subcategory,
                   t.post_count as total_post_count,
                   COALESCE(t.is_promoted, 1) as is_promoted,
                   COUNT(pt.post_id) as today_post_count,
                   MAX(p.created_at) as latest_activity
            FROM topics t
            JOIN post_topics pt ON t.id = pt.topic_id
            JOIN posts p ON pt.post_id = p.tweet_id
            WHERE t.is_active = 1
              AND p.created_at >= date('now')
            GROUP BY t.id
            ORDER BY COALESCE(t.is_promoted, 1) DESC, latest_activity DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]

    def get_subcategory_topic_counts(self, category: str, status: str = "all") -> list[dict]:
        """Return topic counts grouped by subcategory within a parent category."""
        where = ["is_active = 1", "category = ?", "subcategory IS NOT NULL"]
        params: list = [category]
        if (status or "").lower() == "promoted":
            where.append("COALESCE(is_promoted, 1) = 1")
        elif (status or "").lower() == "candidate":
            where.append("COALESCE(is_promoted, 1) = 0")

        cur = self.conn.execute(f"""
            SELECT subcategory, COUNT(*) as topic_count,
                   SUM(post_count) as total_posts
            FROM topics
            WHERE {' AND '.join(where)}
            GROUP BY subcategory
            ORDER BY total_posts DESC
        """, params)
        return [dict(row) for row in cur.fetchall()]

    def get_topics_by_subcategory(self, category: str, subcategory: str) -> list[dict]:
        """Return topics filtered by both category and subcategory."""
        cur = self.conn.execute("""
            SELECT t.id, t.name, t.description, t.category, t.subcategory,
                   t.post_count, t.first_seen_at, t.last_seen_at,
                   COALESCE(t.is_promoted, 1) as is_promoted
            FROM topics t
            WHERE t.is_active = 1
              AND t.category = ?
              AND t.subcategory = ?
            ORDER BY COALESCE(t.is_promoted, 1) DESC, t.post_count DESC
        """, (category, subcategory))
        return [dict(row) for row in cur.fetchall()]

    # ── Voting / Triage ─────────────────────────────────────────────────

    def _normalize_skip_reason(self, vote_type: str, skip_reason: Optional[str]) -> Optional[str]:
        """Return canonical skip reason for skip votes; None for non-skip votes."""
        normalized_vote_type = str(vote_type or "").strip().lower()
        if normalized_vote_type != "skip":
            return None

        valid_reasons = {
            str(v).strip().lower()
            for v in getattr(config, "VALID_SKIP_REASONS", {"not_good_fit", "already_covered"})
            if str(v).strip()
        }
        if not valid_reasons:
            valid_reasons = {"not_good_fit", "already_covered"}

        default_reason = str(getattr(config, "DEFAULT_SKIP_REASON", "not_good_fit") or "").strip().lower()
        if default_reason not in valid_reasons:
            default_reason = "not_good_fit"
        if default_reason not in valid_reasons:
            default_reason = next(iter(valid_reasons))

        normalized_reason = str(skip_reason or "").strip().lower()
        if normalized_reason in valid_reasons:
            return normalized_reason
        return default_reason

    def upsert_vote(
        self,
        topic_id: int,
        voter_name: str,
        vote_type: str,
        week_id: Optional[int] = None,
        skip_reason: Optional[str] = None,
    ):
        """Insert or update a vote for a topic. One vote per voter per topic (+ optional week scope)."""
        from datetime import datetime
        now = datetime.utcnow().isoformat()
        normalized_skip_reason = self._normalize_skip_reason(vote_type, skip_reason)
        if week_id is not None:
            prev = self.conn.execute(
                "SELECT vote_type, skip_reason FROM topic_week_votes WHERE week_id = ? AND topic_id = ? AND voter_name = ?",
                (week_id, topic_id, voter_name),
            ).fetchone()
            self.conn.execute("""
                INSERT INTO topic_week_votes (week_id, topic_id, voter_name, vote_type, skip_reason, voted_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(week_id, topic_id, voter_name)
                DO UPDATE SET vote_type = excluded.vote_type,
                              skip_reason = excluded.skip_reason,
                              voted_at = excluded.voted_at
            """, (week_id, topic_id, voter_name, vote_type, normalized_skip_reason, now))
        else:
            prev = self.conn.execute(
                "SELECT vote_type, skip_reason FROM topic_votes WHERE topic_id = ? AND voter_name = ?",
                (topic_id, voter_name),
            ).fetchone()
        previous_vote_type = (prev["vote_type"] if prev else None)
        previous_skip_reason = self._normalize_skip_reason(
            previous_vote_type or "",
            (prev["skip_reason"] if prev and "skip_reason" in prev.keys() else None),
        )
        self.conn.execute("""
            INSERT INTO topic_votes (topic_id, voter_name, vote_type, skip_reason, voted_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(topic_id, voter_name)
            DO UPDATE SET vote_type = ?, skip_reason = ?, voted_at = ?
        """, (topic_id, voter_name, vote_type, normalized_skip_reason, now, vote_type, normalized_skip_reason, now))
        self.conn.execute("""
            INSERT INTO topic_vote_events (
                topic_id, voter_name, action, vote_type, skip_reason, previous_vote_type, previous_skip_reason, created_at
            ) VALUES (?, ?, 'set', ?, ?, ?, ?, ?)
        """, (
            topic_id,
            voter_name,
            vote_type,
            normalized_skip_reason,
            previous_vote_type,
            previous_skip_reason,
            now,
        ))
        self.conn.commit()

    def delete_vote(self, topic_id: int, voter_name: str, week_id: Optional[int] = None):
        """Remove a voter's vote on a topic (toggle-off behavior)."""
        if week_id is not None:
            prev = self.conn.execute(
                "SELECT vote_type, skip_reason FROM topic_week_votes WHERE week_id = ? AND topic_id = ? AND voter_name = ?",
                (week_id, topic_id, voter_name),
            ).fetchone()
            self.conn.execute("""
                DELETE FROM topic_week_votes
                WHERE week_id = ? AND topic_id = ? AND voter_name = ?
            """, (week_id, topic_id, voter_name))
        else:
            prev = self.conn.execute(
                "SELECT vote_type, skip_reason FROM topic_votes WHERE topic_id = ? AND voter_name = ?",
                (topic_id, voter_name),
            ).fetchone()
        self.conn.execute("""
            DELETE FROM topic_votes WHERE topic_id = ? AND voter_name = ?
        """, (topic_id, voter_name))
        if prev:
            from datetime import datetime
            now = datetime.utcnow().isoformat()
            self.conn.execute("""
                INSERT INTO topic_vote_events (
                    topic_id, voter_name, action, vote_type, skip_reason, previous_vote_type, previous_skip_reason, created_at
                ) VALUES (?, ?, 'clear', NULL, NULL, ?, ?, ?)
            """, (
                topic_id,
                voter_name,
                prev["vote_type"],
                self._normalize_skip_reason(prev["vote_type"] or "", prev["skip_reason"]),
                now,
            ))
        self.conn.commit()

    def set_topic_promoted(self, topic_id: int, promoted: bool, actor: str, reason: str = "manual") -> bool:
        """Manually promote/demote a topic, recording an edit history entry."""
        topic = self.get_topic_by_id(topic_id)
        if not topic:
            return False
        old_val = int(topic.get("is_promoted") or 0)
        new_val = 1 if promoted else 0
        if old_val == new_val:
            return True

        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            UPDATE topics
            SET is_promoted = ?,
                promoted_at = CASE WHEN ? = 1 THEN COALESCE(promoted_at, ?) ELSE promoted_at END,
                promotion_reason = ?
            WHERE id = ?
        """, (new_val, new_val, now, reason, topic_id))
        self.conn.execute("""
            INSERT INTO topic_edits (topic_id, edited_by, field, old_value, new_value, edited_at)
            VALUES (?, ?, 'is_promoted', ?, ?, ?)
        """, (topic_id, actor, str(old_val), str(new_val), now))
        self.conn.commit()
        return True

    def set_topic_editorial_tier(self, topic_id: int, tier: str, actor: str) -> bool:
        """
        Set manual editorial tier override.
        Allowed: slide, bullet, hold, none (clears override).
        """
        topic = self.get_topic_by_id(topic_id)
        if not topic:
            return False

        allowed = {"slide", "bullet", "hold", "none"}
        tier = (tier or "").strip().lower()
        if tier not in allowed:
            raise ValueError("Invalid tier")

        old_val = (topic.get("editorial_tier_override") or "").strip().lower() or None
        new_db_val = None if tier == "none" else tier
        if old_val == new_db_val:
            return True

        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            UPDATE topics
            SET editorial_tier_override = ?,
                editorial_tier_set_by = ?,
                editorial_tier_set_at = ?
            WHERE id = ?
        """, (new_db_val, actor, now, topic_id))
        self.conn.execute("""
            INSERT INTO topic_edits (topic_id, edited_by, field, old_value, new_value, edited_at)
            VALUES (?, ?, 'editorial_tier_override', ?, ?, ?)
        """, (topic_id, actor, old_val or "", new_db_val or "", now))
        self.conn.commit()
        return True

    def get_votes_for_topics(self, topic_ids: list, week_id: Optional[int] = None) -> dict:
        """
        Return all votes for a list of topic IDs.
        Returns:
          {topic_id: [{"voter_name": str, "vote_type": str, "skip_reason": str|None, "voted_at": str}, ...]}
        """
        if not topic_ids:
            return {}
        placeholders = ",".join("?" * len(topic_ids))
        if week_id is not None:
            cur = self.conn.execute(
                f"SELECT topic_id, voter_name, vote_type, skip_reason, voted_at FROM topic_week_votes "
                f"WHERE week_id = ? AND topic_id IN ({placeholders}) "
                f"ORDER BY topic_id, voter_name",
                [week_id, *topic_ids]
            )
        else:
            cur = self.conn.execute(
                f"SELECT topic_id, voter_name, vote_type, skip_reason, voted_at FROM topic_votes "
                f"WHERE topic_id IN ({placeholders}) ORDER BY topic_id, voter_name",
                topic_ids
            )
        result = {}
        for row in cur.fetchall():
            tid = row["topic_id"]
            if tid not in result:
                result[tid] = []
            result[tid].append({
                "voter_name": row["voter_name"],
                "vote_type": row["vote_type"],
                "skip_reason": self._normalize_skip_reason(row["vote_type"], row["skip_reason"]),
                "voted_at": row["voted_at"],
            })
        return result

    # ── Training Data ─────────────────────────────────────────────────

    # ── Summary Bullets ─────────────────────────────────────────────

    def get_topics_needing_summary_refresh(self, min_new_posts: int = 3, limit: int = 5) -> list[dict]:
        """Return topics with enough new posts since last summary update."""
        cur = self.conn.execute("""
            SELECT t.id, t.name, t.description, t.category, t.subcategory,
                   t.summary_bullets, t.summary_key_takeaways,
                   t.summary_updated_at, t.summary_lifetime_posts_seen,
                   t.post_count,
                   COUNT(p.tweet_id) as new_posts_since_summary
            FROM topics t
            JOIN post_topics pt ON t.id = pt.topic_id
            JOIN posts p ON pt.post_id = p.tweet_id
            WHERE t.is_active = 1
              AND (t.summary_updated_at IS NULL OR p.created_at > t.summary_updated_at)
            GROUP BY t.id
            HAVING new_posts_since_summary >= ?
            ORDER BY new_posts_since_summary DESC
            LIMIT ?
        """, (min_new_posts, limit))
        return [dict(row) for row in cur.fetchall()]

    def get_all_posts_for_topic(self, topic_id: int) -> list[dict]:
        """Return ALL live posts linked to topic, chronological (oldest first)."""
        cur = self.conn.execute("""
            SELECT p.tweet_id, p.author_username, p.full_text, p.text,
                   p.created_at, p.public_metrics_json
            FROM posts p
            JOIN post_topics pt ON p.tweet_id = pt.post_id
            WHERE pt.topic_id = ?
            ORDER BY p.created_at ASC
        """, (topic_id,))
        return [dict(row) for row in cur.fetchall()]

    def update_topic_summary(self, topic_id: int, description: str,
                             bullets: list, lifetime_seen: int,
                             key_takeaways: list = None):
        """Write new summary fields and update timestamps."""
        now = datetime.utcnow().isoformat()
        kt_json = json.dumps(key_takeaways) if key_takeaways is not None else None
        self.conn.execute("""
            UPDATE topics
            SET description = ?,
                summary_bullets = ?,
                summary_key_takeaways = COALESCE(?, summary_key_takeaways),
                summary_updated_at = ?,
                summary_lifetime_posts_seen = ?
            WHERE id = ?
        """, (description, json.dumps(bullets), kt_json, now, lifetime_seen, topic_id))
        self.conn.commit()

    def get_topic_by_id(self, topic_id: int) -> Optional[dict]:
        """Return a single topic by ID."""
        cur = self.conn.execute("""
            SELECT t.*,
                   COALESCE(t.is_promoted, 1) as is_promoted
            FROM topics t
            WHERE t.id = ?
        """, (topic_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def log_impressions(self, voter_name: str, topic_ids: list):
        """Log that a voter was shown a set of topics (training data)."""
        now = datetime.utcnow().isoformat()
        for tid in topic_ids:
            self.conn.execute(
                "INSERT INTO training_impressions (voter_name, topic_id, shown_at) VALUES (?, ?, ?)",
                (voter_name, tid, now),
            )
        self.conn.commit()

    def save_vote_snapshot(
        self,
        voter_name: str,
        topic_id: int,
        vote_type: str,
        topic_data: dict,
        posts_json: str,
        skip_reason: Optional[str] = None,
    ):
        """Save a self-contained snapshot of topic + posts at vote time."""
        now = datetime.utcnow().isoformat()
        normalized_skip_reason = self._normalize_skip_reason(vote_type, skip_reason)
        self.conn.execute("""
            INSERT INTO training_vote_snapshots
                (voter_name, topic_id, vote_type, skip_reason, snapshot_at,
                 topic_name, topic_description, topic_category, topic_subcategory,
                 topic_post_count, posts_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            voter_name, topic_id, vote_type, normalized_skip_reason, now,
            topic_data.get("name"), topic_data.get("description"),
            topic_data.get("category"), topic_data.get("subcategory"),
            topic_data.get("post_count"), posts_json,
        ))
        self.conn.commit()

    # ── Topic Merging ─────────────────────────────────────────────────

    def merge_topics(self, winner_id: int, loser_ids: list[int], updates: dict = None):
        """Merge loser topics into the winner: move posts, votes, deactivate losers.

        Args:
            winner_id: The topic ID that survives the merge.
            loser_ids: Topic IDs to merge into the winner.
            updates: Optional dict of metadata updates for the winner
                     (name, description, summary_bullets, category, subcategory).
        """
        if not loser_ids:
            return
        placeholders = ",".join("?" * len(loser_ids))

        # Move post_topics links (ignore duplicates)
        for lid in loser_ids:
            self.conn.execute("""
                INSERT OR IGNORE INTO post_topics (post_id, topic_id)
                SELECT post_id, ? FROM post_topics WHERE topic_id = ?
            """, (winner_id, lid))
            self.conn.execute("DELETE FROM post_topics WHERE topic_id = ?", (lid,))

        # Move topic_votes (ignore duplicates — winner's votes take precedence)
        for lid in loser_ids:
            self.conn.execute("""
                INSERT OR IGNORE INTO topic_votes (topic_id, voter_name, vote_type, skip_reason, voted_at)
                SELECT ?, voter_name, vote_type, skip_reason, voted_at FROM topic_votes WHERE topic_id = ?
            """, (winner_id, lid))
            self.conn.execute("DELETE FROM topic_votes WHERE topic_id = ?", (lid,))

        # Move topic_week_votes (INSERT OR IGNORE; winner's votes take precedence on conflict)
        for lid in loser_ids:
            self.conn.execute("""
                INSERT OR IGNORE INTO topic_week_votes (week_id, topic_id, voter_name, vote_type, skip_reason, voted_at)
                SELECT week_id, ?, voter_name, vote_type, skip_reason, voted_at FROM topic_week_votes WHERE topic_id = ?
            """, (winner_id, lid))
            self.conn.execute("DELETE FROM topic_week_votes WHERE topic_id = ?", (lid,))

        # Move topic_week_outcomes (INSERT OR IGNORE)
        for lid in loser_ids:
            self.conn.execute("""
                INSERT OR IGNORE INTO topic_week_outcomes (week_id, topic_id, outcome, resolved_by, resolved_at, notes)
                SELECT week_id, ?, outcome, resolved_by, resolved_at, notes FROM topic_week_outcomes WHERE topic_id = ?
            """, (winner_id, lid))
            self.conn.execute("DELETE FROM topic_week_outcomes WHERE topic_id = ?", (lid,))

        # Move topic_edits (update topic_id to winner, preserves edit history)
        self.conn.execute(
            f"UPDATE topic_edits SET topic_id = ? WHERE topic_id IN ({placeholders})",
            [winner_id, *loser_ids]
        )

        # Clean up vector data for losers (direct DELETE, not sync_topic_vectors)
        self.conn.execute(
            f"DELETE FROM topic_vector_meta WHERE topic_id IN ({placeholders})", loser_ids
        )
        try:
            self.conn.execute(
                f"DELETE FROM topic_vectors WHERE topic_id IN ({placeholders})", loser_ids
            )
        except Exception:
            pass  # topic_vectors may not exist if sqlite-vec not loaded

        # Clear winner's vector_meta to force re-embedding on next full sync
        self.conn.execute("DELETE FROM topic_vector_meta WHERE topic_id = ?", (winner_id,))

        # Merge coverage state: use most recent values among all merged topics
        all_ids = [winner_id, *loser_ids]
        all_ph = ",".join("?" * len(all_ids))
        cur = self.conn.execute(f"""
            SELECT
                MAX(last_covered_at) as max_covered_at,
                MAX(last_covered_week_id) as max_covered_week_id,
                MAX(coverage_cooldown_until) as max_cooldown,
                MAX(last_covered_total_post_count) as max_covered_posts,
                MAX(last_covered_latest_activity) as max_covered_activity
            FROM topics WHERE id IN ({all_ph})
        """, all_ids)
        cov = dict(cur.fetchone())
        self.conn.execute("""
            UPDATE topics SET
                last_covered_at = ?,
                last_covered_week_id = ?,
                coverage_cooldown_until = ?,
                last_covered_total_post_count = ?,
                last_covered_latest_activity = ?
            WHERE id = ?
        """, (
            cov["max_covered_at"],
            cov["max_covered_week_id"],
            cov["max_cooldown"],
            cov["max_covered_posts"],
            cov["max_covered_activity"],
            winner_id,
        ))

        # Apply optional metadata updates to winner
        if updates:
            set_clauses = []
            params = []
            summary_fields = {"description", "summary_bullets", "summary_key_takeaways"}
            summary_changed = False
            for field in ("name", "description", "summary_bullets", "summary_key_takeaways", "category", "subcategory"):
                if field in updates:
                    set_clauses.append(f"{field} = ?")
                    params.append(updates[field])
                    if field in summary_fields:
                        summary_changed = True
            if summary_changed:
                now = datetime.utcnow().isoformat()
                set_clauses.append("summary_updated_at = ?")
                params.append(now)
            if set_clauses:
                params.append(winner_id)
                self.conn.execute(
                    f"UPDATE topics SET {', '.join(set_clauses)} WHERE id = ?",
                    params
                )

        # Set winner as promoted (merged topics should be promoted)
        self.conn.execute("UPDATE topics SET is_promoted = 1 WHERE id = ?", (winner_id,))

        # Deactivate losers
        self.conn.execute(
            f"UPDATE topics SET is_active = 0 WHERE id IN ({placeholders})", loser_ids
        )

        # Recalculate winner's post_count
        self.conn.execute("""
            UPDATE topics SET
                post_count = (SELECT COUNT(*) FROM post_topics WHERE topic_id = ?),
                last_seen_at = (
                    SELECT MAX(p.created_at) FROM posts p
                    JOIN post_topics pt ON p.tweet_id = pt.post_id
                    WHERE pt.topic_id = ?
                )
            WHERE id = ?
        """, (winner_id, winner_id, winner_id))

        self.conn.commit()

    # ── Topic Splitting ──────────────────────────────────────────────────

    def split_topic(self, source_topic_id: int, post_ids_to_move: list,
                    new_topic_data: dict, source_updates: dict = None,
                    split_by: str = "unknown") -> dict:
        """Split posts from a source topic into a new topic.

        Args:
            source_topic_id: Topic to split posts from
            post_ids_to_move: List of post_id (tweet_id) strings to move
            new_topic_data: Dict with name, description, summary_bullets, category, subcategory
            source_updates: Optional dict with description, summary_bullets for source topic
            split_by: User who performed the split

        Returns:
            Dict with new_topic_id and counts

        Raises:
            ValueError on validation failure (rollback guaranteed)
        """
        if not post_ids_to_move:
            raise ValueError("No posts selected to move")

        cur = self.conn.cursor()
        try:
            # Validate all post_ids belong to source topic
            placeholders = ",".join("?" * len(post_ids_to_move))
            cur.execute(f"""
                SELECT post_id FROM post_topics
                WHERE topic_id = ? AND post_id IN ({placeholders})
            """, [source_topic_id, *post_ids_to_move])
            found = {row[0] for row in cur.fetchall()}
            missing = set(post_ids_to_move) - found
            if missing:
                raise ValueError(f"Posts not linked to source topic: {', '.join(list(missing)[:5])}")

            # Validate at least 1 post remains in source
            cur.execute("SELECT COUNT(*) FROM post_topics WHERE topic_id = ?", (source_topic_id,))
            total_in_source = cur.fetchone()[0]
            if len(post_ids_to_move) >= total_in_source:
                raise ValueError("Cannot move all posts — source topic must keep at least 1 post")

            # Validate name uniqueness
            new_name = (new_topic_data.get("name") or "").strip()
            if not new_name:
                raise ValueError("New topic name is required")
            cur.execute("SELECT id FROM topics WHERE name = ? AND is_active = 1", (new_name,))
            if cur.fetchone():
                raise ValueError(f"Topic name '{new_name}' already exists")

            # Validate taxonomy
            new_cat = new_topic_data.get("category")
            new_subcat = new_topic_data.get("subcategory")
            if new_cat and new_cat not in config.TAXONOMY:
                raise ValueError(f"Invalid category: {new_cat}")
            if new_subcat and new_subcat not in config.SUBCATEGORY_TO_PARENT:
                raise ValueError(f"Invalid subcategory: {new_subcat}")

            now = datetime.utcnow().isoformat()

            # Create new topic
            name_norm = re.sub(r"[^a-z0-9 ]+", "", new_name.lower()).strip()
            new_bullets = new_topic_data.get("summary_bullets")
            bullets_json = json.dumps(new_bullets) if isinstance(new_bullets, list) else new_bullets
            new_kt = new_topic_data.get("summary_key_takeaways")
            kt_json = json.dumps(new_kt) if isinstance(new_kt, list) else new_kt
            has_summary = bullets_json or kt_json

            cur.execute("""
                INSERT INTO topics (name, name_norm, description, category, subcategory,
                    post_count, first_seen_at, last_seen_at, is_active, is_promoted,
                    promoted_at, promotion_reason, created_source,
                    summary_bullets, summary_key_takeaways, summary_updated_at)
                VALUES (?, ?, ?, ?, ?, 0, ?, ?, 1, 1, ?, 'split', 'split', ?, ?, ?)
            """, (
                new_name, name_norm,
                new_topic_data.get("description", ""),
                new_cat, new_subcat,
                now, now, now,
                bullets_json, kt_json, now if has_summary else None,
            ))
            new_topic_id = cur.lastrowid

            # Move post_topics links
            for pid in post_ids_to_move:
                cur.execute(
                    "UPDATE post_topics SET topic_id = ? WHERE post_id = ? AND topic_id = ?",
                    (new_topic_id, pid, source_topic_id)
                )

            # Recount source topic
            cur.execute("""
                SELECT COUNT(*) as cnt, MAX(p.created_at) as latest
                FROM post_topics pt JOIN posts p ON p.tweet_id = pt.post_id
                WHERE pt.topic_id = ?
            """, (source_topic_id,))
            src_stats = cur.fetchone()
            cur.execute("UPDATE topics SET post_count = ?, last_seen_at = ? WHERE id = ?",
                        (src_stats[0], src_stats[1], source_topic_id))

            # Count new topic
            cur.execute("""
                SELECT COUNT(*) as cnt, MAX(p.created_at) as latest
                FROM post_topics pt JOIN posts p ON p.tweet_id = pt.post_id
                WHERE pt.topic_id = ?
            """, (new_topic_id,))
            new_stats = cur.fetchone()
            cur.execute("UPDATE topics SET post_count = ?, last_seen_at = ? WHERE id = ?",
                        (new_stats[0], new_stats[1], new_topic_id))

            # Apply source updates if provided
            if source_updates:
                src_summary_changed = False
                if "description" in source_updates:
                    cur.execute("UPDATE topics SET description = ? WHERE id = ?",
                                (source_updates["description"], source_topic_id))
                    src_summary_changed = True
                if "summary_bullets" in source_updates:
                    sb = source_updates["summary_bullets"]
                    sb_json = json.dumps(sb) if isinstance(sb, list) else sb
                    cur.execute("UPDATE topics SET summary_bullets = ? WHERE id = ?",
                                (sb_json, source_topic_id))
                    src_summary_changed = True
                if "summary_key_takeaways" in source_updates:
                    skt = source_updates["summary_key_takeaways"]
                    skt_json = json.dumps(skt) if isinstance(skt, list) else skt
                    cur.execute("UPDATE topics SET summary_key_takeaways = ? WHERE id = ?",
                                (skt_json, source_topic_id))
                    src_summary_changed = True
                if src_summary_changed:
                    cur.execute("UPDATE topics SET summary_updated_at = ? WHERE id = ?",
                                (now, source_topic_id))

            # Clear vector caches for both topics
            cur.execute("DELETE FROM topic_vector_meta WHERE topic_id IN (?, ?)",
                        (source_topic_id, new_topic_id))
            try:
                cur.execute("DELETE FROM topic_vectors WHERE topic_id IN (?, ?)",
                            (source_topic_id, new_topic_id))
            except Exception:
                pass  # topic_vectors is a vec0 virtual table, may not support DELETE

            # Record edit
            source_topic = self.get_topic_by_id(source_topic_id)
            source_name = source_topic["name"] if source_topic else str(source_topic_id)
            cur.execute("""
                INSERT INTO topic_edits (topic_id, edited_by, field, old_value, new_value, edited_at)
                VALUES (?, ?, 'split', ?, ?, ?)
            """, (source_topic_id, split_by, source_name, new_name, now))

            self.conn.commit()

            return {
                "new_topic_id": new_topic_id,
                "posts_moved": len(post_ids_to_move),
                "source_remaining": src_stats[0],
            }

        except Exception:
            self.conn.rollback()
            raise

    def split_topic_multi(self, source_topic_id: int, new_topics_data: list,
                          source_updates: dict = None,
                          split_by: str = "unknown") -> dict:
        """Split posts from a source topic into N new topics.

        Args:
            source_topic_id: Topic to split posts from
            new_topics_data: List of dicts, each with post_ids, name, description,
                             summary_bullets, summary_key_takeaways, category, subcategory
            source_updates: Optional dict with description, summary_bullets, summary_key_takeaways
            split_by: User who performed the split

        Returns:
            Dict with new_topic_ids list and counts
        """
        if not new_topics_data:
            raise ValueError("No new topics specified")

        # Collect all post IDs across groups and validate no overlaps
        all_moving_ids = []
        seen_ids = set()
        for i, grp in enumerate(new_topics_data):
            grp_ids = grp.get("post_ids", [])
            if not grp_ids:
                raise ValueError(f"Group {i + 1} has no posts selected")
            for pid in grp_ids:
                if pid in seen_ids:
                    raise ValueError(f"Post {pid} assigned to multiple groups")
                seen_ids.add(pid)
            all_moving_ids.extend(grp_ids)

        # Validate all names provided and unique
        names = []
        for grp in new_topics_data:
            name = (grp.get("name") or "").strip()
            if not name:
                raise ValueError("Each new topic requires a name")
            names.append(name)
        if len(set(n.lower() for n in names)) != len(names):
            raise ValueError("New topic names must be unique")

        cur = self.conn.cursor()
        try:
            # Validate all post_ids belong to source topic
            placeholders = ",".join("?" * len(all_moving_ids))
            cur.execute(f"""
                SELECT post_id FROM post_topics
                WHERE topic_id = ? AND post_id IN ({placeholders})
            """, [source_topic_id, *all_moving_ids])
            found = {row[0] for row in cur.fetchall()}
            missing = set(all_moving_ids) - found
            if missing:
                raise ValueError(f"Posts not linked to source topic: {', '.join(list(missing)[:5])}")

            # Validate source keeps at least 1 post
            cur.execute("SELECT COUNT(*) FROM post_topics WHERE topic_id = ?", (source_topic_id,))
            total_in_source = cur.fetchone()[0]
            if len(all_moving_ids) >= total_in_source:
                raise ValueError("Cannot move all posts — source topic must keep at least 1 post")

            # Validate name uniqueness vs DB
            for name in names:
                cur.execute("SELECT id FROM topics WHERE name = ? AND is_active = 1", (name,))
                if cur.fetchone():
                    raise ValueError(f"Topic name '{name}' already exists")

            now = datetime.utcnow().isoformat()
            new_topic_ids = []

            for grp in new_topics_data:
                grp_name = grp["name"].strip()
                name_norm = re.sub(r"[^a-z0-9 ]+", "", grp_name.lower()).strip()

                # Validate taxonomy
                grp_cat = grp.get("category")
                grp_subcat = grp.get("subcategory")
                if grp_cat and grp_cat not in config.TAXONOMY:
                    raise ValueError(f"Invalid category: {grp_cat}")
                if grp_subcat and grp_subcat not in config.SUBCATEGORY_TO_PARENT:
                    raise ValueError(f"Invalid subcategory: {grp_subcat}")

                # Prepare summaries
                new_bullets = grp.get("summary_bullets")
                bullets_json = json.dumps(new_bullets) if isinstance(new_bullets, list) else new_bullets
                new_kt = grp.get("summary_key_takeaways")
                kt_json = json.dumps(new_kt) if isinstance(new_kt, list) else new_kt
                has_summary = bullets_json or kt_json

                # Create topic
                cur.execute("""
                    INSERT INTO topics (name, name_norm, description, category, subcategory,
                        post_count, first_seen_at, last_seen_at, is_active, is_promoted,
                        promoted_at, promotion_reason, created_source,
                        summary_bullets, summary_key_takeaways, summary_updated_at)
                    VALUES (?, ?, ?, ?, ?, 0, ?, ?, 1, 1, ?, 'split', 'split', ?, ?, ?)
                """, (
                    grp_name, name_norm,
                    grp.get("description", ""),
                    grp_cat, grp_subcat,
                    now, now, now,
                    bullets_json, kt_json, now if has_summary else None,
                ))
                new_id = cur.lastrowid
                new_topic_ids.append(new_id)

                # Move post_topics links
                for pid in grp["post_ids"]:
                    cur.execute(
                        "UPDATE post_topics SET topic_id = ? WHERE post_id = ? AND topic_id = ?",
                        (new_id, pid, source_topic_id)
                    )

                # Count new topic
                cur.execute("""
                    SELECT COUNT(*) as cnt, MAX(p.created_at) as latest
                    FROM post_topics pt JOIN posts p ON p.tweet_id = pt.post_id
                    WHERE pt.topic_id = ?
                """, (new_id,))
                new_stats = cur.fetchone()
                cur.execute("UPDATE topics SET post_count = ?, last_seen_at = ? WHERE id = ?",
                            (new_stats[0], new_stats[1], new_id))

            # Recount source topic
            cur.execute("""
                SELECT COUNT(*) as cnt, MAX(p.created_at) as latest
                FROM post_topics pt JOIN posts p ON p.tweet_id = pt.post_id
                WHERE pt.topic_id = ?
            """, (source_topic_id,))
            src_stats = cur.fetchone()
            cur.execute("UPDATE topics SET post_count = ?, last_seen_at = ? WHERE id = ?",
                        (src_stats[0], src_stats[1], source_topic_id))

            # Apply source updates
            if source_updates:
                src_summary_changed = False
                if "description" in source_updates:
                    cur.execute("UPDATE topics SET description = ? WHERE id = ?",
                                (source_updates["description"], source_topic_id))
                    src_summary_changed = True
                if "summary_bullets" in source_updates:
                    sb = source_updates["summary_bullets"]
                    sb_json = json.dumps(sb) if isinstance(sb, list) else sb
                    cur.execute("UPDATE topics SET summary_bullets = ? WHERE id = ?",
                                (sb_json, source_topic_id))
                    src_summary_changed = True
                if "summary_key_takeaways" in source_updates:
                    skt = source_updates["summary_key_takeaways"]
                    skt_json = json.dumps(skt) if isinstance(skt, list) else skt
                    cur.execute("UPDATE topics SET summary_key_takeaways = ? WHERE id = ?",
                                (skt_json, source_topic_id))
                    src_summary_changed = True
                if src_summary_changed:
                    cur.execute("UPDATE topics SET summary_updated_at = ? WHERE id = ?",
                                (now, source_topic_id))

            # Clear vector caches for source + all new topics
            all_topic_ids = [source_topic_id] + new_topic_ids
            id_placeholders = ",".join("?" * len(all_topic_ids))
            cur.execute(f"DELETE FROM topic_vector_meta WHERE topic_id IN ({id_placeholders})",
                        all_topic_ids)
            try:
                cur.execute(f"DELETE FROM topic_vectors WHERE topic_id IN ({id_placeholders})",
                            all_topic_ids)
            except Exception:
                pass

            # Record edits
            source_topic = self.get_topic_by_id(source_topic_id)
            source_name = source_topic["name"] if source_topic else str(source_topic_id)
            new_names = ", ".join(grp["name"].strip() for grp in new_topics_data)
            cur.execute("""
                INSERT INTO topic_edits (topic_id, edited_by, field, old_value, new_value, edited_at)
                VALUES (?, ?, 'split_multi', ?, ?, ?)
            """, (source_topic_id, split_by, source_name, new_names, now))

            self.conn.commit()

            return {
                "new_topic_ids": new_topic_ids,
                "posts_moved": len(all_moving_ids),
                "source_remaining": src_stats[0],
            }

        except Exception:
            self.conn.rollback()
            raise

    # ── Stats ────────────────────────────────────────────────────────────

    def get_stats_extended(self) -> dict:
        """Extended stats including topic counts for the weekly prep view."""
        base = self.get_stats()
        cur = self.conn.execute("""
            SELECT COUNT(*) as total_topics FROM topics WHERE is_active = 1
        """)
        base["total_topics"] = cur.fetchone()[0]
        cur = self.conn.execute("""
            SELECT
                SUM(CASE WHEN is_active = 1 AND COALESCE(is_promoted, 1) = 1 THEN 1 ELSE 0 END) as promoted_topics,
                SUM(CASE WHEN is_active = 1 AND COALESCE(is_promoted, 1) = 0 THEN 1 ELSE 0 END) as candidate_topics
            FROM topics
        """)
        row = cur.fetchone()
        base["promoted_topics"] = (row["promoted_topics"] if row and row["promoted_topics"] is not None else 0)
        base["candidate_topics"] = (row["candidate_topics"] if row and row["candidate_topics"] is not None else 0)
        return base

    # ------------------------------------------------------------------
    # State management (since_id tracking)
    # ------------------------------------------------------------------
    def get_last_since_id(self) -> Optional[str]:
        """Get the last since_id used for fetching."""
        return self.get_state_value("last_since_id")

    def set_last_since_id(self, since_id: str):
        """Set the since_id for the next fetch."""
        self.set_state_value("last_since_id", since_id)

    def get_state_value(self, key: str) -> Optional[str]:
        """Read a state key."""
        cur = self.conn.execute("SELECT value FROM state WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    def set_state_value(self, key: str, value: str):
        """Upsert a state key."""
        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            INSERT INTO state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = ?
        """, (key, value, now, value, now))
        self.conn.commit()

    # ------------------------------------------------------------------
    # Fetch history
    # ------------------------------------------------------------------
    def record_fetch(
        self,
        status: str = "success",
        tweets_fetched: int = 0,
        tweets_new: int = 0,
        tweets_relevant: int = 0,
        since_id: Optional[str] = None,
        newest_id: Optional[str] = None,
        pages_fetched: int = 0,
        error_message: Optional[str] = None,
        topics_created: int = 0,
        topics_matched: int = 0,
        topics_promoted: int = 0,
    ) -> int:
        """Record a fetch cycle in history. Returns the row id."""
        now = datetime.utcnow().isoformat()
        cur = self.conn.execute("""
            INSERT INTO fetch_history
                (started_at, completed_at, status, tweets_fetched,
                 tweets_new, tweets_relevant, since_id, newest_id,
                 pages_fetched, error_message,
                 topics_created, topics_matched, topics_promoted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now, now, status, tweets_fetched, tweets_new,
            tweets_relevant, since_id, newest_id,
            pages_fetched, error_message,
            topics_created, topics_matched, topics_promoted,
        ))
        self.conn.commit()
        return cur.lastrowid

    def get_fetch_history(self, limit: int = 50) -> list[dict]:
        """Return recent fetch history rows."""
        cur = self.conn.execute("""
            SELECT * FROM fetch_history
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # API Usage Tracking
    # ------------------------------------------------------------------
    def record_api_usage(
        self,
        service: str,
        operation: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cost_usd: float = 0.0,
        model: str = None,
        batch_size: int = 0,
    ):
        """Record an API call's token usage and cost."""
        self.conn.execute("""
            INSERT INTO api_usage
                (service, operation, input_tokens, output_tokens,
                 cost_usd, model, batch_size)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (service, operation, input_tokens, output_tokens,
              cost_usd, model, batch_size))
        self.conn.commit()

    def get_api_usage_stats(self) -> dict:
        """Return API usage summaries for dashboard display."""
        cur = self.conn.execute("""
            SELECT
                COALESCE(SUM(cost_usd), 0) as total_cost,
                COALESCE(SUM(input_tokens), 0) as total_input,
                COALESCE(SUM(output_tokens), 0) as total_output,
                COUNT(*) as total_calls
            FROM api_usage
        """)
        all_time = dict(cur.fetchone())

        cur = self.conn.execute("""
            SELECT
                COALESCE(SUM(cost_usd), 0) as week_cost,
                COALESCE(SUM(input_tokens), 0) as week_input,
                COALESCE(SUM(output_tokens), 0) as week_output,
                COUNT(*) as week_calls
            FROM api_usage
            WHERE timestamp >= date('now', '-7 days')
        """)
        this_week = dict(cur.fetchone())

        return {"all_time": all_time, "this_week": this_week}

    def get_cost_tracker_data(self, assumed_lookup_cost_per_post: float = 0.005) -> dict:
        """Return comprehensive cost breakdown for the Cost Tracker view."""

        def _adjust_cost(row):
            """Apply assumed per-post cost for metrics_refresh rows that recorded $0."""
            cost = row["total_cost"]
            items = row.get("total_items") or 0
            if row.get("operation") == "metrics_refresh" and cost == 0 and items > 0:
                return items * assumed_lookup_cost_per_post
            return cost

        # --- Daily costs (last 14 days) ---
        cur = self.conn.execute("""
            SELECT
                DATE(timestamp) AS day,
                service,
                operation,
                COUNT(*) AS calls,
                COALESCE(SUM(input_tokens), 0) AS input_tokens,
                COALESCE(SUM(output_tokens), 0) AS output_tokens,
                COALESCE(SUM(batch_size), 0) AS total_items,
                COALESCE(SUM(cost_usd), 0) AS total_cost
            FROM api_usage
            WHERE timestamp >= date('now', '-14 days')
            GROUP BY day, service, operation
            ORDER BY day DESC, service, operation
        """)
        daily_raw = [dict(r) for r in cur.fetchall()]

        # Build daily aggregates
        day_map: dict[str, dict] = {}
        for r in daily_raw:
            day = r["day"]
            if day not in day_map:
                day_map[day] = {"day": day, "cycles": 0, "x_api_cost": 0.0, "anthropic_cost": 0.0, "total_cost": 0.0}
            adj = _adjust_cost(r)
            if r["service"] == "x_api":
                day_map[day]["x_api_cost"] += adj
                if r["operation"] == "fetch_timeline":
                    day_map[day]["cycles"] += r["calls"]
            else:
                day_map[day]["anthropic_cost"] += adj
            day_map[day]["total_cost"] += adj

        daily = sorted(day_map.values(), key=lambda d: d["day"], reverse=True)
        for d in daily:
            d["x_api_cost"] = round(d["x_api_cost"], 4)
            d["anthropic_cost"] = round(d["anthropic_cost"], 4)
            d["total_cost"] = round(d["total_cost"], 4)

        # --- By operation (all time) ---
        cur = self.conn.execute("""
            SELECT
                operation,
                service,
                model,
                COUNT(*) AS total_calls,
                COALESCE(SUM(input_tokens), 0) AS total_input_tokens,
                COALESCE(SUM(output_tokens), 0) AS total_output_tokens,
                COALESCE(SUM(batch_size), 0) AS total_items,
                COALESCE(SUM(cost_usd), 0) AS total_cost
            FROM api_usage
            GROUP BY operation, service, model
            ORDER BY total_cost DESC
        """)
        by_operation_raw = [dict(r) for r in cur.fetchall()]

        # Compute adjusted costs
        grand_total = 0.0
        for op in by_operation_raw:
            adj = _adjust_cost(op)
            op["recorded_cost"] = round(op["total_cost"], 4)
            op["adjusted_cost"] = round(adj, 4)
            grand_total += adj

        # Compute percentages
        for op in by_operation_raw:
            op["pct"] = round(op["adjusted_cost"] / grand_total * 100, 1) if grand_total > 0 else 0.0

        # Re-sort by adjusted cost descending
        by_operation_raw.sort(key=lambda o: o["adjusted_cost"], reverse=True)

        # --- By service ---
        svc_map: dict[str, float] = {}
        for op in by_operation_raw:
            svc = op["service"]
            svc_map[svc] = svc_map.get(svc, 0.0) + op["adjusted_cost"]

        by_service = []
        for svc, cost in sorted(svc_map.items(), key=lambda x: x[1], reverse=True):
            by_service.append({
                "service": svc,
                "cost": round(cost, 4),
                "pct": round(cost / grand_total * 100, 1) if grand_total > 0 else 0.0,
            })

        # --- Totals ---
        today_cost = day_map.get(datetime.utcnow().strftime("%Y-%m-%d"), {}).get("total_cost", 0.0)

        last_7d = sum(d["total_cost"] for d in daily[:7])
        last_30d_cost = grand_total  # all-time serves as 30d proxy until we have 30d of data

        # Compute actual 30d cost if we have data
        cur = self.conn.execute("""
            SELECT
                operation, service,
                COALESCE(SUM(batch_size), 0) AS total_items,
                COALESCE(SUM(cost_usd), 0) AS total_cost
            FROM api_usage
            WHERE timestamp >= date('now', '-30 days')
            GROUP BY operation, service
        """)
        last_30d_cost = 0.0
        for r in [dict(row) for row in cur.fetchall()]:
            last_30d_cost += _adjust_cost(r)

        # --- Projections (7-day rolling average) ---
        days_with_data = len([d for d in daily if d["total_cost"] > 0])
        days_sampled = min(days_with_data, 7) or 1
        recent_days = [d for d in daily if d["total_cost"] > 0][:days_sampled]
        avg_daily = sum(d["total_cost"] for d in recent_days) / days_sampled if recent_days else 0.0

        return {
            "daily": daily,
            "by_operation": by_operation_raw,
            "by_service": by_service,
            "projections": {
                "avg_daily": round(avg_daily, 2),
                "weekly": round(avg_daily * 7, 2),
                "monthly": round(avg_daily * 30, 2),
                "days_sampled": days_sampled,
            },
            "totals": {
                "today": round(today_cost, 2),
                "last_7d": round(last_7d, 2),
                "last_30d": round(last_30d_cost, 2),
                "all_time": round(grand_total, 2),
            },
        }

    # ------------------------------------------------------------------
    # Data Archiving (retention)
    # ------------------------------------------------------------------
    def archive_old_posts(self, days: int = 14) -> dict:
        """
        Delete posts older than `days` and their post_topics links.
        Recalculate topic stats. Returns summary.
        """
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

        cur = self.conn.execute(
            "SELECT COUNT(*) FROM posts WHERE created_at < ?", (cutoff,)
        )
        post_count = cur.fetchone()[0]

        if post_count == 0:
            return {"archived_posts": 0, "cutoff": cutoff}

        logger.info(f"Archiving {post_count} posts older than {cutoff[:10]}")

        # Preserve relevant posts in archived_posts before deletion
        now_iso = datetime.utcnow().isoformat()
        self.conn.execute("""
            INSERT OR IGNORE INTO archived_posts
                (tweet_id, author_username, full_text, created_at,
                 category, subcategory, relevance_reasoning,
                 public_metrics_json, archived_at)
            SELECT tweet_id, author_username, full_text, created_at,
                   category, subcategory, relevance_reasoning,
                   public_metrics_json, ?
            FROM posts
            WHERE created_at < ? AND is_relevant = 1
        """, (now_iso, cutoff))

        # Delete post_topics links for old posts
        self.conn.execute("""
            DELETE FROM post_topics
            WHERE post_id IN (
                SELECT tweet_id FROM posts WHERE created_at < ?
            )
        """, (cutoff,))

        # Delete old posts
        self.conn.execute(
            "DELETE FROM posts WHERE created_at < ?", (cutoff,)
        )

        # Recalculate topic post_counts
        self.conn.execute("""
            UPDATE topics SET
                post_count = (SELECT COUNT(*) FROM post_topics WHERE topic_id = topics.id)
        """)

        # Deactivate topics with 0 remaining posts
        self.conn.execute("""
            UPDATE topics SET is_active = 0
            WHERE post_count = 0 AND is_active = 1
        """)

        self.conn.commit()

        # Reclaim disk space (important for RPi)
        try:
            self.conn.execute("VACUUM")
        except sqlite3.OperationalError:
            pass  # VACUUM can fail in WAL mode with other connections

        return {"archived_posts": post_count, "cutoff": cutoff}

    # ------------------------------------------------------------------
    # Sortable topic queries (for topics browse page)
    # ------------------------------------------------------------------
    def get_topics_sorted(
        self,
        category: str = None,
        subcategory: str = None,
        sort: str = "popular_week",
        status: str = "all",
        limit: int = 100,
    ) -> list[dict]:
        """Return topics with flexible sort order for browse view."""
        where_clauses = ["t.is_active = 1"]
        params = []
        if category:
            where_clauses.append("t.category = ?")
            params.append(category)
        if subcategory:
            where_clauses.append("t.subcategory = ?")
            params.append(subcategory)
        status_clause, status_params = self._topic_status_filter_clause(status)
        if status_clause:
            where_clauses.append(status_clause)
            params.extend(status_params)

        where = " AND ".join(where_clauses)

        if sort == "popular_week":
            # Join to count posts this week
            params.append(limit)
            cur = self.conn.execute(f"""
                SELECT t.id, t.name, t.description, t.category, t.subcategory,
                       t.post_count, t.first_seen_at, t.last_seen_at,
                       COALESCE(t.is_promoted, 1) as is_promoted,
                       COUNT(CASE WHEN p.created_at >= date('now', '-7 days') THEN 1 END) as week_posts
                FROM topics t
                LEFT JOIN post_topics pt ON t.id = pt.topic_id
                LEFT JOIN posts p ON pt.post_id = p.tweet_id
                WHERE {where}
                GROUP BY t.id
                ORDER BY COALESCE(t.is_promoted, 1) DESC, week_posts DESC, t.post_count DESC
                LIMIT ?
            """, params)
        else:
            if sort == "alpha":
                order = "COALESCE(t.is_promoted, 1) DESC, t.name ASC"
            elif sort == "recent":
                order = "COALESCE(t.is_promoted, 1) DESC, t.last_seen_at DESC"
            else:  # popular_all
                order = "COALESCE(t.is_promoted, 1) DESC, t.post_count DESC"
            params.append(limit)
            cur = self.conn.execute(f"""
                SELECT t.id, t.name, t.description, t.category, t.subcategory,
                       t.post_count, t.first_seen_at, t.last_seen_at,
                       COALESCE(t.is_promoted, 1) as is_promoted
                FROM topics t
                WHERE {where}
                ORDER BY {order}
                LIMIT ?
            """, params)

        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------
    def get_user_by_username(self, username: str) -> Optional[dict]:
        """Return user dict or None."""
        cur = self.conn.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_user_by_id(self, user_id: int) -> Optional[dict]:
        """Return user dict or None."""
        cur = self.conn.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_all_users(self) -> list[dict]:
        """Return all users."""
        cur = self.conn.execute("SELECT id, username, display_name, is_admin, created_at FROM users ORDER BY id")
        return [dict(row) for row in cur.fetchall()]

    def count_users(self) -> int:
        """Return the number of configured dashboard users."""
        cur = self.conn.execute("SELECT COUNT(*) FROM users")
        return int(cur.fetchone()[0])

    def has_users(self) -> bool:
        """Return True when at least one dashboard user exists."""
        return self.count_users() > 0

    def create_user(
        self,
        username: str,
        display_name: str,
        password: str,
        *,
        is_admin: bool = False,
    ) -> dict:
        """Create a dashboard user with an explicit password."""
        normalized_username = config.normalize_username(username)
        display_name = str(display_name or "").strip()
        password = str(password or "")
        if not normalized_username:
            raise ValueError("username is required")
        if not display_name:
            raise ValueError("display_name is required")
        if not password:
            raise ValueError("password is required")
        if self.get_user_by_username(normalized_username):
            raise ValueError(f"user '{normalized_username}' already exists")

        cur = self.conn.execute(
            """
            INSERT INTO users (username, display_name, password_hash, is_admin)
            VALUES (?, ?, ?, ?)
            """,
            (
                normalized_username,
                display_name,
                generate_password_hash(password),
                1 if is_admin else 0,
            ),
        )
        self.conn.commit()
        created = self.get_user_by_id(int(cur.lastrowid))
        return dict(created) if created else {
            "id": int(cur.lastrowid),
            "username": normalized_username,
            "display_name": display_name,
            "is_admin": 1 if is_admin else 0,
        }

    def create_session(self, user_id: int) -> str:
        """Create a new session token for a user. Returns the token."""
        token = secrets.token_urlsafe(32)
        now = datetime.utcnow()
        expires = now + timedelta(days=config.SESSION_MAX_AGE_DAYS)
        self.conn.execute(
            "INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (token, user_id, now.isoformat(), expires.isoformat()),
        )
        self.conn.commit()
        return token

    def get_session(self, token: str) -> Optional[dict]:
        """Return session+user dict if token valid and not expired, else None."""
        now = datetime.utcnow().isoformat()
        cur = self.conn.execute("""
            SELECT s.token, s.user_id, s.expires_at,
                   u.username, u.display_name, u.is_admin
            FROM sessions s
            JOIN users u ON s.user_id = u.id
            WHERE s.token = ? AND s.expires_at > ?
        """, (token, now))
        row = cur.fetchone()
        return dict(row) if row else None

    def delete_session(self, token: str):
        """Delete a session (logout)."""
        self.conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        self.conn.commit()

    def delete_expired_sessions(self):
        """Remove expired sessions."""
        now = datetime.utcnow().isoformat()
        self.conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
        self.conn.commit()

    def update_user_password(self, user_id: int, new_hash: str):
        """Update a user's password hash."""
        self.conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?", (new_hash, user_id)
        )
        self.conn.commit()

    def seed_users(self, users_list: list, default_password: str) -> list[dict]:
        """Legacy helper retained for backwards-compatible migrations only."""
        created = []
        for u in users_list:
            display_name = u["display_name"]
            # Check if any user with this display_name already exists
            cur = self.conn.execute(
                "SELECT id FROM users WHERE display_name = ?", (display_name,)
            )
            if cur.fetchone():
                continue  # Already exists

            username = config.normalize_username(display_name)
            suffix = 1
            while self.get_user_by_username(username):
                suffix += 1
                username = f"{config.normalize_username(display_name)}_{suffix}"

            self.create_user(
                username=username,
                display_name=display_name,
                password=default_password,
                is_admin=bool(u.get("is_admin", 0)),
            )
            created.append({"username": username, "display_name": display_name})
        return created

    # ------------------------------------------------------------------
    # User Topics (creation pipeline)
    # ------------------------------------------------------------------
    def create_user_topic(self, name: str, description: str,
                          category: str, subcategory: str,
                          created_by: str) -> int:
        """Insert a user-created topic request. Returns the user_topics ID."""
        now = datetime.utcnow().isoformat()
        cur = self.conn.execute("""
            INSERT INTO user_topics (name, description, category, subcategory,
                                     created_by, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?)
        """, (name, description, category, subcategory, created_by, now))
        self.conn.commit()
        return cur.lastrowid

    def update_user_topic_status(self, ut_id: int, status: str,
                                  error_message: str = None,
                                  topic_id: int = None):
        """Update user topic pipeline status."""
        now = datetime.utcnow().isoformat()
        activated = now if status == "active" else None
        self.conn.execute("""
            UPDATE user_topics SET status = ?, error_message = ?,
                                   topic_id = ?, activated_at = ?
            WHERE id = ?
        """, (status, error_message, topic_id, activated, ut_id))
        self.conn.commit()

    def get_user_topics(self, status: str = None, limit: int = 50) -> list[dict]:
        """Return user-created topics with optional status filter."""
        if status:
            cur = self.conn.execute("""
                SELECT * FROM user_topics WHERE status = ?
                ORDER BY created_at DESC LIMIT ?
            """, (status, limit))
        else:
            cur = self.conn.execute("""
                SELECT * FROM user_topics ORDER BY created_at DESC LIMIT ?
            """, (limit,))
        return [dict(row) for row in cur.fetchall()]

    def get_user_topic_by_id(self, ut_id: int) -> Optional[dict]:
        """Return a single user topic by ID."""
        cur = self.conn.execute("SELECT * FROM user_topics WHERE id = ?", (ut_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Topic Editing
    # ------------------------------------------------------------------
    def update_topic(self, topic_id: int, updates: dict, edited_by: str):
        """Update topic fields and record edit history."""
        topic = self.get_topic_by_id(topic_id)
        if not topic:
            return

        now = datetime.utcnow().isoformat()
        allowed_fields = {"name", "description", "category", "subcategory",
                          "summary_bullets", "summary_key_takeaways"}
        summary_fields = {"description", "summary_bullets", "summary_key_takeaways"}
        summary_changed = False

        for field, new_val in updates.items():
            if field not in allowed_fields:
                continue
            old_val = topic.get(field)
            if str(old_val or "") == str(new_val or ""):
                continue  # No change

            # Record edit history
            self.conn.execute("""
                INSERT INTO topic_edits (topic_id, edited_by, field, old_value, new_value, edited_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (topic_id, edited_by, field, old_val, new_val, now))

            # Update the topic
            if field == "name":
                self.conn.execute(
                    "UPDATE topics SET name = ?, name_norm = ? WHERE id = ?",
                    (new_val, self._normalize_topic_name(str(new_val or "")), topic_id)
                )
            else:
                self.conn.execute(
                    f"UPDATE topics SET {field} = ? WHERE id = ?", (new_val, topic_id)
                )

            if field in summary_fields:
                summary_changed = True

        # Update summary_updated_at when any summary field changed
        # (prevents auto-refresh from immediately overwriting human edits)
        if summary_changed:
            self.conn.execute(
                "UPDATE topics SET summary_updated_at = ? WHERE id = ?", (now, topic_id)
            )

        self.conn.commit()

    def get_topic_edit_history(self, topic_id: int, limit: int = 50) -> list[dict]:
        """Return edit history for a topic."""
        cur = self.conn.execute("""
            SELECT * FROM topic_edits WHERE topic_id = ?
            ORDER BY edited_at DESC LIMIT ?
        """, (topic_id, limit))
        return [dict(row) for row in cur.fetchall()]

    def get_recent_activity(self, limit: int = 50) -> list[dict]:
        """Return combined recent activity (creations, edits, and vote actions)."""
        # Get recent user topics
        user_topics = self.conn.execute("""
            SELECT 'creation' as activity_type, ut.name as topic_name,
                   ut.created_by as user_name, ut.status, ut.error_message,
                   ut.created_at as timestamp, ut.id as ref_id,
                   ut.topic_id, ut.category,
                   COALESCE(t.transcription_status, 'none') as transcription_status,
                   t.transcription_workflow as transcription_workflow
            FROM user_topics ut
            LEFT JOIN topics t ON t.id = ut.topic_id
            ORDER BY ut.created_at DESC LIMIT ?
        """, (limit,)).fetchall()

        # Get recent edits
        edits = self.conn.execute("""
            SELECT 'edit' as activity_type, t.name as topic_name,
                   te.edited_by as user_name, te.field as status,
                   (te.old_value || ' -> ' || te.new_value) as error_message,
                   te.edited_at as timestamp, te.id as ref_id,
                   te.topic_id, t.category
            FROM topic_edits te
            JOIN topics t ON te.topic_id = t.id
            ORDER BY te.edited_at DESC LIMIT ?
        """, (limit,)).fetchall()

        # Vote actions (all users; includes changes and clears)
        vote_events = self.conn.execute("""
            SELECT 'vote' as activity_type,
                   t.name as topic_name,
                   tve.voter_name as user_name,
                   COALESCE(tve.vote_type, 'clear') as status,
                   NULL as error_message,
                   tve.created_at as timestamp,
                   tve.id as ref_id,
                   tve.topic_id,
                   t.category,
                   tve.action as vote_action,
                   tve.vote_type as vote_type,
                   tve.skip_reason as skip_reason,
                   tve.previous_vote_type as previous_vote_type,
                   tve.previous_skip_reason as previous_skip_reason
            FROM topic_vote_events tve
            JOIN topics t ON tve.topic_id = t.id
            ORDER BY tve.created_at DESC LIMIT ?
        """, (limit,)).fetchall()

        # Combine and sort
        combined = [dict(r) for r in user_topics] + [dict(r) for r in edits] + [dict(r) for r in vote_events]
        combined.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return combined[:limit]

    def get_activity_summary(self) -> dict:
        """Summary metrics for the Pipeline/Activity page."""
        out = {}

        last_fetch = self.conn.execute("""
            SELECT id, started_at, completed_at, status, tweets_fetched, tweets_new,
                   tweets_relevant, error_message
            FROM fetch_history
            ORDER BY id DESC
            LIMIT 1
        """).fetchone()
        out["last_fetch"] = dict(last_fetch) if last_fetch else None

        out["topic_pool"] = {
            "promoted": 0,
            "candidate": 0,
            "total": 0,
        }
        try:
            stats = self.get_stats_extended()
            out["topic_pool"] = {
                "promoted": int(stats.get("promoted_topics") or 0),
                "candidate": int(stats.get("candidate_topics") or 0),
                "total": int(stats.get("total_topics") or 0),
            }
        except Exception:
            pass

        def _int_state(key: str) -> int:
            try:
                return int(self.get_state_value(key) or 0)
            except Exception:
                return 0

        out["parser"] = {
            "last_topic_match_parse_failures": _int_state("last_topic_match_parse_failures"),
            "last_topic_match_guardrail_rejects": _int_state("last_topic_match_guardrail_rejects"),
            "last_topic_match_reused_new_suggestions": _int_state("last_topic_match_reused_new_suggestions"),
        }

        return out

    # ------------------------------------------------------------------
    # Admin maintenance: content reset (preserve auth by default)
    # ------------------------------------------------------------------
    def reset_content_data(
        self,
        preserve_sessions: bool = True,
        dry_run: bool = False,
        vacuum: bool = False,
    ) -> dict:
        """Clear operational/content data while preserving user credentials.

        Preserves:
        - users table (credentials)
        - sessions table by default (optional to clear)
        - state.flask_secret_key
        - state.taxonomy_version

        Clears:
        - posts/topics/linking/votes
        - fetch history + API usage
        - archived/training data
        - user topic requests + edit history
        - vector index tables (best effort if sqlite-vec is available)
        - state.last_since_id
        """
        table_order = [
            "topic_edits",
            "training_impressions",
            "training_vote_snapshots",
            "archived_posts",
            "api_usage",
            "fetch_history",
            "topic_vote_events",
            "topic_votes",
            "post_topics",
            "user_topics",
            "topics",
            "posts",
            "topic_vector_meta",
        ]
        if not preserve_sessions:
            table_order.insert(0, "sessions")

        autoinc_tables = [
            "fetch_history",
            "api_usage",
            "training_impressions",
            "training_vote_snapshots",
            "topics",
            "user_topics",
            "topic_edits",
            "topic_vote_events",
        ]
        if not preserve_sessions:
            # sessions is not AUTOINCREMENT, so nothing to add.
            pass

        def _safe_count(table: str):
            try:
                cur = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
                return int(cur.fetchone()[0])
            except sqlite3.Error as e:
                return f"error: {e}"

        state_rows = self.conn.execute(
            "SELECT key, value FROM state ORDER BY key"
        ).fetchall()
        state_before = {r["key"]: r["value"] for r in state_rows}

        summary = {
            "db_path": self.db_path,
            "dry_run": dry_run,
            "preserve_sessions": preserve_sessions,
            "tables_before": {},
            "state_before_keys": sorted(state_before.keys()),
            "state_deleted_keys": [],
            "vector_rows_before": None,
            "vector_rows_deleted": None,
            "vector_reset_error": None,
            "vacuumed": False,
        }

        for table in table_order:
            summary["tables_before"][table] = _safe_count(table)

        # Best-effort count for sqlite-vec table (requires extension loaded)
        try:
            import sqlite_vec
            sqlite_vec.load(self.conn)
            summary["vector_rows_before"] = _safe_count("topic_vectors")
            vector_ready = True
        except Exception as e:
            summary["vector_rows_before"] = "unavailable"
            summary["vector_reset_error"] = str(e)
            vector_ready = False

        if "last_since_id" in state_before:
            summary["state_deleted_keys"].append("last_since_id")

        if dry_run:
            return summary

        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")

            for table in table_order:
                cur.execute(f"DELETE FROM {table}")

            if vector_ready:
                # vec0 supports DELETE; keep schema so next fetch can repopulate.
                if summary["vector_rows_before"] not in (None, "unavailable"):
                    summary["vector_rows_deleted"] = summary["vector_rows_before"]
                cur.execute("DELETE FROM topic_vectors")

            cur.execute("DELETE FROM state WHERE key = 'last_since_id'")

            # Reset AUTOINCREMENT counters for cleared content tables.
            placeholders = ",".join("?" for _ in autoinc_tables)
            cur.execute(
                f"DELETE FROM sqlite_sequence WHERE name IN ({placeholders})",
                autoinc_tables,
            )

            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

        # Shrink/truncate WAL if possible after large delete.
        try:
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass

        if vacuum:
            try:
                self.conn.execute("VACUUM")
                summary["vacuumed"] = True
            except sqlite3.Error as e:
                summary["vacuumed"] = False
                if not summary["vector_reset_error"]:
                    summary["vector_reset_error"] = f"VACUUM failed: {e}"

        self.conn.commit()
        return summary

    # ------------------------------------------------------------------
    # Engagement Metrics Refresh
    # ------------------------------------------------------------------

    def get_posts_needing_metrics_refresh(
        self,
        max_post_age_days: int = 7,
        limit: int = 300,
        active_topic_ids: set = None,
        stable_threshold: int = 3,
    ) -> list[dict]:
        """Return posts eligible for metrics refresh using tiered age-based backoff.

        Tiers:
          < 24h  — every fetch cycle (no backoff)
          1-2d   — every 4 hours
          2-7d   — every 12 hours
        Posts in active topics (with new posts this cycle) skip backoff.
        """
        now = datetime.utcnow()
        max_age_cutoff = (now - timedelta(days=max_post_age_days)).isoformat()
        one_day_ago = (now - timedelta(hours=24)).isoformat()
        two_days_ago = (now - timedelta(hours=48)).isoformat()
        four_hours_ago = (now - timedelta(hours=4)).isoformat()
        twelve_hours_ago = (now - timedelta(hours=12)).isoformat()

        if active_topic_ids:
            active_placeholders = ",".join("?" for _ in active_topic_ids)
            active_clause = f"OR t.id IN ({active_placeholders})"
            active_case = f"CASE WHEN t.id IN ({active_placeholders}) THEN 1 ELSE 0 END"
            active_params = list(active_topic_ids)
        else:
            active_clause = ""
            active_case = "0"
            active_params = []

        query = f"""
            SELECT
                p.tweet_id,
                p.public_metrics_json,
                p.metrics_refreshed_at,
                p.created_at,
                MAX({active_case}) AS is_active_topic,
                MAX(COALESCE(t.is_promoted, 1)) AS is_promoted_topic
            FROM posts p
            JOIN post_topics pt ON p.tweet_id = pt.post_id
            JOIN topics t ON pt.topic_id = t.id
            WHERE p.is_relevant = 1 AND t.is_active = 1
              AND p.created_at >= ?
              AND COALESCE(p.metrics_unchanged_count, 0) < ?
              AND (
                (p.created_at >= ?)
                OR (p.created_at >= ? AND (p.metrics_refreshed_at IS NULL OR p.metrics_refreshed_at < ?))
                OR (p.created_at < ? AND (p.metrics_refreshed_at IS NULL OR p.metrics_refreshed_at < ?))
                {active_clause}
              )
            GROUP BY p.tweet_id, p.public_metrics_json, p.metrics_refreshed_at, p.created_at
            ORDER BY is_active_topic DESC,
                     is_promoted_topic DESC,
                     p.created_at DESC,
                     p.metrics_refreshed_at ASC
            LIMIT ?
        """
        params = (
            active_params  # for CASE
            + [max_age_cutoff, stable_threshold,
               one_day_ago,
               two_days_ago, four_hours_ago,
               two_days_ago, twelve_hours_ago]
            + active_params  # for OR clause
            + [limit]
        )
        cur = self.conn.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

    def update_post_metrics(
        self,
        tweet_id: str,
        new_metrics: dict,
        changed: bool,
    ):
        """Write updated public_metrics and tracking fields for a post."""
        now = datetime.utcnow().isoformat()
        if changed:
            self.conn.execute("""
                UPDATE posts
                SET public_metrics_json = ?,
                    metrics_refreshed_at = ?,
                    metrics_unchanged_count = 0,
                    metrics_last_changed_at = ?
                WHERE tweet_id = ?
            """, (json.dumps(new_metrics), now, now, tweet_id))
        else:
            self.conn.execute("""
                UPDATE posts
                SET metrics_refreshed_at = ?,
                    metrics_unchanged_count = COALESCE(metrics_unchanged_count, 0) + 1
                WHERE tweet_id = ?
            """, (now, tweet_id))

    def get_topic_ids_for_posts(self, tweet_ids: list[str]) -> set:
        """Return the set of topic IDs linked to the given post IDs."""
        if not tweet_ids:
            return set()
        placeholders = ",".join("?" for _ in tweet_ids)
        cur = self.conn.execute(
            f"SELECT DISTINCT topic_id FROM post_topics WHERE post_id IN ({placeholders})",
            tweet_ids,
        )
        return {row[0] for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
