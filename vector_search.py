"""Vector similarity search for topic candidate pre-filtering.

Uses sentence-transformers (all-MiniLM-L6-v2) for embeddings and
sqlite-vec (vec0 virtual table) for k-NN storage and retrieval.

Integrated into topic_matcher.py Pass 2: instead of sending ALL active
topics to Sonnet, we pre-filter to the top-K most semantically similar
candidates per post, then union across the batch.

Falls back gracefully if dependencies are unavailable (missing packages,
corrupted vec table, etc.) — topic_matcher uses all topics as before.
"""
import hashlib
import logging
from datetime import datetime
from typing import Optional

import numpy as np

from . import config

logger = logging.getLogger("x_feed_intel")


class TopicVectorIndex:
    """Manages topic embeddings in sqlite-vec for fast candidate retrieval.

    Singleton — one instance per fetch cycle, reuses the embedding model
    and sqlite-vec extension across all batches.
    """

    _instance: Optional["TopicVectorIndex"] = None

    def __init__(self, conn):
        self._conn = conn
        self._model = None
        self._vec_ready = False
        self._setup_tables()

    @classmethod
    def get_instance(cls, conn) -> "TopicVectorIndex":
        """Get or create the singleton instance."""
        if cls._instance is None:
            cls._instance = cls(conn)
        return cls._instance

    @classmethod
    def reset(cls):
        """Reset singleton (for testing)."""
        cls._instance = None

    # ------------------------------------------------------------------
    # Schema setup
    # ------------------------------------------------------------------
    def _setup_tables(self):
        """Create vec0 virtual table and metadata table if they don't exist."""
        try:
            import sqlite_vec
            sqlite_vec.load(self._conn)

            dim = config.VECTOR_EMBEDDING_DIM
            self._conn.execute(f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS topic_vectors
                USING vec0(topic_id INTEGER PRIMARY KEY, embedding float[{dim}])
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS topic_vector_meta (
                    topic_id    INTEGER PRIMARY KEY,
                    text_hash   TEXT NOT NULL,
                    embedded_at TEXT NOT NULL
                )
            """)
            self._conn.commit()
            self._vec_ready = True
            logger.info("sqlite-vec loaded, topic_vectors table ready")
        except Exception as e:
            logger.warning(f"sqlite-vec setup failed (vector search disabled): {e}")
            self._vec_ready = False

    # ------------------------------------------------------------------
    # Model loading (lazy)
    # ------------------------------------------------------------------
    def _ensure_model(self):
        """Lazy-load the sentence-transformers model on first use."""
        if self._model is not None:
            return self._model

        from sentence_transformers import SentenceTransformer

        model_name = config.VECTOR_MODEL_NAME
        logger.info(f"Loading embedding model {model_name}...")
        self._model = SentenceTransformer(model_name)
        logger.info("Embedding model loaded")
        return self._model

    # ------------------------------------------------------------------
    # Topic vector sync (incremental)
    # ------------------------------------------------------------------
    def sync_topic_vectors(self, active_topics: list, db) -> None:
        """Sync vec0 table with current active topics.

        Only recomputes embeddings for topics whose composite text
        (name + description + recent post excerpts) has changed.

        Args:
            active_topics: List of topic dicts from db.get_active_topics()
            db: Database instance (for get_posts_for_topic)
        """
        if not self._vec_ready:
            raise RuntimeError("sqlite-vec not available")

        model = self._ensure_model()

        # Build composite text and hash for each active topic
        needed = {}  # topic_id -> (composite_text, hash)
        for t in active_topics:
            text = self._build_topic_text(t, db)
            h = hashlib.md5(text.encode()).hexdigest()
            needed[t["id"]] = (text, h)

        # Get existing hashes from metadata table
        rows = self._conn.execute(
            "SELECT topic_id, text_hash FROM topic_vector_meta"
        ).fetchall()
        existing = {r[0]: r[1] for r in rows}

        # Determine what needs embedding
        to_embed = []
        to_embed_ids = []
        for tid, (text, h) in needed.items():
            if existing.get(tid) != h:
                to_embed.append(text)
                to_embed_ids.append(tid)

        # Remove stale vectors (topics no longer active)
        stale_ids = set(existing.keys()) - set(needed.keys())
        for sid in stale_ids:
            self._conn.execute(
                "DELETE FROM topic_vectors WHERE topic_id = ?", (sid,)
            )
            self._conn.execute(
                "DELETE FROM topic_vector_meta WHERE topic_id = ?", (sid,)
            )

        # Batch embed new/changed topics
        if to_embed:
            logger.info(
                f"Computing embeddings for {len(to_embed)} topics "
                f"({len(stale_ids)} stale removed)"
            )
            embeddings = model.encode(to_embed, show_progress_bar=False)
            now = datetime.utcnow().isoformat()

            for i, tid in enumerate(to_embed_ids):
                vec_blob = self._to_blob(embeddings[i])
                # vec0 doesn't support UPDATE — delete then insert
                self._conn.execute(
                    "DELETE FROM topic_vectors WHERE topic_id = ?", (tid,)
                )
                self._conn.execute(
                    "INSERT INTO topic_vectors (topic_id, embedding) VALUES (?, ?)",
                    (tid, vec_blob),
                )
                # Upsert metadata
                self._conn.execute(
                    """INSERT INTO topic_vector_meta (topic_id, text_hash, embedded_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(topic_id) DO UPDATE
                       SET text_hash = excluded.text_hash,
                           embedded_at = excluded.embedded_at""",
                    (tid, needed[tid][1], now),
                )

            self._conn.commit()
        else:
            if stale_ids:
                self._conn.commit()
            logger.info(
                f"All {len(active_topics)} topic vectors up to date"
                + (f" ({len(stale_ids)} stale removed)" if stale_ids else "")
            )

    # ------------------------------------------------------------------
    # K-NN retrieval
    # ------------------------------------------------------------------
    def get_batch_candidates(
        self,
        posts: list,
        all_topics: list,
        top_k: int = None,
    ) -> set:
        """Return union of top-K topic IDs for a batch of posts.

        Args:
            posts: List of post dicts (must have full_text or text)
            all_topics: Full list of active topics (for fallback info)
            top_k: Candidates per post (default from config)

        Returns:
            Set of topic IDs that are the best candidates for this batch.
        """
        if not self._vec_ready:
            raise RuntimeError("sqlite-vec not available")

        if top_k is None:
            top_k = config.VECTOR_TOP_K

        model = self._ensure_model()

        # Embed all posts in one call
        post_texts = [self._post_text(p) for p in posts]
        post_embeddings = model.encode(post_texts, show_progress_bar=False)

        # Query top-K for each post, collect union
        candidate_ids = set()
        # Also track distances for potential re-ranking
        candidate_min_dist = {}

        for emb in post_embeddings:
            vec_blob = self._to_blob(emb)
            rows = self._conn.execute(
                """SELECT topic_id, distance
                   FROM topic_vectors
                   WHERE embedding MATCH ? AND k = ?""",
                (vec_blob, top_k),
            ).fetchall()

            for r in rows:
                tid = r[0]
                dist = r[1]
                candidate_ids.add(tid)
                # Track minimum distance across all posts
                if tid not in candidate_min_dist or dist < candidate_min_dist[tid]:
                    candidate_min_dist[tid] = dist

        # Cap union size if needed
        max_union = config.VECTOR_MAX_UNION_SIZE
        if len(candidate_ids) > max_union:
            # Keep top N by smallest minimum distance
            sorted_candidates = sorted(
                candidate_min_dist.items(), key=lambda x: x[1]
            )
            candidate_ids = {tid for tid, _ in sorted_candidates[:max_union]}

        logger.info(
            f"Vector search: {len(candidate_ids)} candidate topics "
            f"(from {len(posts)} posts, K={top_k})"
        )
        return candidate_ids

    def search_topic_ids_by_text(self, query_text: str, k: int) -> list[tuple[int, float]]:
        """Search topic vectors by free-text query and return (topic_id, distance)."""
        text = (query_text or "").strip()
        if not text:
            return []
        if not self._vec_ready:
            raise RuntimeError("sqlite-vec not available")

        k = max(1, int(k))
        model = self._ensure_model()
        emb = model.encode([text], show_progress_bar=False)[0]
        vec_blob = self._to_blob(emb)

        rows = self._conn.execute(
            """SELECT topic_id, distance
               FROM topic_vectors
               WHERE embedding MATCH ? AND k = ?""",
            (vec_blob, k),
        ).fetchall()
        return [(int(r[0]), float(r[1])) for r in rows]

    # ------------------------------------------------------------------
    # Rebuild (for maintenance/recovery)
    # ------------------------------------------------------------------
    def rebuild(self):
        """Drop and rebuild the vector index from scratch."""
        self._conn.execute("DROP TABLE IF EXISTS topic_vectors")
        self._conn.execute("DROP TABLE IF EXISTS topic_vector_meta")
        self._conn.commit()
        self._vec_ready = False
        self._setup_tables()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _build_topic_text(topic: dict, db) -> str:
        """Build composite text for topic embedding.

        Includes topic name + description + up to N recent post excerpts.
        This means topic vectors naturally evolve as new posts are linked.
        """
        text = f"{topic['name']} — {topic.get('description') or ''}"

        # Enrich with recent post excerpts
        snippet_count = config.VECTOR_POST_SNIPPETS
        snippet_chars = config.VECTOR_SNIPPET_CHARS
        try:
            posts = db.get_posts_for_topic(topic["id"], limit=snippet_count)
            snippets = []
            for p in posts:
                raw = (p.get("full_text") or p.get("text") or "").strip()
                if raw:
                    snippets.append(raw[:snippet_chars])
            if snippets:
                text += " | Recent: " + " / ".join(snippets)
        except Exception:
            pass  # If post fetch fails, use name+desc only

        return text

    @staticmethod
    def _post_text(post: dict) -> str:
        """Extract text from a post dict for embedding."""
        return (post.get("full_text") or post.get("text") or "").strip()

    @staticmethod
    def _to_blob(embedding) -> bytes:
        """Convert numpy array to bytes for sqlite-vec."""
        return np.array(embedding, dtype=np.float32).tobytes()
