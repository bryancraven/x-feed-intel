"""Training data collector — captures vote context snapshots."""
import json
import logging

logger = logging.getLogger("x_feed_intel")


def snapshot_vote(db, topic_id: int, voter_name: str, vote_type: str, skip_reason: str | None = None):
    """
    Capture a self-contained snapshot of the topic and its posts at vote time.
    Called from the POST /api/vote handler after the vote is persisted.

    This is non-critical — wrapped in try/except so it never breaks the vote flow.
    """
    try:
        # Get topic metadata
        topic = db.get_topic_by_id(topic_id)
        if not topic:
            logger.warning(f"Training snapshot: topic {topic_id} not found")
            return

        # Get posts linked to this topic (up to 50)
        raw_posts = db.get_posts_for_topic(topic_id, limit=50)

        # Slim down post data for the snapshot
        posts = []
        for p in raw_posts:
            metrics = None
            raw_metrics = p.get("public_metrics_json")
            if raw_metrics and isinstance(raw_metrics, str):
                try:
                    metrics = json.loads(raw_metrics)
                except (json.JSONDecodeError, TypeError):
                    pass

            posts.append({
                "tweet_id": p.get("tweet_id"),
                "author_username": p.get("author_username"),
                "full_text": p.get("full_text") or p.get("text", ""),
                "created_at": p.get("created_at"),
                "category": p.get("category"),
                "relevance_reasoning": p.get("relevance_reasoning"),
                "public_metrics": metrics,
            })

        db.save_vote_snapshot(
            voter_name=voter_name,
            topic_id=topic_id,
            vote_type=vote_type,
            skip_reason=skip_reason,
            topic_data=topic,
            posts_json=json.dumps(posts),
        )

        logger.debug(
            f"Training snapshot: topic={topic_id} voter={voter_name} "
            f"vote={vote_type} posts={len(posts)}"
        )
    except Exception as e:
        # Training data collection is non-critical — never break the vote flow
        logger.warning(f"Training snapshot failed (non-fatal): {e}")
