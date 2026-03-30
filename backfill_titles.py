#!/usr/bin/env python3
"""Backfill topic titles — rewrite existing topic names as executive claim titles."""
import argparse
import json
import logging
import re
import sys
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


import anthropic

import config
from database import get_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("x_feed_intel.backfill_titles")

# Thread-local DB connections for parallel mode
_thread_local = threading.local()

TITLE_REWRITE_PROMPT = """Given a topic with its current name, description, key takeaways, supporting bullets, and sample posts, rewrite the topic name as a standalone executive claim title.

Rules:
- The title should be an informative, complete assertion that an executive can understand without additional context
- 6-12 words, entity-first
- Present tense for ongoing situations, past tense for completed events
- Include specific entities, model numbers, quantified claims where available
- Draw from the key takeaways — the title should capture the "so what"
- Title Case, use "&" not "and", cut filler words

Output ONLY the new title string, no quotes or explanation."""


def _get_thread_db():
    """Get a per-thread DB connection."""
    if not hasattr(_thread_local, "db"):
        _thread_local.db = get_db()
    return _thread_local.db


def _build_user_prompt(topic, posts):
    """Build the user prompt with topic context and sample posts."""
    parts = [f"Current name: {topic['name']}"]

    if topic.get("description"):
        parts.append(f"Description: {topic['description']}")

    if topic.get("summary_key_takeaways"):
        try:
            kts = json.loads(topic["summary_key_takeaways"])
            if kts:
                parts.append("Key takeaways:\n" + "\n".join(f"- {kt}" for kt in kts))
        except (json.JSONDecodeError, TypeError):
            pass

    if topic.get("summary_bullets"):
        try:
            bullets = json.loads(topic["summary_bullets"])
            if bullets:
                parts.append("Supporting bullets:\n" + "\n".join(f"- {b}" for b in bullets))
        except (json.JSONDecodeError, TypeError):
            pass

    if topic.get("category"):
        parts.append(f"Category: {topic['category']}")
    if topic.get("subcategory"):
        parts.append(f"Subcategory: {topic['subcategory']}")

    if posts:
        post_lines = []
        for p in posts[:5]:
            text = (p.get("full_text") or p.get("text") or "")[:200]
            author = p.get("author_username", "unknown")
            post_lines.append(f"  @{author}: {text}")
        parts.append("Sample posts:\n" + "\n".join(post_lines))

    return "\n\n".join(parts)


def _apply_title_guardrails(name):
    """Validate a rewritten title. Returns cleaned name or None if rejected."""
    if not name:
        return None

    clean = name.strip().strip('"\'')
    clean = re.sub(r"\s+", " ", clean).strip(" -\u2013\u2014:;,.")

    words = re.findall(r"[A-Za-z0-9$%]+", clean)
    word_count = len(words)

    if word_count < 3:
        logger.info('Rejected too-short rewritten title: "%s"', clean)
        return None
    if word_count > 14:
        logger.info('Rejected too-long rewritten title (%d words): "%s"', word_count, clean)
        return None

    generic = {"ai news", "nvidia updates", "china ai", "cloud computing", "tech news"}
    normalized = re.sub(r"[^a-z0-9]+", " ", clean.lower()).strip()
    if normalized in generic:
        logger.info('Rejected generic rewritten title: "%s"', clean)
        return None

    return clean


def _rewrite_title(topic, posts, client, model):
    """Call Opus to rewrite a single topic title."""
    user_prompt = _build_user_prompt(topic, posts)

    response = client.messages.create(
        model=model,
        max_tokens=100,
        temperature=0.3,
        system=TITLE_REWRITE_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    raw = response.content[0].text.strip()
    usage = {
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
    }
    return raw, usage


def _calculate_cost(usage):
    """Calculate Opus cost from token usage."""
    return (usage["input_tokens"] / 1_000_000 * 5.00) + (usage["output_tokens"] / 1_000_000 * 25.00)


def _process_topic(topic_row, client, model):
    """Process a single topic (thread-safe with per-thread DB)."""
    db = _get_thread_db()
    posts = db.get_posts_for_topic(topic_row["id"], limit=5)

    try:
        raw_title, usage = _rewrite_title(topic_row, posts, client, model)
        new_name = _apply_title_guardrails(raw_title)
        cost = _calculate_cost(usage)

        if not new_name:
            return ("guardrail", topic_row["name"], raw_title, cost, usage)

        if new_name == topic_row["name"]:
            return ("unchanged", topic_row["name"], new_name, cost, usage)

        db.update_topic(topic_row["id"], {"name": new_name}, edited_by="backfill_titles")
        db.record_api_usage(
            service="anthropic",
            operation="backfill_title",
            input_tokens=usage["input_tokens"],
            output_tokens=usage["output_tokens"],
            cost_usd=cost,
            model=model,
            batch_size=1,
        )
        return ("ok", topic_row["name"], new_name, cost, usage)
    except Exception as e:
        return ("error", topic_row["name"], str(e), 0.0, {})


def get_topics_for_title_backfill(db, limit=None, force=False):
    """Get active topics eligible for title rewriting.

    Args:
        force: If True, return ALL active promoted topics. Otherwise only short titles (<=8 words).
    """
    if force:
        query = """
            SELECT id, name, description, category, subcategory, post_count,
                   summary_bullets, summary_key_takeaways
            FROM topics
            WHERE is_active = 1
              AND is_promoted = 1
              AND post_count > 0
            ORDER BY post_count DESC
        """
    else:
        query = """
            SELECT id, name, description, category, subcategory, post_count,
                   summary_bullets, summary_key_takeaways
            FROM topics
            WHERE is_active = 1
              AND is_promoted = 1
              AND post_count > 0
            ORDER BY post_count DESC
        """
    if limit:
        query += f" LIMIT {int(limit)}"
    cur = db.conn.execute(query)
    topics = [dict(row) for row in cur.fetchall()]

    if not force:
        # Filter to short titles only (<=8 words, likely old slide-style)
        filtered = []
        for t in topics:
            words = re.findall(r"[A-Za-z0-9]+", t["name"])
            if len(words) <= 8:
                filtered.append(t)
        return filtered

    return topics


def main():
    parser = argparse.ArgumentParser(description="Backfill topic titles as executive claims")
    parser.add_argument("--dry-run", action="store_true", help="Print topics without calling API")
    parser.add_argument("--force", action="store_true", help="Re-process ALL promoted topics, not just short titles")
    parser.add_argument("--limit", type=int, default=None, help="Max topics to process")
    parser.add_argument("--parallel", "-p", type=int, default=1, help="Number of parallel workers (default: 1)")
    args = parser.parse_args()

    db = get_db()
    db.init_db()

    topics = get_topics_for_title_backfill(db, limit=args.limit, force=args.force)
    if not topics:
        print("No topics need title backfill.")
        return

    mode = "FORCE (all promoted)" if args.force else "standard (short titles only)"
    print(f"Found {len(topics)} topics for title backfill [{mode}]", flush=True)

    if args.dry_run:
        for t in topics:
            words = re.findall(r"[A-Za-z0-9]+", t["name"])
            print(f"  [{t['id']}] ({len(words)}w) {t['name']} ({t['post_count']} posts)")
        print(f"\nDry run complete. Use without --dry-run to process.")
        return

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    model = config.OPUS_MODEL
    workers = max(1, min(args.parallel, 8))
    success = 0
    errors = 0
    guardrails = 0
    unchanged = 0
    total_cost = 0.0
    total = len(topics)

    if workers == 1:
        for i, topic in enumerate(topics):
            print(f"[{i+1}/{total}] {topic['name'][:50]}...", flush=True)
            posts = db.get_posts_for_topic(topic["id"], limit=5)
            try:
                raw_title, usage = _rewrite_title(topic, posts, client, model)
                cost = _calculate_cost(usage)
                total_cost += cost
                new_name = _apply_title_guardrails(raw_title)

                if not new_name:
                    guardrails += 1
                    print(f"  GUARDRAIL: \"{raw_title}\" rejected", flush=True)
                elif new_name == topic["name"]:
                    unchanged += 1
                    print(f"  UNCHANGED", flush=True)
                else:
                    db.update_topic(topic["id"], {"name": new_name}, edited_by="backfill_titles")
                    db.record_api_usage(
                        service="anthropic",
                        operation="backfill_title",
                        input_tokens=usage["input_tokens"],
                        output_tokens=usage["output_tokens"],
                        cost_usd=cost,
                        model=model,
                        batch_size=1,
                    )
                    success += 1
                    print(f"  -> {new_name} (${cost:.4f})", flush=True)
            except Exception as e:
                errors += 1
                print(f"  ERROR: {e}", flush=True)
    else:
        print(f"Running with {workers} parallel workers", flush=True)
        completed = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_process_topic, topic, client, model): topic
                for topic in topics
            }
            for future in as_completed(futures):
                topic = futures[future]
                completed += 1
                result = future.result()
                status = result[0]
                cost = result[3]
                total_cost += cost

                if status == "ok":
                    success += 1
                    print(f"[{completed}/{total}] {topic['name'][:35]} -> {result[2][:45]} (${cost:.4f})", flush=True)
                elif status == "unchanged":
                    unchanged += 1
                    print(f"[{completed}/{total}] UNCHANGED: {topic['name'][:45]}", flush=True)
                elif status == "guardrail":
                    guardrails += 1
                    print(f"[{completed}/{total}] GUARDRAIL: {topic['name'][:35]} -> \"{result[2]}\"", flush=True)
                else:
                    errors += 1
                    print(f"[{completed}/{total}] ERROR: {topic['name'][:35]} — {result[2]}", flush=True)

    # Vector refresh after all titles updated
    if success > 0:
        print(f"\nRefreshing topic vectors for {success} renamed topics...", flush=True)
        try:
            from vector_search import TopicVectorIndex
            vec_index = TopicVectorIndex.get_instance(db.conn)
            all_topics = db.get_active_topics(limit=500)
            vec_index.sync_topic_vectors(all_topics, db)
            print(f"Vector sync complete", flush=True)
        except Exception as e:
            print(f"Vector sync failed (non-fatal): {e}", flush=True)

    print(f"\nTitle backfill complete: {success} renamed, {unchanged} unchanged, "
          f"{guardrails} guardrail-rejected, {errors} errors, ${total_cost:.4f} total cost", flush=True)


if __name__ == "__main__":
    main()
