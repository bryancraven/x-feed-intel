"""Generate evolving topic summary bullets using Claude Opus."""
import json
import logging
import re
import time
from datetime import datetime, timezone

import anthropic

from . import config

logger = logging.getLogger("x_feed_intel")

# ---------------------------------------------------------------------------
# System prompt for summary generation
# ---------------------------------------------------------------------------
SUMMARY_SYSTEM_PROMPT = """You are an editorial assistant for an AI infrastructure market intelligence team.

Given a topic with its current description, existing key takeaways (if any), existing supporting bullets (if any), and linked posts in chronological order, produce an updated 3-tier summary.

The 3 tiers:
1. description — a single concise sentence capturing what this topic is about NOW. Present tense, entity-first.
2. key_takeaways — 1-2 executive-level statements an executive needs to know. Include the "so what" implication. Complete sentences.
3. bullets — 2-6 supporting detail bullets with specific data points, names, dates, numbers.

Guidelines:
- No redundancy across tiers; each adds depth (description = what, takeaways = so what, bullets = evidence)
- Build on existing takeaways and bullets when available — update/replace outdated ones, add new developments
- Posts are ordered oldest-first so you can see how the story evolved
- Focus on facts, data points, and named entities over vague observations
- Use present tense for ongoing situations, past tense for completed events

Output ONLY a JSON object:
{"description": "...", "key_takeaways": ["...", "..."], "bullets": ["...", "..."]}

Respond with ONLY the JSON object, no markdown fences or other text."""


class TopicSummaryGenerator:
    """Generate and refresh topic summary bullets using Claude Opus."""

    def __init__(self):
        self.client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        self.model = config.SUMMARY_MODEL

    def refresh_stale_summaries(self, db) -> dict:
        """
        Find topics needing summary refresh and update them.

        Returns:
            Dict with keys: refreshed (int), errors (int), total_usage (dict)
        """
        result = {
            "refreshed": 0,
            "errors": 0,
            "total_usage": {"input_tokens": 0, "output_tokens": 0},
        }

        topics = db.get_topics_needing_summary_refresh(
            min_new_posts=config.SUMMARY_MIN_NEW_POSTS,
            limit=config.SUMMARY_BATCH_SIZE,
        )
        if not topics:
            logger.info("Summary refresh: no topics need updating")
            return result

        logger.info(f"Summary refresh: {len(topics)} topics eligible")

        for topic in topics:
            try:
                summary, usage = self._generate_summary(db, topic)
                result["total_usage"]["input_tokens"] += usage["input_tokens"]
                result["total_usage"]["output_tokens"] += usage["output_tokens"]

                if summary:
                    # Count total live posts for lifetime counter
                    all_posts = db.get_all_posts_for_topic(topic["id"])
                    lifetime_seen = (topic.get("summary_lifetime_posts_seen") or 0) + topic["new_posts_since_summary"]

                    db.update_topic_summary(
                        topic_id=topic["id"],
                        description=summary["description"],
                        bullets=summary["bullets"],
                        key_takeaways=summary.get("key_takeaways"),
                        lifetime_seen=lifetime_seen,
                    )
                    result["refreshed"] += 1
                    logger.info(
                        f"Summary updated for topic {topic['id']} ({topic['name'][:40]}): "
                        f"{len(summary.get('key_takeaways', []))} takeaways, "
                        f"{len(summary['bullets'])} bullets"
                    )
                else:
                    result["errors"] += 1
                    logger.warning(f"Summary generation returned None for topic {topic['id']}")

                # Record API usage per topic
                cost = self._calculate_cost(usage)
                db.record_api_usage(
                    service="anthropic",
                    operation="summary_refresh",
                    input_tokens=usage["input_tokens"],
                    output_tokens=usage["output_tokens"],
                    cost_usd=cost,
                    model=self.model,
                    batch_size=1,
                )

            except Exception as e:
                result["errors"] += 1
                logger.error(f"Summary generation failed for topic {topic['id']}: {e}")

        logger.info(
            f"Summary refresh complete: {result['refreshed']} updated, "
            f"{result['errors']} errors, "
            f"{result['total_usage']['input_tokens']} in / "
            f"{result['total_usage']['output_tokens']} out tokens"
        )
        return result

    def _generate_summary(self, db, topic: dict) -> tuple[dict | None, dict]:
        """
        Generate summary for a single topic.

        Returns:
            Tuple of (parsed_summary_dict_or_None, usage_dict)
        """
        usage = {"input_tokens": 0, "output_tokens": 0}

        # Fetch all live posts for this topic
        posts = db.get_all_posts_for_topic(topic["id"])
        if not posts:
            return None, usage

        # Build post context with windowing for large topics
        max_posts = config.SUMMARY_MAX_POSTS_CONTEXT
        if len(posts) > max_posts:
            first_posts = posts[:10]
            last_posts = posts[-(max_posts - 10):]
            omitted = len(posts) - max_posts
            post_lines = self._format_posts(first_posts)
            post_lines.append(f"\n[...{omitted} earlier posts omitted...]\n")
            post_lines.extend(self._format_posts(last_posts))
        else:
            post_lines = self._format_posts(posts)

        # Build existing key takeaways context
        existing_takeaways = ""
        if topic.get("summary_key_takeaways"):
            try:
                takeaways = json.loads(topic["summary_key_takeaways"])
                if takeaways:
                    existing_takeaways = "\nCurrent key takeaways:\n" + "\n".join(
                        f"- {t}" for t in takeaways
                    )
            except (json.JSONDecodeError, TypeError):
                pass

        # Build existing bullets context
        existing_bullets = ""
        if topic.get("summary_bullets"):
            try:
                bullets = json.loads(topic["summary_bullets"])
                if bullets:
                    existing_bullets = "\nCurrent supporting bullets:\n" + "\n".join(
                        f"- {b}" for b in bullets
                    )
            except (json.JSONDecodeError, TypeError):
                pass

        user_message = (
            f"Topic: {topic['name']}\n"
            f"Category: {topic.get('category', 'N/A')} / {topic.get('subcategory', 'N/A')}\n"
            f"Current description: {topic.get('description', 'None')}\n"
            f"{existing_takeaways}\n"
            f"{existing_bullets}\n\n"
            f"Linked posts ({len(posts)} total, oldest first):\n\n"
            + "\n".join(post_lines)
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=config.SUMMARY_MAX_TOKENS,
                system=SUMMARY_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            )

            usage = {
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            }

            text = response.content[0].text.strip()
            parsed = self._parse_json_response(text)
            return parsed, usage

        except anthropic.APIError as e:
            logger.error(f"Anthropic API error during summary generation: {e}")
            return None, usage
        except Exception as e:
            logger.error(f"Unexpected error in summary generation: {e}")
            return None, usage

    def _format_posts(self, posts: list[dict]) -> list[str]:
        """Format posts for the prompt context."""
        lines = []
        for p in posts:
            date = (p.get("created_at") or "")[:10]
            author = p.get("author_username", "unknown")
            text = p.get("full_text") or p.get("text", "")
            if len(text) > 300:
                text = text[:297] + "..."

            # Parse engagement metrics if available
            metrics_str = ""
            raw_metrics = p.get("public_metrics_json")
            if raw_metrics:
                try:
                    m = json.loads(raw_metrics) if isinstance(raw_metrics, str) else raw_metrics
                    parts = []
                    if m.get("like_count", 0) > 0:
                        parts.append(f"{m['like_count']} likes")
                    if m.get("retweet_count", 0) > 0:
                        parts.append(f"{m['retweet_count']} reposts")
                    if parts:
                        metrics_str = f" [{', '.join(parts)}]"
                except (json.JSONDecodeError, TypeError):
                    pass

            lines.append(f"[{date}] @{author}{metrics_str}: \"{text}\"")
        return lines

    @staticmethod
    def _parse_json_response(text: str) -> dict | None:
        """Parse JSON object from model response, normalizing 3-tier fields."""
        # Strip markdown code fences
        stripped = re.sub(r"^```(?:json)?\s*\n?", "", text.strip())
        stripped = re.sub(r"\n?```\s*$", "", stripped).strip()

        def _normalize(result: dict) -> dict | None:
            if not isinstance(result, dict) or "description" not in result:
                return None
            # Ensure bullets exists
            if "bullets" not in result:
                result["bullets"] = []
            if not isinstance(result["bullets"], list):
                return None
            result["bullets"] = [str(b) for b in result["bullets"] if b][:6]
            # Normalize key_takeaways (optional field, cap at 2)
            kt = result.get("key_takeaways")
            if isinstance(kt, list):
                result["key_takeaways"] = [str(t) for t in kt if t][:2]
            else:
                result["key_takeaways"] = []
            return result

        # Try direct parse first
        try:
            result = json.loads(stripped)
            normalized = _normalize(result)
            if normalized:
                return normalized
        except json.JSONDecodeError:
            pass

        # Fallback: find outermost JSON object containing "description"
        # Use a balanced-brace approach for nested arrays
        start = stripped.find("{")
        if start >= 0:
            depth = 0
            for i in range(start, len(stripped)):
                if stripped[i] == "{":
                    depth += 1
                elif stripped[i] == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            result = json.loads(stripped[start:i + 1])
                            normalized = _normalize(result)
                            if normalized:
                                return normalized
                        except json.JSONDecodeError:
                            pass
                        break

        logger.warning(f"Failed to parse summary JSON: {text[:200]}")
        return None

    @staticmethod
    def _calculate_cost(usage: dict) -> float:
        """Calculate cost for Opus API call."""
        input_cost = (usage.get("input_tokens", 0) / 1_000_000) * 5.00
        output_cost = (usage.get("output_tokens", 0) / 1_000_000) * 25.00
        return input_cost + output_cost
