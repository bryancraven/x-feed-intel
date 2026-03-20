"""Topic matcher — assigns relevant posts to specific tracked topics using Opus 4.6.

Pass 2 of the classification pipeline:
1. Fetch existing topics from the DB
2. Fetch relevant posts not yet linked to topics
3. Send both to Opus in batches — Opus decides: existing topic or new topic
4. Create new topics / link posts to topics in the DB
"""
import json
import logging
import re
import time
from typing import Optional

import anthropic

from . import config
from .database import get_db

logger = logging.getLogger("x_feed_intel")

# ---------------------------------------------------------------------------
# System prompt for topic matching
# ---------------------------------------------------------------------------
TOPIC_SYSTEM_PROMPT = """You are a topic-matching assistant for an AI market intelligence tracker at AI infrastructure market intelligence.

You will receive:
1. A list of EXISTING TOPICS (each with an ID, name, description, and post count)
2. A batch of NEW POSTS that need to be assigned to topics

For each post, you must decide:
- **EXISTING**: The post matches an existing topic — provide the topic ID(s)
- **NEW**: The post covers a genuinely new subject — propose a new topic name and description

## Topic Naming Rules — Executive Claim Titles

Topic names must read as standalone, informative assertions that an executive can understand without additional context. They appear on editorial cards and go directly into weekly executive summaries.

**Naming rules (MANDATORY):**
1. **Title Case** — capitalize all major words
2. **6-12 words** — aim for 8-10; long enough to convey a complete claim
3. **Entity-first, claim second** — lead with the subject, follow with what is happening
4. **Standalone claims** — each title should be a complete, informative assertion
5. **Present tense for ongoing events, past tense for completed** events
6. **Use "&" not "and"** when joining two concepts
7. **Cut filler** — drop "the", "a", unnecessary prepositions wherever possible
8. **Be specific** — include named entities, model numbers, quantified claims where available

**Good topic names** (standalone executive claims):
- "NVIDIA Blackwell Ramp Accelerates Ahead of Schedule"
- "AMD MI300X Gains Enterprise Traction Against NVIDIA"
- "US-China Export Controls Tighten on AI Chips"
- "Hyperscaler CapEx Surges Past $200B Annually"
- "Meta Open-Sources Llama to Challenge Proprietary Models"
- "DeepSeek R1 Sparks Distillation IP Debate"
- "TSMC Advanced Packaging Capacity Constrains AI Chip Supply"
- "CoreWeave IPO Signals GPU Cloud Market Maturity"
- "EU AI Act Compliance Deadlines Drive Enterprise Urgency"
- "HBM Supply Shortage Persists Through 2026"

**Bad names — NEVER create these:**
- Too broad: "AI News", "NVIDIA Updates", "China AI", "Cloud Computing"
- Too short / noun-phrase only: "NVIDIA Blackwell B200 Ramp" (add the claim: what about it?)
- Too narrow / will only attract 1 post: "LlamaIndex document extraction agent with schema inference"
- Question-like: "How DeepSeek is disrupting the AI model market" (make it a claim instead)

## Topic Granularity Guidelines

Topics should be BROAD ENOUGH to accumulate 3+ posts over a week, but specific enough to be meaningful.

**Merging rules** — STRONGLY prefer matching to existing topics when:
- The post discusses the same product/model family (e.g., all MI300X/MI325X posts → same AMD topic)
- The post discusses the same company initiative (e.g., all Microsoft AI infrastructure posts → same topic)
- The post is a follow-up, reaction, or analysis of an existing story
- The post mentions the same key people in the same context
- The post covers a niche aspect of an existing broader topic
- When in doubt, match to an existing topic rather than creating a new one

**When to create NEW topics:**
- A genuinely new product launch or major version release (not a minor update or rumor)
- A new government policy or regulatory action (not commentary on existing policy)
- A major partnership, acquisition, or corporate restructuring
- AVOID creating topics for: single tweets about a niche aspect of an existing topic, commentary or opinion threads, minor updates to existing products, rumors without substance

## Key People (posts about them should reference their company/lab context):
Lisa Su (AMD), Jensen Huang (NVIDIA), Pat Gelsinger (Intel), Sam Altman (OpenAI),
Dario Amodei & Daniela Amodei (Anthropic), Demis Hassabis (DeepMind), Yann LeCun (Meta AI),
Mark Zuckerberg (Meta), Satya Nadella (Microsoft), Sundar Pichai (Google),
Yoav Shoham (AI21 Labs), Noam Shazeer (Character AI → Google), Ilya Sutskever (SSI),
Andrej Karpathy, Jim Keller (Tenstorrent), George Hotz (tinygrad/comma.ai),
Dylan Patel (SemiAnalysis), Elon Musk (xAI/Tesla), Liang Wenfeng (DeepSeek)

## Output Format

Respond with ONLY a JSON array. Each element:
{
  "post_index": <integer matching input>,
  "topics": [
    {"action": "existing", "topic_id": <int>}
    // or
    {"action": "new", "name": "<executive claim: Title Case, 6-12 words, entity-first claim, use & not and>", "description": "<1-2 sentence description>", "category": "<parent category>", "subcategory": "<subcategory>"}
  ]
}

A post may be assigned to multiple topics if it genuinely covers multiple distinct stories.

NOTE: The existing topics listed below may be a pre-filtered subset of all active topics,
selected by semantic similarity to the posts in this batch. If none of the listed topics
are a good match for a post, propose a NEW topic. The system will periodically merge
duplicate topics if needed.

If multiple posts in this batch relate to the same new story, assign them all to ONE new
topic — do not create separate topics for each post about the same subject.

## Category Assignment Notes
- Technical distillation discussions (knowledge transfer, model compression, student-teacher training) → MODEL_ARCHITECTURE / MEMORY_EFFICIENCY
- Geopolitical distillation (Chinese labs copying US frontier models, export control violations, IP concerns) → GEOPOLITICS_POLICY / EXPORT_CONTROLS
- FRONTIER_MODELS should only be used for posts primarily about frontier model releases, capabilities, or benchmarks. Posts about infrastructure, economics, or policy implications of frontier models should use the more specific category.

Use parent categories from: MACRO_FRAMES, INDUSTRY_NARRATIVES, GEOPOLITICS_POLICY, AI_ECONOMICS, DATA_CENTER_INFRA, HARDWARE_PLATFORMS, NETWORKING, INFERENCE_STACK, MODEL_ARCHITECTURE, FRONTIER_MODELS, AGENTS_AUTONOMY, VERTICAL_APPS, RESEARCH_FRONTIERS, OPERATIONAL_METRICS

Subcategories per parent:
- MACRO_FRAMES: AI_CIVILIZATIONAL_SHIFT | STRUCTURAL_FORCES
- INDUSTRY_NARRATIVES: GLOBAL_AI_MARKET | COMPETITIVE_LANDSCAPE
- GEOPOLITICS_POLICY: EXPORT_CONTROLS | SOVEREIGNTY_POLICY
- AI_ECONOMICS: AI_REVENUE_MODELS | CAPEX_MARGINS
- DATA_CENTER_INFRA: AI_DC_ARCHITECTURE | POWER_ENERGY
- HARDWARE_PLATFORMS: GPU_PLATFORMS | CUSTOM_SILICON | PACKAGING_MANUFACTURING
- NETWORKING: SCALE_UP_NETWORKING | SCALE_OUT_NETWORKING | OPTICAL_PHOTONICS
- INFERENCE_STACK: INFERENCE_ARCHITECTURE | PARALLELISM_STRATEGIES | INFERENCE_FRAMEWORKS
- MODEL_ARCHITECTURE: SCALING_PATTERNS | MEMORY_EFFICIENCY
- FRONTIER_MODELS: PROPRIETARY_FRONTIER | OPEN_CHINA_FRONTIER | CAPABILITY_DOMAINS
- AGENTS_AUTONOMY: AGENT_ARCHITECTURES | AGENT_WORKLOADS | AGENT_ECONOMICS
- VERTICAL_APPS: HEALTHCARE_LIFESCI | ENTERPRISE_SOFTWARE | DEVELOPER_TOOLS | CONSUMER_COMMERCE
- RESEARCH_FRONTIERS: LEARNING_PARADIGMS | BENCHMARKS_EVAL
- OPERATIONAL_METRICS: SUPPLY_CHAIN_METRICS | PRICING_MARKET"""


class TopicMatcher:
    """Assign relevant posts to specific tracked topics using Opus 4.6."""

    BATCH_SIZE = 15  # Slightly smaller than classification batches due to topic context

    def __init__(self):
        self.client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        self.model = config.OPUS_MODEL
        self.db = get_db()
        self._metrics = {
            "parse_failures": 0,
            "guardrail_rejects": 0,
            "reused_new_suggestions": 0,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def match_all_unlinked(self) -> dict:
        """
        Match all relevant posts not yet linked to topics.
        Returns summary: {"posts_processed": N, "topics_created": N, "links_created": N,
                          "usage": {"input_tokens": N, "output_tokens": N}}
        """
        unlinked = self.db.get_posts_unlinked_to_topics(limit=200)
        if not unlinked:
            try:
                self.db.set_state_value("last_topic_match_parse_failures", "0")
                self.db.set_state_value("last_topic_match_guardrail_rejects", "0")
                self.db.set_state_value("last_topic_match_reused_new_suggestions", "0")
            except Exception:
                pass
            logger.info("No unlinked relevant posts to match")
            return {"posts_processed": 0, "topics_created": 0, "links_created": 0,
                    "usage": {"input_tokens": 0, "output_tokens": 0}}

        logger.info(f"Found {len(unlinked)} relevant posts to match to topics")
        self._metrics = {
            "parse_failures": 0,
            "guardrail_rejects": 0,
            "reused_new_suggestions": 0,
        }

        # Initialize vector search for pre-filtering (once per cycle)
        vec_index = None
        all_topics = self.db.get_active_topics(limit=200)
        if config.VECTOR_SEARCH_ENABLED:
            try:
                from .vector_search import TopicVectorIndex
                vec_index = TopicVectorIndex.get_instance(self.db.conn)
                vec_index.sync_topic_vectors(all_topics, self.db)
            except Exception as e:
                logger.warning(f"Vector search init failed, using all topics: {e}")
                vec_index = None

        total_created = 0
        total_links = 0
        total_usage = {"input_tokens": 0, "output_tokens": 0}

        for start in range(0, len(unlinked), self.BATCH_SIZE):
            batch = unlinked[start : start + self.BATCH_SIZE]
            batch_num = (start // self.BATCH_SIZE) + 1
            total_batches = (len(unlinked) + self.BATCH_SIZE - 1) // self.BATCH_SIZE

            logger.info(f"Topic matching batch {batch_num}/{total_batches} ({len(batch)} posts)")

            created, links, usage = self._match_batch(batch, all_topics, vec_index)
            total_created += created
            total_links += links
            total_usage["input_tokens"] += usage.get("input_tokens", 0)
            total_usage["output_tokens"] += usage.get("output_tokens", 0)

            if start + self.BATCH_SIZE < len(unlinked):
                time.sleep(1)

        logger.info(
            f"Topic matching complete: {len(unlinked)} posts processed, "
            f"{total_created} new topics, {total_links} links created"
        )
        try:
            self.db.set_state_value("last_topic_match_parse_failures", str(self._metrics.get("parse_failures", 0)))
            self.db.set_state_value("last_topic_match_guardrail_rejects", str(self._metrics.get("guardrail_rejects", 0)))
            self.db.set_state_value("last_topic_match_reused_new_suggestions", str(self._metrics.get("reused_new_suggestions", 0)))
        except Exception as e:
            logger.warning(f"Failed to persist topic matcher metrics (non-fatal): {e}")
        logger.info(
            "Topic matcher metrics: parse_failures=%s guardrail_rejects=%s reused_new=%s",
            self._metrics.get("parse_failures", 0),
            self._metrics.get("guardrail_rejects", 0),
            self._metrics.get("reused_new_suggestions", 0),
        )
        return {
            "posts_processed": len(unlinked),
            "topics_created": total_created,
            "links_created": total_links,
            "usage": total_usage,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _match_batch(
        self,
        posts: list[dict],
        all_topics: list[dict] = None,
        vec_index=None,
    ) -> tuple[int, int, dict]:
        """
        Match a batch of posts to topics.
        Returns (topics_created, links_created, usage_dict).
        """
        # Get current topics for context (fallback if not passed)
        if all_topics is None:
            all_topics = self.db.get_active_topics(limit=200)

        # Vector pre-filtering: narrow topic list to relevant candidates
        used_vector_filter = False
        if vec_index is not None:
            try:
                candidate_ids = vec_index.get_batch_candidates(
                    posts, all_topics, top_k=config.VECTOR_TOP_K
                )
                existing_topics = [
                    t for t in all_topics if t["id"] in candidate_ids
                ]
                used_vector_filter = True
            except Exception as e:
                logger.warning(
                    f"Vector search failed for batch, using all topics: {e}"
                )
                existing_topics = all_topics
        else:
            existing_topics = all_topics

        total_topic_count = len(all_topics)

        # Build user message
        lines = []

        # Topic context
        if existing_topics:
            if used_vector_filter:
                lines.append(
                    f"EXISTING TOPICS (showing {len(existing_topics)} most "
                    f"relevant of {total_topic_count} total, pre-filtered by "
                    f"semantic similarity):"
                )
            else:
                lines.append(f"EXISTING TOPICS ({len(existing_topics)}):")
            for t in existing_topics:
                lines.append(
                    f"  [ID={t['id']}] \"{t['name']}\" — {t['description'] or 'No description'} "
                    f"({t['post_count']} posts, category: {t['category'] or '?'})"
                )
            lines.append("")
        else:
            lines.append("EXISTING TOPICS: None yet (all topics will be new)")
            lines.append("")

        # Posts to match
        lines.append(f"NEW POSTS ({len(posts)}):")
        for i, p in enumerate(posts, 1):
            username = p.get("author_username", "unknown")
            date = p.get("created_at", "")[:10]
            text = p.get("full_text") or p.get("text", "")
            if len(text) > 400:
                text = text[:397] + "..."
            reasoning = p.get("relevance_reasoning", "")
            lines.append(
                f"  [{i}] @{username} ({date}): \"{text}\""
            )
            if reasoning:
                lines.append(f"       Context: {reasoning}")

        user_message = "\n".join(lines)

        # Call Haiku
        results, usage = self._call_model(user_message)
        if results is None:
            logger.warning("Topic matching Haiku call failed — skipping batch")
            return (0, 0, usage)

        # Process results
        topics_created = 0
        links_created = 0
        new_topic_suggestions_reused = 0

        for result in results:
            post_idx = result.get("post_index", 0) - 1
            if post_idx < 0 or post_idx >= len(posts):
                continue

            post = posts[post_idx]
            tweet_id = post["tweet_id"]

            for topic_assignment in result.get("topics", []):
                action = topic_assignment.get("action", "")

                if action == "existing":
                    topic_id = topic_assignment.get("topic_id")
                    if topic_id:
                        self.db.link_post_to_topic(tweet_id, topic_id)
                        links_created += 1

                elif action == "new":
                    name = topic_assignment.get("name", "").strip()
                    desc = topic_assignment.get("description", "").strip()
                    cat = topic_assignment.get("category")
                    subcat = topic_assignment.get("subcategory")

                    if name:
                        # Validate and auto-correct taxonomy
                        cat, subcat = self._validate_category(cat, subcat)
                        if cat is None:
                            logger.info(
                                'Rejected new topic "%s" — invalid taxonomy '
                                '(category=%s, subcategory=%s)',
                                name,
                                topic_assignment.get("category"),
                                topic_assignment.get("subcategory"),
                            )
                            self._metrics["guardrail_rejects"] += 1
                            continue
                        name, desc = self._apply_new_topic_guardrails(name, desc)
                        if not name:
                            self._metrics["guardrail_rejects"] += 1
                            continue
                        topic_id, was_created = self.db.get_or_create_topic_status(
                            name, desc, cat, subcat,
                            promote=False,
                            created_source="model",
                        )
                        self.db.link_post_to_topic(tweet_id, topic_id)
                        if was_created:
                            topics_created += 1
                        else:
                            new_topic_suggestions_reused += 1
                            self._metrics["reused_new_suggestions"] += 1
                        links_created += 1
                        if was_created:
                            logger.info(f"New topic created: \"{name}\" (id={topic_id})")

        if new_topic_suggestions_reused:
            logger.info(
                "Topic matching batch reused %d model 'new' topic suggestions via dedupe",
                new_topic_suggestions_reused,
            )

        return (topics_created, links_created, usage)

    def _apply_new_topic_guardrails(self, name: str, desc: str) -> tuple[str, str]:
        """
        Reject or normalize low-quality model-created topic names.

        Returns `(name, desc)`; empty name means reject suggestion.
        """
        raw_name = (name or "").strip()
        raw_desc = (desc or "").strip()
        if not raw_name:
            return "", raw_desc

        clean_name = re.sub(r"\s+", " ", raw_name).strip(" -–—:;,.")
        clean_name = re.sub(r"[“”]", '"', clean_name)
        words = [w for w in re.findall(r"[A-Za-z0-9]+", clean_name)]
        word_count = len(words)

        generic_names = {
            "ai news",
            "nvidia updates",
            "china ai",
            "openai updates",
            "semiconductor news",
            "ai labs",
            "cloud computing",
            "tech news",
        }
        normalized = re.sub(r"[^a-z0-9]+", " ", clean_name.lower()).strip()

        # Too generic
        if normalized in generic_names:
            logger.info('Rejected generic new topic suggestion: "%s"', raw_name)
            return "", raw_desc
        # Too short unless clearly a proper-noun product/company pair (basic heuristic)
        if word_count < 3 and not re.search(r"[A-Z].*[A-Z]", clean_name):
            logger.info('Rejected too-short new topic suggestion: "%s"', raw_name)
            return "", raw_desc
        # Too long / sentence-like (executive claim titles should be 6-12 words)
        if word_count > 14:
            logger.info('Rejected too-long new topic suggestion (%d words): "%s"', word_count, raw_name)
            return "", raw_desc

        # Trim verbose descriptions
        if raw_desc:
            raw_desc = re.sub(r"\s+", " ", raw_desc).strip()
            if len(raw_desc) > 320:
                raw_desc = raw_desc[:317].rstrip() + "..."

        return clean_name, raw_desc

    @staticmethod
    def _validate_category(category, subcategory):
        """Validate category/subcategory pair against the taxonomy. Auto-correct mismatches.

        Returns (category, subcategory). If category comes back None, the
        caller should reject the topic suggestion.
        """
        if not category:
            # Maybe the model returned a subcategory as the category
            if subcategory and subcategory in config.SUBCATEGORY_TO_PARENT:
                return config.SUBCATEGORY_TO_PARENT[subcategory], subcategory
            return None, None

        # If it's a valid parent category, check subcategory
        if category in config.CATEGORIES:
            if subcategory and subcategory in config.SUBCATEGORY_TO_PARENT:
                # Subcategory exists — check it belongs to this parent
                correct_parent = config.SUBCATEGORY_TO_PARENT[subcategory]
                if correct_parent != category:
                    return correct_parent, subcategory
            elif subcategory and subcategory not in config.SUBCATEGORY_TO_PARENT:
                return category, None  # Invalid subcategory, drop it
            return category, subcategory

        # Maybe the model returned a subcategory as the category
        if category in config.SUBCATEGORY_TO_PARENT:
            return config.SUBCATEGORY_TO_PARENT[category], category

        # Unknown category — try to map from old taxonomy
        if category in config.OLD_CATEGORY_MAP:
            return config.OLD_CATEGORY_MAP[category], subcategory

        return None, None

    def _call_model(self, user_message: str) -> tuple[Optional[list[dict]], dict]:
        """Call Opus 4.6 for topic matching with adaptive thinking.

        Returns:
            Tuple of (parsed_results, usage_dict). usage_dict always has
            input_tokens and output_tokens (0 on failure).
        """
        usage = {"input_tokens": 0, "output_tokens": 0}
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=config.MAX_TOKENS_TOPIC_MATCH,
                thinking={"type": "adaptive"},
                output_config={"effort": "medium"},
                system=TOPIC_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            )

            # Extract token usage
            usage = {
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            }

            if response.stop_reason == "max_tokens":
                logger.warning("Topic matching response truncated")

            # With adaptive thinking, response may contain ThinkingBlock(s)
            # and the final answer may be split across multiple TextBlocks.
            text_blocks = [
                b.text for b in response.content
                if getattr(b, "type", None) == "text" and getattr(b, "text", None)
            ]
            text = "\n".join(tb.strip() for tb in text_blocks if tb and tb.strip()).strip()

            if not text:
                logger.warning("Topic matching: no text block in response")
                return None, usage

            parsed = self._parse_json_array(text)
            if parsed is None:
                self._metrics["parse_failures"] += 1
                logger.warning(
                    "Topic matching parse failed (stop_reason=%s, text_blocks=%d, chars=%d)",
                    response.stop_reason,
                    len(text_blocks),
                    len(text),
                )
            return parsed, usage

        except anthropic.APIError as e:
            logger.error(f"Anthropic API error in topic matching: {e}")
            return None, usage
        except Exception as e:
            logger.error(f"Unexpected error in topic matching: {e}")
            return None, usage

    def _parse_json_array(self, text: str) -> Optional[list[dict]]:
        """Parse JSON array, handling markdown code fences."""
        stripped = re.sub(r"^```(?:json)?\s*\n?", "", text.strip())
        stripped = re.sub(r"\n?```\s*$", "", stripped).strip()

        try:
            result = json.loads(stripped)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

        try:
            start = stripped.index("[")
            end = stripped.rindex("]") + 1
            result = json.loads(stripped[start:end])
            if isinstance(result, list):
                return result
        except (ValueError, json.JSONDecodeError):
            pass

        # Fix trailing commas
        try:
            cleaned = re.sub(r",\s*]", "]", stripped)
            cleaned = re.sub(r",\s*}", "}", cleaned)
            start = cleaned.index("[")
            end = cleaned.rindex("]") + 1
            result = json.loads(cleaned[start:end])
            if isinstance(result, list):
                return result
        except (ValueError, json.JSONDecodeError):
            pass

        logger.warning(f"Failed to parse topic matching JSON: {text[:200]}...")
        return None
