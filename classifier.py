"""Claude Haiku batch classification — determine relevance of X posts."""
import json
import logging
import re
import time
from typing import Optional

import anthropic

import config

logger = logging.getLogger("x_feed_intel")

# ---------------------------------------------------------------------------
# System prompt for classification
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are an AI market intelligence classifier for AI infrastructure market intelligence. Classify social media posts as relevant or irrelevant to AI infrastructure market intelligence.

IMPORTANT: Be INCLUSIVE. The lists below are examples, not exhaustive. If a post is clearly related to AI infrastructure, AI industry, or AI economics — even if the specific company, person, technology, or product isn't listed — classify it as relevant.

## RELEVANT — a post is relevant if it substantively discusses ANY of these:

**AI Models & Labs:**
- AI model releases, capabilities, benchmarks (e.g. MLPerf, MMLU, Chatbot Arena, LMSYS, Artificial Analysis, ARC-AGI, SWE-Bench, and similar)
- Major AI labs including but not limited to: OpenAI, Anthropic, Google DeepMind, Meta AI (FAIR), xAI, Mistral, Cohere, AI21, Inflection, Adept, Character AI, Stability AI, Runway, Reka, Together AI
- AI startups and emerging labs including but not limited to: Neolabs, Sakana AI, Imbue, Essential AI, Poolside, Magic, World Labs, Safe Superintelligence (SSI), Pika, Ideogram, and any new or lesser-known AI company building models, infrastructure, or developer tools
- Chinese AI including but not limited to: DeepSeek, Baidu (Ernie), ByteDance (Doubao), Alibaba (Qwen), Tencent, 01.AI (Yi), Zhipu AI (GLM/ChatGLM), Moonshot AI (Kimi), MiniMax, SenseTime, iFlytek, and other Chinese AI labs/startups
- Key people including but not limited to: Lisa Su, Jensen Huang, Pat Gelsinger, Sam Altman, Dario Amodei, Daniela Amodei, Demis Hassabis, Yann LeCun, Mark Zuckerberg, Satya Nadella, Sundar Pichai, Elon Musk, Yoav Shoham, Noam Shazeer, Ilya Sutskever, Andrej Karpathy, Jim Keller, George Hotz, Dylan Patel, Semianalysis, Liang Wenfeng — and any other prominent AI researcher, executive, or founder discussing AI strategy or technology

**AI Infrastructure & Hardware:**
- GPUs, accelerators, ASICs including but not limited to: AMD Instinct MI300/MI350/MI400, NVIDIA Blackwell/Rubin (B100/B200/GB200), Intel Gaudi, Google TPU (v6/v7), AWS Trainium/Inferentia, Cerebras, Groq, SambaNova, Tenstorrent, MTIA, and any other AI accelerator or custom silicon
- Networking: InfiniBand, RoCE, UALink, NVLink, NVSwitch, Ultra Ethernet Consortium, Spectrum-X, CPO, and related interconnect/fabric technologies
- Memory: HBM (HBM3/HBM3E/HBM4), GDDR, memory bandwidth, and any other memory technology relevant to AI compute (even if the specific type isn't listed here)
- Inference: vLLM, SGLang, TensorRT-LLM, llama.cpp, Dynamo, llm-d, quantization (GPTQ, AWQ, FP8, INT4), speculative decoding, KV cache, prefill/decode disaggregation, and related inference optimization techniques
- Training: PyTorch, JAX, Triton, CUDA, ROCm, OpenXLA, DeepSpeed, Megatron, FSDP, and related training frameworks/tools
- Platforms including but not limited to: Anyscale, Modal, Replicate, Fireworks AI, Together AI, Baseten, OctoAI, Hugging Face TGI, and similar AI inference/training platforms

**Cloud & Data Centers:**
- Cloud AI including but not limited to: AWS, Azure, GCP, Oracle Cloud (OCI), CoreWeave, Lambda Labs, Crusoe, Voltage Park, and other cloud/GPU-as-a-service providers
- Cloud capex, GPU availability, pricing, capacity constraints
- Data center construction, power (MW/GW), cooling, liquid immersion
- Sovereign AI / national AI compute initiatives

**Semiconductors & Supply Chain:**
- TSMC, Samsung Foundry, Intel Foundry, ASML, and other semiconductor manufacturers/equipment suppliers
- Advanced packaging: CoWoS (S/L/R), SoIC, Foveros, chiplets, 2.5D/3D integration, and related packaging technologies
- Process nodes (N3/N2/A16), EUV, GAA, and related semiconductor process advances
- AMD ecosystem: ROCm adoption, MI-series, competitive dynamics with NVIDIA

**Policy, Economics & Enterprise:**
- AI regulation: EU AI Act, US executive orders, China export controls, CHIPS Act, BIS regimes, and related policy/regulatory developments
- AI economics: token pricing, API economics, CapEx/margins, $/GW, funding rounds for AI companies, AI startup valuations
- Enterprise AI adoption, AI spending trends, agentic workloads
- AI agents, autonomous systems, multi-agent architectures
- AI startup funding, acquisitions, partnerships, and competitive dynamics

## IRRELEVANT — skip these:
- Personal/lifestyle content, entertainment, sports
- Non-AI politics, cryptocurrency (unless AI infrastructure related)
- Generic software engineering unrelated to AI/ML
- Vague motivational or self-help content
- Sarcasm, jokes, memes, or humor about AI topics — even if they mention relevant companies/people, if the post is primarily comedic or ironic rather than informative, mark it irrelevant
- Vague posting, hot takes with no substance, or engagement-bait ("this changes everything", "people aren't ready for this") without concrete information
- Dunking, quote-tweet arguments, or pile-on commentary that adds no new information about the underlying topic
- Hype/doom posts that are purely emotional reactions with no technical or business content

## Output format
JSON array only. Each element:
- "index": integer matching the input post number
- "is_relevant": boolean
- "reasoning": one concise sentence

IMPORTANT: Posts by or mentioning key industry figures about their companies or AI strategy should ALWAYS be classified as relevant.

Respond with ONLY the JSON array."""


class PostClassifier:
    """Classify X posts for relevance using Claude Haiku in batches."""

    def __init__(self):
        self.client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        self.model = config.HAIKU_MODEL

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def classify_batch(self, posts: list[dict]) -> tuple[list[dict], dict]:
        """
        Classify a batch of posts (up to CLASSIFICATION_BATCH_SIZE).

        Args:
            posts: List of dicts with tweet_id, author_username, text,
                   full_text, created_at.

        Returns:
            Tuple of (results_list, usage_dict) where usage_dict has
            input_tokens and output_tokens from the API call.
        """
        if not posts:
            return [], {"input_tokens": 0, "output_tokens": 0}

        # Build the user message
        lines = [f"Classify these {len(posts)} posts:\n"]
        for i, post in enumerate(posts, 1):
            username = post.get("author_username", "unknown")
            date = post.get("created_at", "")[:10]
            text = post.get("full_text") or post.get("text", "")
            # Truncate very long posts to save tokens
            if len(text) > 500:
                text = text[:497] + "..."
            lines.append(f"[{i}] @{username} ({date}): \"{text}\"")

        user_message = "\n".join(lines)

        # Call Haiku
        raw_results, usage = self._call_haiku(user_message)
        if raw_results is None:
            # Mark all as unclassified
            return [
                {
                    "tweet_id": p["tweet_id"],
                    "is_relevant": False,
                    "reasoning": "Classification failed — will retry next cycle",
                    "classified": False,
                }
                for p in posts
            ], usage

        # Map results back to tweet_ids
        results = []
        for i, post in enumerate(posts):
            idx = i + 1
            match = next((r for r in raw_results if r.get("index") == idx), None)
            if match:
                results.append({
                    "tweet_id": post["tweet_id"],
                    "is_relevant": bool(match.get("is_relevant", False)),
                    "reasoning": match.get("reasoning", ""),
                    "classified": True,
                })
            else:
                results.append({
                    "tweet_id": post["tweet_id"],
                    "is_relevant": False,
                    "reasoning": "No classification returned for this post",
                    "classified": False,
                })

        return results, usage

    def classify_all(self, posts: list[dict]) -> tuple[list[dict], dict]:
        """
        Classify all posts, chunking into batches.

        Returns tuple of (all_results, total_usage) where total_usage
        aggregates input_tokens and output_tokens across all batches.
        """
        if not posts:
            return [], {"input_tokens": 0, "output_tokens": 0}

        batch_size = config.CLASSIFICATION_BATCH_SIZE
        all_results: list[dict] = []
        total_usage = {"input_tokens": 0, "output_tokens": 0}

        for start in range(0, len(posts), batch_size):
            batch = posts[start : start + batch_size]
            batch_num = (start // batch_size) + 1
            total_batches = (len(posts) + batch_size - 1) // batch_size

            logger.info(
                f"Classifying batch {batch_num}/{total_batches} "
                f"({len(batch)} posts)"
            )

            results, usage = self.classify_batch(batch)
            all_results.extend(results)
            total_usage["input_tokens"] += usage.get("input_tokens", 0)
            total_usage["output_tokens"] += usage.get("output_tokens", 0)

            # Small delay between batches to be respectful of API
            if start + batch_size < len(posts):
                time.sleep(1)

        return all_results, total_usage

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _call_haiku(
        self, user_message: str, retry: bool = True
    ) -> tuple[Optional[list[dict]], dict]:
        """
        Call Claude Haiku and parse JSON array from response.
        Retries once with a stricter prompt on parse failure.

        Returns:
            Tuple of (parsed_results, usage_dict). usage_dict always has
            input_tokens and output_tokens (0 on failure).
        """
        usage = {"input_tokens": 0, "output_tokens": 0}
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=config.MAX_TOKENS_CLASSIFY,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            )

            # Extract token usage
            usage = {
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            }

            # Check for truncation
            if response.stop_reason == "max_tokens":
                logger.warning(
                    f"Haiku response truncated (max_tokens={config.MAX_TOKENS_CLASSIFY}). "
                    "Consider increasing MAX_TOKENS_CLASSIFY or reducing batch size."
                )

            text = response.content[0].text.strip()
            return self._parse_json_array(text), usage

        except anthropic.APIError as e:
            logger.error(f"Anthropic API error: {e}")
            if retry:
                logger.info("Retrying Haiku classification after API error...")
                time.sleep(2)
                return self._call_haiku(user_message, retry=False)
            return None, usage

        except Exception as e:
            logger.error(f"Unexpected error in Haiku classification: {e}")
            return None, usage

    def _parse_json_array(self, text: str) -> Optional[list[dict]]:
        """
        Parse a JSON array from Haiku's response.
        Handles markdown code fences, direct JSON, and embedded arrays.
        """
        # Strip markdown code fences (```json ... ``` or ``` ... ```)
        stripped = re.sub(r"^```(?:json)?\s*\n?", "", text.strip())
        stripped = re.sub(r"\n?```\s*$", "", stripped).strip()

        # Try direct parse of stripped text
        try:
            result = json.loads(stripped)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

        # Try to extract JSON array from surrounding text
        try:
            # Find the first [ and last ]
            start = stripped.index("[")
            end = stripped.rindex("]") + 1
            result = json.loads(stripped[start:end])
            if isinstance(result, list):
                return result
        except (ValueError, json.JSONDecodeError):
            pass

        # Try to fix common issues: trailing commas, etc.
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

        logger.warning(f"Failed to parse JSON from Haiku response: {text[:200]}...")
        return None
