"""Configuration for X Feed Intel."""
import os
import secrets as _secrets
from pathlib import Path
from dotenv import load_dotenv

# Load .env from this directory
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

# ---------------------------------------------------------------------------
# Anthropic key from environment
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ---------------------------------------------------------------------------
# X API credentials from .env (OAuth 1.0a)
# ---------------------------------------------------------------------------
X_CONSUMER_KEY = os.environ.get("X_CONSUMER_KEY")
X_CONSUMER_SECRET = os.environ.get("X_CONSUMER_SECRET")
X_ACCESS_TOKEN = os.environ.get("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.environ.get("X_ACCESS_TOKEN_SECRET")
X_BEARER_TOKEN = os.environ.get("X_BEARER_TOKEN")
X_USER_ID = os.environ.get("X_USER_ID")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "x_feed_intel.db"
LOG_DIR = Path(os.environ.get("LOG_DIR", str(BASE_DIR / "logs")))

# ---------------------------------------------------------------------------
# Classification models
# ---------------------------------------------------------------------------
HAIKU_MODEL = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-6"
OPUS_MODEL = "claude-opus-4-6"
CLASSIFICATION_BATCH_SIZE = 20  # Posts per Haiku API call
MAX_TOKENS_CLASSIFY = 4096
MAX_TOKENS_TOPIC_MATCH = 16000

# ---------------------------------------------------------------------------
# X API settings
# ---------------------------------------------------------------------------
X_COLLECTION_ENABLED = os.environ.get("X_COLLECTION_ENABLED", "0").strip().lower() in {
    "1", "true", "yes", "on"
}
# Default is paused. Set X_COLLECTION_ENABLED=1 to resume X API collection.
X_API_BASE = "https://api.x.com"
MAX_RESULTS_PER_PAGE = 100
MAX_PAGES_PER_FETCH = 2     # Cost control cap: 200 tweets max per fetch
REQUEST_TIMEOUT = 30
RATE_LIMIT_BUFFER_SEC = 5
# X API pay-per-use pricing is per-endpoint and may change; set current rates
# from the X Developer Console. We support a request component plus a post
# consumption component so local estimates can mirror your billing model.
X_API_TIMELINE_COST_PER_SUCCESS_REQUEST_USD = float(
    os.environ.get("X_API_TIMELINE_COST_PER_SUCCESS_REQUEST_USD", "0")
)
X_API_TIMELINE_COST_PER_POST_USD = float(
    os.environ.get("X_API_TIMELINE_COST_PER_POST_USD", "0")
)
X_API_TWEET_LOOKUP_COST_PER_SUCCESS_REQUEST_USD = float(
    os.environ.get("X_API_TWEET_LOOKUP_COST_PER_SUCCESS_REQUEST_USD", "0")
)
X_API_TWEET_LOOKUP_COST_PER_POST_USD = float(
    os.environ.get("X_API_TWEET_LOOKUP_COST_PER_POST_USD", "0")
)

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 5050
POSTS_PER_PAGE = 50
WEEKLY_PREP_TOPIC_LIMIT = 20
# Cap auto-ranked slide suggestions (forced vote/override slide items can exceed).
WEEKLY_PREP_AUTO_SUGGESTED_MAX = 10
WEEKLY_PREP_BULLET_TARGET = 30
WEEKLY_SCORE_ENGAGEMENT_CAP = 5000
WEEKLY_SCORE_ENGAGEMENT_WEIGHT = 0.10
# Impression weighting (page-view exposure signal; ranking/tiebreaker only)
WEEKLY_SCORE_IMPRESSION_UNIQUE_VIEWER_WEIGHT = 20   # Points per unique team viewer
WEEKLY_SCORE_IMPRESSION_TOTAL_WEIGHT = 0             # Raw exposure rows are noisy
WEEKLY_SCORE_IMPRESSION_TOTAL_CAP = 30               # Cap raw impressions counted
# Auto-ranked slide quality gate (prevents weak backfill from promoted pool)
WEEKLY_PREP_AUTO_SUGGEST_MIN_CONTENT_SCORE = 140
WEEKLY_PREP_AUTO_SUGGEST_MIN_WEEK_POSTS = 2
WEEKLY_PREP_AUTO_SUGGEST_MIN_SOURCES = 2
WEEKLY_PREP_AUTO_SUGGEST_MIN_WEEK_ENGAGEMENT = 300
WEEKLY_PREP_AUTO_SUGGEST_MIN_REPOSTS = 15
WEEKLY_PREP_AUTO_SUGGEST_MIN_AGG_IMPRESSIONS = 1000
# Keep Weekly slide list sparse by default (promoted + forced items only).
# Set True to restore legacy candidate backfill behavior.
WEEKLY_PREP_ALLOW_CANDIDATE_FALLBACK = False
# Weekly cycle boundaries (Friday 12:05 AM local = prior Thursday report is closed)
WEEKLY_CYCLE_TIMEZONE = os.environ.get("WEEKLY_CYCLE_TIMEZONE", "America/Chicago")
WEEKLY_CYCLE_START_WEEKDAY = 4  # Monday=0 ... Friday=4
WEEKLY_CYCLE_START_HOUR = 0
WEEKLY_CYCLE_START_MINUTE = 5
# Covered-topic re-entry thresholds (material change v1)
WEEKLY_COVERED_REENTRY_MIN_POSTS = 3
WEEKLY_COVERED_REENTRY_MIN_AUTHORS = 2
WEEKLY_COVERED_REENTRY_MIN_ENGAGEMENT_DELTA = 1200
# Pre-reset momentum de-prioritization window:
# Thursday 12:00 fixed MST (UTC-7), used to down-rank carry-over topics that
# peaked earlier in the week but did not continue into the post-cutoff window.
WEEKLY_PRE_RESET_CUTOFF_UTC_OFFSET_HOURS = -7
WEEKLY_PRE_RESET_CUTOFF_WEEKDAY = 3  # Monday=0 ... Thursday=3
WEEKLY_PRE_RESET_CUTOFF_HOUR = 12
WEEKLY_PRE_RESET_CUTOFF_MINUTE = 0
WEEKLY_PRE_RESET_DEPRIOR_MIN_PRE_POSTS = 5
WEEKLY_PRE_RESET_DEPRIOR_MIN_PRE_SOURCES = 3
WEEKLY_PRE_RESET_DEPRIOR_MIN_PRE_ENGAGEMENT = 900
WEEKLY_PRE_RESET_DEPRIOR_MAX_POST_CUTOFF_POSTS = 1
WEEKLY_PRE_RESET_DEPRIOR_MAX_POST_CUTOFF_SOURCES = 1
WEEKLY_PRE_RESET_DEPRIOR_MAX_POST_CUTOFF_ENGAGEMENT = 250

# ---------------------------------------------------------------------------
# API Cost Tracking (pricing as of 2026-02)
# ---------------------------------------------------------------------------
HAIKU_INPUT_COST_PER_1M = 0.80     # $/1M input tokens
HAIKU_OUTPUT_COST_PER_1M = 4.00    # $/1M output tokens
SONNET_INPUT_COST_PER_1M = 3.00    # $/1M input tokens
SONNET_OUTPUT_COST_PER_1M = 15.00  # $/1M output tokens
OPUS_INPUT_COST_PER_1M = 5.00     # $/1M input tokens
OPUS_OUTPUT_COST_PER_1M = 25.00   # $/1M output tokens

# ---------------------------------------------------------------------------
# Vector Search (topic pre-filtering for topic_matcher)
# ---------------------------------------------------------------------------
VECTOR_SEARCH_ENABLED = True
VECTOR_MODEL_NAME = "all-MiniLM-L6-v2"
VECTOR_EMBEDDING_DIM = 384
VECTOR_TOP_K = 15            # Candidates per post
VECTOR_MAX_UNION_SIZE = 80   # Max topics after union across batch
VECTOR_POST_SNIPPETS = 3     # Recent post excerpts to include in topic embedding
VECTOR_SNIPPET_CHARS = 150   # Max chars per post snippet

# ---------------------------------------------------------------------------
# Topic Search (lexical + semantic hybrid)
# ---------------------------------------------------------------------------
TOPIC_SEARCH_SEMANTIC_ENABLED = True
TOPIC_SEARCH_HYBRID_ALPHA = 0.65       # Lexical weight in hybrid score
TOPIC_SEARCH_SEMANTIC_K_DEFAULT = 40   # Default vector candidate pool
TOPIC_SEARCH_SEMANTIC_K_MAX = 120      # Hard cap for vector candidate pool
TOPIC_SEARCH_SEMANTIC_MIN_QUERY_LEN = 2

# ---------------------------------------------------------------------------
# Topic Promotion (candidate -> promoted)
# ---------------------------------------------------------------------------
TOPIC_AUTO_PROMOTION_ENABLED = True
TOPIC_PROMOTE_MIN_TOTAL_POSTS = 10              # Strong signal over time
TOPIC_PROMOTE_MIN_WEEK_POSTS = 5                # Faster promotion path
TOPIC_PROMOTE_MIN_WEEK_AUTHORS = 4              # Distinct sources/authors
TOPIC_PROMOTE_ENGAGEMENT_MIN_WEEK_POSTS = 3     # Small-volume but high-signal
TOPIC_PROMOTE_ENGAGEMENT_MIN_WEEK_AUTHORS = 2   # Avoid 1-source singleton promotion
TOPIC_PROMOTE_MIN_WEEK_ENGAGEMENT_SCORE = 1200  # Weighted sum of metrics
# Impression-based promotion (team interest signal)
TOPIC_PROMOTE_USE_IMPRESSIONS_FOR_AUTO_PROMOTION = False
TOPIC_PROMOTE_MIN_IMPRESSIONS = 6                    # Total impression rows
TOPIC_PROMOTE_MIN_IMPRESSION_VIEWERS = 2             # Distinct team members who viewed

# ---------------------------------------------------------------------------
# Data Retention
# ---------------------------------------------------------------------------
ARCHIVE_AFTER_DAYS = 14             # Prune posts older than 2 weeks

# ---------------------------------------------------------------------------
# Topic Summary Generation
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Engagement Metrics Refresh
# ---------------------------------------------------------------------------
METRICS_REFRESH_ENABLED = True
METRICS_REFRESH_MAX_API_REQUESTS = 3      # 3 batches x 100 = 300 posts max/cycle
METRICS_REFRESH_MAX_POST_AGE_DAYS = 7     # Hard cutoff
METRICS_REFRESH_STABLE_THRESHOLD = 3      # Consecutive unchanged checks to mark stable
METRICS_REFRESH_AFFECTS_RANKING = False    # Shadow mode: log only, don't affect ranking yet
# Priority-aware refresh budget controls
METRICS_REFRESH_TARGET_POSTS_PER_CYCLE = int(
    os.environ.get("METRICS_REFRESH_TARGET_POSTS_PER_CYCLE", "200")
)
METRICS_REFRESH_CANDIDATE_SCAN_LIMIT = int(
    os.environ.get("METRICS_REFRESH_CANDIDATE_SCAN_LIMIT", "1000")
)
METRICS_REFRESH_QUOTA_LT12H = int(
    os.environ.get("METRICS_REFRESH_QUOTA_LT12H", "140")
)
METRICS_REFRESH_QUOTA_12_24H = int(
    os.environ.get("METRICS_REFRESH_QUOTA_12_24H", "40")
)
METRICS_REFRESH_QUOTA_24_48H = int(
    os.environ.get("METRICS_REFRESH_QUOTA_24_48H", "20")
)
METRICS_REFRESH_24_48H_PROMOTED_ONLY = os.environ.get(
    "METRICS_REFRESH_24_48H_PROMOTED_ONLY", "1"
).strip().lower() in {"1", "true", "yes", "on"}

SUMMARY_REFRESH_ENABLED = True
SUMMARY_MIN_NEW_POSTS = 3           # New posts since last summary to trigger refresh
SUMMARY_BATCH_SIZE = 5              # Max topics per fetch cycle
SUMMARY_MODEL = OPUS_MODEL          # Opus for highest quality editorial summaries
SUMMARY_MAX_TOKENS = 2048
SUMMARY_MAX_POSTS_CONTEXT = 50      # Max posts to include (first-10 + last-40 windowing if >50)

# ---------------------------------------------------------------------------
# Voting / Triage
# ---------------------------------------------------------------------------
_voters = os.environ.get("VOTER_NAMES", "Admin,User1,User2")
VOTER_NAMES = [v.strip() for v in _voters.split(",")]
VALID_VOTE_TYPES = {"slide", "bullet", "skip", "unsure", "flag"}
VALID_SKIP_REASONS = {"not_good_fit", "already_covered"}
DEFAULT_SKIP_REASON = "not_good_fit"

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
SESSION_COOKIE_NAME = "xfi_session"
SESSION_MAX_AGE_DAYS = 30
STATIC_DIR = BASE_DIR / "static"

# ---------------------------------------------------------------------------
# Transcription Workflow Integration
# ---------------------------------------------------------------------------
TRANSCRIPTION_INTEGRATION_ENABLED = os.environ.get(
    "TRANSCRIPTION_INTEGRATION_ENABLED", "0"
).strip().lower() in {"1", "true", "yes", "on"}
TRANSCRIPTION_INTEGRATION_TOKEN = os.environ.get("TRANSCRIPTION_INTEGRATION_TOKEN", "").strip()
TRANSCRIPTION_CALLBACK_URL = os.environ.get(
    "TRANSCRIPTION_CALLBACK_URL",
    "http://127.0.0.1:5050/api/integrations/transcription-events",
).strip()
TRANSCRIPTION_CALLBACK_TIMEOUT_SEC = float(
    os.environ.get("TRANSCRIPTION_CALLBACK_TIMEOUT_SEC", "10")
)
TRANSCRIPTION_DEFAULT_WORKFLOW = os.environ.get(
    "TRANSCRIPTION_DEFAULT_WORKFLOW", "transcribedeep"
).strip() or "transcribedeep"
TRANSCRIPTION_ALLOWED_WORKFLOWS = {
    "transcribe123",
    "transcribeteam",
    "transcribedeep",
    "transcribeslide",
}
TRANSCRIPTION_TIMEOUT_WATCHDOG_ENABLED = os.environ.get(
    "TRANSCRIPTION_TIMEOUT_WATCHDOG_ENABLED", "1"
).strip().lower() in {"1", "true", "yes", "on"}
TRANSCRIPTION_PENDING_TIMEOUT_MINUTES = int(
    os.environ.get("TRANSCRIPTION_PENDING_TIMEOUT_MINUTES", "45")
)
TRANSCRIPTION_TIMEOUT_WATCHDOG_SCAN_LIMIT = int(
    os.environ.get("TRANSCRIPTION_TIMEOUT_WATCHDOG_SCAN_LIMIT", "200")
)
TRANSCRIPTION_TIMEOUT_WATCHDOG_MIN_INTERVAL_SEC = float(
    os.environ.get("TRANSCRIPTION_TIMEOUT_WATCHDOG_MIN_INTERVAL_SEC", "60")
)

# ---------------------------------------------------------------------------
# Email (optional, for notifications)
# ---------------------------------------------------------------------------
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
_recipients = os.environ.get("EMAIL_RECIPIENTS", "")
EMAIL_RECIPIENTS = [r.strip() for r in _recipients.split(",") if r.strip()]

def _gen_username(name: str) -> str:
    """Generate a username like 'admin_a3f1' from a display name."""
    return f"{name.lower()}_{_secrets.token_hex(2)}"

DEFAULT_USERS = [
    {"display_name": "Admin", "is_admin": 1},
    {"display_name": "User1", "is_admin": 0},
    {"display_name": "User2", "is_admin": 0},
]
DEFAULT_PASSWORD = "changeme"

# ---------------------------------------------------------------------------
# Relevance topics (used in classification prompt)
# ---------------------------------------------------------------------------
RELEVANCE_TOPICS = [
    "AI / AGI / AI infrastructure (training, inference, deployment)",
    "Cloud service providers (AWS, Azure, GCP, Oracle, CoreWeave, etc.)",
    "AI software ecosystem (vLLM, SGLang, TensorRT-LLM, PyTorch, JAX, ROCm, CUDA, Triton)",
    "AI labs (OpenAI, Anthropic, Google DeepMind, Meta AI, xAI, Mistral, Cohere, etc.)",
    "Chinese AI labs and tech (DeepSeek, Baidu, ByteDance, Alibaba/Qwen, Tencent, etc.)",
    "AMD competitors, customers, and supply chain (NVIDIA, Intel, Qualcomm, TSMC, etc.)",
    "AI model releases and benchmarks (MLPerf, MMLU, etc.)",
    "AI hardware (GPUs, accelerators, custom silicon, TPUs, Trainium, HBM, networking)",
    "Semiconductor industry news relevant to AI (TSMC, Samsung foundry, CoWoS, etc.)",
    "AI policy, regulation, export controls, government AI investment",
    "Data center capacity, power, cooling, and infrastructure buildout",
    "Enterprise AI adoption and AI spending trends",
]

# ---------------------------------------------------------------------------
# Two-level taxonomy (from DCGPU MI Weekly Updates report)
# ---------------------------------------------------------------------------
TAXONOMY = {
    "MACRO_FRAMES": {
        "label": "Macro Frames",
        "icon": "\U0001f30d",
        "subcategories": {
            "AI_CIVILIZATIONAL_SHIFT": "AI as general-purpose technology; compute as strategic resource; speed/scale/energy limits",
            "STRUCTURAL_FORCES": "Exponential scaling laws; energy efficiency; vertical vs modular ecosystems; frontier vs diffusion",
        },
    },
    "INDUSTRY_NARRATIVES": {
        "label": "Industry Narratives",
        "icon": "\U0001f4ca",
        "subcategories": {
            "GLOBAL_AI_MARKET": "Training to inference shift; agentic workloads; commoditization vs differentiation",
            "COMPETITIVE_LANDSCAPE": "US hyperscalers vs China; open-weight vs proprietary; custom vs merchant silicon; cloud vs sovereign",
        },
    },
    "GEOPOLITICS_POLICY": {
        "label": "Geopolitics & Policy",
        "icon": "\U0001f3db\ufe0f",
        "subcategories": {
            "EXPORT_CONTROLS": "BIS export regimes; China AI compute constraints; DoD AI strategies; national AI programs",
            "SOVEREIGNTY_POLICY": "Sovereign AI clouds; public-private financing; domestic chip ecosystems; safety vs speed regulation",
        },
    },
    "AI_ECONOMICS": {
        "label": "AI Economics",
        "icon": "\U0001f4b0",
        "subcategories": {
            "AI_REVENUE_MODELS": "API/product/agent pricing; token economics; cache monetization; pay-for-results",
            "CAPEX_MARGINS": "$/GW economics; GPU vs ASIC cost curves; ODM/CSP margin; build-vs-buy compute",
        },
    },
    "DATA_CENTER_INFRA": {
        "label": "Data Center Infra",
        "icon": "\U0001f3d7\ufe0f",
        "subcategories": {
            "AI_DC_ARCHITECTURE": "Hyperscale AI campuses 100MW to multi-GW; power gen; cooling; physical density",
            "POWER_ENERGY": "Power as binding constraint; efficiency per token; 800V DC architectures; power-to-compute",
        },
    },
    "HARDWARE_PLATFORMS": {
        "label": "Hardware Platforms",
        "icon": "\U0001f527",
        "subcategories": {
            "GPU_PLATFORMS": "NVIDIA Blackwell/Rubin; AMD MI-series; NVL topologies; scale-up vs scale-out",
            "CUSTOM_SILICON": "TPU v6/v7; Trainium 2/2.5/3; MTIA; OpenAI+Broadcom custom; MediaTek",
            "PACKAGING_MANUFACTURING": "CoWoS S/L/R; N3/N2/A16; HBM3E capacity; TSMC/ASE/Amkor dynamics",
        },
    },
    "NETWORKING": {
        "label": "Networking",
        "icon": "\U0001f517",
        "subcategories": {
            "SCALE_UP_NETWORKING": "NVLink fabrics; Superpod architectures; bandwidth vs latency tradeoffs",
            "SCALE_OUT_NETWORKING": "Ethernet vs InfiniBand; Spectrum-X; CPO; UEC/ESUN standards",
            "OPTICAL_PHOTONICS": "True CPO switches; photonic engines; fiber array units; energy per bit",
        },
    },
    "INFERENCE_STACK": {
        "label": "Inference Stack",
        "icon": "\u26a1",
        "subcategories": {
            "INFERENCE_ARCHITECTURE": "Scale-up vs disaggregation; latency vs bandwidth-bound; long-context vs throughput",
            "PARALLELISM_STRATEGIES": "TP, CP, Helix, prefill/decode disaggregation, attention-FFN disaggregation",
            "INFERENCE_FRAMEWORKS": "TensorRT-LLM, vLLM, SGLang, Dynamo, llm-d",
        },
    },
    "MODEL_ARCHITECTURE": {
        "label": "Model Architecture",
        "icon": "\U0001f9e0",
        "subcategories": {
            "SCALING_PATTERNS": "Dense vs MoE; hybrid linear attention; sparse experts; long-context optimization",
            "MEMORY_EFFICIENCY": "Conditional/tiered memory; KV-cache optimization; SRAM-resident; quantization INT4/INT8/FP8",
        },
    },
    "FRONTIER_MODELS": {
        "label": "Frontier Models",
        "icon": "\U0001f680",
        "subcategories": {
            "PROPRIETARY_FRONTIER": "GPT-5.x; Claude Opus/Sonnet/Haiku; Gemini 3/Deep Think",
            "OPEN_CHINA_FRONTIER": "GLM-5; Qwen 3.5/397B; Ling/Ring 1T; MiniMax M2.5; Kimi K2.5; DeepSeek",
            "CAPABILITY_DOMAINS": "Reasoning ARC-AGI/IMO; coding agents; computer use; multimodal; long-horizon planning",
        },
    },
    "AGENTS_AUTONOMY": {
        "label": "Agents & Autonomy",
        "icon": "\U0001f916",
        "subcategories": {
            "AGENT_ARCHITECTURES": "Single vs multi-agent; coordinator+specialists; persistent memory; tool-using",
            "AGENT_WORKLOADS": "Software engineering; research/analysis; enterprise workflows; consumer automation",
            "AGENT_ECONOMICS": "Token explosion; I/O asymmetry; cache effects; latency sensitivity",
        },
    },
    "VERTICAL_APPS": {
        "label": "Vertical Apps",
        "icon": "\U0001f4f1",
        "subcategories": {
            "HEALTHCARE_LIFESCI": "Drug discovery; wet-lab/dry-lab; medical imaging; clinical automation",
            "ENTERPRISE_SOFTWARE": "CRM/ERP agents; office productivity; data platforms; regulated-industry AI",
            "DEVELOPER_TOOLS": "ML frameworks; AI dev tools; open-source AI apps; SDKs; Gradio/Streamlit/LangChain; code assistants",
            "CONSUMER_COMMERCE": "Agentic shopping; retail AI; voice assistants; personal productivity",
        },
    },
    "RESEARCH_FRONTIERS": {
        "label": "Research Frontiers",
        "icon": "\U0001f52c",
        "subcategories": {
            "LEARNING_PARADIGMS": "RL scaling; self-improving agents; memory-driven learning; experience replay",
            "BENCHMARKS_EVAL": "ARC-AGI; Humanity's Last Exam; SWE-Bench; OSWorld/AndroidWorld",
        },
    },
    "OPERATIONAL_METRICS": {
        "label": "Operational Metrics",
        "icon": "\U0001f4c8",
        "subcategories": {
            "SUPPLY_CHAIN_METRICS": "CoWoS wafer counts; GPU unit shipments; rack tracking; inventory rollover",
            "PRICING_MARKET": "Spot vs on-demand GPU pricing; capacity blocks; ASP trends; regional pricing",
        },
    },
}

# Derived flat lists for backward compatibility and quick lookups
CATEGORIES = list(TAXONOMY.keys())

SUBCATEGORY_TO_PARENT = {}
ALL_SUBCATEGORIES = []
for _parent, _info in TAXONOMY.items():
    for _sub_key in _info["subcategories"]:
        SUBCATEGORY_TO_PARENT[_sub_key] = _parent
        ALL_SUBCATEGORIES.append(_sub_key)

# Old-to-new category migration mapping
OLD_CATEGORY_MAP = {
    "AI_MODELS": "FRONTIER_MODELS",
    "AI_INFRASTRUCTURE": "HARDWARE_PLATFORMS",
    "CLOUD_PROVIDERS": "DATA_CENTER_INFRA",
    "SOFTWARE_ECOSYSTEM": "INFERENCE_STACK",
    "AI_LABS": "INDUSTRY_NARRATIVES",
    "SEMICONDUCTORS": "HARDWARE_PLATFORMS",
    "AMD_ECOSYSTEM": "HARDWARE_PLATFORMS",
    "CHINA_AI": "GEOPOLITICS_POLICY",
    "POLICY_REGULATION": "GEOPOLITICS_POLICY",
    "DATA_CENTERS": "DATA_CENTER_INFRA",
    "ENTERPRISE_AI": "VERTICAL_APPS",
    "OTHER_RELEVANT": "MACRO_FRAMES",
}


def validate():
    """Validate configuration before running. Raises ValueError on missing creds."""
    errors = []
    if not X_CONSUMER_KEY:
        errors.append("X_CONSUMER_KEY not set in .env")
    if not X_CONSUMER_SECRET:
        errors.append("X_CONSUMER_SECRET not set in .env")
    if not X_ACCESS_TOKEN:
        errors.append("X_ACCESS_TOKEN not set in .env")
    if not X_ACCESS_TOKEN_SECRET:
        errors.append("X_ACCESS_TOKEN_SECRET not set in .env")
    if not X_USER_ID:
        errors.append("X_USER_ID not set in .env")
    if not ANTHROPIC_API_KEY:
        errors.append("ANTHROPIC_API_KEY not set in environment or .env")

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if errors:
        raise ValueError(f"Configuration errors: {'; '.join(errors)}")
    return True
