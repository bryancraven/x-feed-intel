#!/usr/bin/env python3
"""X Feed Intel — Flask web dashboard."""
import json
import math
import os
import re
import secrets
import sys
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from contextlib import contextmanager
from functools import wraps
from typing import Optional
from urllib.parse import urlencode

from logging_config import setup_service_logging

from flask import Flask, render_template, request, jsonify, Response, redirect, make_response, g, has_request_context, url_for, abort
from werkzeug.security import check_password_hash

import config
from database import get_db

logger = setup_service_logging("x_feed_intel_dashboard")
_SLOW_REQUEST_LOG_MS = float(os.environ.get("XFI_SLOW_REQUEST_LOG_MS", "350"))
_COMMON_CTX_STATS_TTL_SEC = float(os.environ.get("XFI_COMMON_CTX_STATS_TTL_SEC", "5"))
_COMMON_CTX_CACHE_LOCK = threading.Lock()
_COMMON_CTX_CACHE: dict[str, dict] = {}
_TRANSCRIPTION_WATCHDOG_LOCK = threading.Lock()
_TRANSCRIPTION_WATCHDOG_LAST_RUN_TS = 0.0

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
    static_folder=os.path.join(os.path.dirname(__file__), "static"),
)


def _perf_now_ms() -> float:
    return time.perf_counter() * 1000.0


def _timing_metric_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", str(name or "metric")).strip("_") or "metric"


def _record_server_timing(name: str, duration_ms: float, desc: Optional[str] = None) -> None:
    """Attach a timing metric to the current request for Server-Timing response header."""
    if not has_request_context():
        return
    metrics = getattr(g, "_server_timing_metrics", None)
    if metrics is None:
        metrics = []
        g._server_timing_metrics = metrics

    metric_name = _timing_metric_name(name)
    metric = f"{metric_name};dur={max(0.0, float(duration_ms)):.1f}"
    if desc:
        safe_desc = str(desc).replace('"', "'")
        metric += f';desc="{safe_desc}"'
    metrics.append(metric)


@contextmanager
def _timed_section(name: str, desc: Optional[str] = None):
    start_ms = _perf_now_ms()
    try:
        yield
    finally:
        _record_server_timing(name, _perf_now_ms() - start_ms, desc=desc)


@app.before_request
def _start_request_timing():
    g._req_started_ms = _perf_now_ms()
    g._server_timing_metrics = []


@app.teardown_appcontext
def _teardown_request_db(_exc):
    db = getattr(g, "_xfi_db", None)
    if db is None:
        return
    try:
        db.close()
    except Exception as e:
        logger.warning("request db close failed: %s", e)
    finally:
        g._xfi_db = None


@app.after_request
def _finish_request_timing(response):
    started_ms = getattr(g, "_req_started_ms", None)
    if started_ms is not None:
        total_ms = _perf_now_ms() - started_ms
        _record_server_timing("app", total_ms, desc="total")
        response.headers["Server-Timing"] = ", ".join(getattr(g, "_server_timing_metrics", []))
        response.headers["X-Response-Time-Ms"] = f"{max(0.0, total_ms):.1f}"

        if total_ms >= _SLOW_REQUEST_LOG_MS:
            logger.info(
                "slow_request method=%s path=%s endpoint=%s status=%s ms=%.1f",
                request.method,
                request.path,
                request.endpoint,
                response.status_code,
                total_ms,
            )
    if request.path.startswith("/static/") and response.status_code == 200:
        response.cache_control.public = True
        response.cache_control.max_age = 604800  # 7d (URLs are cache-busted via ?v=mtime)
        response.cache_control.immutable = True
    return response


def _asset_version(filename: str) -> int:
    """Best-effort static asset version based on mtime."""
    try:
        path = os.path.join(app.static_folder, filename)
        return int(os.path.getmtime(path))
    except Exception:
        return 0


@app.context_processor
def inject_asset_helpers():
    def asset_url(filename: str) -> str:
        return url_for("static", filename=filename, v=_asset_version(filename))

    return {"asset_url": asset_url}


def _parse_summary_fields(topic: dict) -> dict:
    """Parse summary_key_takeaways and summary_bullets JSON into lists."""
    for field, key in [("summary_key_takeaways", "parsed_key_takeaways"),
                       ("summary_bullets", "parsed_bullets")]:
        raw = topic.get(field)
        try:
            topic[key] = json.loads(raw) if raw else []
        except (json.JSONDecodeError, TypeError):
            topic[key] = []
    return topic


def _common_ctx_cache_get(key: str):
    if _COMMON_CTX_STATS_TTL_SEC <= 0:
        return None
    now = time.time()
    with _COMMON_CTX_CACHE_LOCK:
        rec = _COMMON_CTX_CACHE.get(key)
        if not rec:
            return None
        if rec.get("expires_at", 0) <= now:
            _COMMON_CTX_CACHE.pop(key, None)
            return None
        return rec.get("value")


def _common_ctx_cache_set(key: str, value):
    if _COMMON_CTX_STATS_TTL_SEC <= 0:
        return
    expires_at = time.time() + max(0.1, _COMMON_CTX_STATS_TTL_SEC)
    with _COMMON_CTX_CACHE_LOCK:
        _COMMON_CTX_CACHE[key] = {"value": value, "expires_at": expires_at}


def _common_ctx_cached_value(key: str, timing_name: str, loader):
    cached = _common_ctx_cache_get(key)
    if cached is not None:
        _record_server_timing(f"{timing_name}_cache", 0.0, desc="hit")
        return cached
    _record_server_timing(f"{timing_name}_cache", 0.0, desc="miss")
    with _timed_section(timing_name):
        value = loader()
    _common_ctx_cache_set(key, value)
    return value


def _run_transcription_timeout_watchdog(db) -> None:
    """Best-effort watchdog that expires stale pending transcription topics."""
    if not getattr(config, "TRANSCRIPTION_INTEGRATION_ENABLED", True):
        return
    if not getattr(config, "TRANSCRIPTION_TIMEOUT_WATCHDOG_ENABLED", True):
        return

    global _TRANSCRIPTION_WATCHDOG_LAST_RUN_TS
    min_interval = max(
        5.0,
        float(getattr(config, "TRANSCRIPTION_TIMEOUT_WATCHDOG_MIN_INTERVAL_SEC", 60.0) or 60.0),
    )
    now_ts = time.time()
    with _TRANSCRIPTION_WATCHDOG_LOCK:
        if (now_ts - _TRANSCRIPTION_WATCHDOG_LAST_RUN_TS) < min_interval:
            return
        _TRANSCRIPTION_WATCHDOG_LAST_RUN_TS = now_ts

    timeout_minutes = int(getattr(config, "TRANSCRIPTION_PENDING_TIMEOUT_MINUTES", 45) or 45)
    scan_limit = int(getattr(config, "TRANSCRIPTION_TIMEOUT_WATCHDOG_SCAN_LIMIT", 200) or 200)
    timed_out = db.mark_stale_transcription_topics(
        timeout_minutes=timeout_minutes,
        limit=scan_limit,
    )
    if timed_out:
        logger.warning(
            "Transcription watchdog timed out %s topic(s) after %s min: %s",
            len(timed_out),
            timeout_minutes,
            ",".join(str(int(t["id"])) for t in timed_out[:20]),
        )


def _parse_bool_arg(raw_value, default: bool) -> bool:
    """Parse common truthy/falsy query arg values with fallback to default."""
    if raw_value is None:
        return bool(default)
    val = str(raw_value).strip().lower()
    if val in {"1", "true", "yes", "on"}:
        return True
    if val in {"0", "false", "no", "off"}:
        return False
    return bool(default)

# Generate a persistent secret key (stored in DB state table)
def _get_or_create_secret_key(db):
    cur = db.conn.execute("SELECT value FROM state WHERE key = 'flask_secret_key'")
    row = cur.fetchone()
    if row:
        return row["value"]
    key = secrets.token_hex(32)
    now = datetime.utcnow().isoformat()
    db.conn.execute("""
        INSERT INTO state (key, value, updated_at) VALUES ('flask_secret_key', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = ?
    """, (key, now, key, now))
    db.conn.commit()
    return key

# ------------------------------------------------------------------
# Initialize database once at startup (not per-request)
# ------------------------------------------------------------------
with app.app_context():
    _db = get_db()
    _db.init_db()

    # Set Flask secret key
    app.secret_key = _get_or_create_secret_key(_db)

    # Seed default users (idempotent)
    created_users = _db.seed_users(config.DEFAULT_USERS, config.DEFAULT_PASSWORD)
    if created_users:
        print("\n=== NEW USER ACCOUNTS CREATED ===")
        for u in created_users:
            print(f"  {u['display_name']:8s} -> username: {u['username']}  password: {config.DEFAULT_PASSWORD}")
        print("=================================\n")

    # Cleanup expired sessions
    _db.delete_expired_sessions()


# ------------------------------------------------------------------
# Auth helpers
# ------------------------------------------------------------------
def get_current_user():
    """Check session cookie, return user dict or None."""
    token = request.cookies.get(config.SESSION_COOKIE_NAME)
    if not token:
        return None
    db = get_db()
    return db.get_session(token)


def login_required(f):
    """Decorator: redirect to /login if not authenticated."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            # For API endpoints, return 401 JSON instead of redirect
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return redirect("/login")
        request.user = user
        return f(*args, **kwargs)
    return decorated


def _last_thursday():
    """Return the most recent Thursday (or today if it's Thursday) as ISO date string."""
    today = datetime.utcnow().date()
    days_since_thursday = (today.weekday() - 3) % 7
    if days_since_thursday == 0 and datetime.utcnow().hour < 20:
        days_since_thursday = 7
    last_thu = today - timedelta(days=days_since_thursday)
    return last_thu.isoformat()


def _get_weekly_cycle_context(db):
    """Resolve active weekly cycle and auto-roll over if needed."""
    try:
        cycle = db.ensure_current_weekly_cycle(actor="system:web")
        if cycle:
            return {
                "cycle": cycle,
                "week_id": cycle.get("id"),
                "since_query": cycle.get("starts_at") or cycle.get("starts_local_date") or _last_thursday(),
                "since_label": cycle.get("starts_local_date") or _last_thursday(),
            }
    except Exception as e:
        logger.warning("Weekly cycle context failed, using legacy weekly window: %s", e)
    since = _last_thursday()
    return {"cycle": None, "week_id": None, "since_query": since, "since_label": since}


def _get_filters():
    """Extract filter params from the query string."""
    raw_cat = request.args.get("category", "")
    category = ""
    subcategory = ""
    if ":" in raw_cat:
        category, subcategory = raw_cat.split(":", 1)
    else:
        category = raw_cat

    return {
        "category": category,
        "subcategory": subcategory,
        "date_from": request.args.get("date_from", ""),
        "date_to": request.args.get("date_to", ""),
        "search": request.args.get("search", ""),
    }


def _filter_qs(filters):
    """Build a query string fragment (without 'page') for pagination links."""
    parts = {}
    if filters.get("category") and filters.get("subcategory"):
        parts["category"] = f"{filters['category']}:{filters['subcategory']}"
    elif filters.get("category"):
        parts["category"] = filters["category"]
    for k in ("date_from", "date_to", "search"):
        if filters.get(k):
            parts[k] = filters[k]
    if parts:
        return "&" + urlencode(parts)
    return ""


def _enrich_posts(posts):
    """Add reply metadata to posts by parsing referenced_tweets_json."""
    for p in posts:
        p["is_reply"] = False
        p["reply_to_id"] = None
        raw = p.get("referenced_tweets_json")
        if raw:
            try:
                refs = json.loads(raw) if isinstance(raw, str) else raw
                for ref in refs:
                    if ref.get("type") == "replied_to":
                        p["is_reply"] = True
                        p["reply_to_id"] = ref.get("id")
                        break
            except (json.JSONDecodeError, TypeError):
                pass
    return posts


def _common_ctx(**overrides):
    """Build the common template context, merging in overrides."""
    db = get_db()
    try:
        with _timed_section("transcription_watchdog"):
            _run_transcription_timeout_watchdog(db)
    except Exception as e:
        logger.warning("Transcription watchdog failed (non-fatal): %s", e)

    # Get API usage stats for cost display
    try:
        api_stats = _common_ctx_cached_value(
            "api_usage_stats",
            "db_api_usage_stats",
            db.get_api_usage_stats,
        )
    except Exception:
        api_stats = {
            "all_time": {"total_cost": 0, "total_input": 0, "total_output": 0, "total_calls": 0},
            "this_week": {"week_cost": 0, "week_input": 0, "week_output": 0, "week_calls": 0},
        }

    stats = _common_ctx_cached_value(
        "stats_extended",
        "db_stats_extended",
        db.get_stats_extended,
    )

    # Convert last_fetch_time (stored as UTC) to Boise (MT) and Bay Area (PT)
    fetch_boise = ""
    fetch_bayarea = ""
    lft = stats.get("last_fetch_time")
    if lft:
        try:
            naive = datetime.fromisoformat(lft)
            utc = ZoneInfo("UTC")
            mt = ZoneInfo("America/Boise")
            pt = ZoneInfo("America/Los_Angeles")
            aware = naive.replace(tzinfo=utc)
            fetch_boise = aware.astimezone(mt).strftime("%b %d %-I:%M %p MT")
            fetch_bayarea = aware.astimezone(pt).strftime("%-I:%M %p PT")
        except Exception:
            pass

    ctx = {
        "stats": stats,
        "api_stats": api_stats,
        "fetch_boise": fetch_boise,
        "fetch_bayarea": fetch_bayarea,
        "taxonomy": config.TAXONOMY,
        "categories": config.CATEGORIES,
        "filters": _get_filters(),
        "filter_qs": "",
        "current_page": 1,
        "total_pages": 1,
        # Defaults for optional sections
        "posts": [],
        "weekly_topics": [],
        "weekly_bullet_topics": [],
        "weekly_candidate_topics": [],
        "weekly_deprioritized_topics": [],
        "weekly_overflow_promoted_topics": [],
        "weekly_summary": {},
        "weekly_sort_mode": "ranked",
        "trending_today": [],
        "recent_posts": [],
        "detail_topics": [],
        "all_topics": [],
        "cat_topic_counts": [],
        "cat_filter": "",
        "subcat_filter": "",
        "subcat_counts": [],
        "topic_posts": [],
        "topic_detail": None,
        "topic_search_query": "",
        "topic_search_results": [],
        "prefill_topic_name": "",
        "history": [],
        "cost_data": {},
        "activity_summary": {},
        "since_date": "",
        "sort_by": "popular_week",
        "topic_status_filter": "all",
        "votes": {},
        "voter_names": config.VOTER_NAMES,
        "current_user": getattr(request, 'user', None),
        "taxonomy": config.TAXONOMY,
    }
    ctx.update(overrides)
    return ctx


def _build_weekly_sections_ctx(db, weekly_sort_mode: Optional[str] = None) -> dict:
    """Build server-authoritative weekly section data for page render and partial refresh."""
    with _timed_section("weekly_cycle_ctx"):
        weekly_ctx = _get_weekly_cycle_context(db)
    since = weekly_ctx["since_query"]
    week_id = weekly_ctx["week_id"]

    with _timed_section("db_weekly_prep_sections"):
        weekly_sections = db.get_weekly_prep_sections(
            since_date=since,
            slide_target=getattr(config, "WEEKLY_PREP_TOPIC_LIMIT", 20),
            bullet_target=getattr(config, "WEEKLY_PREP_BULLET_TARGET", 30),
            week_id=week_id,
        )

    weekly_topics = weekly_sections.get("slide_topics", [])
    weekly_bullet_topics = weekly_sections.get("bullet_topics", [])
    weekly_unsure_topics = weekly_sections.get("unsure_topics", [])
    weekly_candidate_topics = weekly_sections.get("candidate_topics", [])
    weekly_deprioritized_topics = weekly_sections.get("deprioritized_topics", [])

    # Ensure a fresh vote payload for all weekly cards (used for optimistic reconcile).
    topic_ids = list({
        *[t["id"] for t in weekly_topics],
        *[t["id"] for t in weekly_bullet_topics],
        *[t["id"] for t in weekly_unsure_topics],
        *[t["id"] for t in weekly_candidate_topics],
        *[t["id"] for t in weekly_deprioritized_topics],
    })
    votes = weekly_sections.get("votes", {}) if weekly_sections else {}
    if topic_ids:
        with _timed_section("db_votes_for_topics"):
            fresh_votes = db.get_votes_for_topics(topic_ids, week_id=week_id)
        if fresh_votes:
            votes.update(fresh_votes)

    if weekly_sort_mode is None:
        weekly_sort_mode = request.args.get("wsort", "ranked")
    if weekly_sort_mode not in {"ranked", "recent", "sources", "posts"}:
        weekly_sort_mode = "ranked"

    def _weekly_sort_key(t):
        # Always keep explicit editorial decisions at the top of each section.
        vote_priority = 2 if t.get("is_vote_forced") else (1 if t.get("is_override_forced") else 0)
        if weekly_sort_mode == "recent":
            return (vote_priority, str(t.get("latest_activity") or ""), float(t.get("weekly_score") or 0))
        if weekly_sort_mode == "sources":
            return (vote_priority, int(t.get("source_count") or 0), float(t.get("weekly_score") or 0))
        if weekly_sort_mode == "posts":
            return (vote_priority, int(t.get("week_post_count") or 0), float(t.get("weekly_score") or 0))
        return (vote_priority, float(t.get("weekly_score") or 0), int(t.get("week_post_count") or 0))

    weekly_topics = sorted(weekly_topics, key=_weekly_sort_key, reverse=True)
    weekly_bullet_topics = sorted(weekly_bullet_topics, key=_weekly_sort_key, reverse=True)
    weekly_unsure_topics = sorted(weekly_unsure_topics, key=_weekly_sort_key, reverse=True)
    weekly_candidate_topics = sorted(weekly_candidate_topics, key=_weekly_sort_key, reverse=True)
    weekly_deprioritized_topics = sorted(weekly_deprioritized_topics, key=_weekly_sort_key, reverse=True)

    for tlist in (weekly_topics, weekly_bullet_topics, weekly_unsure_topics, weekly_candidate_topics, weekly_deprioritized_topics):
        for t in tlist:
            _parse_summary_fields(t)

    return {
        "weekly_topics": weekly_topics,
        "weekly_bullet_topics": weekly_bullet_topics,
        "weekly_unsure_topics": weekly_unsure_topics,
        "weekly_candidate_topics": weekly_candidate_topics,
        "weekly_deprioritized_topics": weekly_deprioritized_topics,
        "weekly_overflow_promoted_topics": weekly_sections.get("overflow_promoted_topics", []),
        "weekly_summary": weekly_sections.get("summary", {}),
        "weekly_sort_mode": weekly_sort_mode,
        "since_date": weekly_ctx["since_label"],
        "votes": votes,
        "week_id": week_id,
    }


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    """Login page and authentication."""
    if request.method == "GET":
        # Already logged in? Go home
        if get_current_user():
            return redirect("/")
        return render_template("index.html", page="login", error=None,
                               stats={"relevant_posts": 0, "total_topics": 0, "total_posts": 0},
                               api_stats={"this_week": {"week_cost": 0}})

    # POST — validate credentials
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""

    db = get_db()
    user = db.get_user_by_username(username)

    if not user or not check_password_hash(user["password_hash"], password):
        return render_template("index.html", page="login", error="Invalid username or password.",
                               stats={"relevant_posts": 0, "total_topics": 0, "total_posts": 0},
                               api_stats={"this_week": {"week_cost": 0}})

    # Create session
    token = db.create_session(user["id"])
    resp = make_response(redirect("/"))
    resp.set_cookie(
        config.SESSION_COOKIE_NAME,
        token,
        max_age=config.SESSION_MAX_AGE_DAYS * 86400,
        httponly=True,
        samesite="Lax",
    )
    return resp


@app.route("/logout")
def logout():
    """Clear session and redirect to login."""
    token = request.cookies.get(config.SESSION_COOKIE_NAME)
    if token:
        db = get_db()
        db.delete_session(token)
    resp = make_response(redirect("/login"))
    resp.delete_cookie(config.SESSION_COOKIE_NAME)
    return resp


@app.route("/")
@login_required
def weekly_prep():
    """Default view — Weekly Prep: candidate topics for this week's report."""
    db = get_db()
    weekly_data = _build_weekly_sections_ctx(db)
    weekly_topics = weekly_data["weekly_topics"]
    weekly_bullet_topics = weekly_data["weekly_bullet_topics"]
    weekly_unsure_topics = weekly_data["weekly_unsure_topics"]
    weekly_candidate_topics = weekly_data["weekly_candidate_topics"]
    weekly_deprioritized_topics = weekly_data["weekly_deprioritized_topics"]
    week_id = weekly_data["week_id"]
    with _timed_section("db_trending_today"):
        trending = db.get_trending_today(limit=20)
    with _timed_section("db_recent_posts"):
        recent_posts = _enrich_posts(db.get_relevant_posts(limit=15))

    # Attach primary topic to each recent post for sidebar links
    if recent_posts:
        tweet_ids = [p["tweet_id"] for p in recent_posts if p.get("tweet_id")]
        if tweet_ids:
            placeholders = ",".join("?" * len(tweet_ids))
            with _timed_section("db_recent_post_topic_map"):
                cur = db.conn.execute(f"""
                    SELECT pt.post_id, pt.topic_id, t.name as topic_name
                    FROM post_topics pt
                    JOIN topics t ON t.id = pt.topic_id
                    WHERE pt.post_id IN ({placeholders})
                """, tweet_ids)
            post_topic_map = {}
            for row in cur:
                pid = row["post_id"]
                if pid not in post_topic_map:
                    post_topic_map[pid] = {"topic_id": row["topic_id"], "topic_name": row["topic_name"]}
            for p in recent_posts:
                pt_info = post_topic_map.get(p.get("tweet_id"))
                if pt_info:
                    p["topic_id"] = pt_info["topic_id"]
                    p["topic_name"] = pt_info["topic_name"]

    # Fetch votes for weekly + trending topic IDs in one query
    topic_ids = list({
        *[t["id"] for t in weekly_topics],
        *[t["id"] for t in weekly_bullet_topics],
        *[t["id"] for t in weekly_unsure_topics],
        *[t["id"] for t in weekly_candidate_topics],
        *[t["id"] for t in weekly_deprioritized_topics],
        *[t["id"] for t in trending],
    })
    votes = dict(weekly_data.get("votes", {}) or {})
    if topic_ids:
        # Ensure trending-only topics are included in the vote payload
        with _timed_section("db_votes_for_topics"):
            trending_votes = db.get_votes_for_topics(topic_ids, week_id=week_id)
        if trending_votes:
            votes.update(trending_votes)
    weekly_data = dict(weekly_data)
    weekly_data["votes"] = votes

    with _timed_section("render_template"):
        return render_template(
            "index.html",
            **_common_ctx(
                page="weekly",
                **weekly_data,
                trending_today=trending,
                recent_posts=recent_posts,
            ),
        )


@app.route("/api/weekly/sections")
@login_required
def api_weekly_sections():
    """Return the server-rendered weekly section markup for in-place UI refresh."""
    db = get_db()
    weekly_data = _build_weekly_sections_ctx(db)
    html = render_template(
        "_weekly_sections.html",
        taxonomy=config.TAXONOMY,
        weekly_topics=weekly_data["weekly_topics"],
        weekly_bullet_topics=weekly_data["weekly_bullet_topics"],
        weekly_unsure_topics=weekly_data["weekly_unsure_topics"],
        weekly_candidate_topics=weekly_data["weekly_candidate_topics"],
        weekly_deprioritized_topics=weekly_data["weekly_deprioritized_topics"],
        votes=weekly_data["votes"],
    )
    return jsonify({
        "ok": True,
        "html": html,
        "votes": weekly_data["votes"],
        "week_id": weekly_data["week_id"],
        "weekly_sort_mode": weekly_data["weekly_sort_mode"],
    })


@app.route("/posts")
@login_required
def posts():
    """Paginated relevant posts with filters."""
    db = get_db()
    filters = _get_filters()
    current_page = max(1, request.args.get("page", 1, type=int))
    per_page = config.POSTS_PER_PAGE

    with _timed_section("db_count_relevant_posts"):
        total = db.count_relevant_posts(
            category=filters["category"] or None,
            subcategory=filters["subcategory"] or None,
            date_from=filters["date_from"] or None,
            date_to=filters["date_to"] or None,
            search=filters["search"] or None,
        )
    total_pages = max(1, math.ceil(total / per_page))
    offset = (current_page - 1) * per_page

    with _timed_section("db_get_relevant_posts"):
        post_list = _enrich_posts(db.get_relevant_posts(
            limit=per_page,
            offset=offset,
            category=filters["category"] or None,
            subcategory=filters["subcategory"] or None,
            date_from=filters["date_from"] or None,
            date_to=filters["date_to"] or None,
            search=filters["search"] or None,
        ))

    with _timed_section("render_template"):
        return render_template(
            "index.html",
            **_common_ctx(
                page="posts",
                posts=post_list,
                filters=filters,
                filter_qs=_filter_qs(filters),
                current_page=current_page,
                total_pages=total_pages,
            ),
        )


@app.route("/topics")
@login_required
def topics():
    """Browse topics by category — two-level drill-down with sorting."""
    db = get_db()

    cat_filter = request.args.get("cat", "")
    subcat_filter = request.args.get("subcat", "")
    topic_search_query = (request.args.get("q", "") or "").strip()
    sort_by = request.args.get("sort", "popular_week")
    topic_status_filter = request.args.get("status", "all")
    if topic_status_filter not in {"all", "promoted", "candidate"}:
        topic_status_filter = "all"
    subcat_counts = []

    if topic_search_query:
        with _timed_section("db_search_topics"):
            topic_search_results = db.search_topics_hybrid(topic_search_query, limit=200)
        if topic_status_filter == "promoted":
            topic_search_results = [t for t in topic_search_results if int(t.get("is_promoted") or 0) == 1]
        elif topic_status_filter == "candidate":
            topic_search_results = [t for t in topic_search_results if int(t.get("is_promoted") or 0) == 0]

        with _timed_section("render_template"):
            return render_template(
                "index.html",
                **_common_ctx(
                    page="topics",
                    topic_search_query=topic_search_query,
                    topic_search_results=topic_search_results,
                    sort_by=sort_by,
                    topic_status_filter=topic_status_filter,
                ),
            )

    if cat_filter and subcat_filter:
        with _timed_section("db_get_topics_sorted"):
            detail_topics = db.get_topics_sorted(
                category=cat_filter, subcategory=subcat_filter, sort=sort_by, status=topic_status_filter
            )
    elif cat_filter:
        with _timed_section("db_get_topics_sorted"):
            detail_topics = db.get_topics_sorted(
                category=cat_filter, sort=sort_by, status=topic_status_filter
            )
        with _timed_section("db_subcategory_topic_counts"):
            subcat_counts = db.get_subcategory_topic_counts(cat_filter, status=topic_status_filter)
    else:
        with _timed_section("db_get_topics_sorted"):
            detail_topics = db.get_topics_sorted(sort=sort_by, status=topic_status_filter)

    with _timed_section("db_category_topic_counts"):
        cat_topic_counts = db.get_category_topic_counts(status=topic_status_filter)

    with _timed_section("render_template"):
        return render_template(
            "index.html",
            **_common_ctx(
                page="topics",
                detail_topics=detail_topics if cat_filter else [],
                all_topics=detail_topics if not cat_filter else [],
                cat_topic_counts=cat_topic_counts,
                cat_filter=cat_filter,
                subcat_filter=subcat_filter,
                subcat_counts=subcat_counts,
                sort_by=sort_by,
                topic_status_filter=topic_status_filter,
            ),
        )


@app.route("/topics/new")
@login_required
def topic_new():
    """Dedicated topic creation page."""
    return render_template(
        "index.html",
        **_common_ctx(
            page="topic_new",
            prefill_topic_name=(request.args.get("name", "") or "").strip(),
        ),
    )


@app.route("/topics/<int:topic_id>")
@login_required
def topic_detail(topic_id):
    """Show posts linked to a specific topic (capped at 50)."""
    db = get_db()
    with _timed_section("db_get_posts_for_topic"):
        topic_posts = _enrich_posts(db.get_posts_for_topic(topic_id, limit=50))
    with _timed_section("db_get_topic_by_id"):
        topic_info = db.get_topic_by_id(topic_id)
    votes = {}
    if topic_info:
        with _timed_section("db_votes_for_topics"):
            votes = db.get_votes_for_topics([topic_id])
        # Parse summary JSON fields
        _parse_summary_fields(topic_info)

    # Parse per-post metrics and compute aggregate
    topic_agg_metrics = {"likes": 0, "reposts": 0, "replies": 0, "quotes": 0, "bookmarks": 0, "impressions": 0}
    for p in topic_posts:
        p["metrics"] = {}
        raw = p.get("public_metrics_json")
        if raw:
            try:
                m = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(m, dict):
                    p["metrics"] = {
                        "likes": int(m.get("like_count", 0) or 0),
                        "reposts": int(m.get("retweet_count", 0) or m.get("repost_count", 0) or 0),
                        "replies": int(m.get("reply_count", 0) or 0),
                        "quotes": int(m.get("quote_count", 0) or 0),
                        "bookmarks": int(m.get("bookmark_count", 0) or 0),
                        "impressions": int(m.get("impression_count", 0) or 0),
                    }
                    for k in topic_agg_metrics:
                        topic_agg_metrics[k] += p["metrics"].get(k, 0)
            except (json.JSONDecodeError, TypeError):
                pass

    with _timed_section("render_template"):
        return render_template(
            "index.html",
            **_common_ctx(
                page="topic_detail",
                topic_posts=topic_posts,
                topic_detail=topic_info,
                topic_agg_metrics=topic_agg_metrics,
                votes=votes,
            ),
        )


@app.route("/history")
@login_required
def history():
    """Fetch history log."""
    db = get_db()
    history_list = db.get_fetch_history(limit=100)
    mt = ZoneInfo("America/Boise")
    utc = ZoneInfo("UTC")
    for h in history_list:
        if h.get("started_at"):
            try:
                naive = datetime.fromisoformat(h["started_at"])
                aware = naive.replace(tzinfo=utc)
                h["started_at_mt"] = aware.astimezone(mt).strftime("%b %d %-I:%M %p MT")
            except Exception:
                h["started_at_mt"] = h["started_at"][:16]
    with _timed_section("db_cost_tracker"):
        cost_data = db.get_cost_tracker_data()
    return render_template(
        "index.html",
        **_common_ctx(
            page="history",
            history=history_list,
            cost_data=cost_data,
        ),
    )


# ------------------------------------------------------------------
# Markdown export API
# ------------------------------------------------------------------
@app.route("/api/topic/<int:topic_id>/markdown")
@login_required
def topic_markdown(topic_id):
    """Export a single topic as Markdown for slide generation."""
    db = get_db()
    with _timed_section("db_get_topic_by_id"):
        topic = db.get_topic_by_id(topic_id)
    if not topic:
        return "Topic not found", 404

    with _timed_section("db_get_posts_for_topic"):
        topic_posts = db.get_posts_for_topic(topic_id, limit=10)
    cat_label = config.TAXONOMY.get(topic.get("category"), {}).get("label", topic.get("category", ""))

    lines = []
    lines.append(f"# {topic['name']}")
    lines.append("")
    lines.append(f"**Category:** {cat_label}")
    if topic.get("subcategory"):
        lines.append(f"  \n**Subcategory:** {topic['subcategory'].replace('_', ' ').title()}")
    lines.append(f"  \n**Posts:** {topic['post_count']} | **First seen:** {topic['first_seen_at'][:10]} | **Last activity:** {topic['last_seen_at'][:10]}")
    lines.append("")
    if topic.get("description"):
        lines.append(f"> {topic['description']}")
        lines.append("")
    lines.append("## Key Posts")
    lines.append("")
    for i, p in enumerate(topic_posts, 1):
        username = p.get("author_username", "unknown")
        date = p.get("created_at", "")[:10]
        text = p.get("full_text") or p.get("text", "")
        post_url = f"https://x.com/{username}/status/{p['tweet_id']}"
        lines.append(f"### {i}. @{username} ({date})")
        lines.append("")
        lines.append(text)
        lines.append("")
        lines.append(f"[View on X]({post_url})")
        lines.append("")

    md = "\n".join(lines)
    return Response(md, mimetype="text/markdown",
                    headers={"Content-Disposition": f"inline; filename=topic_{topic_id}.md"})


@app.route("/api/export/weekly")
@login_required
def export_weekly_markdown():
    """Export all weekly topics as a single Markdown document for slides."""
    db = get_db()
    weekly_ctx = _get_weekly_cycle_context(db)
    since_query = weekly_ctx["since_query"]
    since_label = weekly_ctx["since_label"]
    weekly_topics = db.get_weekly_topics(since_query)

    lines = []
    lines.append(f"# X Feed Intel Weekly — Topics Since {since_label}")
    lines.append("")
    lines.append(f"*Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC*")
    lines.append("")

    for t in weekly_topics:
        cat_label = config.TAXONOMY.get(t.get("category"), {}).get("label", t.get("category", ""))
        lines.append(f"## {t['name']}")
        lines.append(f"**{cat_label}** | {t['week_post_count']} posts this week | {t.get('source_count', '?')} sources")
        if t.get("description"):
            lines.append(f"  \n> {t['description']}")
        lines.append("")

        topic_posts = db.get_posts_for_topic(t["id"], limit=5)
        for p in topic_posts:
            username = p.get("author_username", "unknown")
            text = (p.get("full_text") or p.get("text", ""))[:280]
            post_url = f"https://x.com/{username}/status/{p['tweet_id']}"
            lines.append(f"- @{username}: {text}... [link]({post_url})")
        lines.append("")

    md = "\n".join(lines)
    return Response(md, mimetype="text/markdown",
                    headers={"Content-Disposition": "inline; filename=weekly_topics.md"})


@app.route("/api/export/voted")
@login_required
def export_voted_markdown():
    """Export voted topics (slide + bullet) as Markdown with vote labels."""
    db = get_db()
    weekly_ctx = _get_weekly_cycle_context(db)
    since_query = weekly_ctx["since_query"]
    since_label = weekly_ctx["since_label"]
    week_id = weekly_ctx["week_id"]
    weekly_topics = db.get_weekly_topics(since_query, limit=50)

    topic_ids = [t["id"] for t in weekly_topics]
    votes = db.get_votes_for_topics(topic_ids, week_id=week_id) if topic_ids else {}

    slide_topics = []
    bullet_topics = []

    for t in weekly_topics:
        tv = votes.get(t["id"], [])
        vote_types = {v["vote_type"] for v in tv}
        if "slide" in vote_types:
            slide_topics.append((t, tv))
        elif "bullet" in vote_types:
            bullet_topics.append((t, tv))

    lines = []
    lines.append(f"# Voted Topics — Week of {since_label}")
    lines.append("")
    lines.append(f"*Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC*")
    lines.append("")

    def _append_topic_summary(lines, topic_row):
        """Append key takeaways and bullets for a topic to markdown lines."""
        topic_full = db.get_topic_by_id(topic_row["id"])
        if not topic_full:
            return
        if topic_full.get("description"):
            lines.append(f"> {topic_full['description']}")
        # Key takeaways
        try:
            kt = json.loads(topic_full.get("summary_key_takeaways") or "[]")
            if kt:
                for k in kt[:2]:
                    lines.append(f"- **{k}**")
        except (json.JSONDecodeError, TypeError):
            pass
        # Supporting bullets
        try:
            bl = json.loads(topic_full.get("summary_bullets") or "[]")
            if bl:
                for b in bl[:6]:
                    lines.append(f"  - {b}")
        except (json.JSONDecodeError, TypeError):
            pass

    if slide_topics:
        lines.append("## Slide Topics")
        lines.append("")
        for t, tv in slide_topics:
            voters = ", ".join(v["voter_name"] for v in tv if v["vote_type"] == "slide")
            cat_label = config.TAXONOMY.get(t.get("category"), {}).get("label", t.get("category", ""))
            lines.append(f"### {t['name']}")
            lines.append(f"**{cat_label}** | Voted SLIDE by: {voters}")
            _append_topic_summary(lines, t)
            lines.append("")

    if bullet_topics:
        lines.append("## Bullet Topics")
        lines.append("")
        for t, tv in bullet_topics:
            voters = ", ".join(v["voter_name"] for v in tv if v["vote_type"] == "bullet")
            cat_label = config.TAXONOMY.get(t.get("category"), {}).get("label", t.get("category", ""))
            lines.append(f"- **{t['name']}** ({cat_label}) — voted by: {voters}")
            _append_topic_summary(lines, t)
        lines.append("")

    if not slide_topics and not bullet_topics:
        lines.append("*No topics have been voted as Slide or Bullet yet.*")
        lines.append("")

    md = "\n".join(lines)
    return Response(md, mimetype="text/markdown",
                    headers={"Content-Disposition": "inline; filename=voted_topics.md"})


@app.route("/api/export/bullets")
@login_required
def export_bullets_markdown():
    """Export bullet-voted topics as a compact Markdown list."""
    db = get_db()
    weekly_ctx = _get_weekly_cycle_context(db)
    since_query = weekly_ctx["since_query"]
    since_label = weekly_ctx["since_label"]
    week_id = weekly_ctx["week_id"]
    weekly_topics = db.get_weekly_topics(since_query, limit=50)

    topic_ids = [t["id"] for t in weekly_topics]
    votes = db.get_votes_for_topics(topic_ids, week_id=week_id) if topic_ids else {}

    bullet_topics = []
    for t in weekly_topics:
        tv = votes.get(t["id"], [])
        vote_types = {v["vote_type"] for v in tv}
        if "bullet" in vote_types:
            bullet_topics.append((t, tv))

    lines = []
    lines.append(f"# Bullet Topics — Week of {since_label}")
    lines.append("")
    lines.append(f"*Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC*")
    lines.append("")

    if bullet_topics:
        for t, tv in bullet_topics:
            voters = ", ".join(v["voter_name"] for v in tv if v["vote_type"] == "bullet")
            cat_label = config.TAXONOMY.get(t.get("category"), {}).get("label", t.get("category", ""))
            lines.append(f"- **{t['name']}** ({cat_label}) — voted by: {voters}")
            try:
                topic_full = db.get_topic_by_id(t["id"])
                if topic_full:
                    if topic_full.get("description"):
                        lines.append(f"  > {topic_full['description']}")
                    # Key takeaways
                    kt = json.loads(topic_full.get("summary_key_takeaways") or "[]")
                    if isinstance(kt, list):
                        for k in kt[:2]:
                            lines.append(f"  - **{k}**")
                    # Supporting bullets
                    bl = json.loads(topic_full.get("summary_bullets") or "[]")
                    if isinstance(bl, list) and bl:
                        for b in bl[:5]:
                            lines.append(f"    - {b}")
            except Exception:
                desc = t.get("description") or ""
                if desc:
                    lines.append(f"  > {desc}")
            lines.append("")
    else:
        lines.append("*No topics have been voted as Bullet yet.*")
        lines.append("")

    md = "\n".join(lines)
    return Response(md, mimetype="text/markdown",
                    headers={"Content-Disposition": "inline; filename=bullet_topics.md"})


# ------------------------------------------------------------------
# JSON API endpoints
# ------------------------------------------------------------------
@app.route("/api/stats")
@login_required
def api_stats():
    """JSON stats endpoint."""
    db = get_db()
    return jsonify(db.get_stats_extended())


@app.route("/api/posts")
@login_required
def api_posts():
    """JSON posts with filtering."""
    db = get_db()
    filters = _get_filters()
    page = max(1, request.args.get("page", 1, type=int))
    per_page = min(100, request.args.get("per_page", 50, type=int))
    offset = (page - 1) * per_page

    with _timed_section("db_get_relevant_posts"):
        post_list = db.get_relevant_posts(
            limit=per_page,
            offset=offset,
            category=filters["category"] or None,
            subcategory=filters["subcategory"] or None,
            date_from=filters["date_from"] or None,
            date_to=filters["date_to"] or None,
            search=filters["search"] or None,
        )
    with _timed_section("db_count_relevant_posts"):
        total = db.count_relevant_posts(
            category=filters["category"] or None,
            subcategory=filters["subcategory"] or None,
            date_from=filters["date_from"] or None,
            date_to=filters["date_to"] or None,
            search=filters["search"] or None,
        )
    return jsonify({"posts": post_list, "total": total, "page": page, "per_page": per_page})


@app.route("/api/vote", methods=["POST"])
@login_required
def api_vote():
    """Cast or update a vote on a topic."""
    db = get_db()
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    topic_id = data.get("topic_id")
    vote_type = data.get("vote_type")
    skip_reason = data.get("skip_reason")
    voter_name = request.user["display_name"]  # From session, not client

    if not isinstance(topic_id, int):
        return jsonify({"error": "Invalid request"}), 400
    if vote_type not in config.VALID_VOTE_TYPES:
        return jsonify({"error": "Invalid request"}), 400
    if vote_type == "skip":
        valid_skip_reasons = {
            str(v).strip().lower()
            for v in getattr(config, "VALID_SKIP_REASONS", {"not_good_fit", "already_covered"})
            if str(v).strip()
        }
        if not valid_skip_reasons:
            valid_skip_reasons = {"not_good_fit", "already_covered"}
        default_skip_reason = str(getattr(config, "DEFAULT_SKIP_REASON", "not_good_fit") or "").strip().lower()
        if default_skip_reason not in valid_skip_reasons:
            default_skip_reason = "not_good_fit"
        if default_skip_reason not in valid_skip_reasons:
            default_skip_reason = next(iter(valid_skip_reasons)) if valid_skip_reasons else "not_good_fit"
        skip_reason = str(skip_reason or "").strip().lower() or default_skip_reason
        if skip_reason not in valid_skip_reasons:
            return jsonify({"error": "Invalid request"}), 400
    else:
        skip_reason = None

    with _timed_section("weekly_cycle_ctx"):
        week_ctx = _get_weekly_cycle_context(db)
    week_id = week_ctx["week_id"]
    with _timed_section("db_upsert_vote"):
        db.upsert_vote(topic_id, voter_name, vote_type, week_id=week_id, skip_reason=skip_reason)

    # Voting a topic into slide/bullet should immediately make it eligible
    # for Weekly Prep "top making the cut" behavior, even if it was a candidate.
    if vote_type in {"slide", "bullet"}:
        try:
            with _timed_section("db_auto_promote_on_vote"):
                db.set_topic_promoted(
                    topic_id,
                    promoted=True,
                    actor=voter_name,
                    reason=f"auto_promote_on_{vote_type}_vote",
                )
        except Exception as e:
            logger.warning("Auto-promote on vote failed (non-fatal) topic_id=%s: %s", topic_id, e)

    # Capture training snapshot (non-blocking, non-fatal)
    try:
        with _timed_section("snapshot_vote"):
            from training.collector import snapshot_vote
            snapshot_vote(db, topic_id, voter_name, vote_type, skip_reason=skip_reason)
    except Exception:
        pass

    with _timed_section("db_votes_for_topics"):
        votes = db.get_votes_for_topics([topic_id], week_id=week_id)
    return jsonify({"ok": True, "votes": votes.get(topic_id, []), "week_id": week_id})


@app.route("/api/vote", methods=["DELETE"])
@login_required
def api_vote_delete():
    """Clear a voter's vote on a topic."""
    db = get_db()
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    topic_id = data.get("topic_id")
    voter_name = request.user["display_name"]  # From session, not client

    if not isinstance(topic_id, int):
        return jsonify({"error": "Invalid request"}), 400

    with _timed_section("weekly_cycle_ctx"):
        week_ctx = _get_weekly_cycle_context(db)
    week_id = week_ctx["week_id"]
    with _timed_section("db_delete_vote"):
        db.delete_vote(topic_id, voter_name, week_id=week_id)
    with _timed_section("db_votes_for_topics"):
        votes = db.get_votes_for_topics([topic_id], week_id=week_id)
    return jsonify({"ok": True, "votes": votes.get(topic_id, []), "week_id": week_id})


@app.route("/api/impression", methods=["POST"])
@login_required
def api_impression():
    """Log which topics were shown to a voter (training data)."""
    db = get_db()
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"ok": True})

    voter_name = request.user["display_name"]  # From session, not client
    topic_ids = data.get("topic_ids", [])

    if not isinstance(topic_ids, list) or not topic_ids:
        return jsonify({"ok": True})

    # Validate topic_ids are integers, cap at 100 for safety
    valid_ids = [int(t) for t in topic_ids[:100] if isinstance(t, (int, float))]

    if valid_ids:
        try:
            db.log_impressions(voter_name, valid_ids)
        except Exception:
            pass  # Non-critical

    return jsonify({"ok": True})


@app.route("/api/topics/search")
@login_required
def api_search_topics():
    """Search topics for quick navigation / duplicate checking."""
    db = get_db()
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"topics": []})

    limit = min(max(request.args.get("limit", 10, type=int), 1), 25)
    semantic = _parse_bool_arg(
        request.args.get("semantic"),
        bool(getattr(config, "TOPIC_SEARCH_SEMANTIC_ENABLED", True)),
    )
    alpha = request.args.get("alpha", type=float)
    semantic_k = request.args.get("semantic_k", type=int)
    with _timed_section("db_search_topics"):
        results = db.search_topics_hybrid(
            q,
            limit=limit,
            semantic=semantic,
            alpha=alpha,
            semantic_k=semantic_k,
        )

    with _timed_section("serialize_topics_search"):
        payload = []
        for t in results:
            payload.append({
                "id": t["id"],
                "name": t["name"],
                "description": t.get("description"),
                "category": t.get("category"),
                "subcategory": t.get("subcategory"),
                "post_count": t.get("post_count", 0),
                "last_seen_at": t.get("last_seen_at"),
                "is_active": int(t.get("is_active", 1) or 0),
                "is_promoted": int(t.get("is_promoted", 1) or 0),
                "topic_state": "active" if int(t.get("is_promoted", 1) or 0) else "candidate",
                "editorial_tier_override": t.get("editorial_tier_override"),
                "url": f"/topics/{t['id']}",
            })
    return jsonify({"topics": payload})


@app.route("/api/topics/<int:topic_id>/promote", methods=["POST"])
@login_required
def api_topic_promote(topic_id):
    """Manual promote/demote topic visibility for weekly prep."""
    db = get_db()
    data = request.get_json(silent=True) or {}
    promote = data.get("promote", True)
    promote = bool(promote)
    actor = request.user["display_name"]
    ok = db.set_topic_promoted(
        topic_id,
        promoted=promote,
        actor=actor,
        reason="manual_promote" if promote else "manual_demote",
    )
    if not ok:
        return jsonify({"error": "Topic not found"}), 404
    logger.info("Topic promotion override by %s: topic_id=%s promote=%s", actor, topic_id, promote)
    topic = db.get_topic_by_id(topic_id)
    return jsonify({"ok": True, "topic": topic})


@app.route("/api/topics/<int:topic_id>/tier", methods=["POST"])
@login_required
def api_topic_set_tier(topic_id):
    """Set manual editorial tier override (slide/bullet/hold/none)."""
    db = get_db()
    data = request.get_json(silent=True) or {}
    tier = (data.get("tier") or "").strip().lower()
    try:
        ok = db.set_topic_editorial_tier(topic_id, tier=tier, actor=request.user["display_name"])
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if not ok:
        return jsonify({"error": "Topic not found"}), 404
    logger.info("Topic tier override by %s: topic_id=%s tier=%s", request.user["display_name"], topic_id, tier)
    topic = db.get_topic_by_id(topic_id)
    return jsonify({"ok": True, "topic": topic})


@app.route("/api/topics/<int:topic_id>/hold", methods=["POST"])
@login_required
def api_topic_hold(topic_id):
    """Convenience endpoint to hold a topic from auto-surfacing."""
    db = get_db()
    ok = db.set_topic_editorial_tier(topic_id, tier="hold", actor=request.user["display_name"])
    if not ok:
        return jsonify({"error": "Topic not found"}), 404
    logger.info("Topic hold override by %s: topic_id=%s", request.user["display_name"], topic_id)
    topic = db.get_topic_by_id(topic_id)
    return jsonify({"ok": True, "topic": topic})


# ------------------------------------------------------------------
# Transcription Integration Helpers
# ------------------------------------------------------------------
def _parse_url_list(raw, cap: int = 20) -> list[str]:
    """Normalize URL input from arrays/newline-delimited fields."""
    out: list[str] = []
    if isinstance(raw, list):
        candidates = raw
    elif isinstance(raw, str):
        candidates = raw.splitlines()
    else:
        candidates = []
    for item in candidates:
        u = str(item or "").strip()
        if not u:
            continue
        out.append(u)
        if len(out) >= cap:
            break
    return out


def _workflow_base_name(workflow: Optional[str]) -> str:
    wf = str(workflow or "").strip().lower()
    return wf[:-7] if wf.endswith("-digest") else wf


def _workflow_is_allowed(workflow: Optional[str]) -> bool:
    return _workflow_base_name(workflow) in set(getattr(config, "TRANSCRIPTION_ALLOWED_WORKFLOWS", set()))


def _is_youtube_url(url: str) -> bool:
    u = str(url or "").lower()
    return "youtube.com/" in u or "youtu.be/" in u


def _is_x_status_url(url: str) -> bool:
    return bool(re.search(r"(?:x\.com|twitter\.com)/[^/]+/status/\d+", str(url or ""), re.IGNORECASE))


def _x_status_has_video(url: str) -> bool:
    """Best-effort probe to ensure X status URL actually has downloadable video."""
    try:
        from yt_dlp import YoutubeDL

        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": False,
            "socket_timeout": 20,
            "ignore_no_formats_error": True,
        }
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
        if not info:
            return False
        # For video posts, yt-dlp usually surfaces duration/formats.
        if info.get("duration") or info.get("is_live"):
            return True
        return bool(info.get("formats"))
    except Exception:
        return False


def _detect_triggerable_video_source(url: str) -> tuple[bool, Optional[str]]:
    """Return (is_triggerable_video, source_type)."""
    if _is_youtube_url(url):
        return True, "youtube"
    if _is_x_status_url(url):
        return (_x_status_has_video(url), "x_video")
    return False, None


def _first_triggerable_source(source_urls: list[str]) -> tuple[Optional[str], Optional[str]]:
    """Pick first source URL that can trigger transcription."""
    for url in source_urls:
        ok, source_type = _detect_triggerable_video_source(url)
        if ok:
            return url, source_type
    return None, None


def _parse_summary_bullets_value(raw_value) -> list[str]:
    """Parse summary bullets from list/json/newline string."""
    if isinstance(raw_value, list):
        return [str(x).strip() for x in raw_value if str(x).strip()][:8]
    if isinstance(raw_value, str):
        txt = raw_value.strip()
        if not txt:
            return []
        try:
            decoded = json.loads(txt)
            if isinstance(decoded, list):
                return [str(x).strip() for x in decoded if str(x).strip()][:8]
        except Exception:
            pass
        return [ln.strip("- ").strip() for ln in txt.splitlines() if ln.strip()][:8]
    return []


def _build_description_from_bullets(title: str, bullets: list[str]) -> str:
    title = str(title or "").strip()
    if bullets:
        first = bullets[0].strip()
        second = bullets[1].strip() if len(bullets) > 1 else ""
        if second:
            return f"{first} {second}"
        return first
    return f"Auto-ingested from transcription workflow: {title}" if title else "Auto-ingested from transcription workflow."


def _integration_token_valid(req) -> bool:
    expected = (getattr(config, "TRANSCRIPTION_INTEGRATION_TOKEN", "") or "").strip()
    if not expected:
        return False
    auth_header = str(req.headers.get("Authorization") or "")
    provided = ""
    if auth_header.lower().startswith("bearer "):
        provided = auth_header[7:].strip()
    if not provided:
        provided = str(req.headers.get("X-Integration-Token") or "").strip()
    return bool(provided) and secrets.compare_digest(provided, expected)


def _enqueue_transcription_workflow(
    *,
    topic_id: int,
    source_url: str,
    source_type: str,
    workflow: str,
    requested_by: str,
) -> None:
    """Run selected transcription workflow asynchronously for a created topic."""
    workflow_base = _workflow_base_name(workflow)
    sender_email_hint = re.sub(r"[^a-z0-9._-]+", ".", requested_by.lower()).strip(".") + "@xfi.local"
    event_id = f"xfi-topic-{topic_id}-{int(time.time())}-{secrets.token_hex(4)}"
    integration_context = {
        "topic_id_hint": topic_id,
        "source_type": source_type,
        "event_id": event_id,
    }
    sender_message = f"Triggered from X Feed Intel topic {topic_id} by {requested_by}"
    logger.info(
        "Queueing transcription workflow topic_id=%s workflow=%s source_type=%s source_url=%s requested_by=%s event_id=%s",
        topic_id,
        workflow_base,
        source_type,
        source_url,
        requested_by,
        event_id,
    )

    def _runner():
        db_bg = get_db()
        try:
            db_bg.update_topic_source_metadata(
                topic_id,
                source_url=source_url,
                source_type=source_type,
                transcription_status="pending",
                transcription_workflow=workflow_base,
                transcription_event_id=event_id,
            )
            logger.info(
                "Starting transcription workflow topic_id=%s workflow=%s event_id=%s",
                topic_id,
                workflow_base,
                event_id,
            )
            if workflow_base == "transcribedeep":
                import transcribedeep as _wf
                _wf.process_video_url(
                    source_url,
                    sender_email_hint,
                    sender_message=sender_message,
                    integration_context=integration_context,
                )
            elif workflow_base == "transcribe123":
                import transcribe123 as _wf
                _wf.process_video_url(
                    source_url,
                    sender_email_hint,
                    sender_message=sender_message,
                    integration_context=integration_context,
                )
            elif workflow_base == "transcribeteam":
                import transcribe123 as _wf
                _wf.process_video_url_team(
                    source_url,
                    sender_email_hint,
                    sender_message=sender_message,
                    integration_context=integration_context,
                )
            elif workflow_base == "transcribeslide":
                import transcribeslide as _wf
                _wf.process_video_url(
                    source_url,
                    sender_email_hint,
                    sender_message=sender_message,
                    integration_context=integration_context,
                )
            else:
                raise ValueError(f"Unsupported workflow: {workflow_base}")
            # Completion is expected via callback event; leave pending if callback is disabled.
        except Exception as e:
            logger.error("Transcription trigger failed topic_id=%s workflow=%s: %s", topic_id, workflow_base, e)
            db_bg.update_topic_source_metadata(
                topic_id,
                transcription_status="failed",
                transcription_workflow=workflow_base,
            )

    threading.Thread(target=_runner, daemon=True).start()


# ------------------------------------------------------------------
# Transcription Integration API
# ------------------------------------------------------------------
@app.route("/api/integrations/transcription-events", methods=["POST"])
def api_ingest_transcription_event():
    """Internal callback endpoint for completed transcription summary workflows."""
    if not getattr(config, "TRANSCRIPTION_INTEGRATION_ENABLED", True):
        logger.warning("Rejected transcription callback: integration disabled")
        return jsonify({"error": "Integration disabled"}), 404
    if not _integration_token_valid(request):
        logger.warning(
            "Rejected transcription callback: unauthorized remote=%s ua=%s",
            request.remote_addr,
            request.headers.get("User-Agent", ""),
        )
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    event_id = str(data.get("event_id") or "").strip()
    workflow = str(data.get("workflow") or "").strip()
    source_url = str(data.get("source_url") or "").strip()
    video_title = str(data.get("video_title") or "").strip()
    if not event_id or not workflow or not source_url:
        logger.warning(
            "Rejected transcription callback: missing required fields event_id=%s workflow=%s source_url_present=%s",
            event_id,
            workflow,
            bool(source_url),
        )
        return jsonify({"error": "event_id, workflow, and source_url are required"}), 400

    workflow_base = _workflow_base_name(workflow)
    if not _workflow_is_allowed(workflow_base):
        logger.warning(
            "Rejected transcription callback: unsupported workflow event_id=%s workflow=%s",
            event_id,
            workflow,
        )
        return jsonify({"error": f"Unsupported workflow: {workflow}"}), 400

    db = get_db()
    existing = db.get_external_signal_event(event_id)
    if existing:
        logger.info(
            "Deduped transcription callback event_id=%s workflow=%s topic_id=%s",
            event_id,
            workflow_base,
            int(existing["topic_id"]),
        )
        return jsonify({
            "ok": True,
            "duplicate": True,
            "topic_id": int(existing["topic_id"]),
        })

    source_url_norm = db.normalize_source_url(source_url)
    source_type = str(data.get("source_type") or "").strip().lower() or db.infer_source_type(source_url_norm)
    summary_bullets = _parse_summary_bullets_value(data.get("summary_bullets"))
    summary_html = data.get("summary_html")
    sender_email = str(data.get("sender_email") or "").strip() or None
    completed_at = str(data.get("completed_at") or "").strip() or datetime.utcnow().isoformat()

    topic_id_hint_raw = data.get("topic_id_hint")
    topic_id_hint = None
    if topic_id_hint_raw is not None:
        try:
            topic_id_hint = int(topic_id_hint_raw)
        except Exception:
            topic_id_hint = None
            logger.warning(
                "Ignoring non-numeric topic_id_hint in transcription callback event_id=%s raw=%s",
                event_id,
                topic_id_hint_raw,
            )

    topic = db.get_topic_by_id(topic_id_hint) if topic_id_hint else None
    if topic and int(topic.get("is_active") or 0) != 1:
        topic = None
    if not topic:
        topic = db.find_topic_by_source_url(source_url_norm)

    created_topic = False
    if topic:
        topic_id = int(topic["id"])
    else:
        topic_name = video_title or f"Transcription Topic {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"
        topic_desc = _build_description_from_bullets(video_title, summary_bullets)
        topic_id, _ = db.get_or_create_topic_status(
            topic_name,
            topic_desc,
            category=None,
            subcategory=None,
            promote=False,
            created_source="transcription_ingest",
        )
        created_topic = True
        topic = db.get_topic_by_id(topic_id) or {"post_count": 0, "summary_lifetime_posts_seen": 0}

    if summary_bullets:
        baseline_seen = max(
            int(topic.get("summary_lifetime_posts_seen") or 0),
            int(topic.get("post_count") or 0),
        )
        db.update_topic_summary(
            topic_id=topic_id,
            description=_build_description_from_bullets(video_title, summary_bullets),
            bullets=summary_bullets,
            key_takeaways=None,
            lifetime_seen=baseline_seen,
        )

    inserted_signal = db.record_external_signal(
        topic_id=topic_id,
        event_id=event_id,
        workflow=workflow,
        source_url=source_url_norm,
        source_type=source_type,
        video_title=video_title or None,
        summary_bullets=summary_bullets,
        summary_html=summary_html if isinstance(summary_html, str) else None,
        sender_email=sender_email,
        completed_at=completed_at,
    )
    db.update_topic_source_metadata(
        topic_id,
        source_url=source_url_norm,
        source_type=source_type,
        transcription_status="completed",
        transcription_workflow=workflow_base,
        transcription_event_id=event_id,
    )
    logger.info(
        "Processed transcription callback event_id=%s workflow=%s topic_id=%s created_topic=%s bullets=%s source_type=%s",
        event_id,
        workflow_base,
        topic_id,
        created_topic,
        len(summary_bullets),
        source_type,
    )

    return jsonify({
        "ok": True,
        "topic_id": topic_id,
        "created_topic": created_topic,
        "inserted_signal": bool(inserted_signal),
    })


# ------------------------------------------------------------------
# Topic Creation API
# ------------------------------------------------------------------
@app.route("/api/topics", methods=["POST"])
@login_required
def api_create_topic():
    """Create a new topic, optionally linking X posts."""
    db = get_db()
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    name = (data.get("name") or "").strip()
    description = (data.get("description") or "").strip()
    category = data.get("category") or None
    subcategory = data.get("subcategory") or None
    post_urls = _parse_url_list(data.get("post_urls", []))  # Seed X post URLs
    source_urls = _parse_url_list(data.get("source_urls", []))  # Video/source URLs
    trigger_transcription = bool(data.get("trigger_transcription", True))
    requested_workflow = str(
        data.get("transcription_workflow") or getattr(config, "TRANSCRIPTION_DEFAULT_WORKFLOW", "transcribedeep")
    ).strip().lower()

    if not name:
        return jsonify({"error": "Topic name is required"}), 400

    created_by = request.user["display_name"]
    triggerable_source_url = None
    triggerable_source_type = None
    if source_urls and trigger_transcription:
        triggerable_source_url, triggerable_source_type = _first_triggerable_source(source_urls)
        if not triggerable_source_url:
            logger.info(
                "Topic create requested transcription but no triggerable video source found user=%s source_urls=%s",
                created_by,
                len(source_urls),
            )
    selected_source_url = triggerable_source_url or (source_urls[0] if source_urls else None)
    selected_source_type = (
        triggerable_source_type
        or (db.infer_source_type(selected_source_url) if selected_source_url else None)
    )
    if trigger_transcription and triggerable_source_url and not _workflow_is_allowed(requested_workflow):
        return jsonify({"error": f"Unsupported transcription workflow: {requested_workflow}"}), 400
    should_trigger_transcription = bool(
        trigger_transcription
        and getattr(config, "TRANSCRIPTION_INTEGRATION_ENABLED", True)
        and triggerable_source_url
        and _workflow_is_allowed(requested_workflow)
    )
    promote_topic = not should_trigger_transcription
    created_source = "user_video_pending" if should_trigger_transcription else "user"

    try:
        # 1. Track in user_topics pipeline
        ut_id = db.create_user_topic(name, description, category, subcategory, created_by)

        # 2. Insert into main topics table
        topic_id, _ = db.get_or_create_topic_status(
            name, description, category, subcategory,
            promote=promote_topic, created_source=created_source,
        )

        if selected_source_url:
            db.update_topic_source_metadata(
                topic_id,
                source_url=selected_source_url,
                source_type=selected_source_type,
                transcription_status="pending" if should_trigger_transcription else "none",
                transcription_workflow=_workflow_base_name(requested_workflow) if should_trigger_transcription else None,
            )

        # 3. Process X post URLs if provided
        linked_posts = []
        if post_urls:
            linked_posts = _process_post_urls(db, topic_id, post_urls)

        # 4. Update pipeline status immediately (don't block on vectorization)
        db.update_user_topic_status(ut_id, "active", topic_id=topic_id)

        # Persist summary fields if provided
        summary_bullets = data.get("summary_bullets")
        summary_key_takeaways = data.get("summary_key_takeaways")
        if summary_bullets or summary_key_takeaways:
            if isinstance(summary_bullets, str):
                try:
                    summary_bullets = json.loads(summary_bullets)
                except (json.JSONDecodeError, TypeError):
                    summary_bullets = [b.strip() for b in summary_bullets.split("\n") if b.strip()]
            if isinstance(summary_key_takeaways, str):
                try:
                    summary_key_takeaways = json.loads(summary_key_takeaways)
                except (json.JSONDecodeError, TypeError):
                    summary_key_takeaways = [t.strip() for t in summary_key_takeaways.split("\n") if t.strip()]
            bullets_list = summary_bullets if isinstance(summary_bullets, list) else []
            kt_list = summary_key_takeaways if isinstance(summary_key_takeaways, list) else None
            if bullets_list or kt_list:
                db.update_topic_summary(
                    topic_id=topic_id,
                    description=description,
                    bullets=bullets_list,
                    key_takeaways=kt_list,
                    lifetime_seen=len(linked_posts),
                )

        # 5. Vectorize in background thread so response returns fast
        def _bg_vectorize(tid):
            try:
                from vector_search import TopicVectorIndex
                bg_db = get_db()
                idx = TopicVectorIndex.get_instance(bg_db.conn)
                topic = bg_db.get_topic_by_id(tid)
                if topic:
                    idx.sync_topic_vectors([topic], bg_db)
                logger.info(f"Background vectorization complete for topic {tid}")
            except Exception as e:
                logger.warning(f"Background vectorization failed for topic {tid}: {e}")

        threading.Thread(target=_bg_vectorize, args=(topic_id,), daemon=True).start()

        if should_trigger_transcription and triggerable_source_url and triggerable_source_type:
            logger.info(
                "Topic create enqueuing transcription topic_id=%s workflow=%s user=%s source_type=%s",
                topic_id,
                _workflow_base_name(requested_workflow),
                created_by,
                triggerable_source_type,
            )
            _enqueue_transcription_workflow(
                topic_id=topic_id,
                source_url=triggerable_source_url,
                source_type=triggerable_source_type,
                workflow=requested_workflow,
                requested_by=created_by,
            )

        return jsonify({
            "ok": True,
            "topic_id": topic_id,
            "linked_posts": len(linked_posts),
            "topic_state": "candidate" if not promote_topic else "active",
            "transcription_queued": bool(should_trigger_transcription),
            "transcription_workflow": _workflow_base_name(requested_workflow) if should_trigger_transcription else None,
        })
    except Exception as e:
        logger.error(f"Topic creation failed: {e}")
        if 'ut_id' in locals():
            db.update_user_topic_status(ut_id, "error", error_message=str(e))
        return jsonify({"error": str(e)}), 500


def _quick_create_fetch_and_suggest(db, url):
    """Fetch tweet from URL and get AI topic suggestion. Returns (suggestion_dict, tweet_id)."""
    tweet_id_pattern = re.compile(r"(?:x\.com|twitter\.com)/\w+/status/(\d+)")
    match = tweet_id_pattern.search(url)
    if not match:
        raise ValueError("Invalid X post URL")

    tweet_id = match.group(1)

    # Try to find tweet in DB first
    cur = db.conn.execute(
        "SELECT tweet_id, author_username, full_text, text, public_metrics_json FROM posts WHERE tweet_id = ?",
        (tweet_id,)
    )
    row = cur.fetchone()
    if row:
        tweet_data = dict(row)
    else:
        # Fetch from X API
        from x_client import XTimelineClient
        client = XTimelineClient()
        tweet = client.fetch_tweet_by_id(tweet_id)
        x_req_stats = getattr(client, "last_request_stats", {}) or {}
        x_http_success = int(x_req_stats.get("http_requests_succeeded", 0) or 0)
        db.record_api_usage(
            service="x_api", operation="quick_create_fetch",
            input_tokens=0, output_tokens=0,
            cost_usd=(1 if tweet else 0) * config.X_API_TWEET_LOOKUP_COST_PER_POST_USD,
            model=None, batch_size=1 if tweet else 0,
        )
        db.record_api_usage(
            service="x_api", operation="quick_create_fetch_http",
            input_tokens=0, output_tokens=0,
            cost_usd=x_http_success * config.X_API_TWEET_LOOKUP_COST_PER_SUCCESS_REQUEST_USD,
            model=None, batch_size=int(x_req_stats.get("http_requests_attempted", 0) or 0),
        )
        if not tweet:
            raise ValueError("Could not fetch tweet")
        tweet_data = tweet

    tweet_text = tweet_data.get("full_text") or tweet_data.get("text", "")
    author = tweet_data.get("author_username", "unknown")

    # Build taxonomy list
    taxonomy_list = []
    for cat_key, cat_info in config.TAXONOMY.items():
        subs = list(cat_info.get("subcategories", {}).keys())
        taxonomy_list.append(f"  {cat_key}: {', '.join(subs)}")

    user_message = (
        "Given this X post, suggest a topic for our AI infrastructure market intelligence tracker.\n\n"
        f'Post by @{author}:\n"{tweet_text}"\n\n'
        "Available taxonomy:\n" + "\n".join(taxonomy_list) +
        '\n\nOutput JSON: {"name": "...", "description": "...", "bullets": ["..."], "category": "...", "subcategory": "..."}'
        "\nName: 4-8 words, Title Case, slide-ready, entity-first."
        "\nDescription: 1-2 sentences."
        "\nBullets: 3-5 key points."
    )

    import anthropic as _anthropic
    client = _anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    response = client.messages.create(
        model=config.OPUS_MODEL,
        max_tokens=1024,
        system="You are an editorial assistant for an AI infrastructure market intelligence team. Suggest topic metadata. Output ONLY a JSON object.",
        messages=[{"role": "user", "content": user_message}],
    )

    usage = {
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
    }
    cost = (usage["input_tokens"] / 1_000_000) * 5.00 + (usage["output_tokens"] / 1_000_000) * 25.00
    db.record_api_usage(
        service="anthropic", operation="quick_create_suggest",
        input_tokens=usage["input_tokens"], output_tokens=usage["output_tokens"],
        cost_usd=cost, model=config.OPUS_MODEL, batch_size=1,
    )

    text = response.content[0].text.strip()
    stripped = re.sub(r"^```(?:json)?\s*\n?", "", text)
    stripped = re.sub(r"\n?```\s*$", "", stripped).strip()
    suggestion = json.loads(stripped)

    return suggestion, tweet_id


@app.route("/api/topics/quick-create", methods=["POST"])
@login_required
def api_quick_create_suggest():
    """Suggest topic metadata from an X post URL using AI."""
    data = request.get_json(silent=True)
    if not data or not data.get("url"):
        return jsonify({"error": "url required"}), 400

    url = data["url"].strip()
    db = get_db()

    try:
        suggestion, tweet_id = _quick_create_fetch_and_suggest(db, url)
        return jsonify({"ok": True, "suggestion": suggestion, "tweet_id": tweet_id})
    except ValueError as e:
        code = 404 if "Could not fetch" in str(e) else 400
        return jsonify({"error": str(e)}), code
    except Exception as e:
        logger.error(f"Quick create suggestion failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/topics/quick-create-auto", methods=["POST"])
@login_required
def api_quick_create_auto():
    """One-click: fetch tweet, get AI suggestion, create topic, link post, return topic_id."""
    data = request.get_json(silent=True)
    if not data or not data.get("url"):
        return jsonify({"error": "url required"}), 400

    url = data["url"].strip()
    db = get_db()

    try:
        suggestion, tweet_id = _quick_create_fetch_and_suggest(db, url)
    except ValueError as e:
        code = 404 if "Could not fetch" in str(e) else 400
        return jsonify({"error": str(e)}), code
    except Exception as e:
        logger.error(f"Quick create auto failed (suggest): {e}")
        return jsonify({"error": str(e)}), 500

    # Create topic from suggestion
    try:
        name = (suggestion.get("name") or "").strip()
        if not name:
            return jsonify({"error": "AI suggestion returned empty name"}), 500

        desc = suggestion.get("description", "")
        cat = suggestion.get("category")
        subcat = suggestion.get("subcategory")

        topic_id, was_created = db.get_or_create_topic_status(
            name=name, description=desc,
            category=cat, subcategory=subcat,
            promote=True, created_source="user_auto",
        )

        # Persist summary bullets if provided
        bullets = suggestion.get("bullets")
        if bullets and isinstance(bullets, list):
            db.conn.execute(
                "UPDATE topics SET summary_bullets = ?, summary_updated_at = ? WHERE id = ?",
                (json.dumps(bullets), datetime.utcnow().isoformat(), topic_id)
            )
            db.conn.commit()

        # Link the tweet
        _process_post_urls(db, topic_id, [url])

        # Background vectorization
        def _bg_vectorize(tid):
            try:
                from vector_search import TopicVectorIndex
                bg_db = get_db()
                idx = TopicVectorIndex.get_instance(bg_db.conn)
                t = bg_db.get_topic_by_id(tid)
                if t:
                    idx.sync_topic_vectors([t], bg_db)
            except Exception as e:
                logger.warning(f"Background vectorization failed for topic {tid}: {e}")

        threading.Thread(target=_bg_vectorize, args=(topic_id,), daemon=True).start()

        # Record API cost
        db.record_api_usage(
            service="anthropic", operation="quick_create_auto",
            input_tokens=0, output_tokens=0,
            cost_usd=0, model=config.OPUS_MODEL, batch_size=1,
        )

        return jsonify({"ok": True, "topic_id": topic_id, "name": name, "was_created": was_created})
    except Exception as e:
        logger.error(f"Quick create auto failed (create): {e}")
        return jsonify({"error": str(e)}), 500


def _process_post_urls(db, topic_id, post_urls):
    """Parse X post URLs, fetch tweets, link to topic. Returns list of linked tweet IDs."""
    linked = []
    tweet_id_pattern = re.compile(r"(?:x\.com|twitter\.com)/\w+/status/(\d+)")

    for url in post_urls[:20]:  # Cap at 20 URLs
        url = url.strip()
        if not url:
            continue
        match = tweet_id_pattern.search(url)
        if not match:
            continue
        tweet_id = match.group(1)

        # Check if already in posts table
        cur = db.conn.execute("SELECT tweet_id FROM posts WHERE tweet_id = ?", (tweet_id,))
        if cur.fetchone():
            # Already exists — just link it
            db.link_post_to_topic(tweet_id, topic_id)
            linked.append(tweet_id)
            continue

        # Fetch from X API
        try:
            from x_client import XTimelineClient
            client = XTimelineClient()
            tweet = client.fetch_tweet_by_id(tweet_id)
            x_req_stats = getattr(client, "last_request_stats", {}) or {}
            x_http_attempts = int(x_req_stats.get("http_requests_attempted", 0) or 0)
            x_http_success = int(x_req_stats.get("http_requests_succeeded", 0) or 0)
            tweet_count = 1 if tweet else 0

            db.record_api_usage(
                service="x_api",
                operation="fetch_tweet_by_id",
                input_tokens=0,
                output_tokens=0,
                cost_usd=tweet_count * config.X_API_TWEET_LOOKUP_COST_PER_POST_USD,
                model=None,
                batch_size=tweet_count,
            )
            db.record_api_usage(
                service="x_api",
                operation="fetch_tweet_by_id_http_requests",
                input_tokens=0,
                output_tokens=0,
                cost_usd=x_http_success * config.X_API_TWEET_LOOKUP_COST_PER_SUCCESS_REQUEST_USD,
                model=None,
                batch_size=x_http_attempts,
            )

            if tweet:
                db.insert_posts_batch([tweet])
                db.link_post_to_topic(tweet_id, topic_id)
                linked.append(tweet_id)
        except Exception as e:
            logger.warning(f"Failed to fetch tweet {tweet_id}: {e}")

    return linked


# ------------------------------------------------------------------
# Topic Editing API
# ------------------------------------------------------------------
@app.route("/api/topics/<int:topic_id>", methods=["PUT"])
@login_required
def api_edit_topic(topic_id):
    """Edit an existing topic's metadata."""
    db = get_db()
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    edited_by = request.user["display_name"]
    updates = {}
    for field in ("name", "description", "category", "subcategory", "summary_bullets", "summary_key_takeaways"):
        if field in data:
            updates[field] = data[field]

    if not updates:
        return jsonify({"error": "No fields to update"}), 400

    try:
        db.update_topic(topic_id, updates, edited_by)

        # Re-vectorize if name or description changed
        if "name" in updates or "description" in updates:
            try:
                from vector_search import TopicVectorIndex
                idx = TopicVectorIndex.get_instance(db.conn)
                topic = db.get_topic_by_id(topic_id)
                if topic:
                    idx.sync_topic_vectors([topic], db)
            except Exception as e:
                logger.warning(f"Re-vectorization failed for topic {topic_id}: {e}")

        topic = db.get_topic_by_id(topic_id)
        return jsonify({"ok": True, "topic": topic})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/topics/<int:topic_id>/history")
@login_required
def api_topic_history(topic_id):
    """Return edit history for a topic."""
    db = get_db()
    history = db.get_topic_edit_history(topic_id)
    return jsonify({"history": history})


# ------------------------------------------------------------------
# Topic API: Get single topic
# ------------------------------------------------------------------
@app.route("/api/topics/<int:topic_id>", methods=["GET"])
@login_required
def api_get_topic(topic_id):
    """Return a single topic's data as JSON."""
    db = get_db()
    topic = db.get_topic_by_id(topic_id)
    if not topic:
        return jsonify({"error": "Topic not found"}), 404
    return jsonify({"topic": topic})


# ------------------------------------------------------------------
# Topic Merge
# ------------------------------------------------------------------
@app.route("/api/topics/merge/suggest", methods=["POST"])
@login_required
def api_merge_suggest():
    """AI-suggest combined metadata for a set of topics to merge."""
    data = request.get_json(silent=True)
    if not data or not data.get("topic_ids"):
        return jsonify({"error": "topic_ids required"}), 400

    topic_ids = data["topic_ids"]
    if len(topic_ids) < 2:
        return jsonify({"error": "Need at least 2 topics to merge"}), 400

    db = get_db()
    topics = []
    for tid in topic_ids:
        t = db.get_topic_by_id(tid)
        if not t:
            return jsonify({"error": f"Topic {tid} not found"}), 404
        topics.append(t)

    # Fetch sample posts for each topic (up to 5 per topic)
    topic_posts = {}
    for t in topics:
        posts = db.get_all_posts_for_topic(t["id"])
        topic_posts[t["id"]] = posts[:5]

    # Build prompt
    topic_context = []
    for t in topics:
        takeaways = ""
        if t.get("summary_key_takeaways"):
            try:
                kt = json.loads(t["summary_key_takeaways"])
                takeaways = "\n".join(f"  - {k}" for k in kt) if kt else ""
            except (json.JSONDecodeError, TypeError):
                pass
        bullets = ""
        if t.get("summary_bullets"):
            try:
                bl = json.loads(t["summary_bullets"])
                bullets = "\n".join(f"  - {b}" for b in bl) if bl else ""
            except (json.JSONDecodeError, TypeError):
                pass
        tp = topic_posts.get(t["id"], [])
        post_lines_list = []
        for p in tp:
            author = p.get("author_username", "?")
            txt = (p.get("full_text") or p.get("text", ""))[:200]
            post_lines_list.append(f'    @{author}: "{txt}"')
        post_text = "\n".join(post_lines_list)
        takeaways_section = ("  Key takeaways:\n" + takeaways + "\n") if takeaways else ""
        bullets_section = ("  Bullets:\n" + bullets + "\n") if bullets else ""
        topic_context.append(
            f"Topic: {t['name']} (id={t['id']}, {t.get('post_count',0)} posts)\n"
            f"  Category: {t.get('category','N/A')} / {t.get('subcategory','N/A')}\n"
            f"  Description: {t.get('description','')}\n"
            f"{takeaways_section}"
            f"{bullets_section}"
            f"  Sample posts:\n{post_text}"
        )

    # Build taxonomy list for the prompt
    taxonomy_list = []
    for cat_key, cat_info in config.TAXONOMY.items():
        subs = list(cat_info.get("subcategories", {}).keys())
        taxonomy_list.append(f"  {cat_key}: {', '.join(subs)}")

    user_message = (
        "Given these topics being merged, suggest combined metadata.\n\n"
        + "\n\n".join(topic_context)
        + "\n\nAvailable taxonomy:\n" + "\n".join(taxonomy_list)
        + '\n\nOutput JSON: {"name": "...", "description": "...", "key_takeaways": ["..."], "bullets": ["..."], "category": "...", "subcategory": "..."}'
        + "\nName should be 4-8 words, Title Case, slide-ready."
        + "\nkey_takeaways: 1-2 executive-level statements. bullets: 2-6 supporting details."
    )

    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=config.OPUS_MODEL,
            max_tokens=1024,
            system="You are an editorial assistant. Suggest merged topic metadata. Output ONLY a JSON object.",
            messages=[{"role": "user", "content": user_message}],
        )

        usage = {
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
        }
        cost = (usage["input_tokens"] / 1_000_000) * 5.00 + (usage["output_tokens"] / 1_000_000) * 25.00
        db.record_api_usage(
            service="anthropic", operation="merge_suggest",
            input_tokens=usage["input_tokens"], output_tokens=usage["output_tokens"],
            cost_usd=cost, model=config.OPUS_MODEL, batch_size=len(topic_ids),
        )

        text = response.content[0].text.strip()
        # Parse JSON from response
        import re as _re
        stripped = _re.sub(r"^```(?:json)?\s*\n?", "", text)
        stripped = _re.sub(r"\n?```\s*$", "", stripped).strip()
        suggestion = json.loads(stripped)

        return jsonify({"ok": True, "suggestion": suggestion})
    except Exception as e:
        logger.error(f"Merge suggestion failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/topics/merge", methods=["POST"])
@login_required
def api_merge_execute():
    """Execute a topic merge: combine losers into winner with optional metadata updates."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    winner_id = data.get("winner_id")
    loser_ids = data.get("loser_ids", [])

    if not winner_id or not loser_ids:
        return jsonify({"error": "winner_id and loser_ids required"}), 400
    if winner_id in loser_ids:
        return jsonify({"error": "winner_id cannot be in loser_ids"}), 400

    db = get_db()

    # Validate all topics exist and are active
    all_ids = [winner_id] + loser_ids
    for tid in all_ids:
        t = db.get_topic_by_id(tid)
        if not t:
            return jsonify({"error": f"Topic {tid} not found"}), 404
        if not t.get("is_active", 1):
            return jsonify({"error": f"Topic {tid} is not active"}), 400

    # Build updates dict from request
    updates = {}
    for field in ("name", "description", "summary_bullets", "summary_key_takeaways", "category", "subcategory"):
        if field in data:
            updates[field] = data[field]

    # Validate taxonomy if provided
    if "category" in updates and updates["category"]:
        if updates["category"] not in config.TAXONOMY:
            return jsonify({"error": f"Invalid category: {updates['category']}"}), 400
    if "subcategory" in updates and updates["subcategory"]:
        if updates["subcategory"] not in config.SUBCATEGORY_TO_PARENT:
            return jsonify({"error": f"Invalid subcategory: {updates['subcategory']}"}), 400

    # Validate name uniqueness if updating name
    if "name" in updates and updates["name"]:
        cur = db.conn.execute(
            "SELECT id FROM topics WHERE name = ? AND is_active = 1 AND id NOT IN (" +
            ",".join("?" * len(all_ids)) + ")",
            [updates["name"], *all_ids]
        )
        if cur.fetchone():
            return jsonify({"error": f"Topic name '{updates['name']}' already exists"}), 400

    try:
        edited_by = request.user["display_name"]

        # Record merge in topic_edits before executing
        db.conn.execute("""
            INSERT INTO topic_edits (topic_id, edited_by, field, old_value, new_value, edited_at)
            VALUES (?, ?, 'merge', ?, ?, datetime('now'))
        """, (
            winner_id,
            edited_by,
            json.dumps(loser_ids),
            json.dumps(updates) if updates else "{}",
        ))

        db.merge_topics(winner_id, loser_ids, updates=updates or None)

        topic = db.get_topic_by_id(winner_id)
        return jsonify({"ok": True, "topic": topic})
    except Exception as e:
        logger.error(f"Merge execution failed: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------------
# Topic Split
# ------------------------------------------------------------------
@app.route("/api/topics/<int:topic_id>/posts")
@login_required
def api_topic_posts(topic_id):
    """Return all posts for a topic as JSON."""
    db = get_db()
    topic = db.get_topic_by_id(topic_id)
    if not topic:
        return jsonify({"error": "Topic not found"}), 404
    posts = db.get_all_posts_for_topic(topic_id)
    result = []
    for p in posts:
        metrics = {}
        raw = p.get("public_metrics_json")
        if raw:
            try:
                metrics = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                pass
        result.append({
            "tweet_id": p["tweet_id"],
            "author_username": p.get("author_username", "unknown"),
            "text": (p.get("full_text") or p.get("text", ""))[:400],
            "created_at": p.get("created_at", ""),
            "metrics": metrics,
        })
    return jsonify({"ok": True, "posts": result})


@app.route("/api/topics/<int:topic_id>/split/suggest", methods=["POST"])
@login_required
def api_split_suggest(topic_id):
    """AI-suggest metadata for a topic split (supports multi-group)."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    # Support both old format (post_ids) and new multi-group format (groups)
    groups = data.get("groups")
    if not groups:
        # Backward compat: old format with flat post_ids
        post_ids = data.get("post_ids")
        if not post_ids:
            return jsonify({"error": "groups or post_ids required"}), 400
        groups = [{"group_index": 1, "post_ids": post_ids}]

    db = get_db()
    topic = db.get_topic_by_id(topic_id)
    if not topic:
        return jsonify({"error": "Topic not found"}), 404

    all_posts = db.get_all_posts_for_topic(topic_id)
    all_moving_set = set()
    for grp in groups:
        all_moving_set.update(grp.get("post_ids", []))

    staying_posts = [p for p in all_posts if p["tweet_id"] not in all_moving_set]
    num_groups = len(groups)

    def _sample_posts(posts, n=15):
        if len(posts) <= n:
            return posts
        return posts[:5] + posts[-(n - 5):]

    def _format_posts_for_prompt(posts, n=15):
        lines = []
        for p in _sample_posts(posts, n):
            author = p.get("author_username", "?")
            txt = (p.get("full_text") or p.get("text", ""))[:200]
            lines.append(f'    @{author}: "{txt}"')
        return "\n".join(lines)

    existing_takeaways = ""
    if topic.get("summary_key_takeaways"):
        try:
            kt = json.loads(topic["summary_key_takeaways"])
            if kt:
                existing_takeaways = "\n  Existing key takeaways:\n" + "\n".join(f"    - {k}" for k in kt)
        except (json.JSONDecodeError, TypeError):
            pass

    existing_bullets = ""
    if topic.get("summary_bullets"):
        try:
            bl = json.loads(topic["summary_bullets"])
            if bl:
                existing_bullets = "\n" + "\n".join(f"    - {b}" for b in bl)
        except (json.JSONDecodeError, TypeError):
            pass

    taxonomy_list = []
    for cat_key, cat_info in config.TAXONOMY.items():
        subs = list(cat_info.get("subcategories", {}).keys())
        taxonomy_list.append(f"  {cat_key}: {', '.join(subs)}")

    per_group_sample = max(5, 15 // num_groups)

    # Build group descriptions
    group_sections = []
    for grp in groups:
        gidx = grp["group_index"]
        grp_ids = set(grp.get("post_ids", []))
        grp_posts = [p for p in all_posts if p["tweet_id"] in grp_ids]
        group_sections.append(
            f"Posts for NEW TOPIC {gidx} ({len(grp_posts)} total):\n"
            f"{_format_posts_for_prompt(grp_posts, per_group_sample)}"
        )

    # Build output schema based on number of groups
    new_topics_schema = ", ".join(
        '{"name": "...", "description": "...", "key_takeaways": ["..."], "bullets": ["..."], "category": "...", "subcategory": "..."}'
        for _ in groups
    )

    user_message = (
        f"We are splitting topic '{topic['name']}' into {num_groups + 1} topics "
        f"(the original + {num_groups} new).\n"
        f"Category: {topic.get('category', 'N/A')} / {topic.get('subcategory', 'N/A')}\n"
        f"Description: {topic.get('description', '')}\n"
        f"{existing_takeaways}{existing_bullets}\n\n"
        f"Posts STAYING in original topic ({len(staying_posts)} total):\n"
        f"{_format_posts_for_prompt(staying_posts, per_group_sample)}\n\n"
        + "\n\n".join(group_sections) + "\n\n"
        "Available taxonomy:\n" + "\n".join(taxonomy_list) + "\n\n"
        f"Suggest metadata for the source topic and {num_groups} new topic(s). Output ONLY a JSON object:\n"
        '{"source": {"description": "...", "key_takeaways": ["..."], "bullets": ["..."]}, '
        f'"new_topics": [{new_topics_schema}]}}\n'
        "Each name: 4-8 words, Title Case, slide-ready."
        "\nkey_takeaways: 1-2 executive-level statements. bullets: 2-6 supporting details."
    )

    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        max_tokens = min(4096, 1024 + 512 * (num_groups - 1))
        response = client.messages.create(
            model=config.OPUS_MODEL,
            max_tokens=max_tokens,
            system="You are an editorial assistant. Suggest metadata for a topic split. Output ONLY a JSON object.",
            messages=[{"role": "user", "content": user_message}],
        )

        usage = {
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
        }
        cost = (usage["input_tokens"] / 1_000_000) * 5.00 + (usage["output_tokens"] / 1_000_000) * 25.00
        db.record_api_usage(
            service="anthropic", operation="split_suggest",
            input_tokens=usage["input_tokens"], output_tokens=usage["output_tokens"],
            cost_usd=cost, model=config.OPUS_MODEL, batch_size=1,
        )

        text = response.content[0].text.strip()
        stripped = re.sub(r"^```(?:json)?\s*\n?", "", text)
        stripped = re.sub(r"\n?```\s*$", "", stripped).strip()
        suggestion = json.loads(stripped)

        # Backward compat: if single group and response has new_topic instead of new_topics, wrap it
        if "new_topic" in suggestion and "new_topics" not in suggestion:
            suggestion["new_topics"] = [suggestion.pop("new_topic")]

        return jsonify({"ok": True, "suggestion": suggestion})
    except Exception as e:
        logger.error(f"Split suggestion failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/topics/<int:topic_id>/split", methods=["POST"])
@login_required
def api_split_execute(topic_id):
    """Execute a topic split (supports multi-topic)."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    db = get_db()
    topic = db.get_topic_by_id(topic_id)
    if not topic:
        return jsonify({"error": "Topic not found"}), 404

    # Support both old format and new multi-topic format
    new_topics = data.get("new_topics")
    if not new_topics:
        # Backward compat: old format with post_ids + new_topic
        post_ids = data.get("post_ids", [])
        new_topic = data.get("new_topic", {})
        if not post_ids:
            return jsonify({"error": "new_topics or post_ids required"}), 400
        if not new_topic.get("name"):
            return jsonify({"error": "new_topic.name required"}), 400
        new_topics = [{**new_topic, "post_ids": post_ids}]

    source_updates = data.get("source_updates")

    # Validate each new topic has a name
    for i, nt in enumerate(new_topics):
        if not nt.get("name", "").strip():
            return jsonify({"error": f"New topic {i + 1} name is required"}), 400
        if not nt.get("post_ids"):
            return jsonify({"error": f"New topic {i + 1} has no posts"}), 400

    try:
        result = db.split_topic_multi(
            source_topic_id=topic_id,
            new_topics_data=new_topics,
            source_updates=source_updates,
            split_by=request.user["display_name"],
        )

        # Background vectorize source + all new topics
        def _bg_vectorize(tid):
            try:
                from vector_search import TopicVectorIndex
                bg_db = get_db()
                idx = TopicVectorIndex.get_instance(bg_db.conn)
                t = bg_db.get_topic_by_id(tid)
                if t:
                    idx.sync_topic_vectors([t], bg_db)
            except Exception as e:
                logger.warning(f"Background vectorization failed for topic {tid}: {e}")

        threading.Thread(target=_bg_vectorize, args=(topic_id,), daemon=True).start()
        for new_id in result["new_topic_ids"]:
            threading.Thread(target=_bg_vectorize, args=(new_id,), daemon=True).start()

        return jsonify({"ok": True, **result})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Split execution failed: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------------
# Topic Retry
# ------------------------------------------------------------------
@app.route("/api/topics/retry/<int:ut_id>", methods=["POST"])
@login_required
def api_retry_topic(ut_id):
    """Retry a failed user topic creation."""
    db = get_db()
    cur = db.conn.execute("SELECT * FROM user_topics WHERE id = ?", (ut_id,))
    ut = cur.fetchone()
    if not ut:
        return jsonify({"error": "Not found"}), 404
    ut = dict(ut)
    if ut["status"] != "error":
        return jsonify({"error": "Only errored topics can be retried"}), 400

    try:
        db.update_user_topic_status(ut_id, "pending")
        topic_id, _ = db.get_or_create_topic_status(
            ut["name"], ut.get("description"),
            ut.get("category"), ut.get("subcategory"),
            promote=True, created_source="user",
        )
        db.update_user_topic_status(ut_id, "active", topic_id=topic_id)

        # Vectorize in background thread so response returns fast
        def _bg_vectorize_retry(tid):
            try:
                from vector_search import TopicVectorIndex
                bg_db = get_db()
                idx = TopicVectorIndex.get_instance(bg_db.conn)
                topic = bg_db.get_topic_by_id(tid)
                if topic:
                    idx.sync_topic_vectors([topic], bg_db)
                logger.info(f"Background vectorization (retry) complete for topic {tid}")
            except Exception as e:
                logger.warning(f"Background vectorization failed on retry for topic {tid}: {e}")

        threading.Thread(target=_bg_vectorize_retry, args=(topic_id,), daemon=True).start()

        return jsonify({"ok": True, "topic_id": topic_id})
    except Exception as e:
        db.update_user_topic_status(ut_id, "error", error_message=str(e))
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------------
# Pipeline Page (was Activity)
# ------------------------------------------------------------------
@app.route("/activity")
@login_required
def activity():
    """Pipeline: topic creation & edit activity."""
    db = get_db()
    recent_activity = db.get_recent_activity(limit=50)
    activity_summary = db.get_activity_summary()
    lf = activity_summary.get("last_fetch")
    if lf and lf.get("started_at"):
        try:
            mt = ZoneInfo("America/Boise")
            utc = ZoneInfo("UTC")
            naive = datetime.fromisoformat(lf["started_at"])
            aware = naive.replace(tzinfo=utc)
            lf["started_at_mt"] = aware.astimezone(mt).strftime("%b %d %-I:%M %p MT")
        except Exception:
            lf["started_at_mt"] = lf["started_at"][:16]
    return render_template(
        "index.html",
        **_common_ctx(
            page="activity",
            recent_activity=recent_activity,
            activity_summary=activity_summary,
        ),
    )


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
if __name__ == "__main__":
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    app.run(
        host=config.DASHBOARD_HOST,
        port=config.DASHBOARD_PORT,
        debug=False,
    )
