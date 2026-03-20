# X Feed Intel

Automated X (Twitter) timeline intelligence platform for AI infrastructure market monitoring. Fetches timeline posts, classifies them with Claude AI for relevance, matches them to tracked topics, and provides a Flask dashboard for team review and voting.

## Features

- **Claude Haiku classification** -- binary relevance filter on incoming posts
- **Claude Opus topic matching** -- assigns relevant posts to specific tracked topics with category/subcategory taxonomy
- **sqlite-vec vector search** -- semantic pre-filtering of topic candidates using sentence-transformers
- **Flask dashboard** -- weekly prep view, team voting/triage, topic management, pipeline monitoring
- **14-category taxonomy** -- AI infrastructure-focused categorization (hardware, inference, models, policy, etc.)
- **Engagement metrics refresh** -- tiered age-based backoff for post metric updates
- **3-tier topic summaries** -- description + key takeaways + supporting bullets via Opus
- **Training data collection** -- passive vote snapshot and impression logging for future ML scoring
- **Weekly cycle management** -- automated rollover with editorial funnel (slide/bullet/candidate sections)

## Architecture

```
cron (hourly) -> fetcher.py -> x_client.py (X API v2, OAuth 1.0a)
                            -> classifier.py (Claude Haiku 4.5, binary relevance)
                            -> vector_search.py (sqlite-vec + sentence-transformers)
                            -> topic_matcher.py (Claude Opus 4.6, topic + taxonomy assignment)
                            -> database.py (SQLite WAL, category backfill, promotion)
                            -> metrics_refresher.py (engagement metrics, tiered backoff)
                            -> summary_generator.py (3-tier topic summaries)

waitress (WSGI, :5050) -> dashboard.py (Flask)
                       -> templates/index.html (Jinja2)
                       -> static/dashboard.css + dashboard.js
```

## Quick Start

### 1. Clone and set up virtual environment

```bash
git clone https://github.com/your-org/x-feed-intel.git
cd x-feed-intel
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your API keys:
# - ANTHROPIC_API_KEY (required)
# - X API OAuth 1.0a credentials (required for fetching)
# - GMAIL_ADDRESS / GMAIL_APP_PASSWORD (optional, for email notifications)
```

### 3. Initialize database

```bash
python scripts/init_db.py
```

### 4. Run the dashboard

```bash
python -m waitress --listen=0.0.0.0:5050 dashboard:app
# or for development:
# python -c "from dashboard import app; app.run(host='0.0.0.0', port=5050, debug=True)"
```

### 5. Run the fetcher (manually or via cron)

```bash
# Enable X collection first
export X_COLLECTION_ENABLED=1
python fetcher.py
```

## Configuration

All configuration is in `config.py`, with secrets loaded from `.env` via python-dotenv.

### Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `X_COLLECTION_ENABLED` | `0` | Set to `1` to enable X API collection |
| `CLASSIFICATION_BATCH_SIZE` | `20` | Posts per Haiku classification call |
| `VECTOR_SEARCH_ENABLED` | `True` | Enable semantic pre-filtering |
| `TOPIC_AUTO_PROMOTION_ENABLED` | `True` | Auto-promote candidates meeting thresholds |
| `WEEKLY_PREP_TOPIC_LIMIT` | `20` | Target slide topics per week |
| `METRICS_REFRESH_ENABLED` | `True` | Enable engagement metrics refresh |
| `SUMMARY_REFRESH_ENABLED` | `True` | Enable automatic summary updates |
| `ARCHIVE_AFTER_DAYS` | `14` | Prune posts older than this |

### Taxonomy

14 parent categories with 37 subcategories covering AI infrastructure topics:

MACRO_FRAMES, INDUSTRY_NARRATIVES, GEOPOLITICS_POLICY, AI_ECONOMICS, DATA_CENTER_INFRA, HARDWARE_PLATFORMS, NETWORKING, INFERENCE_STACK, MODEL_ARCHITECTURE, FRONTIER_MODELS, AGENTS_AUTONOMY, VERTICAL_APPS, RESEARCH_FRONTIERS, OPERATIONAL_METRICS

## Dashboard Features

- **Weekly Prep**: Editorial funnel with Slide, Bullet Watchlist, Needs Discussion, and Candidate Radar sections
- **Voting/Triage**: Team members vote slide/bullet/skip/unsure/flag on topics; mobile swipe voting
- **Topic Management**: Create topics, search with hybrid lexical+semantic, promote/demote
- **Pipeline View**: Fetch status, topic matcher metrics, error tracking with retry
- **Export**: Markdown export of voted topics (slide + bullet sections)
- **Auth**: Session-based login with admin password management

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/stats` | Dashboard statistics |
| `GET` | `/api/posts` | Paginated posts with filters |
| `GET` | `/api/topics/search?q=...` | Hybrid topic search |
| `POST` | `/api/vote` | Cast vote on a topic |
| `DELETE` | `/api/vote` | Remove a vote |
| `GET` | `/api/export/voted` | Export voted topics as markdown |
| `POST` | `/api/topics/<id>/promote` | Promote/demote a topic |
| `POST` | `/api/topics/<id>/tier` | Set editorial tier override |
| `POST` | `/api/topics` | Create a new topic |

## Systemd Setup

Copy the service files and adjust paths:

```bash
sudo cp x-feed-intel-dashboard.service /etc/systemd/system/
sudo cp x-feed-intel-weekly-rollover.service /etc/systemd/system/
sudo cp x-feed-intel-weekly-rollover.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now x-feed-intel-dashboard.service
sudo systemctl enable --now x-feed-intel-weekly-rollover.timer
```

## Docker

```bash
cp .env.example .env
# Edit .env with your API keys

docker-compose up -d
```

The dashboard will be available at `http://localhost:5050`.

## Admin Tools

```bash
# List users
python -m reset_password --list

# Reset a password
python -m reset_password <username> <newpass>

# Backfill topic summaries
python -m backfill_summaries --force --parallel 4

# Reset content data (preserve users)
python -m reset_data --dry-run
python -m reset_data --yes --vacuum

# Run weekly rollover manually
python weekly_rollover.py
```

## Dependencies

- **Python 3.11+**
- **Flask** + **Waitress** (WSGI server)
- **anthropic** SDK (Haiku for classification, Opus for topic matching and summaries)
- **requests** + **requests-oauthlib** (X API OAuth 1.0a)
- **sentence-transformers** + **torch** (embedding model for vector search)
- **sqlite-vec** (loaded as SQLite extension, not a pip package)

Note: `sqlite-vec` must be installed separately. On aarch64 (e.g., Raspberry Pi 5), use version 0.1.7a10+.

## License

MIT
