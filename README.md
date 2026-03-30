# X Feed Intel

Automated X (Twitter) timeline intelligence platform for AI infrastructure market monitoring. Fetches timeline posts, classifies them with Claude AI for relevance, matches them to tracked topics, and provides a Flask dashboard for team review and voting.

Originally developed to run natively on a Raspberry Pi, this OSS version is being generalized and security-hardened for broader deployment in non-RPi environments.

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

**Model selection rationale:**
- Haiku 4.5: fast, cheap binary relevance filter (high volume, low latency)
- Opus 4.6: best reasoning quality for nuanced topic matching and multi-topic summaries
- Vector search: semantic pre-filtering reduces the number of topics sent to Opus, cutting cost and latency

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
# Edit .env — see Configuration table below for what each variable does
```

At minimum you need:
- `ANTHROPIC_API_KEY` — required for classification and topic matching
- `SECRET_KEY` — generate with `python3 -c "import secrets; print(secrets.token_hex(32))"`
- one bootstrapped admin account created with `python bootstrap_admin.py <username> <display_name> <password>`

### 3. Initialize database

```bash
python scripts/init_db.py
python bootstrap_admin.py admin Admin 'change-me-now'
```

### 4. Run the dashboard

```bash
# Production (waitress WSGI):
waitress-serve --listen=0.0.0.0:5050 --threads=2 dashboard:app

# Development (Flask dev server):
python -c "from dashboard import app; app.run(host='0.0.0.0', port=5050, debug=True)"
```

### 5. Verify setup

```bash
# Confirm database initialized and dashboard can import correctly:
python -c "from database import Database; import config; db = Database(str(config.DB_PATH)); print('DB OK:', config.DB_PATH)"
```

### 6. Run the fetcher (manually or via cron)

X collection is disabled by default. Set `X_COLLECTION_ENABLED=1` in `.env` and provide X API credentials to enable.

```bash
# One-off manual fetch:
X_COLLECTION_ENABLED=1 python fetcher.py

# Or use the cron wrapper (handles locking and backlog clearing):
./run_fetch.sh
```

## Dashboard-Only Mode

You can run the dashboard without X API credentials to manage topics, view posts already in the database, and use the voting/triage UI. Set:

```
X_COLLECTION_ENABLED=0  # default
```

Only `ANTHROPIC_API_KEY`, `SECRET_KEY`, and a bootstrapped dashboard user are required. X API credentials are only validated when `X_COLLECTION_ENABLED=1`.

## Configuration

All configuration is in `config.py`, with secrets loaded from `.env` via python-dotenv.

### Full Configuration Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | — | Anthropic API key for Haiku/Opus |
| `SECRET_KEY` | Yes | — | Flask session key — generate a random 32-byte hex string |
| `VOTER_NAMES` | No | `Admin,User1,User2` | Optional fallback list for team-vote UI labels; dashboard logins are managed explicitly via bootstrap/user tools |
| `SESSION_COOKIE_SECURE` | No | `0` | Set to `1` when the dashboard is served over HTTPS |
| `X_COLLECTION_ENABLED` | No | `0` | Set to `1` to enable X API collection |
| `X_CONSUMER_KEY` | If collection enabled | — | X API consumer key (OAuth 1.0a) |
| `X_CONSUMER_SECRET` | If collection enabled | — | X API consumer secret |
| `X_ACCESS_TOKEN` | If collection enabled | — | X API access token |
| `X_ACCESS_TOKEN_SECRET` | If collection enabled | — | X API access token secret |
| `X_BEARER_TOKEN` | If collection enabled | — | X API bearer token |
| `X_USER_ID` | If collection enabled | — | Your numeric X account ID (find at tweeterid.com) |
| `GMAIL_ADDRESS` | No | — | Gmail address for email notifications |
| `GMAIL_APP_PASSWORD` | No | — | Gmail App Password for SMTP auth |
| `EMAIL_RECIPIENTS` | No | — | Comma-separated notification recipients |
| `TRANSCRIPTION_INTEGRATION_ENABLED` | No | `0` | Enable webhook callbacks from video transcription pipeline |
| `TRANSCRIPTION_INTEGRATION_TOKEN` | No | — | Shared auth token for transcription callbacks |
| `TRANSCRIPTION_CALLBACK_URL` | No | `http://127.0.0.1:5050/...` | Callback endpoint URL |
| `LOG_DIR` | No | `./logs` | Directory for log files |
| `CLASSIFICATION_BATCH_SIZE` | No | `20` | Posts per Haiku classification call |
| `VECTOR_SEARCH_ENABLED` | No | `True` | Enable semantic pre-filtering (requires sqlite-vec) |
| `TOPIC_AUTO_PROMOTION_ENABLED` | No | `True` | Auto-promote candidates meeting thresholds |
| `WEEKLY_PREP_TOPIC_LIMIT` | No | `20` | Target slide topics per week |
| `METRICS_REFRESH_ENABLED` | No | `True` | Enable engagement metrics refresh |
| `SUMMARY_REFRESH_ENABLED` | No | `True` | Enable automatic summary updates |
| `ARCHIVE_AFTER_DAYS` | No | `14` | Prune posts older than this |
| `WEEKLY_CYCLE_TIMEZONE` | No | `America/Chicago` | Timezone for weekly cycle cutoffs |

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

All endpoints require an authenticated session (login via the dashboard UI first).

## Deployment (Systemd)

Use the installer script to render the systemd units with your actual repo path, venv, user, and env-file location:

```bash
chmod +x install_systemd.sh
sudo RUN_AS_USER=xfi INSTALL_DIR=/path/to/x-feed-intel ./install_systemd.sh

# Bootstrap the first dashboard admin before starting the service:
python bootstrap_admin.py admin Admin 'change-me-now'
sudo systemctl start x-feed-intel-dashboard.service

# Verify:
sudo systemctl status x-feed-intel-dashboard.service
journalctl -u x-feed-intel-dashboard.service -f
```

For cron-based fetching, add `run_fetch.sh` to crontab:

```
0 8-20 * * 1-5 /path/to/x-feed-intel/run_fetch.sh   # Hourly 8AM-8PM weekdays
0 23 * * * /path/to/x-feed-intel/run_fetch.sh         # Nightly
```

## Docker

```bash
cp .env.example .env
# Edit .env with your API keys and SECRET_KEY

# Initialize the database schema
docker compose run --rm dashboard python scripts/init_db.py

# Bootstrap the first admin user
docker compose run --rm dashboard python bootstrap_admin.py admin Admin 'change-me-now'

# Start the dashboard and fetcher
docker compose up -d
```

The dashboard container initializes the schema on startup but will fail fast until at least one dashboard user exists. The `fetcher` service in docker-compose runs once on startup; use a cron job or Kubernetes CronJob to schedule recurring collection.

## Admin Tools

```bash
# List users
python reset_password.py --list

# Create a user
python reset_password.py --create analyst Analyst 'change-me-now'

# Reset a user password
python reset_password.py <username> <newpass>

# Backfill topic summaries (regenerate all, or just missing ones)
python backfill_summaries.py --force --parallel 4

# Reset content data, preserve user accounts
python reset_data.py --dry-run
python reset_data.py --yes --vacuum

# Run weekly rollover manually
python weekly_rollover.py
```

## Troubleshooting

**`ImportError: attempted relative import with no known parent package` during init_db.py**
All modules now use absolute imports. Run `python scripts/init_db.py` from the repo root with the venv active.

**`Configuration errors: X_USER_ID not set in .env`**
Add `X_USER_ID=<your-numeric-id>` to `.env`. Find your user ID at [tweeterid.com](https://tweeterid.com) or via the X API developer console. Only required when `X_COLLECTION_ENABLED=1`.

**Dashboard starts but vector search is slow or disabled**
Check the startup logs for `sqlite-vec loaded` or `sqlite-vec setup failed`. If disabled, ensure `sqlite-vec` is installed: `pip install sqlite-vec`. On aarch64 (Raspberry Pi), you may need a version ≥ 0.1.7a10.

**`ValueError: Configuration errors` when running with `X_COLLECTION_ENABLED=0`**
Only `ANTHROPIC_API_KEY`, `SECRET_KEY`, and at least one bootstrapped dashboard user are required for dashboard-only mode. X API credentials are only validated when collection is enabled.

**`waitress-serve: command not found`**
Install with `pip install waitress` (already in requirements.txt). Use `waitress-serve`, not `python -m waitress`.

**User management (add/remove users)**
Fresh deployments do not auto-create users. Bootstrap the first admin with `python bootstrap_admin.py <username> <display_name> <password>`, then use `python reset_password.py --create <username> <display_name> <password>` for additional accounts. There is no user deletion tool; remove accounts directly from the `users` table if needed.

## Dependencies

- **Python 3.11+**
- **Flask** + **Waitress** (WSGI server)
- **anthropic** SDK (Haiku for classification, Opus for topic matching and summaries)
- **requests** + **requests-oauthlib** (X API OAuth 1.0a)
- **sentence-transformers** + **torch** (embedding model for vector search)
- **sqlite-vec** (vector similarity search extension)

## License

MIT
