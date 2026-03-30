"""X API v2 client — OAuth 1.0a, reverse chronological timeline, pagination."""
import time
import logging
from typing import Optional

import requests
from requests_oauthlib import OAuth1

import config

logger = logging.getLogger("x_feed_intel")


class XTimelineClient:
    """Fetch the authenticated user's reverse-chronological home timeline."""

    TIMELINE_URL = "https://api.x.com/2/users/{user_id}/timelines/reverse_chronological"

    # Fields to request from the API
    TWEET_FIELDS = (
        "author_id,created_at,text,entities,context_annotations,"
        "public_metrics,referenced_tweets,note_tweet"
    )
    EXPANSIONS = "author_id"
    USER_FIELDS = "name,username"

    def __init__(self):
        self.auth = OAuth1(
            config.X_CONSUMER_KEY,
            config.X_CONSUMER_SECRET,
            config.X_ACCESS_TOKEN,
            config.X_ACCESS_TOKEN_SECRET,
        )
        self.user_id = config.X_USER_ID
        self.session = requests.Session()
        self.session.auth = self.auth
        self.last_request_stats = {
            "http_requests_attempted": 0,
            "http_requests_succeeded": 0,
            "timeline_pages_fetched": 0,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def fetch_timeline(
        self,
        since_id: Optional[str] = None,
        max_pages: Optional[int] = None,
    ) -> list[dict]:
        """
        Fetch reverse-chronological timeline with pagination.

        Args:
            since_id: Only return tweets newer than this ID.
            max_pages: Override the default page cap.

        Returns:
            List of tweet dicts with author info merged in.
        """
        self.last_request_stats = {
            "http_requests_attempted": 0,
            "http_requests_succeeded": 0,
            "timeline_pages_fetched": 0,
        }
        if not config.X_COLLECTION_ENABLED:
            logger.info("X collection paused; skipping timeline fetch")
            return []

        max_pages = max_pages or config.MAX_PAGES_PER_FETCH
        url = self.TIMELINE_URL.format(user_id=self.user_id)

        params = {
            "max_results": config.MAX_RESULTS_PER_PAGE,
            "tweet.fields": self.TWEET_FIELDS,
            "expansions": self.EXPANSIONS,
            "user.fields": self.USER_FIELDS,
            "exclude": "retweets,replies",
        }
        if since_id:
            params["since_id"] = since_id

        all_tweets: list[dict] = []
        pages_fetched = 0

        while pages_fetched < max_pages:
            pages_fetched += 1
            logger.info(f"Fetching timeline page {pages_fetched} (since_id={since_id})")

            data = self._request_with_retry(url, params)
            if data is None:
                break

            # Build author lookup from includes.users
            users_map = {}
            for u in data.get("includes", {}).get("users", []):
                users_map[u["id"]] = {
                    "author_username": u.get("username", ""),
                    "author_name": u.get("name", ""),
                }

            # Process tweets
            tweets = data.get("data", [])
            if not tweets:
                logger.info("No tweets in response — done paginating")
                break

            for tweet in tweets:
                # Merge author info
                author_info = users_map.get(tweet.get("author_id", ""), {})
                tweet["author_username"] = author_info.get("author_username", "")
                tweet["author_name"] = author_info.get("author_name", "")

                # Extract note_tweet full text if present
                note = tweet.get("note_tweet", {})
                if note and note.get("text"):
                    tweet["full_text"] = note["text"]

                all_tweets.append(tweet)

            logger.info(
                f"Page {pages_fetched}: {len(tweets)} tweets "
                f"(total so far: {len(all_tweets)})"
            )
            self.last_request_stats["timeline_pages_fetched"] = pages_fetched

            # Check for next page
            meta = data.get("meta", {})
            next_token = meta.get("next_token")
            if not next_token:
                logger.info("No next_token — done paginating")
                break

            params["pagination_token"] = next_token

        logger.info(f"Fetched {len(all_tweets)} tweets across {pages_fetched} pages")
        return all_tweets

    def fetch_tweet_by_id(self, tweet_id: str) -> Optional[dict]:
        """Fetch a single tweet by ID using GET /2/tweets/:id.

        Returns enriched tweet dict or None on failure.
        """
        self.last_request_stats = {
            "http_requests_attempted": 0,
            "http_requests_succeeded": 0,
            "timeline_pages_fetched": 0,
        }
        if not config.X_COLLECTION_ENABLED:
            logger.info(f"X collection paused; skipping fetch_tweet_by_id for {tweet_id}")
            return None

        url = f"https://api.x.com/2/tweets/{tweet_id}"
        params = {
            "tweet.fields": self.TWEET_FIELDS,
            "expansions": self.EXPANSIONS,
            "user.fields": self.USER_FIELDS,
        }

        data = self._request_with_retry(url, params)
        if not data or "data" not in data:
            logger.warning(f"Failed to fetch tweet {tweet_id}")
            return None

        tweet = data["data"]

        # Build author lookup from includes.users
        users_map = {}
        for u in data.get("includes", {}).get("users", []):
            users_map[u["id"]] = {
                "author_username": u.get("username", ""),
                "author_name": u.get("name", ""),
            }

        # Merge author info
        author_info = users_map.get(tweet.get("author_id", ""), {})
        tweet["author_username"] = author_info.get("author_username", "")
        tweet["author_name"] = author_info.get("author_name", "")

        # Extract note_tweet full text if present
        note = tweet.get("note_tweet", {})
        if note and note.get("text"):
            tweet["full_text"] = note["text"]

        return tweet

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _request_with_retry(
        self, url: str, params: dict, max_retries: int = 3
    ) -> Optional[dict]:
        """
        Make a GET request with rate-limit handling and retry on errors.
        Returns parsed JSON or None on unrecoverable failure.
        """
        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(
                    url, params=params, timeout=config.REQUEST_TIMEOUT
                )
                self.last_request_stats["http_requests_attempted"] = (
                    self.last_request_stats.get("http_requests_attempted", 0) + 1
                )

                # Rate limit handling
                if resp.status_code == 429:
                    reset_time = resp.headers.get("x-rate-limit-reset")
                    if reset_time:
                        wait = max(int(reset_time) - int(time.time()), 1)
                        wait += config.RATE_LIMIT_BUFFER_SEC
                    else:
                        wait = 60
                    logger.warning(
                        f"Rate limited (429). Waiting {wait}s before retry."
                    )
                    time.sleep(wait)
                    continue

                # Auth errors — don't retry
                if resp.status_code in (401, 403):
                    logger.error(
                        f"Auth error {resp.status_code}: {resp.text[:500]}"
                    )
                    return None

                # Server errors — retry with backoff
                if resp.status_code >= 500:
                    wait = 2 ** attempt
                    logger.warning(
                        f"Server error {resp.status_code} (attempt {attempt}/{max_retries}). "
                        f"Retrying in {wait}s."
                    )
                    time.sleep(wait)
                    continue

                # Success
                resp.raise_for_status()
                self.last_request_stats["http_requests_succeeded"] = (
                    self.last_request_stats.get("http_requests_succeeded", 0) + 1
                )
                data = resp.json()

                # Check proactive rate-limit headers
                remaining = resp.headers.get("x-rate-limit-remaining")
                if remaining and int(remaining) < 10:
                    reset_time = resp.headers.get("x-rate-limit-reset")
                    if reset_time:
                        wait = max(int(reset_time) - int(time.time()), 1)
                        wait += config.RATE_LIMIT_BUFFER_SEC
                        logger.info(
                            f"Rate limit low ({remaining} remaining). "
                            f"Sleeping {wait}s proactively."
                        )
                        time.sleep(wait)

                return data

            except requests.Timeout:
                logger.warning(
                    f"Request timeout (attempt {attempt}/{max_retries})"
                )
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                continue

            except requests.RequestException as e:
                logger.error(
                    f"Request error (attempt {attempt}/{max_retries}): {e}"
                )
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                continue

        logger.error(f"All {max_retries} attempts failed for {url}")
        return None

    def fetch_tweets_batch(self, tweet_ids: list[str]) -> dict:
        """
        Fetch tweets by ID via GET /2/tweets.
        Up to 100 IDs per request; chunks larger lists.
        Returns {tweet_id: public_metrics_dict} for found tweets.
        """
        TWEETS_URL = "https://api.x.com/2/tweets"
        CHUNK_SIZE = 100
        results = {}
        api_requests = 0

        for i in range(0, len(tweet_ids), CHUNK_SIZE):
            chunk = tweet_ids[i : i + CHUNK_SIZE]
            params = {
                "ids": ",".join(chunk),
                "tweet.fields": "public_metrics",
            }
            data = self._request_with_retry(TWEETS_URL, params)
            api_requests += 1
            if data and "data" in data:
                for tweet in data["data"]:
                    pm = tweet.get("public_metrics")
                    if pm:
                        results[tweet["id"]] = pm

        self.last_request_stats["metrics_refresh_api_requests"] = api_requests
        return results
