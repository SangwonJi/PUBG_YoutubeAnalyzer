"""
Weibo Mobile API Client

m.weibo.cn의 비공식 JSON API를 활용하여 게시물 및 댓글을 수집합니다.
인증 없이 기본 수집이 가능하며, 쿠키 설정 시 레이트 리밋이 완화됩니다.

주요 엔드포인트:
- User timeline: /api/container/getIndex?type=uid&value={uid}&containerid=107603{uid}
- Post comments: /api/comments/show?id={mid}&page={n}
- Post detail:   /statuses/show?id={mid}
"""
import re
import time
import json
import random
import logging
from typing import Iterator, Optional
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config

logger = logging.getLogger(__name__)


class WeiboClient:
    """Weibo m.weibo.cn API client for scraping posts and comments."""

    def __init__(self, uid: str = None):
        self.uid = uid or config.WEIBO_UID
        self.base_url = config.WEIBO_BASE_URL
        self.delay = config.WEIBO_REQUEST_DELAY
        self.max_retries = config.WEIBO_MAX_RETRIES
        self.max_comments = config.WEIBO_MAX_COMMENTS_PER_POST
        self.session = self._build_session()
        self._request_count = 0

    def _build_session(self) -> requests.Session:
        """Build a requests session with retry logic and headers."""
        session = requests.Session()

        # Retry strategy with exponential backoff
        retry = Retry(
            total=self.max_retries,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Headers to mimic mobile browser
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/17.0 Mobile/15E148 Safari/604.1"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{self.base_url}/u/{self.uid}",
        })

        # Optional cookie for higher rate limits
        if config.WEIBO_COOKIE:
            session.headers["Cookie"] = config.WEIBO_COOKIE

        return session

    def _throttle(self) -> None:
        """Rate limiting with jitter."""
        jitter = random.uniform(0.5, 1.5)
        sleep_time = self.delay * jitter
        self._request_count += 1
        # Increase delay every 50 requests to avoid blocks
        if self._request_count % 50 == 0:
            sleep_time += random.uniform(5, 15)
            logger.info(f"[Throttle] Extended pause after {self._request_count} requests")
        time.sleep(sleep_time)

    def _get_json(self, url: str, params: dict = None) -> Optional[dict]:
        """Make a GET request and return JSON, handling common errors."""
        self._throttle()
        try:
            resp = self.session.get(url, params=params, timeout=30)

            # Check for login redirect (cookie expired)
            if resp.url and "passport.weibo" in resp.url:
                logger.warning("[Auth] Redirected to login page. Cookie may be expired.")
                return None

            if resp.status_code == 418:
                logger.warning("[Rate] Got 418 (I'm a teapot) - likely rate limited. Waiting 60s...")
                time.sleep(60)
                return None

            resp.raise_for_status()
            data = resp.json()

            if data.get("ok") != 1:
                msg = data.get("msg", "unknown error")
                logger.warning(f"[API] Response not OK: {msg}")
                return None

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"[HTTP] Request failed: {e}")
            return None
        except json.JSONDecodeError:
            logger.error("[JSON] Failed to decode response")
            return None

    # ──────────────────────────────────────────
    # User Profile
    # ──────────────────────────────────────────

    def get_user_info(self) -> Optional[dict]:
        """Fetch user profile information."""
        url = f"{self.base_url}/api/container/getIndex"
        params = {"type": "uid", "value": self.uid}
        data = self._get_json(url, params)
        if not data:
            return None

        user_info = data.get("data", {}).get("userInfo", {})
        return {
            "uid": str(user_info.get("id", "")),
            "screen_name": user_info.get("screen_name", ""),
            "description": user_info.get("description", ""),
            "followers_count": user_info.get("followers_count", 0),
            "follow_count": user_info.get("follow_count", 0),
            "statuses_count": user_info.get("statuses_count", 0),
            "verified": user_info.get("verified", False),
            "verified_reason": user_info.get("verified_reason", ""),
        }

    # ──────────────────────────────────────────
    # Timeline / Posts
    # ──────────────────────────────────────────

    def iter_posts(self, max_pages: int = None, since_date: str = None,
                   resume_since_id: str = None) -> Iterator[dict]:
        """
        Iterate over user's posts via timeline API.

        Args:
            max_pages: Maximum number of pages to fetch (None = all)
            since_date: Stop fetching when post date is older than this (ISO format)
            resume_since_id: Start fetching from this since_id (for resuming)

        Yields:
            Parsed post dicts ready for DB insertion
        """
        container_id = f"107603{self.uid}"
        page = 1
        since_id = resume_since_id  # Resume from last position if provided
        total_fetched = 0

        if resume_since_id:
            logger.info(f"[Fetch] Resuming from since_id={resume_since_id}")

        while True:
            if max_pages and page > max_pages:
                break

            url = f"{self.base_url}/api/container/getIndex"
            params = {
                "type": "uid",
                "value": self.uid,
                "containerid": container_id,
            }
            if since_id:
                params["since_id"] = since_id
            else:
                params["page"] = page

            logger.info(f"[Fetch] Page {page}, since_id={since_id}, total={total_fetched}")
            data = self._get_json(url, params)

            if not data:
                logger.warning(f"[Fetch] No data on page {page}, stopping.")
                break

            cards = data.get("data", {}).get("cards", [])
            if not cards:
                logger.info("[Fetch] No more cards, reached end of timeline.")
                break

            stop = False
            for card in cards:
                if card.get("card_type") != 9:
                    continue

                mblog = card.get("mblog")
                if not mblog:
                    continue

                post = self._parse_post(mblog)
                if not post:
                    continue

                # Check date boundary
                if since_date and post["created_at"] < since_date:
                    logger.info(f"[Fetch] Reached date boundary: {post['created_at']}")
                    stop = True
                    break

                total_fetched += 1
                yield post

            if stop:
                break

            # Pagination: prefer since_id cursor over page number
            cardlist_info = data.get("data", {}).get("cardlistInfo", {})
            new_since_id = cardlist_info.get("since_id")
            if new_since_id:
                since_id = new_since_id
            else:
                page += 1

            # Safety: no infinite loops
            if page > 500:
                logger.warning("[Fetch] Exceeded 500 pages, stopping.")
                break

        logger.info(f"[Fetch] Complete. Total posts fetched: {total_fetched}")

    def _parse_post(self, mblog: dict) -> Optional[dict]:
        """Parse an mblog object into a clean post dict."""
        try:
            mid = str(mblog.get("id", mblog.get("mid", "")))
            if not mid:
                return None

            text_raw = mblog.get("text", "")
            text_clean = self._clean_html(text_raw)

            # Detect media type
            has_video = 0
            has_image = 0
            media_type = "text"
            video_url = None
            thumbnail_url = None
            video_duration = 0
            page_type = None
            page_title = None
            page_url = None

            # Check page_info for video
            page_info = mblog.get("page_info", {})
            if page_info:
                page_type = page_info.get("type", "")
                page_title = page_info.get("page_title", "")
                page_url = page_info.get("page_url", "")

                if page_type == "video":
                    has_video = 1
                    media_type = "video"
                    urls = page_info.get("urls", page_info.get("media_info", {}))
                    if isinstance(urls, dict):
                        video_url = (
                            urls.get("mp4_720p_mp4")
                            or urls.get("mp4_hd_mp4")
                            or urls.get("mp4_sd_mp4")
                            or urls.get("mp4_ld_mp4")
                            or urls.get("stream_url")
                        )
                    thumbnail_url = page_info.get("page_pic", {}).get("url", "")
                    video_duration = page_info.get("media_info", {}).get("duration", 0)
                    if isinstance(video_duration, str):
                        try:
                            video_duration = int(float(video_duration))
                        except (ValueError, TypeError):
                            video_duration = 0

            # Check pics for images
            pics = mblog.get("pics", [])
            if pics and not has_video:
                has_image = 1
                media_type = "image"

            return {
                "mid": mid,
                "bid": mblog.get("bid", ""),
                "created_at": self._parse_weibo_date(mblog.get("created_at", "")),
                "text_raw": text_raw,
                "text_clean": text_clean,
                "source": self._clean_html(mblog.get("source", "")),
                "reposts_count": mblog.get("reposts_count", 0),
                "comments_count": mblog.get("comments_count", 0),
                "attitudes_count": mblog.get("attitudes_count", 0),
                "has_video": has_video,
                "has_image": has_image,
                "media_type": media_type,
                "video_url": video_url,
                "thumbnail_url": thumbnail_url,
                "video_duration": video_duration,
                "page_type": page_type,
                "page_title": page_title,
                "page_url": page_url,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            logger.error(f"[Parse] Failed to parse post: {e}")
            return None

    # ──────────────────────────────────────────
    # Comments
    # ──────────────────────────────────────────

    def iter_comments(self, mid: str, max_comments: int = None) -> Iterator[dict]:
        """
        Iterate over comments for a specific post.

        Args:
            mid: Post ID (status mid)
            max_comments: Max comments to fetch (default from config)

        Yields:
            Parsed comment dicts
        """
        max_comments = max_comments or self.max_comments
        page = 1
        fetched = 0

        while fetched < max_comments:
            url = f"{self.base_url}/api/comments/show"
            params = {"id": mid, "page": page}

            data = self._get_json(url, params)
            if not data:
                break

            comments_data = data.get("data", {}).get("data", [])
            if not comments_data:
                break

            for c in comments_data:
                if fetched >= max_comments:
                    break
                comment = self._parse_comment(c, mid)
                if comment:
                    fetched += 1
                    yield comment

            # Check if there are more pages
            max_page = data.get("data", {}).get("max", 1)
            if page >= max_page:
                break
            page += 1

        logger.debug(f"[Comments] mid={mid}: fetched {fetched} comments")

    def _parse_comment(self, comment: dict, mid: str) -> Optional[dict]:
        """Parse a comment object."""
        try:
            user = comment.get("user", {})
            return {
                "comment_id": str(comment.get("id", "")),
                "mid": mid,
                "reply_to_id": str(comment.get("reply_id", "")) or None,
                "author_uid": str(user.get("id", "")),
                "author_name": user.get("screen_name", ""),
                "text_raw": comment.get("text", ""),
                "text_clean": self._clean_html(comment.get("text", "")),
                "created_at": self._parse_weibo_date(comment.get("created_at", "")),
                "like_count": comment.get("like_count", 0),
                "source": self._clean_html(comment.get("source", "")),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            logger.error(f"[Parse] Failed to parse comment: {e}")
            return None

    # ──────────────────────────────────────────
    # Utilities
    # ──────────────────────────────────────────

    @staticmethod
    def _clean_html(text: str) -> str:
        """Remove HTML tags from Weibo text."""
        if not text:
            return ""
        # Remove HTML tags
        clean = re.sub(r"<[^>]+>", "", text)
        # Decode common entities
        clean = clean.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
        clean = clean.replace("&nbsp;", " ").replace("&quot;", '"')
        return clean.strip()

    @staticmethod
    def _parse_weibo_date(date_str: str) -> str:
        """
        Parse Weibo's various date formats to ISO format.

        Weibo uses multiple formats:
        - "刚刚" (just now)
        - "X分钟前" (X minutes ago)
        - "X小时前" (X hours ago)
        - "昨天 HH:MM" (yesterday)
        - "MM-DD" (this year)
        - "Tue Jan 14 12:00:00 +0800 2025" (full format)
        """
        if not date_str:
            return datetime.now(timezone.utc).isoformat()

        now = datetime.now(timezone.utc)

        # Relative times
        if date_str == "刚刚":
            return now.isoformat()
        m = re.match(r"(\d+)分钟前", date_str)
        if m:
            from datetime import timedelta
            return (now - timedelta(minutes=int(m.group(1)))).isoformat()
        m = re.match(r"(\d+)小时前", date_str)
        if m:
            from datetime import timedelta
            return (now - timedelta(hours=int(m.group(1)))).isoformat()

        # "昨天 HH:MM"
        m = re.match(r"昨天\s*(\d{2}):(\d{2})", date_str)
        if m:
            from datetime import timedelta
            yesterday = now - timedelta(days=1)
            return yesterday.replace(
                hour=int(m.group(1)), minute=int(m.group(2)), second=0
            ).isoformat()

        # "MM-DD" (this year)
        m = re.match(r"^(\d{1,2})-(\d{1,2})$", date_str)
        if m:
            return now.replace(
                month=int(m.group(1)), day=int(m.group(2)),
                hour=0, minute=0, second=0
            ).isoformat()

        # Full format: "Tue Jan 14 12:00:00 +0800 2025"
        try:
            dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass

        # Fallback: try common formats
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d", "%m-%d %H:%M"):
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.year == 1900:
                    dt = dt.replace(year=now.year)
                return dt.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                continue

        logger.warning(f"[Date] Could not parse date: {date_str}")
        return now.isoformat()
