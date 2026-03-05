"""
Pipeline Step 1: Fetch
웨이보에서 게시물과 댓글을 수집하여 DB에 저장합니다.
이전 수집이 중단된 경우, 마지막 지점부터 이어서 수집합니다.
"""
import logging
from datetime import datetime, timedelta, timezone

from clients.weibo_client import WeiboClient
from db.models import (
    init_db, get_conn, upsert_post, upsert_comment,
    get_post_count, get_last_fetched_at
)

logger = logging.getLogger(__name__)


def _get_oldest_mid(conn) -> str | None:
    """DB에서 가장 오래된 게시물의 mid를 가져온다 (이어서 수집용)."""
    row = conn.execute(
        "SELECT mid FROM posts ORDER BY created_at ASC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def run_fetch(days: int = 365, full: bool = False,
              fetch_comments: bool = True) -> dict:
    """
    Fetch posts (and optionally comments) from Weibo.

    Args:
        days: How many days of posts to fetch
        full: If True, re-fetch from the beginning (ignore resume)
        fetch_comments: If True, also fetch comments for each post

    Returns:
        Summary dict with counts
    """
    init_db()
    client = WeiboClient()

    # Show account info
    user_info = client.get_user_info()
    if user_info:
        logger.info(
            f"[Fetch] Account: {user_info['screen_name']} "
            f"(uid={user_info['uid']}, "
            f"posts={user_info['statuses_count']}, "
            f"followers={user_info['followers_count']})"
        )
    else:
        logger.warning("[Fetch] Could not fetch user info. Proceeding anyway...")

    # Determine date boundary
    since_date = (
        datetime.now(timezone.utc) - timedelta(days=days)
    ).isoformat()
    logger.info(f"[Fetch] Fetching posts since: {since_date}")

    stats = {"posts_new": 0, "posts_updated": 0, "comments": 0, "errors": 0}

    with get_conn() as conn:
        existing_before = get_post_count(conn)

        # Resume: 기존 데이터가 있고 full이 아니면 마지막 지점부터 이어서 수집
        resume_since_id = None
        if not full and existing_before > 0:
            resume_since_id = _get_oldest_mid(conn)
            if resume_since_id:
                logger.info(
                    f"[Fetch] RESUMING from oldest mid={resume_since_id} "
                    f"(DB has {existing_before} posts already)"
                )

        for post in client.iter_posts(
            since_date=since_date,
            resume_since_id=resume_since_id
        ):
            try:
                upsert_post(conn, post)
                stats["posts_new"] += 1

                # Fetch comments
                if fetch_comments and post["comments_count"] > 0:
                    try:
                        for comment in client.iter_comments(post["mid"]):
                            upsert_comment(conn, comment)
                            stats["comments"] += 1
                    except Exception as e:
                        logger.warning(
                            f"[Fetch] Comment fetch failed for mid={post['mid']}: {e}"
                        )
                        stats["errors"] += 1

                # Progress log every 20 posts
                if stats["posts_new"] % 20 == 0:
                    logger.info(
                        f"[Fetch] Progress: {stats['posts_new']} posts, "
                        f"{stats['comments']} comments"
                    )

            except Exception as e:
                logger.error(f"[Fetch] Failed to save post: {e}")
                stats["errors"] += 1

        existing_after = get_post_count(conn)
        stats["posts_updated"] = existing_after - existing_before

    logger.info(f"[Fetch] Done. {stats}")
    return stats
