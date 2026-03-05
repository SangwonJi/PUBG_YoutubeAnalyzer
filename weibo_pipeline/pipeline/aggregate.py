"""
Pipeline Step 3: Aggregate
콜라보 파트너별 핵심 지표를 집계합니다.
"""
import json
import logging
from datetime import datetime, timedelta, timezone

from db.models import get_conn, now_iso

logger = logging.getLogger(__name__)


def run_aggregate(days: int = 365) -> dict:
    since_date = (
        datetime.now(timezone.utc) - timedelta(days=days)
    ).isoformat()

    stats = {"partners": 0, "total_posts": 0}

    with get_conn() as conn:
        conn.execute("DELETE FROM collab_agg")
        conn.commit()

        rows = conn.execute("""
            SELECT
                collab_partner,
                collab_category,
                collab_region,
                COUNT(*) as post_count,
                SUM(reposts_count) as total_reposts,
                SUM(comments_count) as total_comments,
                SUM(attitudes_count) as total_attitudes,
                AVG(reposts_count) as avg_reposts,
                AVG(comments_count) as avg_comments,
                AVG(attitudes_count) as avg_attitudes,
                MIN(created_at) as date_start,
                MAX(created_at) as date_end
            FROM posts
            WHERE is_collab = 1
              AND collab_partner IS NOT NULL
              AND collab_partner != ''
              AND created_at >= ?
            GROUP BY collab_partner
            ORDER BY total_attitudes DESC
        """, (since_date,)).fetchall()

        for row in rows:
            partner = row[0]
            category = row[1]

            comment_likes_row = conn.execute("""
                SELECT COALESCE(SUM(c.like_count), 0)
                FROM comments c
                JOIN posts p ON c.mid = p.mid
                WHERE p.collab_partner = ? AND p.created_at >= ?
            """, (partner, since_date)).fetchone()
            total_comment_likes = comment_likes_row[0] if comment_likes_row else 0

            top_posts_rows = conn.execute("""
                SELECT mid, text_clean, attitudes_count, page_title
                FROM posts
                WHERE collab_partner = ? AND created_at >= ?
                ORDER BY attitudes_count DESC
                LIMIT 3
            """, (partner, since_date)).fetchall()

            top_posts = []
            for tp in top_posts_rows:
                title = tp[3] or tp[1][:50] if tp[1] else "No title"
                top_posts.append(f"{tp[0]}|{title}({tp[2]:,} likes)")

            post_count = row[3]
            total_engagement = (row[4] or 0) + (row[5] or 0) + (row[6] or 0)
            engagement_rate = total_engagement / post_count if post_count > 0 else 0

            conn.execute("""
                INSERT OR REPLACE INTO collab_agg (
                    partner_name, category, region,
                    post_count, total_reposts, total_comments,
                    total_attitudes, total_comment_likes,
                    avg_reposts, avg_comments, avg_attitudes,
                    engagement_rate_pct,
                    top_posts,
                    date_range_start, date_range_end,
                    aggregated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                partner, category, row[2],
                post_count, row[4], row[5], row[6], total_comment_likes,
                round(row[7] or 0, 2), round(row[8] or 0, 2),
                round(row[9] or 0, 2),
                round(engagement_rate, 2),
                json.dumps(top_posts, ensure_ascii=False),
                row[10], row[11],
                now_iso(),
            ))

            stats["partners"] += 1
            stats["total_posts"] += post_count

    logger.info(f"[Aggregate] Done. {stats}")
    return stats
