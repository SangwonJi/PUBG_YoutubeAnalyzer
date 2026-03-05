"""
Database models and helper functions for PUBG Weibo Analyzer
"""
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from contextlib import contextmanager

import config


def init_db() -> None:
    """Initialize database with schema."""
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    schema_path = Path(__file__).parent / "schema.sql"
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.executescript(schema_path.read_text(encoding="utf-8"))
    print(f"[DB] Initialized: {config.DB_PATH}")


@contextmanager
def get_conn():
    """Context manager for database connections."""
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ──────────────────────────────────────────────
# Post operations
# ──────────────────────────────────────────────

def upsert_post(conn: sqlite3.Connection, post: dict) -> None:
    """Insert or update a Weibo post."""
    conn.execute("""
        INSERT INTO posts (
            mid, bid, created_at, text_raw, text_clean, source,
            reposts_count, comments_count, attitudes_count,
            has_video, has_image, media_type, video_url, thumbnail_url, video_duration,
            page_type, page_title, page_url,
            fetched_at, updated_at
        ) VALUES (
            :mid, :bid, :created_at, :text_raw, :text_clean, :source,
            :reposts_count, :comments_count, :attitudes_count,
            :has_video, :has_image, :media_type, :video_url, :thumbnail_url, :video_duration,
            :page_type, :page_title, :page_url,
            :fetched_at, :updated_at
        )
        ON CONFLICT(mid) DO UPDATE SET
            reposts_count = excluded.reposts_count,
            comments_count = excluded.comments_count,
            attitudes_count = excluded.attitudes_count,
            text_raw = excluded.text_raw,
            text_clean = excluded.text_clean,
            updated_at = excluded.updated_at
    """, post)


def upsert_comment(conn: sqlite3.Connection, comment: dict) -> None:
    """Insert or update a comment."""
    conn.execute("""
        INSERT INTO comments (
            comment_id, mid, reply_to_id,
            author_uid, author_name,
            text_raw, text_clean, created_at, like_count, source,
            fetched_at
        ) VALUES (
            :comment_id, :mid, :reply_to_id,
            :author_uid, :author_name,
            :text_raw, :text_clean, :created_at, :like_count, :source,
            :fetched_at
        )
        ON CONFLICT(comment_id) DO UPDATE SET
            like_count = excluded.like_count,
            text_raw = excluded.text_raw,
            text_clean = excluded.text_clean
    """, comment)


def get_unclassified_posts(conn: sqlite3.Connection) -> list:
    """Get posts that haven't been classified yet."""
    rows = conn.execute(
        "SELECT * FROM posts WHERE classified_at IS NULL ORDER BY created_at DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def get_classified_collabs(conn: sqlite3.Connection) -> list:
    """Get all posts classified as collabs."""
    rows = conn.execute(
        "SELECT * FROM posts WHERE is_collab = 1 ORDER BY created_at DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def update_classification(conn: sqlite3.Connection, mid: str,
                          is_collab: bool, partner: str, category: str,
                          region: str, classified_by: str, confidence: float) -> None:
    """Update collab classification for a post."""
    conn.execute("""
        UPDATE posts SET
            is_collab = ?, collab_partner = ?, collab_category = ?,
            collab_region = ?, classified_by = ?,
            classification_confidence = ?, classified_at = ?
        WHERE mid = ?
    """, (
        int(is_collab), partner, category,
        region, classified_by, confidence, now_iso(), mid
    ))


def get_post_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]


def get_last_fetched_at(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT MAX(fetched_at) FROM posts"
    ).fetchone()
    return row[0] if row else None


def get_status_summary(conn: sqlite3.Connection) -> dict:
    """Get pipeline status summary."""
    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    classified = conn.execute(
        "SELECT COUNT(*) FROM posts WHERE classified_at IS NOT NULL"
    ).fetchone()[0]
    collabs = conn.execute(
        "SELECT COUNT(*) FROM posts WHERE is_collab = 1"
    ).fetchone()[0]
    comments = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
    last_fetch = conn.execute(
        "SELECT MAX(fetched_at) FROM posts"
    ).fetchone()[0]
    return {
        "total_posts": total,
        "classified": classified,
        "unclassified": total - classified,
        "collabs": collabs,
        "total_comments": comments,
        "last_fetched_at": last_fetch,
    }
