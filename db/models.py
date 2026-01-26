"""
Database models and operations for PUBG Collab Pipeline.
Uses SQLite3 with Pydantic models for validation.
"""

import sqlite3
import json
import hashlib
from pathlib import Path
from datetime import datetime, date
from typing import Optional, List, Any
from pydantic import BaseModel, Field
from contextlib import contextmanager


class Video(BaseModel):
    """Video model."""
    video_id: str
    title: str
    description: Optional[str] = None
    published_at: datetime
    duration: Optional[str] = None
    channel_id: Optional[str] = None
    channel_name: Optional[str] = None
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    is_collab: bool = False
    collab_partner: Optional[str] = None
    collab_category: Optional[str] = None
    collab_region: Optional[str] = None
    collab_summary: Optional[str] = None
    collab_confidence: float = 0.0
    classification_method: Optional[str] = None
    last_fetched_at: datetime = Field(default_factory=datetime.now)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class Comment(BaseModel):
    """Comment model."""
    comment_id: str
    video_id: str
    author_name: Optional[str] = None
    author_channel_id: Optional[str] = None
    text_original: Optional[str] = None
    text_display: Optional[str] = None
    published_at: Optional[datetime] = None
    like_count: int = 0
    parent_id: Optional[str] = None
    is_reply: bool = False
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class CollabAgg(BaseModel):
    """Collab aggregation model."""
    id: Optional[int] = None
    partner_name: str
    category: Optional[str] = None
    region: Optional[str] = None
    date_range_start: date
    date_range_end: date
    video_count: int = 0
    total_views: int = 0
    total_video_likes: int = 0
    total_comments: int = 0
    total_comment_likes: int = 0
    comment_likes_partial: bool = False
    avg_views: float = 0.0
    avg_video_likes: float = 0.0
    like_rate: float = 0.0
    comment_rate: float = 0.0
    top_videos_json: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @property
    def top_videos(self) -> List[dict]:
        """Parse top_videos_json."""
        if self.top_videos_json:
            return json.loads(self.top_videos_json)
        return []


class FetchProgress(BaseModel):
    """Fetch progress tracking model."""
    id: Optional[int] = None
    task_type: str
    target_id: Optional[str] = None
    status: str = "pending"
    page_token: Optional[str] = None
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class Database:
    """SQLite database manager."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize database with schema."""
        schema_path = Path(__file__).parent / "schema.sql"
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = f.read()
        
        with self.get_connection() as conn:
            conn.executescript(schema)
    
    @contextmanager
    def get_connection(self):
        """Get database connection context manager."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    # ==================== Video Operations ====================
    
    def upsert_video(self, video: Video) -> None:
        """Insert or update a video."""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO videos (
                    video_id, title, description, published_at, duration,
                    channel_id, channel_name, view_count, like_count, comment_count,
                    is_collab, collab_partner, collab_category, collab_region,
                    collab_summary, collab_confidence, classification_method,
                    last_fetched_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    view_count = excluded.view_count,
                    like_count = excluded.like_count,
                    comment_count = excluded.comment_count,
                    is_collab = excluded.is_collab,
                    collab_partner = excluded.collab_partner,
                    collab_category = excluded.collab_category,
                    collab_region = excluded.collab_region,
                    collab_summary = excluded.collab_summary,
                    collab_confidence = excluded.collab_confidence,
                    classification_method = excluded.classification_method,
                    last_fetched_at = excluded.last_fetched_at,
                    updated_at = excluded.updated_at
            """, (
                video.video_id, video.title, video.description,
                video.published_at.isoformat(), video.duration,
                video.channel_id, video.channel_name,
                video.view_count, video.like_count, video.comment_count,
                video.is_collab, video.collab_partner, video.collab_category,
                video.collab_region, video.collab_summary, video.collab_confidence,
                video.classification_method, video.last_fetched_at.isoformat(),
                video.created_at.isoformat(), video.updated_at.isoformat()
            ))
    
    def upsert_videos_batch(self, videos: List[Video]) -> None:
        """Batch insert or update videos."""
        for video in videos:
            self.upsert_video(video)
    
    def get_video(self, video_id: str) -> Optional[Video]:
        """Get a single video by ID."""
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM videos WHERE video_id = ?", (video_id,)
            ).fetchone()
            if row:
                return self._row_to_video(row)
        return None
    
    def get_videos_in_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> List[Video]:
        """Get videos within a date range."""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM videos
                WHERE published_at >= ? AND published_at <= ?
                ORDER BY published_at DESC
            """, (start_date.isoformat(), end_date.isoformat())).fetchall()
            return [self._row_to_video(row) for row in rows]
    
    def get_collab_videos(self, days: int = 365) -> List[Video]:
        """Get collab videos from the last N days."""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM videos
                WHERE is_collab = 1
                AND published_at >= datetime('now', ? || ' days')
                ORDER BY published_at DESC
            """, (f"-{days}",)).fetchall()
            return [self._row_to_video(row) for row in rows]
    
    def get_unclassified_videos(self) -> List[Video]:
        """Get videos that haven't been classified yet."""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM videos
                WHERE classification_method IS NULL
                ORDER BY published_at DESC
            """).fetchall()
            return [self._row_to_video(row) for row in rows]
    
    def get_all_videos(self, limit: Optional[int] = None) -> List[Video]:
        """Get all videos."""
        with self.get_connection() as conn:
            query = "SELECT * FROM videos ORDER BY published_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            rows = conn.execute(query).fetchall()
            return [self._row_to_video(row) for row in rows]
    
    def _row_to_video(self, row: sqlite3.Row) -> Video:
        """Convert database row to Video model."""
        data = dict(row)
        # Parse datetime fields
        for field in ['published_at', 'last_fetched_at', 'created_at', 'updated_at']:
            if data.get(field) and isinstance(data[field], str):
                data[field] = datetime.fromisoformat(data[field])
        return Video(**data)
    
    # ==================== Comment Operations ====================
    
    def upsert_comment(self, comment: Comment) -> None:
        """Insert or update a comment."""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO comments (
                    comment_id, video_id, author_name, author_channel_id,
                    text_original, text_display, published_at, like_count,
                    parent_id, is_reply, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(comment_id) DO UPDATE SET
                    text_original = excluded.text_original,
                    text_display = excluded.text_display,
                    like_count = excluded.like_count,
                    updated_at = excluded.updated_at
            """, (
                comment.comment_id, comment.video_id, comment.author_name,
                comment.author_channel_id, comment.text_original, comment.text_display,
                comment.published_at.isoformat() if comment.published_at else None,
                comment.like_count, comment.parent_id, comment.is_reply,
                comment.created_at.isoformat(), comment.updated_at.isoformat()
            ))
    
    def upsert_comments_batch(self, comments: List[Comment]) -> None:
        """Batch insert or update comments."""
        for comment in comments:
            self.upsert_comment(comment)
    
    def get_comments_for_video(self, video_id: str) -> List[Comment]:
        """Get all comments for a video."""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM comments
                WHERE video_id = ?
                ORDER BY published_at DESC
            """, (video_id,)).fetchall()
            return [self._row_to_comment(row) for row in rows]
    
    def get_comment_stats_for_video(self, video_id: str) -> dict:
        """Get comment statistics for a video."""
        with self.get_connection() as conn:
            row = conn.execute("""
                SELECT 
                    COUNT(*) as count,
                    COALESCE(SUM(like_count), 0) as total_likes
                FROM comments
                WHERE video_id = ?
            """, (video_id,)).fetchone()
            return {"count": row["count"], "total_likes": row["total_likes"]}
    
    def _row_to_comment(self, row: sqlite3.Row) -> Comment:
        """Convert database row to Comment model."""
        data = dict(row)
        for field in ['published_at', 'created_at', 'updated_at']:
            if data.get(field) and isinstance(data[field], str):
                data[field] = datetime.fromisoformat(data[field])
        return Comment(**data)
    
    # ==================== Aggregation Operations ====================
    
    def upsert_collab_agg(self, agg: CollabAgg) -> None:
        """Insert or update collab aggregation."""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO collab_agg (
                    partner_name, category, region, date_range_start, date_range_end,
                    video_count, total_views, total_video_likes, total_comments,
                    total_comment_likes, comment_likes_partial, avg_views,
                    avg_video_likes, like_rate, comment_rate, top_videos_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(partner_name, date_range_start, date_range_end) DO UPDATE SET
                    category = excluded.category,
                    region = excluded.region,
                    video_count = excluded.video_count,
                    total_views = excluded.total_views,
                    total_video_likes = excluded.total_video_likes,
                    total_comments = excluded.total_comments,
                    total_comment_likes = excluded.total_comment_likes,
                    comment_likes_partial = excluded.comment_likes_partial,
                    avg_views = excluded.avg_views,
                    avg_video_likes = excluded.avg_video_likes,
                    like_rate = excluded.like_rate,
                    comment_rate = excluded.comment_rate,
                    top_videos_json = excluded.top_videos_json,
                    updated_at = excluded.updated_at
            """, (
                agg.partner_name, agg.category, agg.region,
                agg.date_range_start.isoformat(), agg.date_range_end.isoformat(),
                agg.video_count, agg.total_views, agg.total_video_likes,
                agg.total_comments, agg.total_comment_likes, agg.comment_likes_partial,
                agg.avg_views, agg.avg_video_likes, agg.like_rate, agg.comment_rate,
                agg.top_videos_json, agg.created_at.isoformat(), agg.updated_at.isoformat()
            ))
    
    def get_all_collab_aggs(self) -> List[CollabAgg]:
        """Get all collab aggregations."""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM collab_agg
                ORDER BY total_views DESC
            """).fetchall()
            return [self._row_to_collab_agg(row) for row in rows]
    
    def _row_to_collab_agg(self, row: sqlite3.Row) -> CollabAgg:
        """Convert database row to CollabAgg model."""
        data = dict(row)
        for field in ['date_range_start', 'date_range_end']:
            if data.get(field) and isinstance(data[field], str):
                data[field] = date.fromisoformat(data[field])
        for field in ['created_at', 'updated_at']:
            if data.get(field) and isinstance(data[field], str):
                data[field] = datetime.fromisoformat(data[field])
        return CollabAgg(**data)
    
    # ==================== Progress Tracking ====================
    
    def create_progress(self, task_type: str, target_id: Optional[str] = None) -> int:
        """Create a new fetch progress record."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO fetch_progress (task_type, target_id, status, started_at)
                VALUES (?, ?, 'in_progress', ?)
            """, (task_type, target_id, datetime.now().isoformat()))
            return cursor.lastrowid
    
    def update_progress(
        self,
        progress_id: int,
        status: str,
        page_token: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> None:
        """Update fetch progress."""
        with self.get_connection() as conn:
            completed_at = datetime.now().isoformat() if status in ('completed', 'failed') else None
            conn.execute("""
                UPDATE fetch_progress
                SET status = ?, page_token = ?, error_message = ?,
                    completed_at = ?, updated_at = ?
                WHERE id = ?
            """, (status, page_token, error_message, completed_at,
                  datetime.now().isoformat(), progress_id))
    
    def get_incomplete_progress(self, task_type: str) -> List[FetchProgress]:
        """Get incomplete fetch progress records."""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM fetch_progress
                WHERE task_type = ? AND status IN ('pending', 'in_progress')
                ORDER BY created_at ASC
            """, (task_type,)).fetchall()
            return [self._row_to_progress(row) for row in rows]
    
    def _row_to_progress(self, row: sqlite3.Row) -> FetchProgress:
        """Convert database row to FetchProgress model."""
        data = dict(row)
        for field in ['started_at', 'completed_at', 'created_at', 'updated_at']:
            if data.get(field) and isinstance(data[field], str):
                data[field] = datetime.fromisoformat(data[field])
        return FetchProgress(**data)
    
    # ==================== GPT Cache ====================
    
    def get_gpt_cache(self, input_text: str) -> Optional[str]:
        """Get cached GPT response."""
        cache_key = hashlib.sha256(input_text.encode()).hexdigest()
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT output_json FROM gpt_cache WHERE cache_key = ?",
                (cache_key,)
            ).fetchone()
            if row:
                return row["output_json"]
        return None
    
    def set_gpt_cache(self, input_text: str, output_json: str, model: str) -> None:
        """Cache GPT response."""
        cache_key = hashlib.sha256(input_text.encode()).hexdigest()
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO gpt_cache (cache_key, input_text, output_json, model, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (cache_key, input_text, output_json, model, datetime.now().isoformat()))
    
    # ==================== Utility Methods ====================
    
    def get_video_count(self) -> int:
        """Get total video count."""
        with self.get_connection() as conn:
            row = conn.execute("SELECT COUNT(*) as count FROM videos").fetchone()
            return row["count"]
    
    def get_comment_count(self) -> int:
        """Get total comment count."""
        with self.get_connection() as conn:
            row = conn.execute("SELECT COUNT(*) as count FROM comments").fetchone()
            return row["count"]
    
    def get_last_video_date(self) -> Optional[datetime]:
        """Get the most recent video publish date."""
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT MAX(published_at) as max_date FROM videos"
            ).fetchone()
            if row and row["max_date"]:
                return datetime.fromisoformat(row["max_date"])
        return None
