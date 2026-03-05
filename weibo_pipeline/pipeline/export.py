"""
Pipeline Step 4: Export
CSV 리포트를 생성하고 선택적으로 클라우드에 업로드합니다.
"""
import csv
import json
import logging
from pathlib import Path

import config
from db.models import get_conn

logger = logging.getLogger(__name__)


def run_export(out: str = None, full: bool = False,
               upload: bool = False) -> dict:
    """
    Export aggregated data to CSV.

    Args:
        out: Output file path or directory
        full: If True, export multiple detailed CSVs
        upload: If True, upload to cloud storage

    Returns:
        Summary dict with file paths
    """
    output_path = Path(out) if out else config.OUTPUT_DIR
    output_path.parent.mkdir(parents=True, exist_ok=True)

    files = []

    if full:
        # Export multiple CSVs
        if output_path.suffix:
            output_dir = output_path.parent
        else:
            output_dir = output_path
            output_dir.mkdir(parents=True, exist_ok=True)

        f1 = _export_collab_report(output_dir / "collab_report.csv")
        f2 = _export_all_posts(output_dir / "all_posts.csv")
        f3 = _export_collab_posts_detail(output_dir / "collab_posts_detail.csv")
        f4 = _export_video_posts(output_dir / "video_posts.csv")
        files = [f for f in [f1, f2, f3, f4] if f]
    else:
        # Single collab report
        if output_path.suffix == ".csv":
            f1 = _export_collab_report(output_path)
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            f1 = _export_collab_report(output_path / "collab_report.csv")
        files = [f1] if f1 else []

    # Optional cloud upload
    if upload and files:
        try:
            from clients.cloud_client import CloudClient
            cloud = CloudClient()
            for f in files:
                result = cloud.upload(Path(f), description="Weibo collab analysis")
                if result.success:
                    logger.info(f"[Upload] {f} → {result.url}")
                else:
                    logger.warning(f"[Upload] Failed: {f} - {result.error}")
        except Exception as e:
            logger.error(f"[Upload] Cloud upload error: {e}")

    logger.info(f"[Export] Done. Files: {files}")
    return {"files": files}


def _export_collab_report(filepath: Path) -> str:
    """Export collab aggregation report."""
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM collab_agg ORDER BY total_attitudes DESC
        """).fetchall()

        if not rows:
            logger.warning("[Export] No collab data to export")
            return ""

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                "partner_name", "category", "region",
                "post_count", "total_reposts", "total_comments",
                "total_attitudes", "total_comment_likes",
                "avg_reposts", "avg_comments", "avg_attitudes",
                "engagement_rate", "top_posts",
                "date_range_start", "date_range_end",
            ])
            for row in rows:
                d = dict(row)
                writer.writerow([
                    d["partner_name"], d["category"], d["region"],
                    d["post_count"], d["total_reposts"], d["total_comments"],
                    d["total_attitudes"], d["total_comment_likes"],
                    d["avg_reposts"], d["avg_comments"], d["avg_attitudes"],
                    d["engagement_rate_pct"], d["top_posts"],
                    d["date_range_start"], d["date_range_end"],
                ])

    logger.info(f"[Export] Collab report: {filepath} ({len(rows)} partners)")
    return str(filepath)


def _export_all_posts(filepath: Path) -> str:
    """Export all posts with metadata."""
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT mid, bid, created_at, text_clean,
                   reposts_count, comments_count, attitudes_count,
                   media_type, has_video, page_title,
                   is_collab, collab_partner, collab_category, classified_by
            FROM posts
            ORDER BY created_at DESC
        """).fetchall()

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                "mid", "bid", "created_at", "text_preview",
                "reposts", "comments", "attitudes",
                "media_type", "has_video", "page_title",
                "is_collab", "collab_partner", "collab_category", "classified_by",
            ])
            for row in rows:
                d = dict(row)
                text_preview = (d["text_clean"] or "")[:100]
                writer.writerow([
                    d["mid"], d["bid"], d["created_at"], text_preview,
                    d["reposts_count"], d["comments_count"], d["attitudes_count"],
                    d["media_type"], d["has_video"], d["page_title"],
                    d["is_collab"], d["collab_partner"], d["collab_category"],
                    d["classified_by"],
                ])

    logger.info(f"[Export] All posts: {filepath} ({len(rows)} posts)")
    return str(filepath)


def _export_collab_posts_detail(filepath: Path) -> str:
    """Export detailed collab posts with full text."""
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT mid, bid, created_at, text_clean,
                   reposts_count, comments_count, attitudes_count,
                   media_type, page_title, page_url,
                   collab_partner, collab_category, collab_region,
                   classified_by, classification_confidence
            FROM posts
            WHERE is_collab = 1
            ORDER BY attitudes_count DESC
        """).fetchall()

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                "mid", "bid", "created_at", "text",
                "reposts", "comments", "attitudes",
                "media_type", "page_title", "page_url",
                "partner", "category", "region",
                "classified_by", "confidence",
            ])
            for row in rows:
                d = dict(row)
                writer.writerow([
                    d["mid"], d["bid"], d["created_at"], d["text_clean"],
                    d["reposts_count"], d["comments_count"], d["attitudes_count"],
                    d["media_type"], d["page_title"], d["page_url"],
                    d["collab_partner"], d["collab_category"], d["collab_region"],
                    d["classified_by"], d["classification_confidence"],
                ])

    logger.info(f"[Export] Collab detail: {filepath} ({len(rows)} posts)")
    return str(filepath)


def _export_video_posts(filepath: Path) -> str:
    """Export video posts only (maps to YouTube's video tab)."""
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT mid, bid, created_at, text_clean, page_title,
                   reposts_count, comments_count, attitudes_count,
                   video_url, video_duration, thumbnail_url,
                   is_collab, collab_partner, collab_category
            FROM posts
            WHERE has_video = 1
            ORDER BY attitudes_count DESC
        """).fetchall()

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                "mid", "bid", "created_at", "text_preview", "video_title",
                "reposts", "comments", "attitudes",
                "video_url", "duration_sec", "thumbnail",
                "is_collab", "partner", "category",
            ])
            for row in rows:
                d = dict(row)
                text_preview = (d["text_clean"] or "")[:80]
                writer.writerow([
                    d["mid"], d["bid"], d["created_at"], text_preview,
                    d["page_title"],
                    d["reposts_count"], d["comments_count"], d["attitudes_count"],
                    d["video_url"], d["video_duration"], d["thumbnail_url"],
                    d["is_collab"], d["collab_partner"], d["collab_category"],
                ])

    logger.info(f"[Export] Video posts: {filepath} ({len(rows)} videos)")
    return str(filepath)
