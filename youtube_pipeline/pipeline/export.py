"""
Export pipeline for generating reports and CSV files.
"""

import json
import csv
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, Callable

import pandas as pd

from config import get_config
from db.models import Database, CollabAgg
from clients.gpt_client import GPTClient
from clients.cloud_client import CloudClient


def export_to_csv(
    db: Database,
    output_path: Path | str,
    days: int = 365,
    progress_callback: Optional[Callable[[str], None]] = None
) -> Path:
    """
    Export collab aggregation data to CSV.
    
    Args:
        db: Database instance
        output_path: Output file path
        days: Number of days to include
        progress_callback: Progress callback
    
    Returns:
        Path to generated CSV file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    log(f"Exporting collab report to {output_path}...")
    
    # Get aggregation data
    aggs = db.get_all_collab_aggs()
    
    if not aggs:
        log("No aggregation data found. Run 'aggregate' first.")
        return output_path
    
    # Prepare data for CSV
    rows = []
    for agg in aggs:
        top_videos = agg.top_videos[:3] if agg.top_videos else []
        top_videos_str = "; ".join([
            f"{v.get('video_id', '')}|{v.get('title', '')[:50]}"
            for v in top_videos
        ])
        
        rows.append({
            "partner_name": agg.partner_name,
            "category": agg.category,
            "region": agg.region,
            "video_count": agg.video_count,
            "total_views": agg.total_views,
            "total_video_likes": agg.total_video_likes,
            "total_comments": agg.total_comments,
            "total_comment_likes": agg.total_comment_likes,
            "comment_likes_partial": agg.comment_likes_partial,
            "avg_views": round(agg.avg_views, 2),
            "like_rate_pct": round(agg.like_rate * 100, 4),
            "comment_rate_pct": round(agg.comment_rate * 100, 4),
            "top_videos": top_videos_str,
            "date_range_start": agg.date_range_start.isoformat(),
            "date_range_end": agg.date_range_end.isoformat()
        })
    
    # Sort by total views
    rows.sort(key=lambda x: x["total_views"], reverse=True)
    
    # Write CSV
    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    
    log(f"Exported {len(rows)} partners to {output_path}")
    
    return output_path


def export_videos_csv(
    db: Database,
    output_path: Path | str,
    days: int = 365,
    only_collabs: bool = True,
    progress_callback: Optional[Callable[[str], None]] = None
) -> Path:
    """
    Export video data to CSV.
    
    Args:
        db: Database instance
        output_path: Output file path
        days: Number of days to include
        only_collabs: Only export collab videos
        progress_callback: Progress callback
    
    Returns:
        Path to generated CSV file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Get videos
    if only_collabs:
        videos = db.get_collab_videos(days=days)
        log(f"Exporting {len(videos)} collab videos...")
    else:
        cutoff = datetime.now() - timedelta(days=days)
        videos = db.get_videos_in_date_range(cutoff, datetime.now())
        log(f"Exporting {len(videos)} videos...")
    
    # Prepare data
    rows = []
    for video in videos:
        rows.append({
            "video_id": video.video_id,
            "title": video.title,
            "published_at": video.published_at.isoformat(),
            "duration": video.duration,
            "view_count": video.view_count,
            "like_count": video.like_count,
            "comment_count": video.comment_count,
            "is_collab": video.is_collab,
            "collab_partner": video.collab_partner,
            "collab_category": video.collab_category,
            "collab_region": video.collab_region,
            "collab_summary": video.collab_summary,
            "collab_confidence": video.collab_confidence,
            "classification_method": video.classification_method,
            "youtube_url": f"https://www.youtube.com/watch?v={video.video_id}"
        })
    
    # Sort by publish date
    rows.sort(key=lambda x: x["published_at"], reverse=True)
    
    # Write CSV
    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    
    log(f"Exported {len(rows)} videos to {output_path}")
    
    return output_path


def export_comments_csv(
    db: Database,
    output_path: Path | str,
    video_id: Optional[str] = None,
    progress_callback: Optional[Callable[[str], None]] = None
) -> Path:
    """
    Export comments to CSV.
    
    Args:
        db: Database instance
        output_path: Output file path
        video_id: Optional specific video ID
        progress_callback: Progress callback
    
    Returns:
        Path to generated CSV file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Get comments
    if video_id:
        comments = db.get_comments_for_video(video_id)
        log(f"Exporting {len(comments)} comments for video {video_id}...")
    else:
        # Get comments for all collab videos
        videos = db.get_collab_videos()
        comments = []
        for video in videos:
            comments.extend(db.get_comments_for_video(video.video_id))
        log(f"Exporting {len(comments)} comments from {len(videos)} videos...")
    
    # Prepare data
    rows = []
    for comment in comments:
        rows.append({
            "comment_id": comment.comment_id,
            "video_id": comment.video_id,
            "author_name": comment.author_name,
            "text": comment.text_original,
            "published_at": comment.published_at.isoformat() if comment.published_at else None,
            "like_count": comment.like_count,
            "is_reply": comment.is_reply,
            "parent_id": comment.parent_id
        })
    
    # Write CSV
    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    
    log(f"Exported {len(rows)} comments to {output_path}")
    
    return output_path


def export_report(
    db: Database,
    output_dir: Path | str,
    days: int = 365,
    include_sentiment: bool = False,
    progress_callback: Optional[Callable[[str], None]] = None
) -> dict:
    """
    Generate comprehensive report with multiple CSV files.
    
    Args:
        db: Database instance
        output_dir: Output directory
        days: Number of days to include
        include_sentiment: Include sentiment analysis (requires GPT)
        progress_callback: Progress callback
    
    Returns:
        Dict with paths to generated files
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    results = {
        "collab_report": None,
        "videos": None,
        "comments": None,
        "category_summary": None,
        "region_summary": None,
        "sentiment": None
    }
    
    # 1. Main collab report
    results["collab_report"] = export_to_csv(
        db=db,
        output_path=output_dir / f"collab_report_{timestamp}.csv",
        days=days,
        progress_callback=progress_callback
    )
    
    # 2. Videos detail
    results["videos"] = export_videos_csv(
        db=db,
        output_path=output_dir / f"collab_videos_{timestamp}.csv",
        days=days,
        only_collabs=True,
        progress_callback=progress_callback
    )
    
    # 3. Comments (optional, can be large)
    try:
        results["comments"] = export_comments_csv(
            db=db,
            output_path=output_dir / f"collab_comments_{timestamp}.csv",
            progress_callback=progress_callback
        )
    except Exception as e:
        log(f"Warning: Could not export comments: {e}")
    
    # 4. Category summary
    from .aggregate import get_category_summary, get_region_summary
    
    cat_summary = get_category_summary(db)
    cat_df = pd.DataFrame(cat_summary)
    cat_path = output_dir / f"category_summary_{timestamp}.csv"
    cat_df.to_csv(cat_path, index=False, encoding="utf-8-sig")
    results["category_summary"] = cat_path
    log(f"Exported category summary to {cat_path}")
    
    # 5. Region summary
    region_summary = get_region_summary(db)
    region_df = pd.DataFrame(region_summary)
    region_path = output_dir / f"region_summary_{timestamp}.csv"
    region_df.to_csv(region_path, index=False, encoding="utf-8-sig")
    results["region_summary"] = region_path
    log(f"Exported region summary to {region_path}")
    
    # 6. Sentiment analysis (optional)
    if include_sentiment:
        try:
            results["sentiment"] = _export_sentiment_analysis(
                db=db,
                output_dir=output_dir,
                timestamp=timestamp,
                progress_callback=progress_callback
            )
        except Exception as e:
            log(f"Warning: Could not generate sentiment analysis: {e}")
    
    log(f"\nReport generation complete. Files in {output_dir}")
    
    return results


def _export_sentiment_analysis(
    db: Database,
    output_dir: Path,
    timestamp: str,
    progress_callback: Optional[Callable[[str], None]] = None
) -> Path:
    """
    Generate sentiment analysis report using GPT.
    
    Args:
        db: Database instance
        output_dir: Output directory
        timestamp: Timestamp for filename
        progress_callback: Progress callback
    
    Returns:
        Path to sentiment report
    """
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    log("Generating sentiment analysis (this may take a while)...")
    
    gpt_client = GPTClient()
    
    # Get collab videos with comments
    videos = db.get_collab_videos()
    
    sentiment_data = []
    for video in videos[:20]:  # Limit to avoid excessive API costs
        comments = db.get_comments_for_video(video.video_id)
        if not comments:
            continue
        
        comment_texts = [c.text_original for c in comments if c.text_original]
        
        if len(comment_texts) < 5:
            continue
        
        try:
            sentiment = gpt_client.analyze_comment_sentiment(
                comments=comment_texts,
                video_title=video.title
            )
            
            sentiment_data.append({
                "video_id": video.video_id,
                "title": video.title,
                "collab_partner": video.collab_partner,
                "overall_sentiment": sentiment.overall_sentiment,
                "positive_ratio": sentiment.positive_ratio,
                "negative_ratio": sentiment.negative_ratio,
                "key_topics": "; ".join(sentiment.key_topics),
                "summary": sentiment.summary
            })
            
        except Exception as e:
            log(f"  Skipping {video.video_id}: {e}")
    
    # Write CSV
    output_path = output_dir / f"sentiment_analysis_{timestamp}.csv"
    df = pd.DataFrame(sentiment_data)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    
    log(f"Exported sentiment analysis for {len(sentiment_data)} videos")
    
    return output_path


def upload_to_cloud(
    files: list[Path],
    description: Optional[str] = None,
    progress_callback: Optional[Callable[[str], None]] = None
) -> dict:
    """
    Upload files to cloud storage.
    
    Args:
        files: List of file paths to upload
        description: Optional description
        progress_callback: Progress callback
    
    Returns:
        Dict with upload results
    """
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    cloud = CloudClient()
    
    if not cloud.is_configured:
        log("Cloud client not configured. Skipping upload.")
        return {"success": False, "error": "Cloud client not configured"}
    
    results = {
        "success": True,
        "uploaded": [],
        "failed": []
    }
    
    for file_path in files:
        if not file_path.exists():
            results["failed"].append({"path": str(file_path), "error": "File not found"})
            continue
        
        log(f"Uploading {file_path.name}...")
        
        result = cloud.upload_file(file_path)
        
        if result.success:
            results["uploaded"].append({
                "path": str(file_path),
                "url": result.url
            })
            log(f"  Uploaded: {result.url or 'success'}")
        else:
            results["failed"].append({
                "path": str(file_path),
                "error": result.error
            })
            log(f"  Failed: {result.error}")
    
    if results["failed"]:
        results["success"] = False
    
    return results
