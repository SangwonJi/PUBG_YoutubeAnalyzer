"""
Data fetching pipeline for YouTube videos and comments.
Handles incremental updates and resumable fetching.
"""

from datetime import datetime, timedelta
from typing import Optional, Callable
from tqdm import tqdm

from config import get_config
from db.models import Database, Video
from clients.youtube_client import YouTubeClient


def fetch_videos(
    db: Database,
    days: int = 365,
    channel_handle: str = "@PUBGMOBILE",
    incremental: bool = True,
    progress_callback: Optional[Callable[[str], None]] = None
) -> dict:
    """
    Fetch videos from YouTube channel.
    
    Args:
        db: Database instance
        days: Number of days to look back
        channel_handle: YouTube channel handle
        incremental: If True, only fetch new videos since last fetch
        progress_callback: Optional callback for progress updates
    
    Returns:
        Dict with fetch statistics
    """
    config = get_config()
    youtube = YouTubeClient()
    
    stats = {
        "videos_fetched": 0,
        "videos_new": 0,
        "videos_updated": 0,
        "errors": []
    }
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    log(f"Fetching videos from {channel_handle} (last {days} days)...")
    
    # Determine start date for incremental fetch
    if incremental:
        last_date = db.get_last_video_date()
        if last_date:
            # Start from last fetched video (with some overlap for safety)
            cutoff = max(
                datetime.now() - timedelta(days=days),
                last_date - timedelta(days=1)
            )
            log(f"Incremental mode: fetching videos since {cutoff.date()}")
        else:
            cutoff = datetime.now() - timedelta(days=days)
            log(f"First run: fetching videos since {cutoff.date()}")
    else:
        cutoff = datetime.now() - timedelta(days=days)
        log(f"Full fetch mode: fetching videos since {cutoff.date()}")
    
    # Create fetch progress tracking
    progress_id = db.create_progress("videos")
    
    try:
        for video_batch in youtube.fetch_channel_videos(
            channel_handle=channel_handle,
            days=days,
            progress_callback=log
        ):
            for video in video_batch:
                existing = db.get_video(video.video_id)
                
                if existing:
                    # Update existing video
                    video.created_at = existing.created_at
                    video.is_collab = existing.is_collab
                    video.collab_partner = existing.collab_partner
                    video.collab_category = existing.collab_category
                    video.collab_region = existing.collab_region
                    video.collab_summary = existing.collab_summary
                    video.collab_confidence = existing.collab_confidence
                    video.classification_method = existing.classification_method
                    stats["videos_updated"] += 1
                else:
                    stats["videos_new"] += 1
                
                db.upsert_video(video)
                stats["videos_fetched"] += 1
        
        db.update_progress(progress_id, "completed")
        log(f"Completed: {stats['videos_new']} new, {stats['videos_updated']} updated")
        
    except Exception as e:
        db.update_progress(progress_id, "failed", error_message=str(e))
        stats["errors"].append(str(e))
        log(f"Error during video fetch: {e}")
        raise
    
    return stats


def fetch_comments(
    db: Database,
    max_comments_per_video: int = 200,
    only_collab: bool = False,
    video_ids: Optional[list[str]] = None,
    progress_callback: Optional[Callable[[str], None]] = None
) -> dict:
    """
    Fetch comments for videos.
    
    Args:
        db: Database instance
        max_comments_per_video: Maximum comments to fetch per video
        only_collab: Only fetch comments for collab videos
        video_ids: Specific video IDs to fetch comments for
        progress_callback: Optional callback for progress updates
    
    Returns:
        Dict with fetch statistics
    """
    youtube = YouTubeClient()
    
    stats = {
        "videos_processed": 0,
        "comments_fetched": 0,
        "errors": []
    }
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Get video IDs to process
    if video_ids:
        target_video_ids = video_ids
    elif only_collab:
        videos = db.get_collab_videos()
        target_video_ids = [v.video_id for v in videos]
        log(f"Fetching comments for {len(target_video_ids)} collab videos...")
    else:
        videos = db.get_all_videos()
        target_video_ids = [v.video_id for v in videos]
        log(f"Fetching comments for {len(target_video_ids)} videos...")
    
    # Process each video
    for video_id, comments in youtube.fetch_all_comments(
        target_video_ids,
        max_comments_per_video=max_comments_per_video,
        show_progress=True
    ):
        try:
            # Create progress record
            progress_id = db.create_progress("comments", video_id)
            
            # Save comments
            db.upsert_comments_batch(comments)
            
            stats["videos_processed"] += 1
            stats["comments_fetched"] += len(comments)
            
            db.update_progress(progress_id, "completed")
            
        except Exception as e:
            stats["errors"].append(f"{video_id}: {str(e)}")
            log(f"Error fetching comments for {video_id}: {e}")
    
    log(f"Completed: {stats['comments_fetched']} comments from {stats['videos_processed']} videos")
    
    return stats


def fetch_incremental(
    db: Database,
    days: int = 365,
    channel_handle: str = "@PUBGMOBILE",
    fetch_new_comments: bool = True,
    progress_callback: Optional[Callable[[str], None]] = None
) -> dict:
    """
    Perform incremental fetch of videos and optionally comments.
    
    Args:
        db: Database instance
        days: Number of days to look back
        channel_handle: YouTube channel handle
        fetch_new_comments: Whether to fetch comments for new videos
        progress_callback: Progress callback
    
    Returns:
        Combined statistics
    """
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Get existing video IDs before fetch
    existing_videos = {v.video_id for v in db.get_all_videos()}
    
    # Fetch videos
    video_stats = fetch_videos(
        db=db,
        days=days,
        channel_handle=channel_handle,
        incremental=True,
        progress_callback=progress_callback
    )
    
    # Get new video IDs
    all_videos = {v.video_id for v in db.get_all_videos()}
    new_video_ids = list(all_videos - existing_videos)
    
    comment_stats = {"videos_processed": 0, "comments_fetched": 0, "errors": []}
    
    if fetch_new_comments and new_video_ids:
        log(f"Fetching comments for {len(new_video_ids)} new videos...")
        comment_stats = fetch_comments(
            db=db,
            video_ids=new_video_ids,
            progress_callback=progress_callback
        )
    
    return {
        "videos": video_stats,
        "comments": comment_stats,
        "new_video_ids": new_video_ids
    }


def resume_fetch(
    db: Database,
    task_type: str = "videos",
    progress_callback: Optional[Callable[[str], None]] = None
) -> dict:
    """
    Resume an interrupted fetch operation.
    
    Args:
        db: Database instance
        task_type: Type of task to resume ('videos' or 'comments')
        progress_callback: Progress callback
    
    Returns:
        Statistics of resumed operation
    """
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Get incomplete progress records
    incomplete = db.get_incomplete_progress(task_type)
    
    if not incomplete:
        log(f"No incomplete {task_type} tasks found.")
        return {"resumed": 0}
    
    log(f"Found {len(incomplete)} incomplete {task_type} tasks. Resuming...")
    
    # Resume based on task type
    if task_type == "videos":
        # For videos, just restart the full fetch (it handles deduplication)
        return fetch_videos(
            db=db,
            incremental=True,
            progress_callback=progress_callback
        )
    elif task_type == "comments":
        # For comments, retry the specific video IDs
        video_ids = [p.target_id for p in incomplete if p.target_id]
        if video_ids:
            return fetch_comments(
                db=db,
                video_ids=video_ids,
                progress_callback=progress_callback
            )
    
    return {"resumed": 0}
