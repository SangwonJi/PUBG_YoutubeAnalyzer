"""
Aggregation pipeline for collab metrics.
Computes per-partner statistics and rankings.
"""

import json
from datetime import datetime, date, timedelta
from typing import Optional, Callable, List
from collections import defaultdict

from db.models import Database, Video, CollabAgg


def aggregate_collabs(
    db: Database,
    days: Optional[int] = 365,
    source_channel: Optional[str] = None,
    progress_callback: Optional[Callable[[str], None]] = None
) -> dict:
    """
    Aggregate collab metrics by partner.
    
    Args:
        db: Database instance
        days: Number of days to aggregate (None = all)
        source_channel: Filter by source channel ('pubgm' or 'freefire')
        progress_callback: Progress callback
    
    Returns:
        Aggregation statistics
    """
    stats = {
        "partners_processed": 0,
        "total_videos": 0,
        "total_views": 0,
        "errors": []
    }
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Calculate date range
    end_date = date.today()
    if days is None:
        start_date = date(2000, 1, 1)  # Far past date for "all data"
        log(f"Aggregating ALL collab metrics up to {end_date}...")
    else:
        start_date = end_date - timedelta(days=days)
        log(f"Aggregating collab metrics from {start_date} to {end_date}...")
    
    # Get collab videos
    if source_channel:
        videos = db.get_collab_videos_by_channel(source_channel, days=days)
    else:
        videos = db.get_collab_videos(days=days)
    
    if not videos:
        log("No collab videos found in date range.")
        return stats
    
    stats["total_videos"] = len(videos)
    log(f"Found {len(videos)} collab videos")
    
    # Group videos by partner
    partner_videos: dict[str, list[Video]] = defaultdict(list)
    for video in videos:
        partner = video.collab_partner or "Unknown"
        partner_videos[partner].append(video)
    
    log(f"Found {len(partner_videos)} unique partners")
    
    # Calculate metrics for each partner
    for partner, vids in partner_videos.items():
        try:
            agg = _calculate_partner_metrics(
                db=db,
                partner_name=partner,
                videos=vids,
                start_date=start_date,
                end_date=end_date,
                source_channel=source_channel or vids[0].source_channel
            )
            
            db.upsert_collab_agg(agg)
            stats["partners_processed"] += 1
            stats["total_views"] += agg.total_views
            
        except Exception as e:
            stats["errors"].append(f"{partner}: {str(e)}")
            log(f"Error processing partner {partner}: {e}")
    
    log(f"Aggregation complete: {stats['partners_processed']} partners, {stats['total_views']:,} total views")
    
    return stats


def _calculate_partner_metrics(
    db: Database,
    partner_name: str,
    videos: list[Video],
    start_date: date,
    end_date: date,
    source_channel: str = "pubgm"
) -> CollabAgg:
    """
    Calculate aggregated metrics for a partner.
    
    Args:
        db: Database instance
        partner_name: Partner name
        videos: List of videos for this partner
        start_date: Start of date range
        end_date: End of date range
        source_channel: Source channel identifier
    
    Returns:
        CollabAgg model with calculated metrics
    """
    # Basic counts
    video_count = len(videos)
    total_views = sum(v.view_count for v in videos)
    total_video_likes = sum(v.like_count for v in videos)
    total_comments = sum(v.comment_count for v in videos)
    
    # Get comment likes (partial - only from collected comments)
    total_comment_likes = 0
    comment_likes_partial = False
    
    for video in videos:
        comment_stats = db.get_comment_stats_for_video(video.video_id)
        total_comment_likes += comment_stats["total_likes"]
        
        # Mark as partial if we haven't collected all comments
        if comment_stats["count"] < video.comment_count:
            comment_likes_partial = True
    
    # Calculate rates
    avg_views = total_views / video_count if video_count > 0 else 0
    avg_video_likes = total_video_likes / video_count if video_count > 0 else 0
    like_rate = total_video_likes / total_views if total_views > 0 else 0
    comment_rate = total_comments / total_views if total_views > 0 else 0
    
    # Get top 3 videos by view count
    sorted_videos = sorted(videos, key=lambda v: v.view_count, reverse=True)[:3]
    top_videos = [
        {
            "video_id": v.video_id,
            "title": v.title,
            "view_count": v.view_count,
            "like_count": v.like_count
        }
        for v in sorted_videos
    ]
    
    # Determine category and region (most common among videos)
    categories = [v.collab_category for v in videos if v.collab_category]
    regions = [v.collab_region for v in videos if v.collab_region]
    
    category = max(set(categories), key=categories.count) if categories else "Other"
    region = max(set(regions), key=regions.count) if regions else "Unknown"
    
    return CollabAgg(
        partner_name=partner_name,
        category=category,
        region=region,
        source_channel=source_channel,
        date_range_start=start_date,
        date_range_end=end_date,
        video_count=video_count,
        total_views=total_views,
        total_video_likes=total_video_likes,
        total_comments=total_comments,
        total_comment_likes=total_comment_likes,
        comment_likes_partial=comment_likes_partial,
        avg_views=avg_views,
        avg_video_likes=avg_video_likes,
        like_rate=like_rate,
        comment_rate=comment_rate,
        top_videos_json=json.dumps(top_videos, ensure_ascii=False)
    )


def get_partner_rankings(
    db: Database,
    metric: str = "total_views",
    limit: int = 20
) -> list[dict]:
    """
    Get partner rankings by specified metric.
    
    Args:
        db: Database instance
        metric: Metric to rank by (total_views, video_count, like_rate, etc.)
        limit: Maximum number of results
    
    Returns:
        List of partner rankings
    """
    aggs = db.get_all_collab_aggs()
    
    # Sort by metric
    if hasattr(aggs[0] if aggs else None, metric):
        aggs = sorted(aggs, key=lambda a: getattr(a, metric, 0), reverse=True)
    
    rankings = []
    for i, agg in enumerate(aggs[:limit], 1):
        rankings.append({
            "rank": i,
            "partner_name": agg.partner_name,
            "category": agg.category,
            "region": agg.region,
            "video_count": agg.video_count,
            "total_views": agg.total_views,
            "total_video_likes": agg.total_video_likes,
            "total_comments": agg.total_comments,
            "avg_views": round(agg.avg_views, 2),
            "like_rate": round(agg.like_rate * 100, 4),
            "comment_rate": round(agg.comment_rate * 100, 4),
            "top_videos": agg.top_videos
        })
    
    return rankings


def get_category_summary(db: Database) -> list[dict]:
    """
    Get summary statistics by category.
    
    Args:
        db: Database instance
    
    Returns:
        List of category summaries
    """
    aggs = db.get_all_collab_aggs()
    
    category_data: dict[str, dict] = defaultdict(lambda: {
        "partner_count": 0,
        "video_count": 0,
        "total_views": 0,
        "total_likes": 0,
        "total_comments": 0
    })
    
    for agg in aggs:
        cat = agg.category or "Other"
        category_data[cat]["partner_count"] += 1
        category_data[cat]["video_count"] += agg.video_count
        category_data[cat]["total_views"] += agg.total_views
        category_data[cat]["total_likes"] += agg.total_video_likes
        category_data[cat]["total_comments"] += agg.total_comments
    
    summaries = []
    for category, data in sorted(category_data.items(), key=lambda x: x[1]["total_views"], reverse=True):
        avg_views = data["total_views"] / data["video_count"] if data["video_count"] > 0 else 0
        like_rate = data["total_likes"] / data["total_views"] if data["total_views"] > 0 else 0
        
        summaries.append({
            "category": category,
            "partner_count": data["partner_count"],
            "video_count": data["video_count"],
            "total_views": data["total_views"],
            "avg_views_per_video": round(avg_views, 2),
            "like_rate": round(like_rate * 100, 4)
        })
    
    return summaries


def get_region_summary(db: Database) -> list[dict]:
    """
    Get summary statistics by region.
    
    Args:
        db: Database instance
    
    Returns:
        List of region summaries
    """
    aggs = db.get_all_collab_aggs()
    
    region_data: dict[str, dict] = defaultdict(lambda: {
        "partner_count": 0,
        "video_count": 0,
        "total_views": 0,
        "total_likes": 0
    })
    
    for agg in aggs:
        reg = agg.region or "Unknown"
        region_data[reg]["partner_count"] += 1
        region_data[reg]["video_count"] += agg.video_count
        region_data[reg]["total_views"] += agg.total_views
        region_data[reg]["total_likes"] += agg.total_video_likes
    
    summaries = []
    for region, data in sorted(region_data.items(), key=lambda x: x[1]["total_views"], reverse=True):
        summaries.append({
            "region": region,
            "partner_count": data["partner_count"],
            "video_count": data["video_count"],
            "total_views": data["total_views"],
            "total_likes": data["total_likes"]
        })
    
    return summaries
