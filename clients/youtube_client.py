"""
YouTube Data API v3 Client.
Handles video and comment fetching with rate limiting and retry logic.
"""

import time
from datetime import datetime, timedelta
from typing import Optional, List, Generator, Tuple
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
from tqdm import tqdm

from config import get_config
from db.models import Video, Comment


class YouTubeClient:
    """YouTube Data API v3 client with rate limiting and retry logic."""
    
    def __init__(self, api_key: Optional[str] = None):
        config = get_config()
        self.api_key = api_key or config.youtube.api_key
        self.max_results = config.youtube.max_results_per_page
        self.comments_per_video = config.youtube.comments_per_video
        self.rate_limit_delay = config.youtube.rate_limit_delay
        
        if not self.api_key:
            raise ValueError("YouTube API key not provided. Set YOUTUBE_API_KEY in .env")
        
        self.youtube = build("youtube", "v3", developerKey=self.api_key)
        self._channel_id_cache: dict[str, str] = {}
        self._uploads_playlist_cache: dict[str, str] = {}
    
    @retry(
        retry=retry_if_exception_type(HttpError),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5)
    )
    def _api_call(self, request):
        """Execute API request with retry logic."""
        time.sleep(self.rate_limit_delay)
        return request.execute()
    
    def resolve_channel_handle(self, handle: str) -> str:
        """
        Resolve channel handle (@PUBGMOBILE) to channel ID.
        
        Args:
            handle: Channel handle (e.g., "@PUBGMOBILE")
        
        Returns:
            Channel ID (e.g., "UCX...")
        """
        if handle in self._channel_id_cache:
            return self._channel_id_cache[handle]
        
        # Remove @ if present
        handle_clean = handle.lstrip("@")
        
        # Use search or channels.list with forHandle parameter
        request = self.youtube.channels().list(
            part="id,snippet,contentDetails",
            forHandle=handle_clean
        )
        response = self._api_call(request)
        
        if not response.get("items"):
            raise ValueError(f"Channel not found: {handle}")
        
        channel = response["items"][0]
        channel_id = channel["id"]
        
        self._channel_id_cache[handle] = channel_id
        
        # Also cache the uploads playlist ID
        uploads_playlist = channel["contentDetails"]["relatedPlaylists"]["uploads"]
        self._uploads_playlist_cache[channel_id] = uploads_playlist
        
        return channel_id
    
    def get_uploads_playlist_id(self, channel_id: str) -> str:
        """
        Get the uploads playlist ID for a channel.
        
        Args:
            channel_id: YouTube channel ID
        
        Returns:
            Uploads playlist ID
        """
        if channel_id in self._uploads_playlist_cache:
            return self._uploads_playlist_cache[channel_id]
        
        request = self.youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = self._api_call(request)
        
        if not response.get("items"):
            raise ValueError(f"Channel not found: {channel_id}")
        
        uploads_playlist = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        self._uploads_playlist_cache[channel_id] = uploads_playlist
        
        return uploads_playlist
    
    def get_video_ids_from_playlist(
        self,
        playlist_id: str,
        published_after: Optional[datetime] = None,
        max_videos: Optional[int] = None,
        page_token: Optional[str] = None
    ) -> Generator[Tuple[List[str], Optional[str]], None, None]:
        """
        Get video IDs from a playlist with pagination.
        Yields batches of video IDs along with the next page token.
        
        Args:
            playlist_id: YouTube playlist ID (uploads playlist)
            published_after: Filter videos published after this date
            max_videos: Maximum number of videos to fetch
            page_token: Resume from this page token
        
        Yields:
            Tuple of (list of video IDs, next page token)
        """
        total_fetched = 0
        current_token = page_token
        
        while True:
            request = self.youtube.playlistItems().list(
                part="contentDetails,snippet",
                playlistId=playlist_id,
                maxResults=self.max_results,
                pageToken=current_token
            )
            response = self._api_call(request)
            
            video_ids = []
            for item in response.get("items", []):
                published_at_str = item["contentDetails"].get("videoPublishedAt")
                if published_at_str:
                    published_at = datetime.fromisoformat(
                        published_at_str.replace("Z", "+00:00")
                    )
                    
                    # Filter by date if specified
                    if published_after and published_at < published_after:
                        # Playlist is in reverse chronological order
                        # Once we hit videos older than the cutoff, stop
                        yield video_ids, None
                        return
                    
                    video_ids.append(item["contentDetails"]["videoId"])
            
            total_fetched += len(video_ids)
            current_token = response.get("nextPageToken")
            
            yield video_ids, current_token
            
            if not current_token:
                break
            
            if max_videos and total_fetched >= max_videos:
                break
    
    def get_videos_details(
        self,
        video_ids: List[str],
        channel_id: Optional[str] = None,
        channel_name: Optional[str] = None
    ) -> List[Video]:
        """
        Get detailed video information for a batch of video IDs.
        Calls videos.list with batch of up to 50 IDs.
        
        Args:
            video_ids: List of video IDs (max 50)
            channel_id: Optional channel ID to attach
            channel_name: Optional channel name to attach
        
        Returns:
            List of Video models
        """
        if not video_ids:
            return []
        
        # YouTube API allows max 50 IDs per request
        videos = []
        
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            
            request = self.youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(batch_ids)
            )
            response = self._api_call(request)
            
            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                statistics = item.get("statistics", {})
                content_details = item.get("contentDetails", {})
                
                published_at_str = snippet.get("publishedAt", "")
                published_at = datetime.fromisoformat(
                    published_at_str.replace("Z", "+00:00")
                ) if published_at_str else datetime.now()
                
                video = Video(
                    video_id=item["id"],
                    title=snippet.get("title", ""),
                    description=snippet.get("description", ""),
                    published_at=published_at,
                    duration=content_details.get("duration"),
                    channel_id=channel_id or snippet.get("channelId"),
                    channel_name=channel_name or snippet.get("channelTitle"),
                    view_count=int(statistics.get("viewCount", 0)),
                    like_count=int(statistics.get("likeCount", 0)),
                    comment_count=int(statistics.get("commentCount", 0)),
                    last_fetched_at=datetime.now()
                )
                videos.append(video)
        
        return videos
    
    def get_comments_for_video(
        self,
        video_id: str,
        max_comments: Optional[int] = None,
        order: str = "relevance",
        page_token: Optional[str] = None
    ) -> Generator[Tuple[List[Comment], Optional[str]], None, None]:
        """
        Get comments for a video with pagination.
        
        Args:
            video_id: YouTube video ID
            max_comments: Maximum number of comments to fetch
            order: Sort order ("relevance" or "time")
            page_token: Resume from this page token
        
        Yields:
            Tuple of (list of Comments, next page token)
        """
        max_comments = max_comments or self.comments_per_video
        total_fetched = 0
        current_token = page_token
        
        while total_fetched < max_comments:
            try:
                request = self.youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=min(100, max_comments - total_fetched),
                    order=order,
                    pageToken=current_token
                )
                response = self._api_call(request)
            except HttpError as e:
                if e.resp.status == 403:
                    # Comments disabled for this video
                    yield [], None
                    return
                raise
            
            comments = []
            for item in response.get("items", []):
                # Top-level comment
                top_comment = item["snippet"]["topLevelComment"]["snippet"]
                published_at_str = top_comment.get("publishedAt", "")
                published_at = datetime.fromisoformat(
                    published_at_str.replace("Z", "+00:00")
                ) if published_at_str else None
                
                comment = Comment(
                    comment_id=item["snippet"]["topLevelComment"]["id"],
                    video_id=video_id,
                    author_name=top_comment.get("authorDisplayName"),
                    author_channel_id=top_comment.get("authorChannelId", {}).get("value"),
                    text_original=top_comment.get("textOriginal"),
                    text_display=top_comment.get("textDisplay"),
                    published_at=published_at,
                    like_count=int(top_comment.get("likeCount", 0)),
                    parent_id=None,
                    is_reply=False
                )
                comments.append(comment)
                
                # Replies (if any)
                replies = item.get("replies", {}).get("comments", [])
                for reply_item in replies:
                    reply_snippet = reply_item["snippet"]
                    reply_published_at_str = reply_snippet.get("publishedAt", "")
                    reply_published_at = datetime.fromisoformat(
                        reply_published_at_str.replace("Z", "+00:00")
                    ) if reply_published_at_str else None
                    
                    reply = Comment(
                        comment_id=reply_item["id"],
                        video_id=video_id,
                        author_name=reply_snippet.get("authorDisplayName"),
                        author_channel_id=reply_snippet.get("authorChannelId", {}).get("value"),
                        text_original=reply_snippet.get("textOriginal"),
                        text_display=reply_snippet.get("textDisplay"),
                        published_at=reply_published_at,
                        like_count=int(reply_snippet.get("likeCount", 0)),
                        parent_id=reply_snippet.get("parentId"),
                        is_reply=True
                    )
                    comments.append(reply)
            
            total_fetched += len([c for c in comments if not c.is_reply])
            current_token = response.get("nextPageToken")
            
            yield comments, current_token
            
            if not current_token:
                break
    
    def fetch_channel_videos(
        self,
        channel_handle: str = "@PUBGMOBILE",
        days: int = 365,
        progress_callback=None
    ) -> Generator[List[Video], None, None]:
        """
        Fetch all videos from a channel within the specified date range.
        
        Args:
            channel_handle: Channel handle (e.g., "@PUBGMOBILE")
            days: Number of days to look back
            progress_callback: Optional callback for progress updates
        
        Yields:
            Batches of Video models
        """
        # Resolve channel handle to ID
        channel_id = self.resolve_channel_handle(channel_handle)
        uploads_playlist_id = self.get_uploads_playlist_id(channel_id)
        
        # Calculate date cutoff
        published_after = datetime.now() - timedelta(days=days)
        published_after = published_after.replace(tzinfo=None)
        
        # Get channel name
        request = self.youtube.channels().list(
            part="snippet",
            id=channel_id
        )
        response = self._api_call(request)
        channel_name = response["items"][0]["snippet"]["title"] if response.get("items") else None
        
        # Fetch video IDs
        all_video_ids = []
        for video_ids, next_token in self.get_video_ids_from_playlist(
            uploads_playlist_id,
            published_after=published_after
        ):
            all_video_ids.extend(video_ids)
            if progress_callback:
                progress_callback(f"Fetched {len(all_video_ids)} video IDs...")
        
        # Fetch video details in batches
        for i in range(0, len(all_video_ids), 50):
            batch_ids = all_video_ids[i:i+50]
            videos = self.get_videos_details(batch_ids, channel_id, channel_name)
            
            if progress_callback:
                progress_callback(f"Fetched details for {i + len(videos)}/{len(all_video_ids)} videos")
            
            yield videos
    
    def fetch_all_comments(
        self,
        video_ids: List[str],
        max_comments_per_video: Optional[int] = None,
        show_progress: bool = True
    ) -> Generator[Tuple[str, List[Comment]], None, None]:
        """
        Fetch comments for multiple videos.
        
        Args:
            video_ids: List of video IDs
            max_comments_per_video: Max comments per video
            show_progress: Show progress bar
        
        Yields:
            Tuple of (video_id, list of Comments)
        """
        max_comments = max_comments_per_video or self.comments_per_video
        
        iterator = tqdm(video_ids, desc="Fetching comments") if show_progress else video_ids
        
        for video_id in iterator:
            all_comments = []
            for comments, _ in self.get_comments_for_video(video_id, max_comments=max_comments):
                all_comments.extend(comments)
            
            yield video_id, all_comments
