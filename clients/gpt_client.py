"""
OpenAI GPT API Client for collab classification.
Handles classification requests with caching and retry logic.
"""

import json
import re
from pathlib import Path
from typing import Optional
from openai import OpenAI
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
from pydantic import BaseModel

from config import get_config


class CollabClassification(BaseModel):
    """Collab classification result from GPT."""
    is_collab: bool
    partner_name: Optional[str] = None
    category: Optional[str] = None  # IP/Brand/Artist/Game/Anime/Movie/Other
    region: Optional[str] = None    # Global/KR/JP/NA/EU/SEA/Other/Unknown
    one_line_summary: Optional[str] = None
    confidence: float = 0.0


class CommentSentimentSummary(BaseModel):
    """Comment sentiment analysis result."""
    overall_sentiment: str  # positive/negative/neutral/mixed
    positive_ratio: float
    negative_ratio: float
    key_topics: list[str]
    summary: str


class GPTClient:
    """OpenAI GPT client for collab classification and sentiment analysis."""
    
    def __init__(self, api_key: Optional[str] = None):
        config = get_config()
        self.api_key = api_key or config.gpt.api_key
        self.model = config.gpt.model
        self.max_tokens = config.gpt.max_tokens
        self.temperature = config.gpt.temperature
        
        if not self.api_key:
            raise ValueError("GPT API key not provided. Set GPT_API_KEY in .env")
        
        self.client = OpenAI(api_key=self.api_key)
        
        # Load prompts
        self.collab_system_prompt = self._load_prompt("collab_classifier.md", "system")
        self.collab_user_template = self._load_prompt("collab_classifier.md", "user")
    
    def _load_prompt(self, filename: str, section: str) -> str:
        """Load prompt from markdown file."""
        prompt_path = Path(__file__).parent.parent / "prompts" / filename
        
        if not prompt_path.exists():
            # Return default prompts if file doesn't exist
            if filename == "collab_classifier.md":
                if section == "system":
                    return self._default_collab_system_prompt()
                else:
                    return self._default_collab_user_template()
            return ""
        
        content = prompt_path.read_text(encoding="utf-8")
        
        # Parse markdown to extract sections
        # Looking for ## System Prompt and ## User Prompt Template sections
        system_match = re.search(
            r"## System Prompt\s*\n(.*?)(?=\n## |\Z)",
            content,
            re.DOTALL
        )
        user_match = re.search(
            r"## User Prompt Template\s*\n(.*?)(?=\n## |\Z)",
            content,
            re.DOTALL
        )
        
        if section == "system" and system_match:
            return system_match.group(1).strip()
        elif section == "user" and user_match:
            return user_match.group(1).strip()
        
        return ""
    
    def _default_collab_system_prompt(self) -> str:
        """Default system prompt for collab classification."""
        return """You are an expert analyst for PUBG MOBILE content classification.
Your task is to determine if a YouTube video is a collaboration content and identify the collaboration partner.

Classification Guidelines:
1. A "collab" is content featuring partnership with external brands, IPs, artists, games, anime, movies, etc.
2. Regular game updates, tournaments, esports, or community content are NOT collabs.
3. Extract the exact partner name from the title/description when possible.
4. Normalize partner names (e.g., "BLACKPINK" not "black pink").

Categories:
- IP: Intellectual Property collaborations (franchises, characters)
- Brand: Commercial brand partnerships (cars, fashion, tech)
- Artist: Musicians, bands, content creators
- Game: Cross-game collaborations
- Anime: Anime/manga collaborations
- Movie: Movie/TV show tie-ins
- Other: Uncategorized partnerships

Region codes:
- Global: Worldwide release
- KR: Korea-focused
- JP: Japan-focused
- NA: North America
- EU: Europe
- SEA: Southeast Asia
- LATAM: Latin America
- MENA: Middle East/North Africa
- Other/Unknown: Cannot determine

You must respond ONLY with valid JSON in this exact format:
{
  "is_collab": true/false,
  "partner_name": "string or null",
  "category": "IP/Brand/Artist/Game/Anime/Movie/Other",
  "region": "Global/KR/JP/NA/EU/SEA/LATAM/MENA/Other/Unknown",
  "one_line_summary": "Brief description of the collaboration",
  "confidence": 0.0-1.0
}"""
    
    def _default_collab_user_template(self) -> str:
        """Default user prompt template."""
        return """Analyze this PUBG MOBILE YouTube video and classify if it's a collaboration:

Title: {title}

Description:
{description}

Respond with JSON only."""
    
    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(3)
    )
    def classify_collab(
        self,
        title: str,
        description: str,
        use_cache: bool = True,
        db=None
    ) -> CollabClassification:
        """
        Classify if a video is a collaboration using GPT.
        
        Args:
            title: Video title
            description: Video description
            use_cache: Whether to use cached results
            db: Database instance for caching
        
        Returns:
            CollabClassification result
        """
        # Truncate description to avoid token limits
        description_truncated = description[:2000] if description else ""
        
        # Create cache key
        cache_input = f"{title}\n{description_truncated}"
        
        # Check cache
        if use_cache and db:
            cached = db.get_gpt_cache(cache_input)
            if cached:
                try:
                    data = json.loads(cached)
                    return CollabClassification(**data)
                except (json.JSONDecodeError, ValueError):
                    pass
        
        # Build prompt
        user_prompt = self.collab_user_template.format(
            title=title,
            description=description_truncated
        )
        
        # Call GPT API
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": self.collab_system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=self.max_tokens,
            temperature=self.temperature,
            response_format={"type": "json_object"}
        )
        
        # Parse response
        content = response.choices[0].message.content
        try:
            data = json.loads(content)
            result = CollabClassification(**data)
            
            # Cache result
            if db:
                db.set_gpt_cache(cache_input, content, self.model)
            
            return result
        except (json.JSONDecodeError, ValueError) as e:
            # Return non-collab with low confidence on parse error
            return CollabClassification(
                is_collab=False,
                confidence=0.0
            )
    
    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(3)
    )
    def analyze_comment_sentiment(
        self,
        comments: list[str],
        video_title: str = ""
    ) -> CommentSentimentSummary:
        """
        Analyze sentiment of comments using GPT.
        
        Args:
            comments: List of comment texts
            video_title: Video title for context
        
        Returns:
            CommentSentimentSummary
        """
        # Limit and truncate comments
        sample_comments = comments[:50]  # Limit to 50 comments
        comments_text = "\n".join([
            f"- {c[:200]}" for c in sample_comments
        ])
        
        system_prompt = """You are an expert sentiment analyst for social media comments.
Analyze the provided YouTube comments and summarize the overall sentiment.

Respond with JSON only:
{
  "overall_sentiment": "positive/negative/neutral/mixed",
  "positive_ratio": 0.0-1.0,
  "negative_ratio": 0.0-1.0,
  "key_topics": ["topic1", "topic2", ...],
  "summary": "2-3 sentence summary of audience reaction"
}"""
        
        user_prompt = f"""Video: {video_title}

Comments to analyze:
{comments_text}

Analyze the sentiment and key themes."""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=500,
            temperature=0.3,
            response_format={"type": "json_object"}
        )
        
        content = response.choices[0].message.content
        try:
            data = json.loads(content)
            return CommentSentimentSummary(**data)
        except (json.JSONDecodeError, ValueError):
            return CommentSentimentSummary(
                overall_sentiment="unknown",
                positive_ratio=0.0,
                negative_ratio=0.0,
                key_topics=[],
                summary="Unable to analyze comments"
            )
    
    def batch_classify(
        self,
        videos: list[dict],
        db=None,
        show_progress: bool = True
    ) -> dict[str, CollabClassification]:
        """
        Batch classify multiple videos.
        
        Args:
            videos: List of dicts with 'video_id', 'title', 'description'
            db: Database for caching
            show_progress: Show progress bar
        
        Returns:
            Dict mapping video_id to CollabClassification
        """
        from tqdm import tqdm
        
        results = {}
        iterator = tqdm(videos, desc="GPT Classification") if show_progress else videos
        
        for video in iterator:
            classification = self.classify_collab(
                title=video.get("title", ""),
                description=video.get("description", ""),
                db=db
            )
            results[video["video_id"]] = classification
        
        return results
