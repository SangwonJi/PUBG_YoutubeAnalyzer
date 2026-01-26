"""
Configuration management for PUBG Collab Pipeline.
Loads environment variables and provides global settings.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import Optional

# Load .env file
load_dotenv()


class YouTubeConfig(BaseModel):
    """YouTube API configuration."""
    api_key: str = Field(default_factory=lambda: os.getenv("YOUTUBE_API_KEY", ""))
    channel_handle: str = "@PUBGMOBILE"
    channel_url: str = "https://www.youtube.com/@PUBGMOBILE"
    max_results_per_page: int = 50
    comments_per_video: int = 200
    rate_limit_delay: float = 0.1  # seconds between API calls


class GPTConfig(BaseModel):
    """GPT API configuration."""
    api_key: str = Field(default_factory=lambda: os.getenv("GPT_API_KEY", ""))
    model: str = Field(default_factory=lambda: os.getenv("GPT_MODEL", "gpt-4o-mini"))
    max_tokens: int = Field(default_factory=lambda: int(os.getenv("GPT_MAX_TOKENS", "500")))
    temperature: float = 0.3


class CloudConfig(BaseModel):
    """Cloud Storage API configuration."""
    api_key: str = Field(default_factory=lambda: os.getenv("CLOUD_API_KEY", ""))
    upload_url: str = Field(default_factory=lambda: os.getenv("CLOUD_UPLOAD_URL", ""))


class DatabaseConfig(BaseModel):
    """Database configuration."""
    db_path: Path = Field(default_factory=lambda: Path(os.getenv("DB_PATH", "./data/pubg_collab.db")))


class PipelineConfig(BaseModel):
    """Main pipeline configuration."""
    youtube: YouTubeConfig = Field(default_factory=YouTubeConfig)
    gpt: GPTConfig = Field(default_factory=GPTConfig)
    cloud: CloudConfig = Field(default_factory=CloudConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    output_dir: Path = Field(default_factory=lambda: Path(os.getenv("OUTPUT_DIR", "./output")))
    
    # Collab classification settings
    collab_keywords: list[str] = [
        "collab", "collaboration", "x ", " x ", "×", "콜라보", "コラボ",
        "with ", "featuring", "feat.", "ft.", "crossover", "partnership",
        "limited", "exclusive", "special"
    ]
    
    # Partner name normalization mapping
    partner_aliases: dict[str, str] = {
        "black pink": "BLACKPINK",
        "blackpink": "BLACKPINK",
        "블랙핑크": "BLACKPINK",
        "newjeans": "NewJeans",
        "new jeans": "NewJeans",
        "뉴진스": "NewJeans",
        "lamborghini": "Lamborghini",
        "mclaren": "McLaren",
        "bugatti": "Bugatti",
        "koenigsegg": "Koenigsegg",
        "jujutsu kaisen": "Jujutsu Kaisen",
        "주술회전": "Jujutsu Kaisen",
        "dragon ball": "Dragon Ball",
        "dragonball": "Dragon Ball",
        "드래곤볼": "Dragon Ball",
        "neon genesis evangelion": "Evangelion",
        "evangelion": "Evangelion",
        "에반게리온": "Evangelion",
        "attack on titan": "Attack on Titan",
        "진격의 거인": "Attack on Titan",
        "arcane": "Arcane",
        "아케인": "Arcane",
        "the boys": "The Boys",
        "godzilla": "Godzilla",
        "고질라": "Godzilla",
        "kong": "Kong",
        "walking dead": "The Walking Dead",
        "워킹데드": "The Walking Dead",
        "resident evil": "Resident Evil",
        "레지던트 이블": "Resident Evil",
        "metro": "Metro Exodus",
        "alan walker": "Alan Walker",
        "앨런 워커": "Alan Walker",
    }
    
    # Category definitions
    collab_categories: list[str] = [
        "IP",       # Intellectual Property (games, anime, movies, etc.)
        "Brand",    # Commercial brands (cars, fashion, etc.)
        "Artist",   # Musicians, artists
        "Game",     # Other games
        "Anime",    # Anime/Manga
        "Movie",    # Movies/TV Shows
        "Other"     # Uncategorized
    ]
    
    # Region definitions
    regions: list[str] = [
        "Global", "KR", "JP", "NA", "EU", "SEA", "LATAM", "MENA", "Other", "Unknown"
    ]


# Global config instance
config = PipelineConfig()


def get_config() -> PipelineConfig:
    """Get the global configuration instance."""
    return config


def validate_config() -> dict[str, bool]:
    """Validate that required API keys are set."""
    return {
        "youtube_api": bool(config.youtube.api_key),
        "gpt_api": bool(config.gpt.api_key),
        "cloud_api": bool(config.cloud.api_key and config.cloud.upload_url),
    }
