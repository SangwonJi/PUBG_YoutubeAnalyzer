"""
Configuration management for PUBG Weibo Analyzer
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────
# Weibo Target Account
# ──────────────────────────────────────────────
WEIBO_UID = os.getenv("WEIBO_UID", "7095404909")  # 和平精英 official
WEIBO_ACCOUNT_NAME = os.getenv("WEIBO_ACCOUNT_NAME", "和平精英")

# ──────────────────────────────────────────────
# Weibo Scraping Settings
# ──────────────────────────────────────────────
WEIBO_BASE_URL = "https://m.weibo.cn"
WEIBO_COOKIE = os.getenv("WEIBO_COOKIE", "")  # Optional: for higher rate limits
WEIBO_REQUEST_DELAY = float(os.getenv("WEIBO_REQUEST_DELAY", "3.0"))  # seconds between requests
WEIBO_MAX_COMMENTS_PER_POST = int(os.getenv("WEIBO_MAX_COMMENTS_PER_POST", "200"))
WEIBO_MAX_RETRIES = int(os.getenv("WEIBO_MAX_RETRIES", "5"))

# ──────────────────────────────────────────────
# GPT Settings (for collab classification)
# ──────────────────────────────────────────────
GPT_API_KEY = os.getenv("GPT_API_KEY", "")
GPT_MODEL = os.getenv("GPT_MODEL", "gpt-4o-mini")

# ──────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────
DB_PATH = Path(os.getenv("DB_PATH", "./data/pubg_weibo.db"))

# ──────────────────────────────────────────────
# Output
# ──────────────────────────────────────────────
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./output"))

# ──────────────────────────────────────────────
# Cloud Upload (optional)
# ──────────────────────────────────────────────
CLOUD_API_KEY = os.getenv("CLOUD_API_KEY", "")
CLOUD_UPLOAD_URL = os.getenv("CLOUD_UPLOAD_URL", "")
