-- PUBG Collab Pipeline Database Schema
-- SQLite3 compatible

-- Videos table: stores video metadata and collab classification
CREATE TABLE IF NOT EXISTS videos (
    video_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    published_at TIMESTAMP NOT NULL,
    duration TEXT,  -- ISO 8601 duration format (e.g., PT5M30S)
    channel_id TEXT,
    channel_name TEXT,
    
    -- Statistics (updated periodically)
    view_count INTEGER DEFAULT 0,
    like_count INTEGER DEFAULT 0,
    comment_count INTEGER DEFAULT 0,
    
    -- Collab classification
    is_collab BOOLEAN DEFAULT FALSE,
    collab_partner TEXT,
    collab_category TEXT,  -- IP/Brand/Artist/Game/Anime/Movie/Other
    collab_region TEXT,    -- Global/KR/JP/NA/EU/SEA/Other/Unknown
    collab_summary TEXT,
    collab_confidence REAL DEFAULT 0.0,
    classification_method TEXT,  -- 'rule' or 'gpt'
    
    -- Metadata
    last_fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comments table: stores video comments
CREATE TABLE IF NOT EXISTS comments (
    comment_id TEXT PRIMARY KEY,
    video_id TEXT NOT NULL,
    author_name TEXT,
    author_channel_id TEXT,
    text_original TEXT,
    text_display TEXT,
    published_at TIMESTAMP,
    like_count INTEGER DEFAULT 0,
    parent_id TEXT,  -- NULL for top-level comments
    is_reply BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
);

-- Collab aggregation table: pre-computed metrics per partner
CREATE TABLE IF NOT EXISTS collab_agg (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partner_name TEXT NOT NULL,
    category TEXT,
    region TEXT,
    date_range_start DATE NOT NULL,
    date_range_end DATE NOT NULL,
    
    -- Aggregated metrics
    video_count INTEGER DEFAULT 0,
    total_views INTEGER DEFAULT 0,
    total_video_likes INTEGER DEFAULT 0,
    total_comments INTEGER DEFAULT 0,
    total_comment_likes INTEGER DEFAULT 0,
    comment_likes_partial BOOLEAN DEFAULT FALSE,  -- TRUE if comment likes are partial
    
    -- Calculated rates
    avg_views REAL DEFAULT 0.0,
    avg_video_likes REAL DEFAULT 0.0,
    like_rate REAL DEFAULT 0.0,      -- total_video_likes / total_views
    comment_rate REAL DEFAULT 0.0,   -- total_comments / total_views
    
    -- Top videos (JSON array of video_id)
    top_videos_json TEXT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(partner_name, date_range_start, date_range_end)
);

-- Fetch progress table: tracks incremental fetching
CREATE TABLE IF NOT EXISTS fetch_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_type TEXT NOT NULL,  -- 'videos', 'comments', 'classify'
    target_id TEXT,           -- video_id or NULL for videos task
    status TEXT NOT NULL,     -- 'pending', 'in_progress', 'completed', 'failed'
    page_token TEXT,          -- for resuming pagination
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- GPT cache table: caches GPT classification results
CREATE TABLE IF NOT EXISTS gpt_cache (
    cache_key TEXT PRIMARY KEY,  -- hash of input
    input_text TEXT NOT NULL,
    output_json TEXT NOT NULL,
    model TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_videos_published_at ON videos(published_at);
CREATE INDEX IF NOT EXISTS idx_videos_is_collab ON videos(is_collab);
CREATE INDEX IF NOT EXISTS idx_videos_collab_partner ON videos(collab_partner);
CREATE INDEX IF NOT EXISTS idx_comments_video_id ON comments(video_id);
CREATE INDEX IF NOT EXISTS idx_comments_published_at ON comments(published_at);
CREATE INDEX IF NOT EXISTS idx_collab_agg_partner ON collab_agg(partner_name);
CREATE INDEX IF NOT EXISTS idx_fetch_progress_status ON fetch_progress(status);
