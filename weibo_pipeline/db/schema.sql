-- ============================================
-- PUBG Weibo Analyzer - Database Schema
-- ============================================

-- 웨이보 게시물 (posts) 테이블
CREATE TABLE IF NOT EXISTS posts (
    mid             TEXT PRIMARY KEY,           -- 微博 status mid (unique post ID)
    bid             TEXT,                        -- 微博 bid (short alphanumeric ID)
    created_at      TEXT,                        -- 게시 시간 (ISO format)
    text_raw        TEXT,                        -- 원본 텍스트 (HTML tags 포함)
    text_clean      TEXT,                        -- 클린 텍스트 (HTML 제거)
    source          TEXT,                        -- 게시 소스 (예: iPhone客户端)
    
    -- Engagement metrics
    reposts_count   INTEGER DEFAULT 0,           -- 转发 (리포스트/공유)
    comments_count  INTEGER DEFAULT 0,           -- 评论 (댓글)
    attitudes_count INTEGER DEFAULT 0,           -- 点赞 (좋아요)
    
    -- Media info
    has_video       INTEGER DEFAULT 0,           -- 비디오 포함 여부 (0/1)
    has_image       INTEGER DEFAULT 0,           -- 이미지 포함 여부 (0/1)
    media_type      TEXT,                        -- video / image / text
    video_url       TEXT,                        -- 비디오 URL (있을 경우)
    thumbnail_url   TEXT,                        -- 썸네일 URL
    video_duration  INTEGER DEFAULT 0,           -- 비디오 길이 (초)
    
    -- Page info (for linked content like videos)
    page_type       TEXT,                        -- 페이지 타입 (video, article 등)
    page_title      TEXT,                        -- 페이지 제목
    page_url        TEXT,                        -- 페이지 URL
    
    -- Collab classification
    is_collab       INTEGER DEFAULT 0,           -- 콜라보 여부 (0/1)
    collab_partner  TEXT,                        -- 콜라보 파트너명
    collab_category TEXT,                        -- 분류 카테고리
    collab_region   TEXT DEFAULT 'CN',           -- 대상 지역
    classified_by   TEXT,                        -- 분류 방식 (rule/gpt/manual)
    classification_confidence REAL DEFAULT 0.0,  -- 분류 확신도 (0.0~1.0)
    
    -- Pipeline metadata
    fetched_at      TEXT,                        -- 최초 수집 시간
    updated_at      TEXT,                        -- 마지막 업데이트 시간
    classified_at   TEXT                         -- 분류 시간
);

-- 댓글 (comments) 테이블
CREATE TABLE IF NOT EXISTS comments (
    comment_id      TEXT PRIMARY KEY,            -- 댓글 고유 ID
    mid             TEXT NOT NULL,               -- 소속 게시물 mid (FK)
    reply_to_id     TEXT,                        -- 대댓글인 경우 원댓글 ID
    
    -- Author info
    author_uid      TEXT,                        -- 작성자 uid
    author_name     TEXT,                        -- 작성자 닉네임
    
    -- Content
    text_raw        TEXT,                        -- 원본 텍스트
    text_clean      TEXT,                        -- 클린 텍스트
    created_at      TEXT,                        -- 작성 시간
    like_count      INTEGER DEFAULT 0,           -- 댓글 좋아요 수
    source          TEXT,                        -- 작성 소스
    
    -- Pipeline metadata
    fetched_at      TEXT,                        -- 수집 시간
    
    FOREIGN KEY (mid) REFERENCES posts(mid)
);

-- 콜라보 집계 (collab_agg) 테이블
CREATE TABLE IF NOT EXISTS collab_agg (
    partner_name        TEXT PRIMARY KEY,
    category            TEXT,
    region              TEXT,
    
    -- Aggregate metrics
    post_count          INTEGER DEFAULT 0,
    total_reposts       INTEGER DEFAULT 0,
    total_comments      INTEGER DEFAULT 0,
    total_attitudes     INTEGER DEFAULT 0,
    total_comment_likes INTEGER DEFAULT 0,
    
    -- Derived metrics
    avg_reposts         REAL DEFAULT 0.0,
    avg_comments        REAL DEFAULT 0.0,
    avg_attitudes       REAL DEFAULT 0.0,
    engagement_rate_pct REAL DEFAULT 0.0,       -- (reposts+comments+attitudes) / post_count 기반
    
    -- Top posts
    top_posts           TEXT,                    -- JSON: top 3 posts by attitudes
    
    -- Date range
    date_range_start    TEXT,
    date_range_end      TEXT,
    aggregated_at       TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);
CREATE INDEX IF NOT EXISTS idx_posts_is_collab ON posts(is_collab);
CREATE INDEX IF NOT EXISTS idx_posts_collab_partner ON posts(collab_partner);
CREATE INDEX IF NOT EXISTS idx_posts_has_video ON posts(has_video);
CREATE INDEX IF NOT EXISTS idx_comments_mid ON comments(mid);
CREATE INDEX IF NOT EXISTS idx_comments_author ON comments(author_uid);
