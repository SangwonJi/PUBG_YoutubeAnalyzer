"""
Verify data accuracy by comparing DB with YouTube API.
"""

import sqlite3
import os
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()

YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

def get_video_from_api(video_id):
    """Fetch video details from YouTube API."""
    response = youtube.videos().list(
        part='snippet,statistics',
        id=video_id
    ).execute()
    
    if response['items']:
        item = response['items'][0]
        return {
            'title': item['snippet']['title'],
            'view_count': int(item['statistics'].get('viewCount', 0)),
            'like_count': int(item['statistics'].get('likeCount', 0)),
            'comment_count': int(item['statistics'].get('commentCount', 0))
        }
    return None


def search_collab_videos_api(query, max_results=50):
    """Search for videos on PUBG MOBILE channel."""
    # PUBG MOBILE 채널 ID
    channel_id = 'UCqFodm-E3wfRkKre3hkd1Fg'
    
    response = youtube.search().list(
        part='snippet',
        channelId=channel_id,
        q=query,
        type='video',
        maxResults=max_results
    ).execute()
    
    return [item['id']['videoId'] for item in response.get('items', [])]


print("=" * 60)
print("DATA VERIFICATION REPORT")
print("=" * 60)

# 1. 총 영상 수 확인
cursor.execute("SELECT COUNT(*) FROM videos")
total_db = cursor.fetchone()[0]
print(f"\n1. Total videos in DB: {total_db}")

# 2. 콜라보 파트너별 영상 수 확인
print("\n2. Collab Partner Video Counts:")
print("-" * 40)

test_partners = [
    'BLACKPINK', 
    'Peaky Blinders', 
    'Dragon Ball Super', 
    'Arcane',
    'LIONEL MESSI',
    'Samsung',
    'Spider-Man'
]

for partner in test_partners:
    cursor.execute(
        "SELECT COUNT(*), SUM(view_count) FROM videos WHERE collab_partner LIKE ?",
        (f'%{partner}%',)
    )
    result = cursor.fetchone()
    count = result[0]
    views = result[1] or 0
    print(f"  {partner}: {count} videos, {views:,} views")

# 3. 특정 콜라보 상세 검증 - Peaky Blinders
print("\n3. Detailed Check: Peaky Blinders")
print("-" * 40)

cursor.execute("""
    SELECT video_id, title, view_count, like_count, comment_count 
    FROM videos 
    WHERE collab_partner LIKE '%Peaky%' OR title LIKE '%Peaky%'
""")
peaky_videos = cursor.fetchall()
print(f"  Found in DB: {len(peaky_videos)} videos")

for video_id, title, views, likes, comments in peaky_videos:
    print(f"\n  DB: {title[:50]}...")
    print(f"      Views: {views:,}, Likes: {likes:,}, Comments: {comments:,}")
    
    # API에서 최신 데이터 가져오기
    api_data = get_video_from_api(video_id)
    if api_data:
        print(f"  API: Views: {api_data['view_count']:,}, Likes: {api_data['like_count']:,}, Comments: {api_data['comment_count']:,}")
        
        # 차이 계산
        view_diff = api_data['view_count'] - views
        print(f"  Diff: Views +{view_diff:,} (데이터 수집 이후 증가분)")

# 4. YouTube API로 Peaky Blinders 검색해서 놓친 영상 있는지 확인
print("\n4. Search API for 'Peaky Blinders' on PUBG MOBILE channel:")
print("-" * 40)

api_video_ids = search_collab_videos_api('Peaky Blinders')
print(f"  API search results: {len(api_video_ids)} videos")

# DB에 없는 영상 확인
missing = []
for vid in api_video_ids:
    cursor.execute("SELECT 1 FROM videos WHERE video_id = ?", (vid,))
    if not cursor.fetchone():
        missing.append(vid)
        api_data = get_video_from_api(vid)
        if api_data:
            print(f"  MISSING: {api_data['title'][:60]}")

if not missing:
    print("  All Peaky Blinders videos are in DB!")

# 5. BLACKPINK 검증
print("\n5. Detailed Check: BLACKPINK")
print("-" * 40)

cursor.execute("""
    SELECT video_id, title, view_count 
    FROM videos 
    WHERE collab_partner LIKE '%BLACKPINK%' OR title LIKE '%BLACKPINK%'
    ORDER BY view_count DESC
    LIMIT 5
""")
bp_videos = cursor.fetchall()
print(f"  Found in DB: checking top 5 by views")

for video_id, title, views in bp_videos:
    api_data = get_video_from_api(video_id)
    if api_data:
        status = "OK" if abs(api_data['view_count'] - views) / views < 0.1 else "!!"
        print(f"  {status} {title[:40]}...")
        print(f"      DB: {views:,} | API: {api_data['view_count']:,}")

# 6. API로 BLACKPINK 검색
print("\n6. Search API for 'BLACKPINK' on channel:")
print("-" * 40)

api_video_ids = search_collab_videos_api('BLACKPINK')
print(f"  API search results: {len(api_video_ids)} videos")

cursor.execute("SELECT video_id FROM videos WHERE collab_partner LIKE '%BLACKPINK%'")
db_bp_ids = set(row[0] for row in cursor.fetchall())

missing_bp = []
for vid in api_video_ids:
    if vid not in db_bp_ids:
        cursor.execute("SELECT 1 FROM videos WHERE video_id = ?", (vid,))
        if cursor.fetchone():
            # DB에 있지만 BLACKPINK로 분류 안됨
            cursor.execute("SELECT title, collab_partner FROM videos WHERE video_id = ?", (vid,))
            row = cursor.fetchone()
            print(f"  In DB but not classified as BLACKPINK: {row[0][:50]} (partner: {row[1]})")
        else:
            api_data = get_video_from_api(vid)
            if api_data:
                print(f"  MISSING from DB: {api_data['title'][:60]}")
                missing_bp.append(vid)

if not missing_bp:
    print("  All BLACKPINK videos found in DB!")

conn.close()
print("\n" + "=" * 60)
print("Verification complete!")
