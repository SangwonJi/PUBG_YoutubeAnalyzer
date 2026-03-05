"""
Full verification: Compare DB with YouTube channel data.
"""

import sqlite3
import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

load_dotenv()

YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

CHANNEL_ID = 'UCqFodm-E3wfRkKre3hkd1Fg'  # PUBG MOBILE

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("=" * 60)
print("FULL DATA VERIFICATION")
print("=" * 60)

# 1. 채널 총 영상 수 확인
print("\n1. Channel Video Count Check")
print("-" * 40)

try:
    response = youtube.channels().list(
        part='statistics',
        id=CHANNEL_ID
    ).execute()

    if response.get('items'):
        channel_video_count = int(response['items'][0]['statistics']['videoCount'])
        print(f"   YouTube Channel: {channel_video_count} videos")
    else:
        print(f"   Could not fetch channel stats (API response: {response})")
        channel_video_count = None
except Exception as e:
    print(f"   Error fetching channel: {e}")
    channel_video_count = None

cursor.execute("SELECT COUNT(*) FROM videos")
db_count = cursor.fetchone()[0]
print(f"   Database: {db_count} videos")

if channel_video_count:
    diff = channel_video_count - db_count
    if diff > 0:
        print(f"   -> Missing {diff} videos from DB!")
    elif diff < 0:
        print(f"   -> DB has {-diff} extra videos (deleted from YouTube?)")
    else:
        print(f"   -> MATCH!")

# 2. 이상한 파트너명 확인
print("\n2. Suspicious Partner Names")
print("-" * 40)

cursor.execute("""
    SELECT collab_partner, COUNT(*) 
    FROM videos 
    WHERE is_collab = 1 AND collab_partner IS NOT NULL
    GROUP BY collab_partner
    ORDER BY COUNT(*) DESC
""")

suspicious = []
for partner, count in cursor.fetchall():
    # 의심스러운 패턴: 너무 짧거나, 특수문자로 시작하거나, 일반적이지 않은 것
    if (len(partner) < 3 or 
        partner.startswith('-') or 
        partner.startswith('X') or
        'Map' in partner and 'PV' in partner or
        'PUBG' in partner or
        'clusive' in partner.lower() or
        'Live' in partner):
        suspicious.append((partner, count))

if suspicious:
    print("   Found suspicious partner names:")
    for partner, count in suspicious[:20]:
        print(f"   - '{partner}' ({count} videos)")
else:
    print("   No suspicious partner names found!")

# 3. 중복 가능성 있는 파트너 (비슷한 이름)
print("\n3. Potential Duplicate Partners")
print("-" * 40)

cursor.execute("""
    SELECT DISTINCT collab_partner 
    FROM videos 
    WHERE is_collab = 1 AND collab_partner IS NOT NULL
""")
all_partners = [row[0] for row in cursor.fetchall()]

duplicates = []
checked = set()
for p1 in all_partners:
    for p2 in all_partners:
        if p1 != p2 and (p1, p2) not in checked and (p2, p1) not in checked:
            # 한 파트너명이 다른 것에 포함되는 경우
            if p1.upper() in p2.upper() or p2.upper() in p1.upper():
                cursor.execute("SELECT COUNT(*), SUM(view_count) FROM videos WHERE collab_partner = ?", (p1,))
                c1, v1 = cursor.fetchone()
                cursor.execute("SELECT COUNT(*), SUM(view_count) FROM videos WHERE collab_partner = ?", (p2,))
                c2, v2 = cursor.fetchone()
                duplicates.append((p1, c1, v1 or 0, p2, c2, v2 or 0))
                checked.add((p1, p2))

if duplicates:
    print("   Potential duplicates found:")
    for p1, c1, v1, p2, c2, v2 in duplicates[:15]:
        print(f"   - '{p1}' ({c1}) vs '{p2}' ({c2})")
else:
    print("   No duplicates found!")

# 4. 랜덤 샘플 10개 조회수 확인
print("\n4. Random Sample Verification (10 videos)")
print("-" * 40)

cursor.execute("SELECT video_id, title, view_count FROM videos ORDER BY RANDOM() LIMIT 10")
samples = cursor.fetchall()

match_count = 0
for video_id, title, db_views in samples:
    try:
        response = youtube.videos().list(part='statistics', id=video_id).execute()
        if response['items']:
            api_views = int(response['items'][0]['statistics'].get('viewCount', 0))
            # 10% 이내 차이면 OK (시간 경과로 인한 증가)
            if db_views > 0:
                diff_pct = abs(api_views - db_views) / db_views * 100
                status = "OK" if diff_pct < 50 else "OUTDATED"  # 50% 이상 차이나면 outdated
            else:
                status = "OK" if api_views < 1000 else "CHECK"
            
            if status == "OK":
                match_count += 1
            print(f"   [{status}] {title[:35]}...")
            print(f"        DB: {db_views:,} | API: {api_views:,}")
    except Exception as e:
        print(f"   [ERROR] {title[:35]}... - {e}")

print(f"\n   Match rate: {match_count}/10")

conn.close()
print("\n" + "=" * 60)
print("Verification complete!")
