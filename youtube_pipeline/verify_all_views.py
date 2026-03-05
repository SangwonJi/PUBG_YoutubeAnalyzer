"""
Verify ALL video view counts against YouTube API.
Process in batches of 50 (API limit per request).
"""

import sqlite3
import os
import sys
import time
from dotenv import load_dotenv
from googleapiclient.discovery import build
from tqdm import tqdm

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
load_dotenv()

YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("=" * 70)
print("FULL VIEW COUNT VERIFICATION")
print("=" * 70)

# 모든 영상 가져오기
cursor.execute("SELECT video_id, title, view_count, like_count, comment_count FROM videos")
all_videos = cursor.fetchall()
print(f"\nTotal videos to verify: {len(all_videos)}")

# 배치 처리 (50개씩 - YouTube API 제한)
BATCH_SIZE = 50
batches = [all_videos[i:i+BATCH_SIZE] for i in range(0, len(all_videos), BATCH_SIZE)]

total_checked = 0
total_matched = 0
total_updated = 0
total_deleted = 0
errors = []
large_discrepancies = []

print(f"Processing {len(batches)} batches...\n")

for batch_idx, batch in enumerate(tqdm(batches, desc="Verifying")):
    video_ids = [v[0] for v in batch]
    db_data = {v[0]: {'title': v[1], 'views': v[2], 'likes': v[3], 'comments': v[4]} for v in batch}
    
    try:
        # API 호출
        response = youtube.videos().list(
            part='statistics',
            id=','.join(video_ids)
        ).execute()
        
        api_data = {}
        for item in response.get('items', []):
            vid = item['id']
            stats = item['statistics']
            api_data[vid] = {
                'views': int(stats.get('viewCount', 0)),
                'likes': int(stats.get('likeCount', 0)),
                'comments': int(stats.get('commentCount', 0))
            }
        
        # 비교 및 업데이트
        for video_id in video_ids:
            total_checked += 1
            
            if video_id not in api_data:
                # 영상이 삭제됨
                total_deleted += 1
                errors.append((video_id, db_data[video_id]['title'][:40], 'DELETED'))
                continue
            
            db = db_data[video_id]
            api = api_data[video_id]
            
            # 조회수 비교 (DB 값이 0이 아닌 경우)
            if db['views'] and db['views'] > 0:
                diff_pct = abs(api['views'] - db['views']) / db['views'] * 100
                
                if diff_pct < 5:
                    total_matched += 1
                else:
                    # 업데이트 필요
                    cursor.execute("""
                        UPDATE videos 
                        SET view_count = ?, like_count = ?, comment_count = ?
                        WHERE video_id = ?
                    """, (api['views'], api['likes'], api['comments'], video_id))
                    total_updated += 1
                    
                    if diff_pct > 50:
                        large_discrepancies.append((
                            video_id, 
                            db_data[video_id]['title'][:30],
                            db['views'],
                            api['views'],
                            diff_pct
                        ))
            else:
                # DB에 0이면 API 값으로 업데이트
                cursor.execute("""
                    UPDATE videos 
                    SET view_count = ?, like_count = ?, comment_count = ?
                    WHERE video_id = ?
                """, (api['views'], api['likes'], api['comments'], video_id))
                total_updated += 1
        
        # 커밋 (배치당)
        if batch_idx % 10 == 0:
            conn.commit()
            
    except Exception as e:
        errors.append((batch_idx, f"Batch error: {str(e)[:50]}", 'API_ERROR'))
    
    # Rate limiting
    time.sleep(0.1)

conn.commit()

# 결과 출력
print("\n" + "=" * 70)
print("VERIFICATION RESULTS")
print("=" * 70)
print(f"  Total checked: {total_checked}")
print(f"  Matched (within 5%): {total_matched} ({total_matched/total_checked*100:.1f}%)")
print(f"  Updated: {total_updated}")
print(f"  Deleted on YouTube: {total_deleted}")
print(f"  Errors: {len(errors)}")

if large_discrepancies:
    print(f"\n  Large discrepancies (>50% diff): {len(large_discrepancies)}")
    for vid, title, db_v, api_v, pct in large_discrepancies[:10]:
        print(f"    - {title}... DB:{db_v:,} -> API:{api_v:,} ({pct:.0f}%)")

if errors:
    print(f"\n  Errors/Deleted videos:")
    for item in errors[:10]:
        print(f"    - {item}")

# 최종 통계
cursor.execute("SELECT SUM(view_count), SUM(like_count), SUM(comment_count) FROM videos")
totals = cursor.fetchone()
print(f"\n  Final totals:")
print(f"    Total views: {totals[0]:,}")
print(f"    Total likes: {totals[1]:,}")
print(f"    Total comments: {totals[2]:,}")

conn.close()
print("\nVerification complete!")
