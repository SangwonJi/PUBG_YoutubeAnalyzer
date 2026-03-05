"""
Find and fix misclassified collab videos.
If title contains a known collab partner but classified differently, fix it.
"""

import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

# 주요 콜라보 파트너 목록 (정확한 매칭 필요)
KNOWN_PARTNERS = {
    'BLACKPINK': 'Artist',
    'Peaky Blinders': 'IP',
    'Dragon Ball': 'Anime',
    'Arcane': 'IP',
    'LIONEL MESSI': 'Brand',
    'MESSI': 'Brand',
    'Spider-Man': 'Movie',
    'JUJUTSU KAISEN': 'Anime',
    'Jujutsu Kaisen': 'Anime',
    'Godzilla': 'Movie',
    'Neon Genesis': 'Anime',
    'EVANGELION': 'Anime',
    'McLaren': 'Brand',
    'Lamborghini': 'Brand',
    'Bugatti': 'Brand',
    'Koenigsegg': 'Brand',
    'ZAHA HADID': 'Brand',
    'Alan Walker': 'Artist',
    'BABYMONSTER': 'Artist',
    'Resident Evil': 'Game',
    'Metro Exodus': 'Game',
    'Kaiju No. 8': 'Anime',
    'The Walking Dead': 'IP',
    'Attack on Titan': 'Anime',
}

print("=== Finding Misclassified Videos ===\n")

fixed_count = 0
for partner, category in KNOWN_PARTNERS.items():
    # 제목에 파트너명이 있는데 collab_partner가 다른 경우
    cursor.execute("""
        SELECT video_id, title, collab_partner 
        FROM videos 
        WHERE UPPER(title) LIKE UPPER(?) 
        AND (collab_partner NOT LIKE ? OR collab_partner IS NULL)
    """, (f'%{partner}%', f'%{partner}%'))
    
    results = cursor.fetchall()
    if results:
        print(f"\n[{partner}] - Found {len(results)} misclassified:")
        for video_id, title, current_partner in results:
            print(f"  ID: {video_id}")
            print(f"  Title: {title[:60]}...")
            print(f"  Current Partner: {current_partner} -> Fix to: {partner}")
            
            # 수정
            cursor.execute("""
                UPDATE videos 
                SET collab_partner = ?, collab_category = ?, is_collab = 1, content_type = 'Collab'
                WHERE video_id = ?
            """, (partner, category, video_id))
            fixed_count += 1

conn.commit()

print(f"\n{'=' * 40}")
print(f"Total fixed: {fixed_count} videos")

# 수정 후 확인
print("\n=== Verification After Fix ===")
for partner in ['Peaky Blinders', 'BLACKPINK', 'Dragon Ball', 'MESSI', 'Arcane']:
    cursor.execute(
        "SELECT COUNT(*) FROM videos WHERE collab_partner LIKE ?",
        (f'%{partner}%',)
    )
    count = cursor.fetchone()[0]
    print(f"  {partner}: {count} videos")

conn.close()
print("\nDone!")
