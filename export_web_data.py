"""
Export data for web dashboard.
Creates data.json (collab partners) and others.json (non-collab content types).
"""

import sqlite3
import json
import sys
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("Exporting data for web dashboard...\n")

# 1. Collab Partners Data
print("[1] Exporting collab partners...")

cursor.execute("""
    SELECT collab_partner, collab_category, 
           COUNT(*) as video_count,
           SUM(view_count) as total_views,
           SUM(like_count) as total_likes,
           SUM(comment_count) as total_comments,
           MIN(published_at) as first_collab,
           MAX(published_at) as last_collab
    FROM videos 
    WHERE is_collab = 1 AND collab_partner IS NOT NULL
    GROUP BY collab_partner 
    ORDER BY total_views DESC
""")

partners = []
for row in cursor.fetchall():
    partner_name = row[0]
    
    # Get videos for this partner
    cursor.execute("""
        SELECT video_id, title, published_at, view_count, like_count, comment_count
        FROM videos 
        WHERE collab_partner = ?
        ORDER BY view_count DESC
    """, (partner_name,))
    
    videos = []
    for v in cursor.fetchall():
        videos.append({
            'video_id': v[0],
            'title': v[1],
            'published_at': v[2][:10] if v[2] else '',
            'view_count': v[3] or 0,
            'like_count': v[4] or 0,
            'comment_count': v[5] or 0,
            'url': f'https://www.youtube.com/watch?v={v[0]}'
        })
    
    partners.append({
        'name': partner_name,
        'category': row[1] or 'Other',
        'video_count': row[2],
        'total_views': row[3] or 0,
        'total_likes': row[4] or 0,
        'total_comments': row[5] or 0,
        'first_collab': row[6][:10] if row[6] else '',
        'last_collab': row[7][:10] if row[7] else '',
        'videos': videos
    })

with open('docs/data.json', 'w', encoding='utf-8') as f:
    json.dump(partners, f, ensure_ascii=False, indent=2)

print(f"  Exported {len(partners)} partners to docs/data.json")

# 2. Others (Non-collab by content type)
print("\n[2] Exporting non-collab content types...")

cursor.execute("""
    SELECT content_type,
           COUNT(*) as video_count,
           SUM(view_count) as total_views,
           SUM(like_count) as total_likes,
           SUM(comment_count) as total_comments
    FROM videos 
    WHERE is_collab = 0
    GROUP BY content_type 
    ORDER BY total_views DESC
""")

content_types = []
for row in cursor.fetchall():
    content_type = row[0] or 'Other'
    
    # Get top 50 videos for this content type
    cursor.execute("""
        SELECT video_id, title, published_at, view_count, like_count, comment_count
        FROM videos 
        WHERE is_collab = 0 AND (content_type = ? OR (content_type IS NULL AND ? = 'Other'))
        ORDER BY view_count DESC
        LIMIT 50
    """, (content_type, content_type))
    
    videos = []
    for v in cursor.fetchall():
        videos.append({
            'video_id': v[0],
            'title': v[1],
            'published_at': v[2][:10] if v[2] else '',
            'view_count': v[3] or 0,
            'like_count': v[4] or 0,
            'comment_count': v[5] or 0,
            'url': f'https://www.youtube.com/watch?v={v[0]}'
        })
    
    content_types.append({
        'name': content_type,
        'video_count': row[1],
        'total_views': row[2] or 0,
        'total_likes': row[3] or 0,
        'total_comments': row[4] or 0,
        'videos': videos
    })

# Calculate totals for non-collab
cursor.execute("""
    SELECT COUNT(*), SUM(view_count), SUM(like_count), SUM(comment_count)
    FROM videos WHERE is_collab = 0
""")
totals = cursor.fetchone()

# Get top 100 non-collab videos overall
cursor.execute("""
    SELECT video_id, title, published_at, view_count, like_count, comment_count, content_type
    FROM videos 
    WHERE is_collab = 0
    ORDER BY view_count DESC
    LIMIT 100
""")

top_others = []
for v in cursor.fetchall():
    top_others.append({
        'video_id': v[0],
        'title': v[1],
        'published_at': v[2][:10] if v[2] else '',
        'view_count': v[3] or 0,
        'like_count': v[4] or 0,
        'comment_count': v[5] or 0,
        'content_type': v[6] or 'Other',
        'url': f'https://www.youtube.com/watch?v={v[0]}'
    })

others_data = {
    'video_count': totals[0],
    'total_views': totals[1] or 0,
    'total_likes': totals[2] or 0,
    'total_comments': totals[3] or 0,
    'content_types': content_types,
    'videos': top_others
}

with open('docs/others.json', 'w', encoding='utf-8') as f:
    json.dump(others_data, f, ensure_ascii=False, indent=2)

print(f"  Exported {len(content_types)} content types to docs/others.json")

# 3. Summary stats
print("\n[3] Summary:")
cursor.execute("SELECT COUNT(*) FROM videos")
total_videos = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM videos WHERE is_collab = 1")
collab_videos = cursor.fetchone()[0]
cursor.execute("SELECT SUM(view_count) FROM videos")
total_views = cursor.fetchone()[0]

print(f"  Total videos: {total_videos:,}")
print(f"  Collab videos: {collab_videos:,}")
print(f"  Collab partners: {len(partners)}")
print(f"  Total views: {total_views:,}")

conn.close()
print("\nExport complete!")
