"""
Export data for web dashboard.
Creates separate JSON files for each channel (PUBGM, Free Fire).
"""

import sqlite3
import json
import sys
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Channel configurations
CHANNELS = {
    'pubgm': {
        'name': 'PUBG MOBILE',
        'data_file': 'pubgm_data.json',
        'others_file': 'pubgm_others.json'
    },
    'freefire': {
        'name': 'Free Fire',
        'data_file': 'freefire_data.json',
        'others_file': 'freefire_others.json'
    }
}

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("Exporting data for web dashboard...\n")

# Check if source_channel column exists
cursor.execute("PRAGMA table_info(videos)")
columns = [row[1] for row in cursor.fetchall()]
has_source_channel = 'source_channel' in columns

if not has_source_channel:
    print("Warning: source_channel column not found. Run migrate_db.py first.")
    print("Exporting all data as PUBGM...")

all_channels_data = {}

for channel_id, channel_info in CHANNELS.items():
    print(f"\n{'='*60}")
    print(f"Processing: {channel_info['name']} ({channel_id})")
    print('='*60)
    
    # Build WHERE clause for channel filter
    if has_source_channel:
        channel_filter = f"source_channel = '{channel_id}'"
    else:
        # If no source_channel, assume all data is PUBGM
        if channel_id != 'pubgm':
            print(f"  Skipping {channel_info['name']} (no source_channel data)")
            continue
        channel_filter = "1=1"
    
    # 1. Collab Partners Data
    print(f"\n[1] Exporting collab partners for {channel_info['name']}...")
    
    cursor.execute(f"""
        SELECT collab_partner, collab_category, 
               COUNT(*) as video_count,
               SUM(view_count) as total_views,
               SUM(like_count) as total_likes,
               SUM(comment_count) as total_comments,
               MIN(published_at) as first_collab,
               MAX(published_at) as last_collab
        FROM videos 
        WHERE is_collab = 1 AND collab_partner IS NOT NULL AND {channel_filter}
        GROUP BY collab_partner 
        ORDER BY total_views DESC
    """)
    
    partners = []
    for row in cursor.fetchall():
        partner_name = row[0]
        
        # Get videos for this partner
        cursor.execute(f"""
            SELECT video_id, title, published_at, view_count, like_count, comment_count
            FROM videos 
            WHERE collab_partner = ? AND {channel_filter}
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
    
    with open(f'docs/{channel_info["data_file"]}', 'w', encoding='utf-8') as f:
        json.dump(partners, f, ensure_ascii=False, indent=2)
    
    print(f"  Exported {len(partners)} partners to docs/{channel_info['data_file']}")
    
    # 2. Others (Non-collab by content type)
    print(f"\n[2] Exporting non-collab content types for {channel_info['name']}...")
    
    cursor.execute(f"""
        SELECT content_type,
               COUNT(*) as video_count,
               SUM(view_count) as total_views,
               SUM(like_count) as total_likes,
               SUM(comment_count) as total_comments
        FROM videos 
        WHERE is_collab = 0 AND {channel_filter}
        GROUP BY content_type 
        ORDER BY total_views DESC
    """)
    
    content_types = []
    for row in cursor.fetchall():
        content_type = row[0] or 'Other'
        
        # Get top 50 videos for this content type
        cursor.execute(f"""
            SELECT video_id, title, published_at, view_count, like_count, comment_count
            FROM videos 
            WHERE is_collab = 0 AND (content_type = ? OR (content_type IS NULL AND ? = 'Other'))
                  AND {channel_filter}
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
    cursor.execute(f"""
        SELECT COUNT(*), SUM(view_count), SUM(like_count), SUM(comment_count)
        FROM videos WHERE is_collab = 0 AND {channel_filter}
    """)
    totals = cursor.fetchone()
    
    # Get top 100 non-collab videos overall
    cursor.execute(f"""
        SELECT video_id, title, published_at, view_count, like_count, comment_count, content_type
        FROM videos 
        WHERE is_collab = 0 AND {channel_filter}
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
        'video_count': totals[0] or 0,
        'total_views': totals[1] or 0,
        'total_likes': totals[2] or 0,
        'total_comments': totals[3] or 0,
        'content_types': content_types,
        'videos': top_others
    }
    
    with open(f'docs/{channel_info["others_file"]}', 'w', encoding='utf-8') as f:
        json.dump(others_data, f, ensure_ascii=False, indent=2)
    
    print(f"  Exported {len(content_types)} content types to docs/{channel_info['others_file']}")
    
    # Store for summary
    all_channels_data[channel_id] = {
        'partners': len(partners),
        'collab_videos': sum(p['video_count'] for p in partners),
        'noncollab_videos': totals[0] or 0
    }

# Also create legacy combined files for backward compatibility
print("\n" + "="*60)
print("Creating combined data files (legacy)...")
print("="*60)

# Combined partners from all channels
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

print(f"  Exported combined {len(partners)} partners to docs/data.json")

# 3. Summary stats
print("\n" + "="*60)
print("Summary:")
print("="*60)

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

if has_source_channel:
    print("\n  By channel:")
    for channel_id, data in all_channels_data.items():
        print(f"    {CHANNELS[channel_id]['name']}: {data['collab_videos']} collabs, {data['partners']} partners")

conn.close()
print("\nExport complete!")
