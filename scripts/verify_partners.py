"""Verify partner video counts."""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, '.')
from db.models import Database
from pathlib import Path

db = Database(Path('./data/pubg_collab.db'))

partners_to_check = [
    ('BABYMONSTER', ['BABYMONSTER', 'Baby Monster', 'babymonster']),
    ('Sonic', ['Sonic']),
    ('Dying Light', ['Dying Light', 'DyingLight']),
    ('Porsche', ['Porsche']),
    ('Bugatti', ['Bugatti']),
    ('TRANSFORMERS', ['TRANSFORMERS', 'Transformers', 'transformer']),
    ('Peaky Blinders', ['Peaky Blinders', 'Peaky', 'Blinders']),
]

print("=" * 80)
print("Partner Video Count Verification")
print("=" * 80)

for partner_name, keywords in partners_to_check:
    print(f"\n### {partner_name} ###")
    
    # Search by title
    all_videos = []
    with db.get_connection() as conn:
        for kw in keywords:
            cursor = conn.execute('''
                SELECT DISTINCT video_id, title, collab_partner, view_count, published_at
                FROM videos 
                WHERE title LIKE ? OR description LIKE ? OR collab_partner LIKE ?
                ORDER BY view_count DESC
            ''', (f'%{kw}%', f'%{kw}%', f'%{kw}%'))
            rows = cursor.fetchall()
            for r in rows:
                if r['video_id'] not in [v['video_id'] for v in all_videos]:
                    all_videos.append(dict(r))
    
    # Sort by view count
    all_videos.sort(key=lambda x: x['view_count'], reverse=True)
    
    total_views = sum(v['view_count'] for v in all_videos)
    print(f"Found {len(all_videos)} videos | Total views: {total_views:,}")
    
    for i, v in enumerate(all_videos, 1):
        title = v['title'][:50] + "..." if len(v['title']) > 50 else v['title']
        partner = v['collab_partner'] or 'None'
        print(f"  {i}. {title}")
        print(f"     Views: {v['view_count']:,} | Partner: {partner} | ID: {v['video_id']}")
