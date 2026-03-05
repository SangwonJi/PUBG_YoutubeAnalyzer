"""Search for videos in DB."""
import sys
sys.path.insert(0, '.')
from db.models import Database
from pathlib import Path

db = Database(Path('./data/pubg_collab.db'))

search_term = sys.argv[1] if len(sys.argv) > 1 else 'WOW'

with db.get_connection() as conn:
    cursor = conn.execute('''
        SELECT video_id, title, collab_partner, view_count, published_at
        FROM videos 
        WHERE title LIKE ? OR description LIKE ?
        ORDER BY published_at DESC
        LIMIT 10
    ''', (f'%{search_term}%', f'%{search_term}%'))
    rows = cursor.fetchall()
    
print(f'Search for "{search_term}": Found {len(rows)} videos')
print('=' * 70)
for r in rows:
    title = r["title"][:50] + "..." if len(r["title"]) > 50 else r["title"]
    print(f'{r["published_at"]} | {title}')
    print(f'  Partner: {r["collab_partner"]} | Views: {r["view_count"]:,} | ID: {r["video_id"]}')
    print()
