"""Show latest videos in DB."""
import sys
sys.path.insert(0, '.')
from db.models import Database
from pathlib import Path

db = Database(Path('./data/pubg_collab.db'))

with db.get_connection() as conn:
    cursor = conn.execute('''
        SELECT video_id, title, published_at, view_count
        FROM videos 
        ORDER BY published_at DESC
        LIMIT 10
    ''')
    rows = cursor.fetchall()
    
print('Latest 10 videos in DB:')
print('=' * 70)
for r in rows:
    title = r["title"][:50] + "..." if len(r["title"]) > 50 else r["title"]
    print(f'{r["published_at"][:10]} | {title}')
    print(f'  ID: {r["video_id"]}')
