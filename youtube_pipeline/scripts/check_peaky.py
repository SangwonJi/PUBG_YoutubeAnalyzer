"""Check Peaky Blinders videos."""
import sys
sys.path.insert(0, '.')
from db.models import Database
from pathlib import Path

db = Database(Path('./data/pubg_collab.db'))

# Search for Peaky Blinders in all videos
with db.get_connection() as conn:
    cursor = conn.execute('''
        SELECT video_id, title, collab_partner, view_count 
        FROM videos 
        WHERE title LIKE '%Peaky%' OR title LIKE '%Blinder%' 
           OR description LIKE '%Peaky%' OR collab_partner LIKE '%Peaky%'
        ORDER BY view_count DESC
    ''')
    rows = cursor.fetchall()
    
print(f'Found {len(rows)} Peaky Blinders related videos:')
print('=' * 70)
for r in rows:
    title = r["title"][:55] + "..." if len(r["title"]) > 55 else r["title"]
    print(f'Title: {title}')
    print(f'  Partner: {r["collab_partner"]} | Views: {r["view_count"]:,}')
    print(f'  ID: {r["video_id"]}')
    print()
