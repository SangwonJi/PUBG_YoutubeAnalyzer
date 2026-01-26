"""Export data to JSON with partner merging."""
import json
import sys
sys.path.insert(0, '.')

from db.models import Database
from pathlib import Path

db = Database(Path('./data/pubg_collab.db'))
videos = db.get_collab_videos()

# Merge rules - combine similar partners
merge_map = {
    'BABYMONSTER Live': 'BABYMONSTER',
    'BABYMONSTER': 'BABYMONSTER',
    'Unknown': 'Others',
    None: 'Others',
    '': 'Others'
}

partners_data = {}
for v in videos:
    # Apply merge rules
    original_partner = v.collab_partner
    partner = merge_map.get(original_partner, original_partner) or 'Others'
    
    if partner not in partners_data:
        partners_data[partner] = {
            'name': partner,
            'category': v.collab_category or 'Other',
            'videos': [],
            'total_views': 0,
            'total_likes': 0,
            'total_comments': 0
        }
    
    # Update category for merged partners
    if partner == 'BABYMONSTER':
        partners_data[partner]['category'] = 'Artist'
    
    partners_data[partner]['videos'].append({
        'video_id': v.video_id,
        'title': v.title,
        'published_at': v.published_at.strftime('%Y-%m-%d'),
        'view_count': v.view_count,
        'like_count': v.like_count,
        'comment_count': v.comment_count,
        'url': 'https://www.youtube.com/watch?v=' + v.video_id
    })
    partners_data[partner]['total_views'] += v.view_count
    partners_data[partner]['total_likes'] += v.like_count
    partners_data[partner]['total_comments'] += v.comment_count

for partner in partners_data.values():
    partner['video_count'] = len(partner['videos'])
    partner['videos'].sort(key=lambda x: x['view_count'], reverse=True)

partners_list = sorted(partners_data.values(), key=lambda x: x['total_views'], reverse=True)

with open('docs/data.json', 'w', encoding='utf-8') as f:
    json.dump(partners_list, f, ensure_ascii=False, indent=2)

print(f'Exported {len(partners_list)} partners')
print('Top 5:')
for i, p in enumerate(partners_list[:5], 1):
    print(f"  {i}. {p['name']}: {p['total_views']:,} views, {p['video_count']} videos")
