"""
Export UGC (User Generated Content) comparison data: Craftland vs WOW
"""
import sqlite3
import json
import sys
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("Exporting Craftland vs WOW comparison data...\n")

def get_videos(query, params=()):
    cursor.execute(query, params)
    videos = []
    for r in cursor.fetchall():
        videos.append({
            'video_id': r[0],
            'title': r[1],
            'published_at': r[2][:10] if r[2] else '',
            'view_count': r[3] or 0,
            'like_count': r[4] or 0,
            'comment_count': r[5] or 0,
            'url': f'https://www.youtube.com/watch?v={r[0]}'
        })
    return videos

# Craftland (Free Fire)
print("[1] Fetching Craftland videos...")
craftland_videos = get_videos('''
    SELECT video_id, title, published_at, view_count, like_count, comment_count 
    FROM videos 
    WHERE source_channel='freefire' 
      AND (LOWER(title) LIKE '%craftland%' OR LOWER(description) LIKE '%craftland%')
    ORDER BY published_at ASC
''')

# WOW / World of Wonder (PUBGM)
print("[2] Fetching WOW videos...")
wow_videos = get_videos('''
    SELECT video_id, title, published_at, view_count, like_count, comment_count 
    FROM videos 
    WHERE source_channel='pubgm' 
      AND (LOWER(title) LIKE '%world of wonder%' 
           OR LOWER(title) LIKE '% wow %'
           OR LOWER(title) LIKE '%#wow%'
           OR LOWER(title) LIKE '%wow %'
           OR LOWER(description) LIKE '%world of wonder%')
    ORDER BY published_at ASC
''')

# Calculate stats
def calc_stats(videos):
    if not videos:
        return {'video_count': 0, 'total_views': 0, 'total_likes': 0, 'total_comments': 0, 'avg_views': 0}
    
    total_views = sum(v['view_count'] for v in videos)
    total_likes = sum(v['like_count'] for v in videos)
    total_comments = sum(v['comment_count'] for v in videos)
    
    return {
        'video_count': len(videos),
        'total_views': total_views,
        'total_likes': total_likes,
        'total_comments': total_comments,
        'avg_views': round(total_views / len(videos)) if videos else 0,
        'avg_likes': round(total_likes / len(videos)) if videos else 0,
        'like_rate': round(total_likes / total_views * 100, 2) if total_views > 0 else 0,
        'comment_rate': round(total_comments / total_views * 100, 4) if total_views > 0 else 0,
        'first_video': videos[0]['published_at'] if videos else '',
        'last_video': videos[-1]['published_at'] if videos else ''
    }

# Monthly aggregation
def aggregate_monthly(videos):
    monthly = {}
    for v in videos:
        month = v['published_at'][:7]  # YYYY-MM
        if month not in monthly:
            monthly[month] = {'videos': 0, 'views': 0, 'likes': 0, 'comments': 0}
        monthly[month]['videos'] += 1
        monthly[month]['views'] += v['view_count']
        monthly[month]['likes'] += v['like_count']
        monthly[month]['comments'] += v['comment_count']
    
    return [{'month': k, **v} for k, v in sorted(monthly.items())]

craftland_stats = calc_stats(craftland_videos)
wow_stats = calc_stats(wow_videos)

data = {
    'generated_at': datetime.now().isoformat(),
    'craftland': {
        'name': 'Craftland',
        'game': 'Free Fire',
        'stats': craftland_stats,
        'monthly': aggregate_monthly(craftland_videos),
        'videos': sorted(craftland_videos, key=lambda x: x['view_count'], reverse=True)
    },
    'wow': {
        'name': 'World of Wonder',
        'game': 'PUBG MOBILE',
        'stats': wow_stats,
        'monthly': aggregate_monthly(wow_videos),
        'videos': sorted(wow_videos, key=lambda x: x['view_count'], reverse=True)
    }
}

with open('docs/ugc_comparison.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"\nExported to docs/ugc_comparison.json")
print(f"\n=== Summary ===")
print(f"Craftland (Free Fire): {craftland_stats['video_count']} videos, {craftland_stats['total_views']:,} views")
print(f"WOW (PUBG MOBILE): {wow_stats['video_count']} videos, {wow_stats['total_views']:,} views")

conn.close()
