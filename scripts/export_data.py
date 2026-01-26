"""Export data to JSON with partner merging and sentiment."""
import json
import sys
import csv
from pathlib import Path
sys.path.insert(0, '.')

from db.models import Database

db = Database(Path('./data/pubg_collab.db'))
videos = db.get_collab_videos()

# Load sentiment data
sentiment_map = {}
sentiment_file = Path('./output/sentiment_20260126_partners.csv')
if sentiment_file.exists():
    with open(sentiment_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sentiment_map[row['partner_name']] = {
                'total_comments_analyzed': int(row['total_comments']),
                'positive': int(row['positive']),
                'negative': int(row['negative']),
                'neutral': int(row['neutral']),
                'positive_ratio': float(row['positive_ratio']),
                'negative_ratio': float(row['negative_ratio']),
                'neutral_ratio': float(row['neutral_ratio']),
                'avg_compound': float(row['avg_compound']),
                'overall_sentiment': row['overall_sentiment']
            }

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
            'total_comments': 0,
            'sentiment': None
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

# Merge sentiment data for combined partners
for partner_name, data in partners_data.items():
    if partner_name == 'BABYMONSTER':
        # Combine BABYMONSTER + BABYMONSTER Live sentiment
        sent1 = sentiment_map.get('BABYMONSTER', {})
        sent2 = sentiment_map.get('BABYMONSTER Live', {})
        if sent1 or sent2:
            total = sent1.get('total_comments_analyzed', 0) + sent2.get('total_comments_analyzed', 0)
            pos = sent1.get('positive', 0) + sent2.get('positive', 0)
            neg = sent1.get('negative', 0) + sent2.get('negative', 0)
            neu = sent1.get('neutral', 0) + sent2.get('neutral', 0)
            if total > 0:
                data['sentiment'] = {
                    'total_comments_analyzed': total,
                    'positive': pos,
                    'negative': neg,
                    'neutral': neu,
                    'positive_ratio': round(pos / total * 100, 2),
                    'negative_ratio': round(neg / total * 100, 2),
                    'neutral_ratio': round(neu / total * 100, 2),
                    'overall_sentiment': 'positive' if pos > neg else 'negative' if neg > pos else 'neutral'
                }
    elif partner_name == 'Others':
        # Use Unknown sentiment
        sent = sentiment_map.get('Unknown')
        if sent:
            data['sentiment'] = sent
    else:
        sent = sentiment_map.get(partner_name)
        if sent:
            data['sentiment'] = sent

for partner in partners_data.values():
    partner['video_count'] = len(partner['videos'])
    partner['videos'].sort(key=lambda x: x['view_count'], reverse=True)

# Separate Others from rankings
others_data = partners_data.pop('Others', None)

# Sort the rest by views
partners_list = sorted(partners_data.values(), key=lambda x: x['total_views'], reverse=True)

# Export main partners (without Others)
with open('docs/data.json', 'w', encoding='utf-8') as f:
    json.dump(partners_list, f, ensure_ascii=False, indent=2)

# Export Others separately
if others_data:
    with open('docs/others.json', 'w', encoding='utf-8') as f:
        json.dump(others_data, f, ensure_ascii=False, indent=2)

print(f'Exported {len(partners_list)} partners (excluding Others)')
print('Top 5:')
for i, p in enumerate(partners_list[:5], 1):
    sent_info = ''
    if p.get('sentiment'):
        sent_info = f" | Sentiment: +{p['sentiment']['positive_ratio']:.0f}%/-{p['sentiment']['negative_ratio']:.0f}%"
    print(f"  {i}. {p['name']}: {p['total_views']:,} views, {p['video_count']} videos{sent_info}")

if others_data:
    print(f"\nOthers: {others_data['total_views']:,} views, {others_data['video_count']} videos")
