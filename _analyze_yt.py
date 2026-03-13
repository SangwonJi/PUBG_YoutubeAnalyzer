import csv
from collections import Counter

path = r'c:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\output\yt_regional_all_videos.csv'
with open(path, 'r', encoding='utf-8-sig') as f:
    rows = list(csv.DictReader(f))

print(f'Total rows: {len(rows)}')
collab = [r for r in rows if r.get('is_collab','0') == '1']
non_collab = [r for r in rows if r.get('is_collab','0') == '0']
empty = [r for r in rows if r.get('is_collab','') == '']
print(f'is_collab=1: {len(collab)}')
print(f'is_collab=0: {len(non_collab)}')
print(f'is_collab empty: {len(empty)}')

regions = Counter(r['region'] for r in rows)
print(f'Regions: {dict(regions)}')

has_partner = [r for r in rows if r.get('partner','').strip()]
print(f'Has partner: {len(has_partner)}')
has_content_cat = [r for r in rows if r.get('content_category','').strip()]
print(f'Has content_category: {len(has_content_cat)}')

# Date range per region
for region in ['MENA', 'Turkey', 'Indonesia', 'LATAM']:
    rr = [r for r in rows if r['region'] == region]
    dates = sorted([r['published_at'][:10] for r in rr if r.get('published_at')])
    if dates:
        print(f'{region}: {dates[0]} ~ {dates[-1]} ({len(rr)} videos)')

# Sample collab entries
if collab:
    print('\nSample collab entries:')
    for c in collab[:5]:
        print(f"  [{c['region']}] {c['title'][:60]} | partner={c.get('partner','')} | cat={c.get('collab_category','')}")
