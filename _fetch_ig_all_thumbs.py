"""
Batch-fetch Instagram thumbnails from og:image meta tags.
Updates ig_data.json and ig_others.json with thumbnail URLs.
"""
import json, re, time, urllib.request, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

DOCS = r'c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs'

def fetch_og_image(shortcode, retries=2):
    url = f'https://www.instagram.com/p/{shortcode}/'
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode('utf-8', errors='ignore')[:30000]
            m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
            if m:
                return m.group(1)
            return ''
        except Exception:
            if attempt < retries:
                time.sleep(2)
            continue
    return ''

# Collect all shortcodes
all_shortcodes = set()
with open(f'{DOCS}/ig_data.json', 'r', encoding='utf-8') as f:
    ig_data = json.load(f)
with open(f'{DOCS}/ig_others.json', 'r', encoding='utf-8') as f:
    ig_others = json.load(f)

for p in ig_data:
    for v in p.get('videos', []):
        if v.get('shortcode'):
            all_shortcodes.add(v['shortcode'])
if isinstance(ig_others, dict):
    for ct in ig_others.get('content_types', []):
        for v in ct.get('videos', []):
            if v.get('shortcode'):
                all_shortcodes.add(v['shortcode'])

print(f'Total shortcodes to fetch: {len(all_shortcodes)}', flush=True)

# Batch fetch with thread pool (careful with rate limits)
thumb_map = {}
done = 0

def worker(sc):
    return sc, fetch_og_image(sc)

with ThreadPoolExecutor(max_workers=5) as pool:
    futures = {pool.submit(worker, sc): sc for sc in all_shortcodes}
    for future in as_completed(futures):
        sc, thumb = future.result()
        thumb_map[sc] = thumb
        done += 1
        if done % 100 == 0 or done == len(all_shortcodes):
            ok = sum(1 for v in thumb_map.values() if v)
            print(f'  [{done}/{len(all_shortcodes)}] {ok} thumbnails found', flush=True)

ok = sum(1 for v in thumb_map.values() if v)
print(f'\nFetched: {ok}/{len(all_shortcodes)} thumbnails', flush=True)

# Update ig_data.json
for p in ig_data:
    for v in p.get('videos', []):
        sc = v.get('shortcode', '')
        if sc and thumb_map.get(sc):
            v['thumbnail'] = thumb_map[sc]
with open(f'{DOCS}/ig_data.json', 'w', encoding='utf-8') as f:
    json.dump(ig_data, f, ensure_ascii=False)

# Update ig_others.json
if isinstance(ig_others, dict):
    for ct in ig_others.get('content_types', []):
        for v in ct.get('videos', []):
            sc = v.get('shortcode', '')
            if sc and thumb_map.get(sc):
                v['thumbnail'] = thumb_map[sc]
with open(f'{DOCS}/ig_others.json', 'w', encoding='utf-8') as f:
    json.dump(ig_others, f, ensure_ascii=False)

print('JSON files updated!', flush=True)
