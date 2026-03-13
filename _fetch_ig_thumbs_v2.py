"""
Fetch Instagram thumbnails using crawler UA to get og:image.
Sequential with delay to avoid rate limiting.
"""
import json, re, time, urllib.request

DOCS = r'c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs'
UA = 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)'

def fetch_og_image(shortcode):
    url = f'https://www.instagram.com/p/{shortcode}/'
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA})
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode('utf-8', errors='ignore')[:50000]
            m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
            if m:
                return m.group(1)
            return ''
        except Exception:
            if attempt == 0:
                time.sleep(3)
    return ''

# Load data
with open(f'{DOCS}/ig_data.json', 'r', encoding='utf-8') as f:
    ig_data = json.load(f)
with open(f'{DOCS}/ig_others.json', 'r', encoding='utf-8') as f:
    ig_others = json.load(f)

# Collect unique shortcodes
all_shortcodes = set()
for p in ig_data:
    for v in p.get('videos', []):
        if v.get('shortcode'):
            all_shortcodes.add(v['shortcode'])
if isinstance(ig_others, dict):
    for ct in ig_others.get('content_types', []):
        for v in ct.get('videos', []):
            if v.get('shortcode'):
                all_shortcodes.add(v['shortcode'])

print(f'Total shortcodes: {len(all_shortcodes)}', flush=True)

# Fetch sequentially with delay
thumb_map = {}
ok_count = 0

for i, sc in enumerate(all_shortcodes):
    thumb = fetch_og_image(sc)
    thumb_map[sc] = thumb
    if thumb:
        ok_count += 1

    if (i + 1) % 50 == 0 or (i + 1) == len(all_shortcodes):
        print(f'  [{i+1}/{len(all_shortcodes)}] {ok_count} thumbnails', flush=True)

    time.sleep(0.3)

print(f'\nTotal: {ok_count}/{len(all_shortcodes)} thumbnails found', flush=True)

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
