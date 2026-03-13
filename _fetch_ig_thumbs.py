"""
Fetch Instagram post thumbnail URLs via oEmbed endpoint.
Instagram allows public oEmbed requests without auth token for public posts.
"""
import json, time, urllib.request, urllib.parse

DOCS = r'c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs'

# Collect all shortcodes from both ig_data.json and ig_others.json
shortcodes = set()
for fn in ['ig_data.json', 'ig_others.json']:
    with open(f'{DOCS}/{fn}', 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, list):
        for p in data:
            for v in p.get('videos', []):
                sc = v.get('shortcode', '')
                if sc:
                    shortcodes.add(sc)
    elif isinstance(data, dict):
        for ct in data.get('content_types', []):
            for v in ct.get('videos', []):
                sc = v.get('shortcode', '')
                if sc:
                    shortcodes.add(sc)

print(f'Total shortcodes: {len(shortcodes)}')

# Test oEmbed for a few
OEMBED_URL = 'https://graph.facebook.com/v18.0/instagram_oembed?url={}&access_token=NONE'
# Alternative: noembed
NOEMBED_URL = 'https://noembed.com/embed?url={}'

test_codes = list(shortcodes)[:5]
for sc in test_codes:
    post_url = f'https://www.instagram.com/p/{sc}/'
    encoded = urllib.parse.quote(post_url, safe='')

    # Try noembed (no auth needed)
    try:
        req = urllib.request.Request(
            f'https://noembed.com/embed?url={encoded}',
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
        thumb = result.get('thumbnail_url', '')
        print(f'{sc}: thumbnail={thumb[:100] if thumb else "NONE"}')
        print(f'  keys: {list(result.keys())}')
    except Exception as e:
        print(f'{sc}: Error - {e}')
    time.sleep(1)
