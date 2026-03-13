import urllib.request, re, json

DOCS = r'c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs'
with open(f'{DOCS}/ig_data.json', 'r', encoding='utf-8') as f:
    ig_data = json.load(f)

sc = ig_data[0]['videos'][0]['shortcode']
url = f'https://www.instagram.com/p/{sc}/'
print(f'Shortcode: {sc}')

req = urllib.request.Request(url, headers={
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
})
with urllib.request.urlopen(req, timeout=15) as resp:
    html = resp.read().decode('utf-8', errors='ignore')

# Search for cdninstagram image URLs
cdn_urls = re.findall(r'https://scontent[^"\\]+\.jpg[^"\\]*', html)
print(f'CDN image URLs found: {len(cdn_urls)}')
for u in cdn_urls[:5]:
    print(f'  {u[:150]}')

# Search for og:image in different formats
patterns = [
    r'og:image.*?content="([^"]+)"',
    r'"og:image":"([^"]+)"',
    r'"display_url":"([^"]+)"',
    r'"thumbnail_src":"([^"]+)"',
    r'"display_resources".*?"src":"([^"]+)"',
]
for pat in patterns:
    m = re.search(pat, html)
    if m:
        val = m.group(1).replace('\\u0026', '&')
        print(f'Pattern {pat[:30]}... => {val[:150]}')
