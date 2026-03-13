import urllib.request, re, json

DOCS = r'c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs'
with open(f'{DOCS}/ig_data.json', 'r', encoding='utf-8') as f:
    ig_data = json.load(f)

sc = ig_data[0]['videos'][0]['shortcode']
print(f'Testing shortcode: {sc}')

url = f'https://www.instagram.com/p/{sc}/'
print(f'URL: {url}')

try:
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        status = resp.status
        html = resp.read().decode('utf-8', errors='ignore')
        print(f'Status: {status}, HTML length: {len(html)}')

        m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
        if m:
            print(f'og:image: {m.group(1)[:150]}')
        else:
            print('og:image NOT found')
            # Check what we got
            if 'login' in html.lower()[:500]:
                print('  -> Login page detected!')
            print(f'  First 500 chars: {html[:500]}')
except Exception as e:
    print(f'Error: {type(e).__name__}: {e}')
