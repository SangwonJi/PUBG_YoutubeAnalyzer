"""
Try fetching Instagram thumbnail from og:image meta tag or /media/ redirect.
"""
import urllib.request, re, time

test_codes = ['DVnbBZmiHSk', 'DUrV2vPj_yN', 'DFYd8YEJ8El']

for sc in test_codes:
    # Method 1: /media/ endpoint (redirects to CDN)
    media_url = f'https://www.instagram.com/p/{sc}/media/?size=m'
    try:
        req = urllib.request.Request(media_url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        req.method = 'HEAD'
        with urllib.request.urlopen(req, timeout=10) as resp:
            final_url = resp.url
            print(f'{sc} /media/: {final_url[:120]}')
    except Exception as e:
        print(f'{sc} /media/: Error - {type(e).__name__}: {e}')

    # Method 2: Fetch page and parse og:image
    try:
        page_url = f'https://www.instagram.com/p/{sc}/'
        req = urllib.request.Request(page_url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode('utf-8', errors='ignore')[:20000]
        m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
        if m:
            print(f'{sc} og:image: {m.group(1)[:120]}')
        else:
            print(f'{sc} og:image: NOT FOUND')
    except Exception as e:
        print(f'{sc} og:image: Error - {type(e).__name__}: {e}')

    time.sleep(2)
    print()
