import urllib.request, re

# Test if Instagram is still blocking us
sc = 'DVnbBZmiHSk'
url = f'https://www.instagram.com/p/{sc}/'
try:
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        html = resp.read().decode('utf-8', errors='ignore')
    print(f'HTML length: {len(html)}')
    m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
    if m:
        print(f'SUCCESS: {m.group(1)[:120]}')
    else:
        # Check for login redirect
        if 'login' in html[:2000].lower():
            print('BLOCKED: Login page returned')
        else:
            print('NO og:image found')
            # Try alternative pattern in script tags
            m2 = re.search(r'"thumbnail_src":"(https://[^"]+)"', html)
            if m2:
                print(f'thumbnail_src: {m2.group(1)[:120]}')
except Exception as e:
    print(f'Error: {type(e).__name__}: {e}')
