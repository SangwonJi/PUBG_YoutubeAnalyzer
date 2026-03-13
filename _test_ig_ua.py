import urllib.request, re

sc = 'DVnbBZmiHSk'
url = f'https://www.instagram.com/p/{sc}/'

user_agents = [
    ('Googlebot', 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'),
    ('Twitterbot', 'Twitterbot/1.0'),
    ('facebookexternalhit', 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)'),
    ('curl', 'curl/7.68.0'),
]

for name, ua in user_agents:
    try:
        req = urllib.request.Request(url, headers={'User-Agent': ua})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode('utf-8', errors='ignore')
        m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
        found = m.group(1)[:80] if m else 'NOT FOUND'
        print(f'{name}: HTML={len(html)} og:image={found}')
    except Exception as e:
        print(f'{name}: Error - {e}')
