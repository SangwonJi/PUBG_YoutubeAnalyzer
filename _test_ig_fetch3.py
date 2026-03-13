import urllib.request, json, re

sc = 'DLTR1suKkP7'

# Method: Instagram GraphQL endpoint
gql_url = f'https://www.instagram.com/graphql/query/?query_hash=b3055c01b4b222b8a47dc12b090e4e64&variables={{"shortcode":"{sc}"}}'
try:
    req = urllib.request.Request(gql_url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'X-Requested-With': 'XMLHttpRequest',
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    print('GraphQL response keys:', list(data.keys()) if isinstance(data, dict) else type(data))
    if 'data' in data:
        media = data['data'].get('shortcode_media', {})
        display = media.get('display_url', '')
        thumb = media.get('thumbnail_src', '')
        print(f'display_url: {display[:150]}')
        print(f'thumbnail_src: {thumb[:150]}')
except Exception as e:
    print(f'GraphQL Error: {type(e).__name__}: {e}')

# Method: ?__a=1&__d=dis
try:
    api_url = f'https://www.instagram.com/p/{sc}/?__a=1&__d=dis'
    req = urllib.request.Request(api_url, headers={
        'User-Agent': 'Instagram 219.0.0.12.117 Android',
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    print(f'\n__a=1 response type: {type(data)}')
    if isinstance(data, dict):
        print(f'Keys: {list(data.keys())}')
        items = data.get('graphql', {}).get('shortcode_media', {})
        if items:
            print(f'display_url: {items.get("display_url", "")[:150]}')
        else:
            items2 = data.get('items', [])
            if items2:
                print(f'First item keys: {list(items2[0].keys())}')
                ic = items2[0].get('image_versions2', {}).get('candidates', [])
                if ic:
                    print(f'Image: {ic[0].get("url","")[:150]}')
except Exception as e:
    print(f'__a=1 Error: {type(e).__name__}: {e}')
