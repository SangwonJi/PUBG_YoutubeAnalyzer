import json

with open(r'c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs\ig_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

for p in data[:3]:
    v = p.get('videos', [{}])[0]
    name = p.get('name', '?')
    print(f'Partner: {name}')
    print(f'  Keys: {list(v.keys())}')
    print(f'  thumbnail: {v.get("thumbnail", "MISSING")}')
    print(f'  image_url: {v.get("image_url", "MISSING")}')
    print()

with open(r'c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs\ig_others.json', 'r', encoding='utf-8') as f:
    others = json.load(f)

ct = others.get('content_types', [])
if ct:
    v = ct[0].get('videos', [{}])[0]
    print(f'Non-collab sample:')
    print(f'  Keys: {list(v.keys())}')
    print(f'  thumbnail: {v.get("thumbnail", "MISSING")}')
    print(f'  image_url: {v.get("image_url", "MISSING")}')
