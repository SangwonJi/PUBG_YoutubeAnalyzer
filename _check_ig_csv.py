import csv, os, glob

search_paths = [
    r'c:\Users\sangwon.ji\pubg_collab_pipeline_v2\instagram_data',
    r'c:\Users\sangwon.ji\pubg_collab_pipeline_v2',
    r'c:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\output',
]

for base in search_paths:
    if not os.path.isdir(base):
        continue
    for fn in os.listdir(base):
        if 'ig' in fn.lower() and fn.endswith('.csv'):
            path = os.path.join(base, fn)
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            cols = list(rows[0].keys()) if rows else []
            print(f'\n{path}')
            print(f'  Rows: {len(rows)}, Cols: {cols}')
            img_cols = [c for c in cols if 'image' in c.lower() or 'thumb' in c.lower() or 'url' in c.lower() or 'display' in c.lower()]
            print(f'  Image-related cols: {img_cols}')
            if rows and img_cols:
                for ic in img_cols:
                    has = sum(1 for r in rows if r.get(ic, '').strip())
                    print(f'    {ic}: {has}/{len(rows)} populated')
                    if has > 0:
                        sample = next(r[ic] for r in rows if r.get(ic, '').strip())
                        print(f'    Sample: {sample[:120]}')
