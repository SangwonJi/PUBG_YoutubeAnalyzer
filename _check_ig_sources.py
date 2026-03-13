import csv, json, sqlite3, os

# 1. Check dataset CSV
dataset_dir = r'C:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer'
for fn in os.listdir(dataset_dir):
    if fn.startswith('dataset_instagram') and fn.endswith('.csv'):
        path = os.path.join(dataset_dir, fn)
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        print(f'Dataset CSV: {fn}')
        print(f'  Rows: {len(rows)}')
        if rows:
            print(f'  Columns: {list(rows[0].keys())}')
            img_cols = [c for c in rows[0].keys() if 'image' in c.lower() or 'thumb' in c.lower() or 'display' in c.lower() or 'url' in c.lower()]
            print(f'  Image cols: {img_cols}')
            for ic in img_cols:
                has = sum(1 for r in rows if r.get(ic, '').strip())
                print(f'    {ic}: {has}/{len(rows)} populated')
                if has > 0:
                    sample = next(r[ic] for r in rows if r.get(ic, '').strip())
                    print(f'    Sample: {sample[:150]}')
        print()

# 2. Check DB
db_path = r'C:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\data\pubgm_instagram.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f'Instagram DB tables: {[t[0] for t in tables]}')
    for tbl in tables:
        cols = cur.execute(f"PRAGMA table_info({tbl[0]})").fetchall()
        col_names = [c[1] for c in cols]
        count = cur.execute(f"SELECT COUNT(*) FROM {tbl[0]}").fetchone()[0]
        print(f'  {tbl[0]}: {count} rows, cols: {col_names}')
        img_cols = [c for c in col_names if 'image' in c.lower() or 'thumb' in c.lower() or 'display' in c.lower() or 'url' in c.lower()]
        if img_cols:
            for ic in img_cols:
                has = cur.execute(f"SELECT COUNT(*) FROM {tbl[0]} WHERE {ic} IS NOT NULL AND {ic} != ''").fetchone()[0]
                print(f'    {ic}: {has}/{count} populated')
                if has > 0:
                    sample = cur.execute(f"SELECT {ic} FROM {tbl[0]} WHERE {ic} IS NOT NULL AND {ic} != '' LIMIT 1").fetchone()[0]
                    print(f'    Sample: {str(sample)[:150]}')
    conn.close()

# 3. Check ig_collab_report
cr_path = os.path.join(dataset_dir, 'ig_collab_report.csv')
if os.path.exists(cr_path):
    with open(cr_path, 'r', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    print(f'\nig_collab_report.csv: {len(rows)} rows')
    if rows:
        print(f'  Columns: {list(rows[0].keys())}')
