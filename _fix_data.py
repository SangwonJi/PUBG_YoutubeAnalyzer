"""Fix video sorting and remove old categories at video level."""
import json, sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DOCS = 'docs'
OLD_TO_NEW = {
    'Anime':'Animation','Movie':'Film','Brand':'Vehicle','IP':'Character',
    'Creator':'Artist','Celebrity':'Artist','Sports':'Other',
    'Entertainment':'Other','Esports':'Other','Event':'Other','Designer':'Fashion',
    'Movie/IP':'Film',
}
VALID_CATS = {'Animation','Artist','Character','Fashion','Film','Game','Vehicle','Other'}

files = [f for f in os.listdir(DOCS) if f.endswith('_data.json')]
total_sort_fixed = 0
total_cat_fixed = 0

for fn in sorted(files):
    path = os.path.join(DOCS, fn)
    data = json.load(open(path, 'r', encoding='utf-8'))
    sort_fixed = 0
    cat_fixed = 0

    for p in data:
        videos = p.get('videos', p.get('posts', []))
        is_weibo = 'posts' in p
        sort_key = 'reposts' if is_weibo else 'view_count'

        sorted_vids = sorted(videos, key=lambda v: v.get(sort_key, 0), reverse=True)
        if videos != sorted_vids:
            sort_fixed += 1
            if 'videos' in p:
                p['videos'] = sorted_vids
            elif 'posts' in p:
                p['posts'] = sorted_vids

        for v in (p.get('videos', []) + p.get('posts', [])):
            if 'category' in v:
                old = v['category']
                if old not in VALID_CATS:
                    new = OLD_TO_NEW.get(old, 'Other')
                    v['category'] = new
                    cat_fixed += 1

        old_cat = p.get('category', 'Other')
        if old_cat not in VALID_CATS:
            p['category'] = OLD_TO_NEW.get(old_cat, 'Other')
            cat_fixed += 1

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

    if sort_fixed or cat_fixed:
        print(f'{fn}: {sort_fixed} partners re-sorted, {cat_fixed} categories fixed')
    total_sort_fixed += sort_fixed
    total_cat_fixed += cat_fixed

print(f'\nTotal: {total_sort_fixed} sort fixes, {total_cat_fixed} category fixes')
