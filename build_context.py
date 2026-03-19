"""
Build maximum-density all-platform context for AI chatbot with RAG.
No hard char budget — Worker uses smart retrieval to filter relevant sections.
Tiered video detail: top partners get more video info.
"""
import json, os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DOCS = 'docs'
TIER1_N = 30       # top 30 partners: full video detail
TIER1_VIDS = 10
TIER2_N = 70       # next 70 partners (31-100): brief video detail
TIER2_VIDS = 3
TITLE_LEN = 35

SOURCES = [
    ('YouTube Global (PUBG MOBILE)', 'data.json', 'yt'),
    ('YouTube MENA', 'yt_mena_data.json', 'yt'),
    ('YouTube India', 'yt_india_data.json', 'yt'),
    ('YouTube Indonesia', 'yt_indonesia_data.json', 'yt'),
    ('YouTube LATAM', 'yt_latam_data.json', 'yt'),
    ('YouTube Malaysia', 'yt_malaysia_data.json', 'yt'),
    ('YouTube Pakistan', 'yt_pakistan_data.json', 'yt'),
    ('YouTube Taiwan', 'yt_taiwan_data.json', 'yt'),
    ('YouTube Thailand', 'yt_thailand_data.json', 'yt'),
    ('YouTube Turkey', 'yt_turkey_data.json', 'yt'),
    ('YouTube CIS', 'yt_cis_data.json', 'yt'),
    ('YouTube Korea', 'yt_korea_data.json', 'yt'),
    ('YouTube Japan', 'yt_japan_data.json', 'yt'),
    ('Instagram (PUBG MOBILE)', 'ig_data.json', 'ig'),
    ('Weibo (和平精英/Game For Peace China)', 'weibo_data.json', 'weibo'),
    ('YouTube Free Fire', 'freefire_data.json', 'yt'),
]

def n(val):
    return f"{int(val):,}"

def build():
    header = "=== PUBG MOBILE & FREE FIRE COLLAB DATA (ALL REGIONS) ===\n"
    header += "16 sources: YouTube(13 regions) + Instagram + Weibo(China) + Free Fire\n"
    header += "Numbers are exact comma-separated integers. Worker uses smart retrieval.\n"
    header += "Partner: #rank NAME [category] N videos | VIEWS views | LIKES likes | COMMENTS comments | PERIOD\n"
    header += "Video: \"TITLE\" DATE | VIEWS views | LIKES likes\n\n"

    overview_lines = []
    all_sections = []

    for label, fname, ptype in SOURCES:
        path = os.path.join(DOCS, fname)
        if not os.path.exists(path):
            continue
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        data.sort(key=lambda p: p.get('total_views', p.get('total_reposts', 0)), reverse=True)

        total_v = sum(p.get('total_views', p.get('total_reposts', 0)) for p in data)
        total_l = sum(p.get('total_likes', p.get('total_attitudes', 0)) for p in data)
        total_c = sum(p.get('total_comments', 0) for p in data)
        total_items = sum(len(p.get('videos', p.get('posts', []))) for p in data)

        overview_lines.append(
            f"  {label}: {len(data)} partners, {n(total_items)} items, "
            f"{n(total_v)} views, {n(total_l)} likes, {n(total_c)} comments"
        )

        section = f"\n[{label}] {len(data)} partners, {n(total_items)} items, {n(total_v)} total views\n"

        for i, p in enumerate(data):
            name = p.get('partner_name') or p.get('name') or '?'
            cat = p.get('category', '?')
            items = p.get('videos', p.get('posts', []))
            tv = p.get('total_views', p.get('total_reposts', 0))
            tl = p.get('total_likes', p.get('total_attitudes', 0))
            tc = p.get('total_comments', 0)
            first = p.get('first_collab', '')[:10]
            last = p.get('last_collab', '')[:10]
            dr = f" | {first}~{last}" if first else ""

            if ptype == 'weibo':
                section += f"#{i+1} {name} [{cat}] | {n(tv)} reposts | {n(tl)} attitudes | {n(tc)} comments{dr}\n"
                continue
            elif ptype == 'ig':
                section += f"#{i+1} {name} [{cat}] {len(items)} posts | {n(tl)} likes | {n(tc)} comments{dr}\n"
            else:
                section += f"#{i+1} {name} [{cat}] {len(items)} videos | {n(tv)} views | {n(tl)} likes | {n(tc)} comments{dr}\n"

            max_vids = TIER1_VIDS if i < TIER1_N else (TIER2_VIDS if i < TIER1_N + TIER2_N else 0)
            if max_vids == 0:
                continue

            shown = sorted(items, key=lambda x: x.get('view_count', 0), reverse=True)[:max_vids]
            for v in shown:
                title = (v.get('title') or '').replace('"', "'")[:TITLE_LEN]
                date = (v.get('published_at') or '?')[:10]
                vc = v.get('view_count', 0)
                lk = v.get('like_count', 0)
                if ptype == 'ig':
                    section += f'  "{title}" {date} | {n(lk)} likes | {n(v.get("comment_count", 0))} comments\n'
                else:
                    section += f'  "{title}" {date} | {n(vc)} views | {n(lk)} likes\n'
            if len(items) > max_vids:
                section += f'  (+{len(items)-max_vids} more)\n'

        all_sections.append(section)

    ctx = header
    ctx += "--- OVERVIEW ---\n"
    ctx += "\n".join(overview_lines)
    ctx += "\n\n"
    ctx += "".join(all_sections)

    return ctx


if __name__ == '__main__':
    ctx = build()
    chars = len(ctx)
    tokens_est = chars // 3
    print(f"Total: {chars:,} chars (~{tokens_est:,} tokens)")

    out_path = os.path.join(DOCS, 'all_context.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(ctx)
    print(f"Written to {out_path} ({os.path.getsize(out_path)/1024:.1f} KB)")

    # Section breakdown
    import re
    sections = re.findall(r'\n(\[[^\]]+\])', ctx)
    for s in sections:
        start = ctx.find(s)
        end = ctx.find('\n[', start + 1)
        if end < 0: end = len(ctx)
        print(f"  {s}: {end-start:,} chars")
