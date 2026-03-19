"""
Build maximum-density all-platform context for AI chatbot.
Budget: 385K chars. Claude Sonnet 4 fails at ~400K chars through AI Gateway.
Strategy: ALL partners get video details (top N per partner by views).
Numbers are exact comma-separated integers with clear word labels.
"""
import json, os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DOCS = 'docs'
CHAR_BUDGET = 385_000
MAX_VIDS_PER_PARTNER = 2

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
    ctx = "=== PUBG MOBILE & FREE FIRE COLLAB DATA (ALL REGIONS) ===\n"
    ctx += "16 sources: YouTube(13 regions) + Instagram + Weibo(China) + Free Fire\n"
    ctx += "All numbers are exact integers with commas (e.g. 1,376,211). Up to 30 top videos per partner.\n"
    ctx += "Partner format: #rank NAME [category] N videos | VIEWS views | LIKES likes | COMMENTS comments | PERIOD\n"
    ctx += "Video format: \"TITLE\" DATE | VIEWS views | LIKES likes\n\n"

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
            f"  {label}: {len(data)} partners, {n(total_items)} items, {n(total_v)} views, {n(total_l)} likes, {n(total_c)} comments"
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

            shown = sorted(items, key=lambda x: x.get('view_count', 0), reverse=True)[:MAX_VIDS_PER_PARTNER]
            for v in shown:
                title = (v.get('title') or '').replace('"', "'")[:20]
                date = (v.get('published_at') or '?')[:10]
                vc = v.get('view_count', 0)
                lk = v.get('like_count', 0)
                if ptype == 'ig':
                    section += f'  "{title}" {date} | {n(lk)} likes | {n(v.get("comment_count", 0))} comments\n'
                else:
                    section += f'  "{title}" {date} | {n(vc)} views | {n(lk)} likes\n'
            if len(items) > MAX_VIDS_PER_PARTNER:
                section += f'  (+{len(items)-MAX_VIDS_PER_PARTNER} more videos)\n'

        all_sections.append(section)

    ctx += "--- OVERVIEW ---\n"
    ctx += "\n".join(overview_lines)
    ctx += "\n\n"
    ctx += "".join(all_sections)

    raw_len = len(ctx)
    if raw_len > CHAR_BUDGET:
        ctx = ctx[:CHAR_BUDGET - 50] + "\n\n[... truncated to fit context limit]\n"

    return ctx, raw_len


if __name__ == '__main__':
    ctx, raw_len = build()
    chars = len(ctx)
    tokens_est = chars // 3
    print(f"Raw: {raw_len:,} chars | Final: {chars:,} chars (~{tokens_est:,} tokens)")
    print(f"Budget {CHAR_BUDGET:,}: {'OK' if raw_len <= CHAR_BUDGET else f'OVER by {raw_len - CHAR_BUDGET:,} - truncated'}")

    out_path = os.path.join(DOCS, 'all_context.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(ctx)
    print(f"Written to {out_path} ({os.path.getsize(out_path)/1024:.1f} KB)")
