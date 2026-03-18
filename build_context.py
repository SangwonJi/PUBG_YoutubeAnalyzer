"""
Build compressed all-platform context for AI chatbot.
Strategy:
  - ALL partners get 1-line summary (name, category, count, views, likes, comments)
  - Top 50 partners per region: include individual video titles + dates + views
  - Partners ranked 51+: summary only (no individual videos)
  - Weibo: partner summary only (post text is Chinese, too large)
  - Individual videos: title(50 chars) + date + views only (skip likes/comments)
Target: ~150K tokens (450K chars)
"""
import json, os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DOCS = 'docs'
VIDEO_DETAIL_TOP_N = 50

SOURCES = [
    ('YouTube Global (PUBG MOBILE)', 'pubgm_data.json', 'yt'),
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
    ('Instagram (PUBG MOBILE)', 'ig_data.json', 'ig'),
    ('Weibo (和平精英/Game For Peace China)', 'weibo_data.json', 'weibo'),
    ('YouTube Free Fire', 'freefire_data.json', 'yt'),
]

def fmt(n):
    if n >= 1e6: return f"{n/1e6:.1f}M"
    if n >= 1e3: return f"{n/1e3:.1f}K"
    return str(int(n))

def build():
    ctx = "=== PUBG MOBILE & FREE FIRE COLLAB DATA (ALL REGIONS) ===\n"
    ctx += "YouTube(11 regions), Instagram(Global), Weibo(China), Free Fire YouTube\n"
    ctx += "v=views L=likes C=comments. Top 50 partners/region include video details.\n\n"

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

        overview_lines.append(f"  {label}: {len(data)}p {total_items}items {fmt(total_v)}v {fmt(total_l)}L {fmt(total_c)}C")

        section = f"\n[{label}] {len(data)}p {total_items}items {fmt(total_v)}v\n"

        for i, p in enumerate(data):
            name = p.get('partner_name') or p.get('name') or '?'
            cat = p.get('category', '?')
            items = p.get('videos', p.get('posts', []))
            tv = p.get('total_views', p.get('total_reposts', 0))
            tl = p.get('total_likes', p.get('total_attitudes', 0))
            tc = p.get('total_comments', 0)
            first = p.get('first_collab', '')[:7]
            last = p.get('last_collab', '')[:7]
            dr = f" {first}~{last}" if first else ""

            section += f"#{i+1} {name} [{cat}] {len(items)}편 {fmt(tv)}v {fmt(tl)}L {fmt(tc)}C{dr}\n"

            if ptype == 'weibo':
                continue
            if i >= VIDEO_DETAIL_TOP_N:
                continue

            for v in items:
                title = (v.get('title') or v.get('text_preview') or '')[:50]
                date = (v.get('published_at') or v.get('created_at') or '?')[:10]
                vc = v.get('view_count', v.get('reposts', 0))
                section += f' "{title}" {date} {fmt(vc)}v\n'

        all_sections.append(section)

    ctx += "--- OVERVIEW ---\n"
    ctx += "\n".join(overview_lines)
    ctx += "\n\n"
    ctx += "".join(all_sections)

    return ctx


if __name__ == '__main__':
    ctx = build()
    chars = len(ctx)
    tokens_est = chars // 3
    print(f"Total context: {chars:,} chars (~{tokens_est:,} tokens)")
    print(f"Claude 200K limit: {'OK' if tokens_est < 180000 else 'OVER'}")

    out_path = os.path.join(DOCS, 'all_context.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(ctx)
    print(f"Written to {out_path} ({os.path.getsize(out_path)/1024:.1f} KB)")

    print(f"\nFirst 500 chars:\n{ctx[:500]}")
    print(f"\n... last 200 chars:\n{ctx[-200:]}")
