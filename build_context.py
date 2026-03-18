"""
Build maximum-density all-platform context for AI chatbot.
Budget: 400K chars (~133K tokens). Claude Sonnet supports 200K tokens.
Strategy:
  - ALL partners: summary with views, likes, comments, date range
  - Top 40 partners/region: ALL video titles + dates + views
  - Partners 41-100: top 3 videos only
  - Partners 101+: summary only
  - Weibo: partner summary + top 3 post dates for top 40
  - Titles: 40 chars (Tier1), 35 chars (Tier2)
"""
import json, os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DOCS = 'docs'
CHAR_BUDGET = 400_000
TIER1_N = 40
TIER2_N = 100
MAX_VIDS_PER_PARTNER = 30

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
    ('YouTube Korea', 'yt_korea_data.json', 'yt'),
    ('YouTube Japan', 'yt_japan_data.json', 'yt'),
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
    ctx += "16 sources: YouTube(13regions) + Instagram + Weibo(China) + Free Fire\n"
    ctx += "v=views L=likes C=comments. Tier1(top40): all videos. Tier2(41-100): top3 videos.\n\n"

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
            f"  {label}: {len(data)}p {total_items}items {fmt(total_v)}v {fmt(total_l)}L {fmt(total_c)}C"
        )

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
                if i < TIER1_N and items:
                    top_posts = sorted(items, key=lambda x: x.get('reposts', 0), reverse=True)[:3]
                    for v in top_posts:
                        date = (v.get('created_at') or '?')[:10]
                        rp = v.get('reposts', 0)
                        at = v.get('attitudes', 0)
                        section += f' {date} {fmt(rp)}rp {fmt(at)}att\n'
                continue

            if i < TIER1_N:
                shown = sorted(items, key=lambda x: x.get('view_count', 0), reverse=True)[:MAX_VIDS_PER_PARTNER]
                for v in shown:
                    title = (v.get('title') or '')[:40]
                    date = (v.get('published_at') or '?')[:10]
                    vc = v.get('view_count', 0)
                    section += f' "{title}" {date} {fmt(vc)}v\n'
                if len(items) > MAX_VIDS_PER_PARTNER:
                    section += f' (+{len(items)-MAX_VIDS_PER_PARTNER} more videos)\n'
            elif i < TIER2_N:
                top_vids = sorted(items, key=lambda x: x.get('view_count', 0), reverse=True)[:3]
                for v in top_vids:
                    title = (v.get('title') or '')[:35]
                    date = (v.get('published_at') or '?')[:10]
                    vc = v.get('view_count', 0)
                    section += f' "{title}" {date} {fmt(vc)}v\n'

        all_sections.append(section)

    ctx += "--- OVERVIEW ---\n"
    ctx += "\n".join(overview_lines)
    ctx += "\n\n"
    ctx += "".join(all_sections)

    if len(ctx) > CHAR_BUDGET:
        ctx = ctx[:CHAR_BUDGET - 50] + "\n\n[... truncated to fit context limit]\n"

    return ctx


if __name__ == '__main__':
    ctx = build()
    chars = len(ctx)
    tokens_est = chars // 3
    print(f"Total: {chars:,} chars (~{tokens_est:,} tokens)")
    print(f"Budget {CHAR_BUDGET:,}: {'OK' if chars <= CHAR_BUDGET else 'OVER - truncated'}")

    out_path = os.path.join(DOCS, 'all_context.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(ctx)
    print(f"Written to {out_path} ({os.path.getsize(out_path)/1024:.1f} KB)")
