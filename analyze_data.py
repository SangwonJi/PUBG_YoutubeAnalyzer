import json, os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

docs = 'docs'
files = {
    'YouTube Global': 'pubgm_data.json',
    'YouTube MENA': 'yt_mena_data.json',
    'YouTube India': 'yt_india_data.json',
    'YouTube Indonesia': 'yt_indonesia_data.json',
    'YouTube LATAM': 'yt_latam_data.json',
    'YouTube Malaysia': 'yt_malaysia_data.json',
    'YouTube Pakistan': 'yt_pakistan_data.json',
    'YouTube Taiwan': 'yt_taiwan_data.json',
    'YouTube Thailand': 'yt_thailand_data.json',
    'YouTube Turkey': 'yt_turkey_data.json',
    'YouTube CIS': 'yt_cis_data.json',
    'Instagram': 'ig_data.json',
    'Weibo': 'weibo_data.json',
    'Free Fire YouTube': 'freefire_data.json',
}

total_partners = 0
total_videos = 0
total_chars = 0

for label, fname in files.items():
    path = os.path.join(docs, fname)
    if not os.path.exists(path):
        print(f"{label:25s} | FILE NOT FOUND: {fname}")
        continue
    size_kb = os.path.getsize(path) / 1024
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    partners = len(data)
    vids = sum(len(p.get('videos', p.get('posts', []))) for p in data)
    total_partners += partners
    total_videos += vids
    
    def fmt(n):
        if n >= 1e6: return f"{n/1e6:.1f}M"
        if n >= 1e3: return f"{round(n/1e3)}K"
        return str(n)
    
    ctx = f"[{label}] {partners} partners\n"
    for i, p in enumerate(data):
        name = p.get('partner_name') or p.get('name') or '?'
        items = p.get('videos', p.get('posts', []))
        tv = p.get('total_views', p.get('total_reposts', 0))
        tl = p.get('total_likes', p.get('total_attitudes', 0))
        tc = p.get('total_comments', 0)
        ctx += f"#{i+1} {name} [{p.get('category','?')}] {len(items)}편 {fmt(tv)}v {fmt(tl)}L {fmt(tc)}C\n"
        for v in items:
            title = (v.get('title') or v.get('text_preview') or '')[:60]
            date = (v.get('published_at') or v.get('created_at') or '?')[:10]
            vc = v.get('view_count', v.get('reposts', 0))
            lc = v.get('like_count', v.get('attitudes', 0))
            cc = v.get('comment_count', v.get('comments', 0))
            ctx += f' "{title}" {date} {fmt(vc)}v {fmt(lc)}L {fmt(cc)}C\n'
    
    total_chars += len(ctx)
    print(f"{label:25s} | {partners:4d} partners | {vids:5d} videos | {size_kb:8.1f} KB | ctx: {len(ctx):,} chars")

print(f"\n{'TOTAL':25s} | {total_partners:4d} partners | {total_videos:5d} videos | context: {total_chars:,} chars")
print(f"Estimated tokens: ~{total_chars // 3:,}")
print(f"Claude Sonnet 4 context window: 200,000 tokens")
print(f"Fits in context: {'YES' if total_chars // 3 < 180000 else 'NO - need compression'}")
