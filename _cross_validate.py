"""Cross-validate all_context.txt against JSON source files."""
import json, os, sys, io, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DOCS = 'docs'

with open(os.path.join(DOCS, 'all_context.txt'), 'r', encoding='utf-8') as f:
    ctx = f.read()

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

errors = []
total_checks = 0

for label, fname, ptype in SOURCES:
    path = os.path.join(DOCS, fname)
    if not os.path.exists(path):
        errors.append(f"Missing file: {fname}")
        continue
    
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if isinstance(data, list):
        partners = data
    elif isinstance(data, dict):
        partners = list(data.values()) if all(isinstance(v, dict) for v in data.values()) else [data]
    
    section_match = re.search(rf'\[{re.escape(label)}\]([^\[]*)', ctx)
    if not section_match:
        errors.append(f"Section [{label}] not found in context")
        continue
    
    section_text = section_match.group(0)
    
    sorted_partners = sorted(partners, key=lambda x: sum(
        v.get('view_count', 0) for v in (x.get('videos') or x.get('items') or x.get('posts') or [])
    ), reverse=True)
    
    top5 = sorted_partners[:5]
    print(f"\n[{label}] — Top 5 cross-check:")
    
    for p in top5:
        name = (p.get('partner_name') or p.get('name') or '').strip()
        cat = (p.get('category') or p.get('partner_category') or '')
        items = p.get('videos') or p.get('items') or p.get('posts') or []
        total_v = sum(v.get('view_count', 0) for v in items)
        
        if not name:
            continue
        
        total_checks += 1
        
        name_in_ctx = name.upper() in section_text.upper()
        cat_pattern = rf'{re.escape(name)}\s*\[([^\]]+)\]'
        cat_match = re.search(cat_pattern, section_text, re.IGNORECASE)
        
        views_pattern = rf'{re.escape(name.upper())}[^\n]*?{total_v:,}\s*views'
        views_match = re.search(views_pattern, section_text, re.IGNORECASE)
        
        status_parts = []
        if not name_in_ctx:
            status_parts.append("NAME_MISSING")
        if cat_match and cat_match.group(1) != cat:
            status_parts.append(f"CAT_MISMATCH(json={cat}, ctx={cat_match.group(1)})")
            errors.append(f"[{label}] {name}: category mismatch — JSON='{cat}' vs CTX='{cat_match.group(1)}'")
        if name_in_ctx and not views_match and total_v > 0:
            views_in_section = re.search(rf'{re.escape(name.upper())}[^\n]*?([\d,]+)\s*views', section_text, re.IGNORECASE)
            if views_in_section:
                ctx_views = int(views_in_section.group(1).replace(',', ''))
                if ctx_views != total_v:
                    status_parts.append(f"VIEWS_MISMATCH(json={total_v:,}, ctx={ctx_views:,})")
                    errors.append(f"[{label}] {name}: views mismatch — JSON={total_v:,} vs CTX={ctx_views:,}")
                else:
                    status_parts.append("VIEWS_OK")
            else:
                status_parts.append("VIEWS_NOT_FOUND")
        elif views_match:
            status_parts.append("VIEWS_OK")
        
        status = " | ".join(status_parts) if status_parts else "ALL_OK"
        icon = "✅" if "MISMATCH" not in status and "MISSING" not in status else "❌"
        print(f"  {icon} {name} [{cat}] {len(items)} items, {total_v:,} views — {status}")

print(f"\n{'='*60}")
print(f"Total cross-checks: {total_checks}")
print(f"Errors found: {len(errors)}")
for e in errors:
    print(f"  ❌ {e}")
if not errors:
    print("🎯 ALL CROSS-CHECKS PASSED")
