"""
ULTIMATE QA: 100,000% Verification Script
Checks ALL JSON files, all_context.txt, cross-validates data.
"""
import json, os, sys, io, re
from collections import Counter, defaultdict
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DOCS = 'docs'
VALID_CATEGORIES = {
    'Animation', 'Artist', 'Vehicle', 'Film', 'Fashion', 'Character', 'Game', 'Other', 'Food', 'Sports'
}

KNOWN_MISCLASS = {
    'LIONEL MESSI': {'Vehicle'},
    'LIVERPOOL FC': {'Vehicle'},
    'BALENCIAGA': {'Vehicle'},
    'BE@RBRICK': {'Vehicle'},
    'SAMSUNG': {'Vehicle'},
    'QUALCOMM': {'Vehicle'},
    'VIVO': {'Vehicle'},
    'KFC': {'Vehicle'},
    'BURGER KING': {'Vehicle'},
    'PUBG MOBILE': {'Vehicle'},
    'PEAKY BLINDERS': {'Vehicle', 'Other'},
    'VENOM': {'Vehicle', 'Other'},
    'JVKE': {'Vehicle', 'Other'},
    'AESPA': {'Vehicle'},
    'DEMON SLAYER': {'Vehicle', 'Other'},
    'KIMETSU NO YAIBA': {'Vehicle', 'Other'},
}

VEHICLE_BRANDS = {
    'BUGATTI', 'KOENIGSEGG', 'LAMBORGHINI', 'MCLAREN', 'PAGANI', 'DODGE',
    'MASERATI', 'ASTON MARTIN', 'PORSCHE', 'BMW', 'MERCEDES', 'FERRARI',
    'SHELBY', 'DUCATI', 'FORD', 'TESLA', 'NISSAN', 'TOYOTA', 'HONDA',
    'YAMAHA', 'KTM', 'HARLEY', 'JEEP', 'MINI COOPER', 'ROLLS ROYCE',
    'BENTLEY', 'RANGE ROVER', 'LAND ROVER',
}

errors = []
warnings = []
stats = defaultdict(int)

# ======= PHASE 1: JSON Category Integrity =======
print("=" * 70)
print("PHASE 1: JSON Category Integrity Check")
print("=" * 70)

json_files = [f for f in os.listdir(DOCS) if f.endswith('.json') and not f.startswith('.')]
print(f"Found {len(json_files)} JSON files")

all_partners_by_file = {}
total_partners = 0
total_items = 0
category_counts = Counter()
vehicle_nonvehicle = []

for jf in sorted(json_files):
    path = os.path.join(DOCS, jf)
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if isinstance(data, dict):
        data = list(data.values()) if all(isinstance(v, dict) for v in data.values()) else [data]
    
    partners_in_file = []
    for p in data:
        if not isinstance(p, dict):
            continue
        name = (p.get('partner_name') or p.get('name') or p.get('channel_name') or '').strip().upper()
        cat = (p.get('category') or p.get('partner_category') or '').strip()
        
        if not name:
            continue
        
        partners_in_file.append((name, cat))
        total_partners += 1
        category_counts[cat] += 1
        
        items = p.get('videos') or p.get('items') or p.get('posts') or []
        total_items += len(items)
        
        # Check 1: Valid category
        if cat and cat not in VALID_CATEGORIES:
            errors.append(f"[{jf}] {name}: unknown category '{cat}'")
        
        # Check 2: Known misclassifications still present?
        if name in KNOWN_MISCLASS and cat in KNOWN_MISCLASS[name]:
            errors.append(f"[{jf}] {name}: STILL has wrong category '{cat}' (should NOT be {KNOWN_MISCLASS[name]})")
        
        # Check 3: Non-vehicle in Vehicle category
        if cat == 'Vehicle' and name not in VEHICLE_BRANDS:
            is_vehicle_keyword = any(kw in name for kw in ['CAR', 'AUTO', 'MOTOR', 'RACING'])
            if not is_vehicle_keyword:
                vehicle_nonvehicle.append(f"[{jf}] {name}")
        
        # Check 4: Messi/Liverpool specifically Other
        if name in ('LIONEL MESSI', 'LIVERPOOL FC', 'LIVERPOOL') and cat != 'Other':
            errors.append(f"[{jf}] {name}: category is '{cat}' but should be 'Other'")
        
        stats['partners'] += 1
    
    all_partners_by_file[jf] = partners_in_file
    print(f"  {jf}: {len(partners_in_file)} partners, {len(data)} entries")

print(f"\nTotal: {total_partners} partner entries, {total_items} items")
print(f"Categories: {dict(category_counts)}")

if vehicle_nonvehicle:
    print(f"\n⚠ Non-standard Vehicle entries ({len(vehicle_nonvehicle)}):")
    for v in vehicle_nonvehicle:
        warnings.append(f"Suspicious Vehicle: {v}")
        print(f"  {v}")

# ======= PHASE 2: all_context.txt Cross-Validation =======
print("\n" + "=" * 70)
print("PHASE 2: all_context.txt Cross-Validation")
print("=" * 70)

ctx_path = os.path.join(DOCS, 'all_context.txt')
with open(ctx_path, 'r', encoding='utf-8') as f:
    ctx = f.read()

ctx_len = len(ctx)
print(f"Context length: {ctx_len:,} chars")

# Check section count
sections = re.findall(r'\n\[([^\]]+)\]', ctx)
print(f"Sections found: {len(sections)}")
for s in sections:
    print(f"  [{s}]")

# Check for known corrections in context
print("\nKnown correction checks:")
checks = [
    ('LIONEL MESSI', 'Other', ['Vehicle']),
    ('LIVERPOOL FC', 'Other', ['Vehicle', 'Artist']),
    ('BALENCIAGA', 'Fashion', ['Vehicle']),
    ('SAMSUNG', 'Other', ['Vehicle']),
    ('BUGATTI', 'Vehicle', []),
    ('KOENIGSEGG', 'Vehicle', []),
    ('PEAKY BLINDERS', 'Film', ['Vehicle', 'Other']),
    ('JVKE', 'Artist', ['Vehicle', 'Other']),
    ('AESPA', 'Artist', ['Vehicle']),
    ('DEMON SLAYER', 'Animation', ['Vehicle', 'Other']),
]

for name, expected_cat, wrong_cats in checks:
    pattern = rf'#{1,3}\d*\s*{re.escape(name)}\s*\[([^\]]+)\]'
    matches = re.findall(pattern, ctx, re.IGNORECASE)
    if matches:
        unique_cats = set(matches)
        all_correct = all(c == expected_cat for c in matches)
        if all_correct:
            print(f"  ✅ {name}: [{expected_cat}] x{len(matches)} — correct")
        else:
            for c in unique_cats:
                if c != expected_cat:
                    errors.append(f"all_context.txt: {name} has [{c}] instead of [{expected_cat}]")
            print(f"  ❌ {name}: found {unique_cats} — expected [{expected_cat}]")
    else:
        print(f"  ⚠ {name}: not found in context (might not be top-tier)")

# Check number formatting (no bare numbers without commas for large values)
bare_numbers = re.findall(r'(?<!\d)(\d{4,})(?:views|likes|comments|items)', ctx)
if bare_numbers:
    warnings.append(f"Found {len(bare_numbers)} bare numbers without separators")
    print(f"\n⚠ Bare numbers: {bare_numbers[:10]}")

# Check for embedded double quotes in titles
dq_count = ctx.count('"')
print(f"\nEmbedded double quotes in context: {dq_count}")
if dq_count > 0:
    warnings.append(f"{dq_count} embedded double quotes found")

# ======= PHASE 3: Data Consistency (JSON vs Context numbers) =======
print("\n" + "=" * 70)
print("PHASE 3: Top Partner Number Cross-Check (JSON vs Context)")
print("=" * 70)

# Check Global YouTube (data.json)
data_path = os.path.join(DOCS, 'data.json')
if os.path.exists(data_path):
    with open(data_path, 'r', encoding='utf-8') as f:
        gdata = json.load(f)
    
    if isinstance(gdata, list):
        gdata_sorted = sorted(gdata, key=lambda x: sum(v.get('view_count', 0) for v in (x.get('videos') or [])), reverse=True)
    else:
        gdata_sorted = sorted(gdata.values(), key=lambda x: sum(v.get('view_count', 0) for v in (x.get('videos') or [])), reverse=True)
    
    print("Top 5 Global YouTube partners (from JSON):")
    for i, p in enumerate(gdata_sorted[:5]):
        name = (p.get('partner_name') or p.get('name') or '').strip()
        cat = (p.get('category') or p.get('partner_category') or '')
        vids = p.get('videos') or []
        total_v = sum(v.get('view_count', 0) for v in vids)
        total_l = sum(v.get('like_count', 0) for v in vids)
        print(f"  #{i+1} {name} [{cat}] {len(vids)} vids | {total_v:,} views | {total_l:,} likes")
        
        # Cross-check in context
        ctx_pattern = rf'{re.escape(name.upper())}\s*\[[^\]]+\]\s*(\d+)\s*videos?\s*\|\s*([\d,]+)\s*views'
        ctx_match = re.search(ctx_pattern, ctx, re.IGNORECASE)
        if ctx_match:
            ctx_vids = int(ctx_match.group(1))
            ctx_views = int(ctx_match.group(2).replace(',', ''))
            if ctx_vids != len(vids):
                errors.append(f"Global {name}: JSON has {len(vids)} videos but context has {ctx_vids}")
                print(f"    ❌ Video count mismatch: JSON={len(vids)}, CTX={ctx_vids}")
            elif ctx_views != total_v:
                errors.append(f"Global {name}: JSON views={total_v:,} but context views={ctx_views:,}")
                print(f"    ❌ View count mismatch: JSON={total_v:,}, CTX={ctx_views:,}")
            else:
                print(f"    ✅ Matches context")
        else:
            print(f"    ⚠ Not found in context for cross-check")

# ======= PHASE 4: Vehicle Category Deep Check =======
print("\n" + "=" * 70)
print("PHASE 4: Vehicle Category Deep Audit (ALL files)")
print("=" * 70)

vehicle_partners_all = defaultdict(list)
for jf, partners in all_partners_by_file.items():
    for name, cat in partners:
        if cat == 'Vehicle':
            vehicle_partners_all[name].append(jf)

print(f"Total unique Vehicle partners: {len(vehicle_partners_all)}")
for name in sorted(vehicle_partners_all.keys()):
    is_known = name in VEHICLE_BRANDS
    files = vehicle_partners_all[name]
    status = "✅" if is_known else "⚠ REVIEW"
    print(f"  {status} {name} [{len(files)} files]")
    if not is_known:
        warnings.append(f"Vehicle partner '{name}' not in known brands list — review needed")

# ======= PHASE 5: Messi/Liverpool Specific Deep Check =======
print("\n" + "=" * 70)
print("PHASE 5: Messi & Liverpool Specific Check (ALL JSON files)")
print("=" * 70)

for target in ['LIONEL MESSI', 'LIVERPOOL FC', 'LIVERPOOL']:
    found = []
    for jf, partners in all_partners_by_file.items():
        for name, cat in partners:
            if target in name:
                found.append((jf, name, cat))
    if found:
        print(f"\n  {target}:")
        for jf, name, cat in found:
            ok = cat == 'Other'
            print(f"    {'✅' if ok else '❌'} {jf}: [{cat}]")
            if not ok:
                errors.append(f"{jf}: {name} still has category '{cat}' instead of 'Other'")
    else:
        print(f"\n  {target}: not found in any file")

# ======= SUMMARY =======
print("\n" + "=" * 70)
print("FINAL SUMMARY")
print("=" * 70)
print(f"Total JSON files checked: {len(json_files)}")
print(f"Total partner entries: {total_partners}")
print(f"Total items: {total_items}")
print(f"Context file: {ctx_len:,} chars, {len(sections)} sections")
print(f"\n❌ ERRORS: {len(errors)}")
for e in errors:
    print(f"  {e}")
print(f"\n⚠ WARNINGS: {len(warnings)}")
for w in warnings:
    print(f"  {w}")

if len(errors) == 0:
    print("\n🎯 ALL CHECKS PASSED — DATA INTEGRITY 100%")
else:
    print(f"\n🚨 {len(errors)} ERRORS FOUND — NEEDS FIXING")
