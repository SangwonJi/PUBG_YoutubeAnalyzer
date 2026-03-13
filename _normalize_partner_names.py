"""
Partner Name Normalizer: Uses GPT to build canonical name mappings,
then applies them to all data JSON files (merging duplicate partners).
"""
import json, os, time, glob
from pathlib import Path
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv(r"c:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\.env")
from openai import OpenAI
client = OpenAI(api_key=os.getenv("GPT_API_KEY", ""))
MODEL = "gpt-4o-mini"

DOCS = Path(r"c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs")
MAPPING_FILE = DOCS.parent / "_partner_name_mapping.json"

DATA_FILES = [
    "data.json", "ig_data.json", "weibo_data.json", "freefire_data.json",
    "yt_mena_data.json", "yt_turkey_data.json", "yt_indonesia_data.json",
    "yt_latam_data.json", "yt_cis_data.json", "yt_india_data.json",
    "yt_malaysia_data.json", "yt_pakistan_data.json", "yt_taiwan_data.json",
    "yt_thailand_data.json",
]

# ── Step 1: Collect all unique partner names ──
print("[1/4] Collecting all partner names...", flush=True)
all_names = set()
for fname in DATA_FILES:
    fp = DOCS / fname
    if not fp.exists():
        continue
    with open(fp, "r", encoding="utf-8") as f:
        data = json.load(f)
    for p in data:
        name = p.get("name", "").strip()
        if name:
            all_names.add(name)

print(f"  Found {len(all_names)} unique partner names", flush=True)

# ── Step 2: Pre-group by lowercase to find obvious case variants ──
lower_groups = defaultdict(set)
for name in all_names:
    lower_groups[name.lower()].add(name)

case_variant_groups = {k: v for k, v in lower_groups.items() if len(v) > 1}
print(f"  {len(case_variant_groups)} groups with case variants", flush=True)

# ── Step 3: Use GPT to find semantic duplicates and pick canonical names ──
print("[2/4] Asking GPT to normalize partner names...", flush=True)

# Load existing mapping if available
if MAPPING_FILE.exists():
    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    print(f"  Loaded existing mapping with {len(mapping)} entries", flush=True)
else:
    mapping = {}

names_list = sorted(all_names)
BATCH = 200
new_mappings = 0

for i in range(0, len(names_list), BATCH):
    batch = names_list[i:i+BATCH]
    already_mapped = all(n in mapping for n in batch)
    if already_mapped:
        continue

    prompt = f"""You are a data normalization expert. Below is a list of collaboration partner names from a PUBG MOBILE / Free Fire gaming collaboration database.

Many names refer to the SAME entity but are spelled differently (case differences, extra prefixes like "PUBG MOBILE X", language variants, abbreviations, etc.).

For each name, return the CANONICAL (standard) version. Rules:
1. Use the most common/official English name in UPPER CASE for brands/IPs (e.g., "BLACKPINK", "MCLAREN", "ALAN WALKER")
2. For people's names, use Title Case (e.g., "Lionel Messi", "Alan Walker")
3. Remove prefixes like "PUBG MOBILE X", "PUBGM x", "FF x" etc - just keep the partner name
4. If a name is already canonical, keep it as-is
5. Group names that clearly refer to the same entity

Return ONLY a JSON object mapping each input name to its canonical form.
Example: {{"BLACKPINK": "BLACKPINK", "BlackPink": "BLACKPINK", "Blackpink": "BLACKPINK", "PUBG MOBILE X BLACKPINK": "BLACKPINK"}}

Names:
{json.dumps(batch, ensure_ascii=False)}"""

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        result = json.loads(resp.choices[0].message.content)
        for name, canonical in result.items():
            if name in all_names:
                mapping[name] = canonical.strip()
                new_mappings += 1
        print(f"  Batch [{i+BATCH}/{len(names_list)}] - {len(result)} mappings", flush=True)
    except Exception as e:
        print(f"  Error batch {i}: {e}", flush=True)
        for name in batch:
            if name not in mapping:
                mapping[name] = name

    time.sleep(0.3)

# Save mapping
with open(MAPPING_FILE, "w", encoding="utf-8") as f:
    json.dump(mapping, f, ensure_ascii=False, indent=2)
print(f"  Saved mapping: {len(mapping)} entries ({new_mappings} new)", flush=True)

# ── Step 4: Apply mapping to all JSON files ──
print("[3/4] Applying normalization to JSON files...", flush=True)

total_merged = 0
for fname in DATA_FILES:
    fp = DOCS / fname
    if not fp.exists():
        continue

    with open(fp, "r", encoding="utf-8") as f:
        partners = json.load(f)

    # Apply name mapping
    for p in partners:
        old_name = p.get("name", "")
        if old_name in mapping:
            p["name"] = mapping[old_name]

    # Merge partners with the same canonical name
    merged = {}
    for p in partners:
        name = p["name"]
        if name not in merged:
            merged[name] = p
        else:
            existing = merged[name]
            existing["video_count"] = existing.get("video_count", 0) + p.get("video_count", 0)
            existing["post_count"] = existing.get("post_count", 0) + p.get("post_count", 0)
            existing["total_views"] = existing.get("total_views", 0) + p.get("total_views", 0)
            existing["total_likes"] = existing.get("total_likes", 0) + p.get("total_likes", 0)
            existing["total_comments"] = existing.get("total_comments", 0) + p.get("total_comments", 0)
            # Merge video lists
            existing_vids = existing.get("videos", [])
            new_vids = p.get("videos", [])
            seen_ids = {v.get("video_id") for v in existing_vids if v.get("video_id")}
            for v in new_vids:
                vid = v.get("video_id")
                if vid and vid not in seen_ids:
                    existing_vids.append(v)
                    seen_ids.add(vid)
                elif not vid:
                    existing_vids.append(v)
            existing["videos"] = existing_vids
            # Date ranges
            if p.get("first_collab") and (not existing.get("first_collab") or p["first_collab"] < existing["first_collab"]):
                existing["first_collab"] = p["first_collab"]
            if p.get("last_collab") and (not existing.get("last_collab") or p["last_collab"] > existing["last_collab"]):
                existing["last_collab"] = p["last_collab"]

            total_merged += 1

    result = sorted(merged.values(), key=lambda x: x.get("total_views", 0), reverse=True)
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)

    old_count = len(partners)
    new_count = len(result)
    if old_count != new_count:
        print(f"  {fname}: {old_count} -> {new_count} partners (merged {old_count - new_count})", flush=True)
    else:
        print(f"  {fname}: {new_count} partners (no merges)", flush=True)

print(f"\n[4/4] Done! Total merges: {total_merged}", flush=True)

# Summary of canonical name changes
changes = {k: v for k, v in mapping.items() if k != v}
print(f"\nName changes applied: {len(changes)}")
if changes:
    for old, new in sorted(changes.items(), key=lambda x: x[1])[:50]:
        print(f"  '{old}' -> '{new}'")
    if len(changes) > 50:
        print(f"  ... and {len(changes) - 50} more")
