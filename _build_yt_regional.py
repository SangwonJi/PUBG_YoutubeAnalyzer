"""
YouTube Regional Channels: classify collabs via GPT + generate dashboard JSON.
Regions: MENA, Turkey, Indonesia, LATAM
"""
import csv, json, os, re, time, sys
from collections import defaultdict
from pathlib import Path

INPUT_CSV = Path(r"c:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\output\yt_regional_all_videos.csv")
DOCS = Path(r"c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs")

from dotenv import load_dotenv
load_dotenv(r"c:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\.env")
from openai import OpenAI
client = OpenAI(api_key=os.getenv("GPT_API_KEY", ""))
MODEL = "gpt-4o-mini"

REGIONS = {
    'MENA': {'name': 'PUBG MOBILE MENA', 'dateRange': '2019 - 2026'},
    'Turkey': {'name': 'PUBG MOBILE Turkey', 'dateRange': '2019 - 2026'},
    'Indonesia': {'name': 'PUBG MOBILE Indonesia', 'dateRange': '2025 - 2026'},
    'LATAM': {'name': 'PUBG MOBILE LATAM', 'dateRange': '2025 - 2026'},
}

# ---------- Load data ----------
print("[1/4] Loading data...", flush=True)
with open(INPUT_CSV, 'r', encoding='utf-8-sig') as f:
    all_rows = list(csv.DictReader(f))
print(f"  {len(all_rows)} total videos", flush=True)

# ---------- GPT Collab Classification ----------
print("[2/4] Classifying collabs via GPT...", flush=True)

COLLAB_SYSTEM = """You classify PUBG Mobile YouTube video titles as collaboration or not.
A collaboration video features a specific external brand, IP, celebrity, artist, game, or creator.
Regular updates, events, esports, tutorials, patch notes, or seasonal content are NOT collaborations.

For each title, respond with a JSON object:
{"results": [{"collab": true/false, "partner": "PartnerName or empty", "category": "Brand/Anime/Artist/Game/Movie/IP/Entertainment/Creator/Other or empty"}]}

Only mark as collab if there's a clear external partner."""

BATCH = 60
classified = {}

for i in range(0, len(all_rows), BATCH):
    batch = all_rows[i:i+BATCH]
    titles = [f"{j+1}. [{r['region']}] {r['title'][:100]}" for j, r in enumerate(batch)]
    prompt = "\n".join(titles)

    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": COLLAB_SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=3000,
                response_format={"type": "json_object"},
            )
            result = json.loads(resp.choices[0].message.content)
            results = result.get("results", [])
            if not isinstance(results, list):
                results = list(result.values())[0] if result else []
            
            for j, r in enumerate(batch):
                if j < len(results):
                    entry = results[j]
                    if isinstance(entry, dict):
                        classified[r['video_id']] = {
                            'is_collab': entry.get('collab', False),
                            'partner': entry.get('partner', ''),
                            'category': entry.get('category', ''),
                        }
                    else:
                        classified[r['video_id']] = {'is_collab': False, 'partner': '', 'category': ''}
                else:
                    classified[r['video_id']] = {'is_collab': False, 'partner': '', 'category': ''}
            break
        except Exception as e:
            if attempt == 2:
                print(f"  Error batch {i}: {e}", flush=True)
                for r in batch:
                    classified[r['video_id']] = {'is_collab': False, 'partner': '', 'category': ''}
            else:
                time.sleep(2)

    done = min(i + BATCH, len(all_rows))
    if done % 500 == 0 or done == len(all_rows):
        print(f"  [{done}/{len(all_rows)}]", flush=True)

# ---------- Non-collab content category classification ----------
print("[3/4] Classifying non-collab content categories...", flush=True)

non_collab_rows = [r for r in all_rows if not classified.get(r['video_id'], {}).get('is_collab', False)]
print(f"  {len(non_collab_rows)} non-collab videos to classify", flush=True)

NC_SYSTEM = """Classify PUBG Mobile YouTube videos into one category:
Update, Esports, Event, Promotion, Creative, Tutorial, Livestream, Shorts, Community, Announcement, Festive, Other.
Reply with JSON: {"c":["cat1","cat2",...]}"""

nc_categories = {}
for i in range(0, len(non_collab_rows), BATCH):
    batch = non_collab_rows[i:i+BATCH]
    titles = [f"{j+1}. {r['title'][:80]}" for j, r in enumerate(batch)]
    prompt = "\n".join(titles)

    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": NC_SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=1500,
                response_format={"type": "json_object"},
            )
            result = json.loads(resp.choices[0].message.content)
            cats = list(result.values())[0] if isinstance(result, dict) else ["Other"] * len(batch)
            if not isinstance(cats, list):
                cats = ["Other"] * len(batch)
            while len(cats) < len(batch):
                cats.append("Other")
            for j, r in enumerate(batch):
                nc_categories[r['video_id']] = str(cats[j]) if j < len(cats) else "Other"
            break
        except Exception as e:
            if attempt == 2:
                print(f"  Error batch {i}: {e}", flush=True)
                for r in batch:
                    nc_categories[r['video_id']] = "Other"
            else:
                time.sleep(1)

    done = min(i + BATCH, len(non_collab_rows))
    if done % 500 == 0 or done == len(non_collab_rows):
        print(f"  [{done}/{len(non_collab_rows)}]", flush=True)

# ---------- Generate JSON per region ----------
print("[4/4] Generating JSON files...", flush=True)

for region, info in REGIONS.items():
    region_rows = [r for r in all_rows if r['region'] == region]
    
    # Collab data
    collab_partners = defaultdict(list)
    noncollab_posts = []
    
    for r in region_rows:
        vid = r['video_id']
        cl = classified.get(vid, {})
        
        post = {
            'video_id': vid,
            'url': f"https://www.youtube.com/watch?v={vid}",
            'title': r['title'][:120],
            'published_at': r['published_at'][:10],
            'view_count': int(r.get('views', 0) or 0),
            'like_count': int(r.get('likes', 0) or 0),
            'comment_count': int(r.get('comments', 0) or 0),
            'thumbnail': r.get('thumbnail_url', ''),
        }
        
        if cl.get('is_collab') and cl.get('partner'):
            collab_partners[cl['partner']].append({**post, 'category': cl.get('category', 'Other')})
        else:
            post['content_category'] = nc_categories.get(vid, 'Other')
            noncollab_posts.append(post)
    
    # Build collab JSON (same structure as data.json)
    data_list = []
    for partner, posts in collab_partners.items():
        cat = posts[0].get('category', 'Other') if posts else 'Other'
        total_views = sum(p['view_count'] for p in posts)
        total_likes = sum(p['like_count'] for p in posts)
        total_comments = sum(p['comment_count'] for p in posts)
        data_list.append({
            'name': partner,
            'category': cat,
            'post_count': len(posts),
            'total_views': total_views,
            'total_likes': total_likes,
            'total_comments': total_comments,
            'video_count': len(posts),
            'first_collab': min((p['published_at'] for p in posts if p['published_at']), default=''),
            'videos': sorted(posts, key=lambda x: x.get('published_at', ''), reverse=True),
        })
    data_list.sort(key=lambda x: x['total_views'], reverse=True)
    
    # Build non-collab JSON
    cat_groups = defaultdict(list)
    for p in noncollab_posts:
        cat_groups[p.get('content_category', 'Other')].append(p)
    
    content_types = []
    for cat, videos in sorted(cat_groups.items(), key=lambda x: -len(x[1])):
        content_types.append({
            'name': cat,
            'video_count': len(videos),
            'total_views': sum(v['view_count'] for v in videos),
            'videos': sorted(videos, key=lambda x: x.get('published_at', ''), reverse=True),
        })
    
    others = {
        'video_count': len(noncollab_posts),
        'total_views': sum(p['view_count'] for p in noncollab_posts),
        'total_likes': sum(p['like_count'] for p in noncollab_posts),
        'total_comments': sum(p['comment_count'] for p in noncollab_posts),
        'content_types': content_types,
    }
    
    region_key = region.lower()
    data_file = DOCS / f"yt_{region_key}_data.json"
    others_file = DOCS / f"yt_{region_key}_others.json"
    
    with open(data_file, 'w', encoding='utf-8') as f:
        json.dump(data_list, f, ensure_ascii=False)
    with open(others_file, 'w', encoding='utf-8') as f:
        json.dump(others, f, ensure_ascii=False)
    
    print(f"  {region}: {len(data_list)} partners, {sum(p['video_count'] for p in data_list)} collab videos, {len(noncollab_posts)} non-collab", flush=True)

print("\nDone!", flush=True)
