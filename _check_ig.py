import csv

path = r'c:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\dataset_instagram-post-scraper_2026-03-10_07-00-00-573.csv'
with open(path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"Total posts: {len(rows)}")

owners = {}
for r in rows:
    o = r.get('ownerUsername', '')
    owners[o] = owners.get(o, 0) + 1
print("\nOwners:")
for o, c in sorted(owners.items(), key=lambda x: -x[1])[:10]:
    print(f"  {o}: {c}")

dates = sorted([r.get('timestamp', '')[:10] for r in rows if r.get('timestamp')])
print(f"\nDate range: {dates[0]} ~ {dates[-1]}")

sample = rows[0]
print(f"\nSample URL: {sample.get('url', '')[:80]}")
print(f"Sample likes: {sample.get('likesCount')}")
print(f"Sample comments: {sample.get('commentsCount')}")
print(f"Sample timestamp: {sample.get('timestamp')}")
print(f"Sample caption (50 chars): {sample.get('caption', '')[:50]}")

# Check ig_collab_report
report_path = r'c:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\ig_collab_report.csv'
with open(report_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    report = list(reader)
print(f"\n--- Collab Report ---")
print(f"Total partners: {len(report)}")
total_posts = sum(int(r.get('posts', 0)) for r in report)
total_likes = sum(int(r.get('total_likes', 0)) for r in report)
print(f"Total collab posts: {total_posts}")
print(f"Total likes: {total_likes:,}")
cats = {}
for r in report:
    c = r.get('category', 'Unknown')
    cats[c] = cats.get(c, 0) + 1
print(f"Categories: {cats}")
