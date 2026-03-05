"""Final data summary after all fixes."""

import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("=" * 60)
print("FINAL DATA SUMMARY")
print("=" * 60)

# 1. 총 영상 수
cursor.execute("SELECT COUNT(*) FROM videos")
total = cursor.fetchone()[0]
print(f"\nTotal videos: {total}")

# 2. 콜라보 vs 비콜라보
cursor.execute("SELECT COUNT(*) FROM videos WHERE is_collab = 1")
collabs = cursor.fetchone()[0]
print(f"Collab videos: {collabs}")
print(f"Non-collab videos: {total - collabs}")

# 3. 콘텐츠 타입별 분류
print("\n" + "-" * 40)
print("Content Type Distribution:")
print("-" * 40)
cursor.execute("""
    SELECT content_type, COUNT(*), SUM(view_count) 
    FROM videos 
    GROUP BY content_type 
    ORDER BY COUNT(*) DESC
""")
for row in cursor.fetchall():
    content_type, count, views = row
    views = views or 0
    print(f"  {content_type or 'None':<15} {count:>6} videos  {views:>15,} views")

# 4. Top 20 콜라보 파트너
print("\n" + "-" * 40)
print("Top 20 Collab Partners (by views):")
print("-" * 40)
cursor.execute("""
    SELECT collab_partner, COUNT(*), SUM(view_count) as total_views
    FROM videos 
    WHERE is_collab = 1 AND collab_partner IS NOT NULL
    GROUP BY collab_partner 
    ORDER BY total_views DESC
    LIMIT 20
""")
for i, row in enumerate(cursor.fetchall(), 1):
    partner, count, views = row
    print(f"  {i:2}. {partner[:25]:<25} {count:>3} videos  {views:>15,} views")

# 5. 콜라보 카테고리별
print("\n" + "-" * 40)
print("Collab by Category:")
print("-" * 40)
cursor.execute("""
    SELECT collab_category, COUNT(*), SUM(view_count)
    FROM videos 
    WHERE is_collab = 1
    GROUP BY collab_category 
    ORDER BY COUNT(*) DESC
""")
for row in cursor.fetchall():
    cat, count, views = row
    views = views or 0
    print(f"  {cat or 'Unknown':<15} {count:>5} videos  {views:>15,} views")

# 6. 연도별 콜라보 추이
print("\n" + "-" * 40)
print("Collab Trend by Year:")
print("-" * 40)
cursor.execute("""
    SELECT strftime('%Y', published_at) as year, COUNT(*), SUM(view_count)
    FROM videos 
    WHERE is_collab = 1
    GROUP BY year 
    ORDER BY year
""")
for row in cursor.fetchall():
    year, count, views = row
    views = views or 0
    print(f"  {year}: {count:>4} collabs  {views:>15,} views")

conn.close()
print("\n" + "=" * 60)
