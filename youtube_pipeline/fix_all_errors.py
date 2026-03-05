"""
Comprehensive data cleanup:
1. Fix incorrect partner names (GPT errors)
2. Merge duplicate partners
3. Remove PUBG self-events from collabs
"""

import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("=" * 70)
print("COMPREHENSIVE DATA CLEANUP")
print("=" * 70)

# ============================================================
# STEP 1: Fix incorrect partner names (GPT extraction errors)
# ============================================================
print("\n[STEP 1] Fixing incorrect partner names...")
print("-" * 50)

# 패턴: 제목에서 잘못 추출된 것들
INVALID_PARTNERS = [
    # "-"로 시작하는 것들 (X-Suit에서 잘못 추출)
    '-Suit', '-Suits', '-Suits Return', '-Suit Animated Wallpaper', '-Suit Gameplay Trailer',
    # "Exclusive"에서 잘못 추출
    'clusive', 'clusive Rewards', 'clusive Nordic Map', 'clusive Vehicles', 
    'clusive Standing Pose for the Gilt Set Serpengleam Set for a Limited Time',
    'clusive Rewards with the new Bonus Pass',
    # "Explained"에서 잘못 추출
    'plained', 'plained 1',
    # "Explore"에서 잘못 추출
    'plore', 'plore the new Themed Arena Maps Today', 'plore the Sect Atop the Mountain', 
    'plore the All',
    # "Explosive"에서 잘못 추출
    'plosive at the PUBG MOBILE',
    # 기타 잘못된 추출
    'xx', 'mas', 'DP', 'y Note9 PUBG MOBILE STAR CHALLENGE',
    'scopes in PUBG Mobile with BUSHKA', 'your UAZ PUBG MOBILE',
    'PUBG MOBILE x G', 'odus X PUBG MOBILE',
    'WOW Map PV',  # Peaky Blinders 영상에서 잘못 추출
]

fixed_invalid = 0
for invalid in INVALID_PARTNERS:
    cursor.execute("""
        UPDATE videos 
        SET is_collab = 0, collab_partner = NULL, collab_category = NULL, content_type = NULL
        WHERE collab_partner = ?
    """, (invalid,))
    if cursor.rowcount > 0:
        print(f"  Fixed '{invalid}': {cursor.rowcount} videos -> non-collab")
        fixed_invalid += cursor.rowcount

print(f"  Total fixed: {fixed_invalid}")

# ============================================================
# STEP 2: Remove PUBG self-events from collabs
# ============================================================
print("\n[STEP 2] Removing PUBG self-events from collabs...")
print("-" * 50)

PUBG_SELF_EVENTS = [
    'PUBG MOBILE India Tour',
    'PUBG MOBILE India Series', 
    'PUBG Mobile Pakistan Challenge',
    'PUBG MOBILE Club Open',
    'PUBG MOBILE Pro League',
    'PUBG MOBILE Star Challenge',
]

fixed_pubg = 0
for event in PUBG_SELF_EVENTS:
    cursor.execute("""
        UPDATE videos 
        SET is_collab = 0, collab_partner = NULL, collab_category = NULL
        WHERE collab_partner LIKE ?
    """, (f'%{event}%',))
    if cursor.rowcount > 0:
        print(f"  Removed '{event}': {cursor.rowcount} videos")
        fixed_pubg += cursor.rowcount

# 또한 collab_partner에 'PUBG'가 포함된 것들 확인
cursor.execute("""
    SELECT DISTINCT collab_partner, COUNT(*) 
    FROM videos 
    WHERE collab_partner LIKE '%PUBG%' AND is_collab = 1
    GROUP BY collab_partner
""")
pubg_partners = cursor.fetchall()
if pubg_partners:
    print(f"  Remaining PUBG-related partners (review needed):")
    for p, c in pubg_partners:
        print(f"    - '{p}' ({c} videos)")

print(f"  Total removed: {fixed_pubg}")

# ============================================================
# STEP 3: Merge duplicate partner names
# ============================================================
print("\n[STEP 3] Merging duplicate partner names...")
print("-" * 50)

# 통합 규칙: (old_names -> new_name, category)
MERGE_RULES = [
    # BABYMONSTER
    (['BABYMONSTER Live', 'BABYMONSTER Drip Dance'], 'BABYMONSTER', 'Artist'),
    # Metro
    (['Metro'], 'Metro Exodus', 'Game'),
    # 대소문자 통일
    (['BALENCIAGA'], 'Balenciaga', 'Brand'),
    (['ButterBear'], 'Butterbear', 'Brand'),
    (['Guinness World Records'], 'GUINNESS WORLD RECORD', 'Other'),
    # Dragon Ball 통합
    (['Dragon Ball'], 'Dragon Ball Super', 'Anime'),
    # MESSI 통합
    (['MESSI', 'Messi'], 'LIONEL MESSI', 'Brand'),
    # Spider-Man 통합  
    (['Spider-Man', 'Spider'], 'Spider-Man: No Way Home', 'Movie'),
    # Evangelion 통합
    (['Neon Genesis', 'EVANGELION', 'Evangelion'], 'Neon Genesis Evangelion', 'Anime'),
    # HONOR 분리 (SANParks에서 잘못 매칭된 것 방지)
]

merged_count = 0
for old_names, new_name, category in MERGE_RULES:
    for old_name in old_names:
        cursor.execute("""
            UPDATE videos 
            SET collab_partner = ?, collab_category = ?
            WHERE collab_partner = ?
        """, (new_name, category, old_name))
        if cursor.rowcount > 0:
            print(f"  Merged '{old_name}' -> '{new_name}': {cursor.rowcount} videos")
            merged_count += cursor.rowcount

print(f"  Total merged: {merged_count}")

# ============================================================
# STEP 4: Re-classify content types for fixed videos
# ============================================================
print("\n[STEP 4] Re-classifying content types...")
print("-" * 50)

# 콜라보로 남은 것들은 Collab
cursor.execute("UPDATE videos SET content_type = 'Collab' WHERE is_collab = 1")
print(f"  Set Collab type: {cursor.rowcount} videos")

# 비콜라보 중 content_type이 NULL인 것들 재분류
CATEGORIES = {
    'Esports': ['PMGC', 'PMWC', 'PMPL', 'PMSL', 'PMSC', 'PMCO', 'CHAMPIONSHIP', 'LEAGUE', 'TOURNAMENT', 'ESPORTS', 'GRAND FINALS'],
    'Chicko': ['CHICKO'],
    'Gilt': ['GILT'],
    'Shorts': ['#SHORTS', 'SHORTS'],
    'Update': ['UPDATE', 'PATCH', 'VERSION'],
    'WOW': ['WOW'],
    'Pass': ['PASS', 'ROYALE PASS'],
    'PDP': ['PDP'],
    'Event': ['EVENT', 'LIMITED', 'FESTIVAL'],
    'Mode': ['MODE', 'PAYLOAD', 'METRO', 'INFECTION'],
    'Map': ['MAP', 'ERANGEL', 'MIRAMAR', 'SANHOK'],
    'Promotional': ['TRAILER', 'TEASER'],
    'Guide': ['GUIDE', 'TUTORIAL', 'TIPS'],
    'Skin': ['SET', 'OUTFIT', 'SKIN', 'MYTHIC'],
}

cursor.execute("SELECT video_id, title FROM videos WHERE is_collab = 0 AND content_type IS NULL")
null_type_videos = cursor.fetchall()

reclassified = 0
for video_id, title in null_type_videos:
    title_upper = title.upper()
    assigned = False
    for cat, keywords in CATEGORIES.items():
        for kw in keywords:
            if kw in title_upper:
                cursor.execute("UPDATE videos SET content_type = ? WHERE video_id = ?", (cat, video_id))
                assigned = True
                reclassified += 1
                break
        if assigned:
            break
    if not assigned:
        cursor.execute("UPDATE videos SET content_type = 'Other' WHERE video_id = ?", (video_id,))
        reclassified += 1

print(f"  Re-classified: {reclassified} videos")

conn.commit()

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 70)
print("CLEANUP SUMMARY")
print("=" * 70)

cursor.execute("SELECT COUNT(*) FROM videos WHERE is_collab = 1")
collab_count = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM videos WHERE is_collab = 0")
non_collab_count = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(DISTINCT collab_partner) FROM videos WHERE is_collab = 1")
partner_count = cursor.fetchone()[0]

print(f"  Total videos: {collab_count + non_collab_count}")
print(f"  Collab videos: {collab_count}")
print(f"  Non-collab videos: {non_collab_count}")
print(f"  Unique partners: {partner_count}")

conn.close()
print("\nCleanup complete!")
