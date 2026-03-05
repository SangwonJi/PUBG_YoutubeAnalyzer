"""Final cleanup: Merge duplicates and remove remaining invalid entries."""

import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("=== FINAL CLEANUP ===\n")

# 1. 중복 파트너 통합
MERGES = [
    # (old, new, category)
    ('Lionel Messi', 'LIONEL MESSI', 'Brand'),
    ('ARCANE', 'Arcane', 'IP'),
    ('KAROLG', 'Karol G', 'Artist'),
    ('RE2', 'Resident Evil 2', 'Game'),
    ('TheBushka', 'Bushka', 'IP'),
    ('BRUCELEE', 'Bruce Lee', 'IP'),
    ('BRUCELEE Collection', 'Bruce Lee', 'IP'),
    ('BRUCELEE Official Trailer', 'Bruce Lee', 'IP'),
    ('Tesla', 'TESLA', 'Brand'),
    ("Tesla's CYBERTRUCK", 'TESLA', 'Brand'),
    ('GRUBHUB', 'Grubhub', 'Brand'),
    ('McLAREN', 'McLaren', 'Brand'),
    ('VOLKSWAGEN', 'Volkswagen', 'Brand'),
    ('Samsung Galaxy', 'Samsung', 'Brand'),
    ('Venom The Last Dance', 'Venom: The Last Dance', 'Movie'),
    ('Godzilla vs. Kong', 'Godzilla', 'Movie'),
    ('SPYxFAMILY', 'FAMILY', 'Anime'),
    ('FAMILY Prize Path', 'FAMILY', 'Anime'),
    ('Automobili Lamborghini', 'Lamborghini', 'Brand'),
    ('Butter Bear 2', 'Butterbear', 'IP'),
    ('Indian Motorcycles', 'Indian Motorcycle', 'IP'),
    ('TEKKEN 8 Teaser', 'TEKKEN 8', 'IP'),
    ('Tekken 8 Showcase', 'TEKKEN 8', 'IP'),
    ('SKAI ISYOURGOD', '揽佬SKAI ISYOURGOD', 'Artist'),  # Keep the full name
]

merged = 0
for old, new, cat in MERGES:
    cursor.execute("""
        UPDATE videos 
        SET collab_partner = ?, collab_category = ?
        WHERE collab_partner = ?
    """, (new, cat, old))
    if cursor.rowcount > 0:
        print(f"  Merged '{old}' -> '{new}': {cursor.rowcount}")
        merged += cursor.rowcount

# 2. 잘못된 항목 제거
REMOVE = [
    'WOW',  # PUBG 내부 모드
    'Kid',  # 잘못된 추출
    'Gaming',  # 너무 일반적
    'Vehicle',  # 잘못된 추출
    'Force',  # 잘못된 추출
    'Shadow',  # 잘못된 추출
    'Mission',  # 잘못된 추출
    'Subs',  # 잘못된 추출
    'Matchmaking',  # 잘못된 추출
    'Showcase',  # 잘못된 추출
    'Sounds',  # 잘못된 추출
    'Cosplay',  # 잘못된 추출
    'Test',  # 잘못된 추출
    'Cheaters',  # 잘못된 추출
    'PMIT',  # PUBG 내부 이벤트
    'Ledge Grab',  # 게임 기능
    'Laser Sights',  # 게임 아이템
    'Power Armor',  # 게임 아이템
    'Upgraded Authorized Login',  # 기능
    'Out in the Cold',  # 잘못된 추출
    'Cosmic Hoverboard',  # 게임 아이템
    'Improved Battery Performance',  # 기능
    'Super Smooth',  # 기능
    'Super Smooth Graphics',  # 기능
    'Team Share',  # 기능
    'Redemption Shop Available Now',  # 기능
    'Lights Out',  # 이벤트
    'Ocean Odyssey',  # 이벤트
    'Popular Upgradable Firearms',  # 기능
    'Gilt Set Serpengleam Set',  # 아이템
    'Icicle Spike',  # 아이템
    'Cleric Dragoon',  # 아이템
    'Beryl M762',  # 무기
    'M249',  # 무기
    'Riding Guide',  # 가이드
    'Bloodbane Parasite',  # 아이템
    'Royale Pass M12',  # RP
    'Mechs in World of Wonder',  # 기능
    'Zorb Football Vehicle Guide',  # 가이드
    'Dying Light The Beast Dev Talk',  # 단순 토크
    'SSC Map Recommendations',  # 가이드
    'Empyrean Charm',  # 아이템
    'Nour',  # 불명확
    'Muscle Chicken',  # Chicko 관련
    'Frantic Frames',  # 불명확
]

removed = 0
for partner in REMOVE:
    cursor.execute("""
        UPDATE videos 
        SET is_collab = 0, collab_partner = NULL, collab_category = NULL
        WHERE collab_partner = ?
    """, (partner,))
    if cursor.rowcount > 0:
        print(f"  Removed '{partner}': {cursor.rowcount}")
        removed += cursor.rowcount

# 3. EWC 관련 통합 (esports world cup)
cursor.execute("""
    UPDATE videos 
    SET collab_partner = 'Esports World Cup', collab_category = 'Other'
    WHERE collab_partner LIKE 'EWC %'
""")
print(f"  Merged EWC events: {cursor.rowcount}")

conn.commit()

# 결과
cursor.execute("SELECT COUNT(*) FROM videos WHERE is_collab = 1")
collab_count = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(DISTINCT collab_partner) FROM videos WHERE is_collab = 1")
partner_count = cursor.fetchone()[0]

print(f"\n  Total merged: {merged}")
print(f"  Total removed: {removed}")
print(f"  Final collabs: {collab_count}")
print(f"  Final partners: {partner_count}")

conn.close()
print("\nFinal cleanup complete!")
