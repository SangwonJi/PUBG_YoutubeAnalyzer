"""
Classify videos into content categories (v2).
- Collab videos remain as Collab
- Galaxy/Note9 reclassified as Samsung collab
- Added: Chicko, Gilt, Shorts categories
- PMSC/PMCO added to Esports
"""

import sqlite3
from tqdm import tqdm

# 카테고리 정의 (우선순위 순서대로 체크)
CATEGORIES = {
    'Esports': ['PMGC', 'PMWC', 'PMPL', 'PMSL', 'PMSC', 'PMCO', 'CHAMPIONSHIP', 'LEAGUE', 'TOURNAMENT', 'ESPORTS', 'GRAND FINALS', 'GROUP STAGE', 'HIGHLIGHTS', 'SEMIFINAL', 'QUARTER'],
    'Chicko': ['CHICKO'],
    'Gilt': ['GILT'],
    'Shorts': ['#SHORTS', 'SHORTS'],
    'Update': ['UPDATE', 'PATCH', 'VERSION', 'V3.', 'V4.', 'V5.'],
    'WOW': ['WOW'],
    'Pass': ['PASS', 'ROYALE PASS', 'RP ', 'BONUS PASS'],
    'PDP': ['PDP'],
    'Event': ['EVENT', 'LIMITED', 'EXCLUSIVE', 'FESTIVAL', 'GIVEAWAY'],
    'Mode': ['MODE', 'PAYLOAD', 'METRO', 'INFECTION', 'ZOMBIE', 'ARENA'],
    'Map': ['MAP', 'ERANGEL', 'MIRAMAR', 'SANHOK', 'VIKENDI', 'LIVIK', 'KARAKIN', 'NUSA'],
    'Promotional': ['TRAILER', 'TEASER', 'ANNOUNCEMENT', 'COMING SOON'],
    'Guide': ['GUIDE', 'TUTORIAL', 'HOW TO', 'TIPS', 'GAMEPLAY'],
    'Season': ['SEASON', 'CYCLE'],
    'Draw': ['DRAW', 'LUCKY SPIN', 'CRATE', 'OPENING'],
    'Skin': ['SET', 'OUTFIT', 'SKIN', 'COSTUME', 'MYTHIC'],
    'Community': ['MEET', 'FAN', 'COMMUNITY', 'OFFLINE', 'POP-UP'],
}

# 삼성 콜라보 키워드 (시즌 번호와 겹치지 않도록 주의)
SAMSUNG_KEYWORDS = ['GALAXY', 'NOTE9', 'NOTE 9', 'SAMSUNG', 'NOTE10', 'NOTE 10', 'GALAXY S']


def classify_video(title: str) -> str:
    """Classify a non-collab video by its title."""
    title_upper = title.upper()
    
    for category, keywords in CATEGORIES.items():
        for keyword in keywords:
            if keyword in title_upper:
                return category
    
    return 'Other'


def is_samsung_collab(title: str) -> bool:
    """Check if title contains Samsung-related keywords."""
    title_upper = title.upper()
    for keyword in SAMSUNG_KEYWORDS:
        if keyword in title_upper:
            return True
    return False


def main():
    conn = sqlite3.connect('data/pubg_collab.db')
    cursor = conn.cursor()
    
    # 1. 삼성 콜라보 재분류 (비콜라보 중 Galaxy/Note9 포함된 것)
    cursor.execute("SELECT video_id, title FROM videos WHERE is_collab = 0")
    non_collabs = cursor.fetchall()
    
    samsung_count = 0
    for video_id, title in non_collabs:
        if is_samsung_collab(title):
            cursor.execute("""
                UPDATE videos 
                SET is_collab = 1, 
                    collab_partner = 'Samsung',
                    collab_category = 'Brand',
                    content_type = 'Collab'
                WHERE video_id = ?
            """, (video_id,))
            samsung_count += 1
    
    print(f"Samsung collabs reclassified: {samsung_count}")
    
    # 2. 콜라보 영상: content_type = 'Collab'
    cursor.execute("UPDATE videos SET content_type = 'Collab' WHERE is_collab = 1")
    collab_count = cursor.rowcount
    print(f"Total Collab videos: {collab_count}")
    
    # 3. 비콜라보 영상 재분류
    cursor.execute("SELECT video_id, title FROM videos WHERE is_collab = 0")
    non_collabs = cursor.fetchall()
    print(f"Non-collab videos to classify: {len(non_collabs)}")
    
    # 카테고리별 카운트
    category_counts = {}
    
    for video_id, title in tqdm(non_collabs, desc="Classifying"):
        category = classify_video(title)
        cursor.execute(
            "UPDATE videos SET content_type = ? WHERE video_id = ?",
            (category, video_id)
        )
        category_counts[category] = category_counts.get(category, 0) + 1
    
    conn.commit()
    
    # 결과 출력
    print("\n" + "=" * 40)
    print("Classification Results")
    print("=" * 40)
    print(f"{'Category':<15} {'Count':>8}")
    print("-" * 25)
    
    # 콜라보 먼저
    print(f"{'Collab':<15} {collab_count:>8}")
    print("-" * 25)
    
    # 나머지 카테고리 (개수 순)
    sorted_cats = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
    for cat, count in sorted_cats:
        print(f"{cat:<15} {count:>8}")
    
    print("-" * 25)
    print(f"{'Total':<15} {collab_count + len(non_collabs):>8}")
    
    # 삼성 콜라보 확인
    print("\n=== Samsung Collab Sample ===")
    cursor.execute("SELECT title FROM videos WHERE collab_partner = 'Samsung' LIMIT 5")
    for row in cursor.fetchall():
        print(f"  - {row[0][:70]}")
    
    conn.close()
    print("\nClassification complete!")


if __name__ == "__main__":
    main()
