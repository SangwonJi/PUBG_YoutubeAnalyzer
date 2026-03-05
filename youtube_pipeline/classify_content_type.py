"""
Classify non-collab videos into content categories.
Collab videos remain classified by partner name.
"""

import sqlite3
from tqdm import tqdm

# 카테고리 정의 (우선순위 순서)
CATEGORIES = {
    'Esports': ['PMGC', 'PMWC', 'PMPL', 'PMSL', 'CHAMPIONSHIP', 'LEAGUE', 'TOURNAMENT', 'ESPORTS', 'GRAND FINALS', 'GROUP STAGE', 'HIGHLIGHTS'],
    'Update': ['UPDATE', 'PATCH', 'VERSION', 'V3.', 'V4.', 'V5.'],
    'WOW': ['WOW'],
    'Pass': ['PASS', 'ROYALE PASS', 'RP ', 'BONUS PASS'],
    'PDP': ['PDP'],
    'Event': ['EVENT', 'LIMITED', 'EXCLUSIVE'],
    'Mode': ['MODE', 'PAYLOAD', 'METRO', 'INFECTION', 'ZOMBIE', 'ARENA'],
    'Map': ['MAP', 'ERANGEL', 'MIRAMAR', 'SANHOK', 'VIKENDI', 'LIVIK', 'KARAKIN', 'NUSA'],
    'Promotional': ['TRAILER', 'TEASER', 'ANNOUNCEMENT', 'COMING SOON'],
    'Guide': ['GUIDE', 'TUTORIAL', 'HOW TO', 'TIPS', 'GAMEPLAY'],
    'Season': ['SEASON', 'CYCLE'],
    'Draw': ['DRAW', 'LUCKY SPIN', 'CRATE', 'OPENING'],
}

def classify_video(title: str) -> str:
    """Classify a non-collab video by its title."""
    title_upper = title.upper()
    
    for category, keywords in CATEGORIES.items():
        for keyword in keywords:
            if keyword in title_upper:
                return category
    
    return 'Other'


def main():
    conn = sqlite3.connect('data/pubg_collab.db')
    cursor = conn.cursor()
    
    # content_type 컬럼 추가 (없으면)
    cursor.execute("PRAGMA table_info(videos)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'content_type' not in columns:
        cursor.execute("ALTER TABLE videos ADD COLUMN content_type TEXT")
        print("Added content_type column")
    
    # 콜라보 영상: content_type = 'Collab'
    cursor.execute("UPDATE videos SET content_type = 'Collab' WHERE is_collab = 1")
    collab_count = cursor.rowcount
    print(f"Collab videos marked: {collab_count}")
    
    # 비콜라보 영상 가져오기
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
    print("\n=== Classification Results ===")
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
    
    conn.close()
    print("\nClassification complete!")


if __name__ == "__main__":
    main()
