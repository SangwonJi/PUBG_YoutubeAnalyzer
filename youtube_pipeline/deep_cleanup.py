"""
Deep cleanup: Remove all incorrectly extracted partner names.
These are fragments from titles, not actual collaboration partners.
"""

import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("=" * 70)
print("DEEP CLEANUP - Removing Invalid Partner Names")
print("=" * 70)

# 잘못된 파트너명 패턴들
INVALID_PATTERNS = [
    # X-Suit 관련 (앞에서 잘렸음)
    '-Suit%', '-suit%', '-Challenge%',
    
    # Exclusive에서 잘렸음
    'clusive%',
    
    # Explained에서 잘렸음  
    'plained%',
    
    # Explore에서 잘렸음
    'plore%', 'ploring%', 'ploration%',
    
    # Experience에서 잘렸음
    'perience%',
    
    # Express에서 잘렸음
    'press%',
    
    # Extreme에서 잘렸음
    'treme%', 'traction%',
    
    # Exclusive에서 잘렸음
    'hibit%',
    
    # Exhibition에서 잘렸음
    'istence%',
    
    # Templar에서 잘렸음
    'emplar%',
    
    # 기타 잘못된 추출
    'the %',  # "the" 로 시작하는 것들
    'a %',    # "a" 로 시작하는 것들
    'your %', # "your" 로 시작하는 것들
    'to %',   # "to" 로 시작하는 것들
    'for %',  # "for" 로 시작하는 것들
    'our %',  # "our" 로 시작하는 것들
    'y %',    # 짧은 것들
    'e %',
    't %',
    
    # 기타 명백히 잘못된 것들
    'citing%',
    'ury %',
    'erbolt%',
    'en Sinistra%',
    'er Set',
    'something wild',
    'special rewards',
    'new Royale Pass',
    'friends',
    'unparalleled style',
    'pected%',
    'pedite%',
    'tourist',
]

# 짧은 파트너명 (3자 이하)
cursor.execute("""
    SELECT DISTINCT collab_partner FROM videos 
    WHERE is_collab = 1 AND LENGTH(collab_partner) <= 3
""")
short_partners = [row[0] for row in cursor.fetchall()]

# 패턴으로 삭제
total_fixed = 0

for pattern in INVALID_PATTERNS:
    cursor.execute("""
        SELECT COUNT(*) FROM videos WHERE collab_partner LIKE ? AND is_collab = 1
    """, (pattern,))
    count = cursor.fetchone()[0]
    
    if count > 0:
        cursor.execute("""
            UPDATE videos 
            SET is_collab = 0, collab_partner = NULL, collab_category = NULL
            WHERE collab_partner LIKE ? AND is_collab = 1
        """, (pattern,))
        print(f"  Removed pattern '{pattern}': {count} videos")
        total_fixed += count

# 짧은 파트너명 삭제
for partner in short_partners:
    # 예외: 실제 브랜드 (BAPE, XP-PEN 등)
    if partner.upper() in ['AW', 'DP']:  # 이건 잘못된 것
        cursor.execute("""
            UPDATE videos 
            SET is_collab = 0, collab_partner = NULL, collab_category = NULL
            WHERE collab_partner = ?
        """, (partner,))
        if cursor.rowcount > 0:
            print(f"  Removed short partner '{partner}': {cursor.rowcount} videos")
            total_fixed += cursor.rowcount

# 추가로 의심스러운 파트너명 확인
print("\n  Checking remaining suspicious partners...")
cursor.execute("""
    SELECT collab_partner, COUNT(*) 
    FROM videos 
    WHERE is_collab = 1 AND collab_partner IS NOT NULL
    GROUP BY collab_partner
    HAVING COUNT(*) = 1
    ORDER BY collab_partner
""")

single_partners = cursor.fetchall()
print(f"  Partners with only 1 video: {len(single_partners)}")

# 단일 영상 파트너 중 의심스러운 것들 (소문자로 시작하거나 특수문자 포함)
suspicious_singles = []
for partner, count in single_partners:
    if (partner[0].islower() or 
        partner.startswith('-') or 
        partner.startswith('™') or
        ' ' in partner and len(partner.split()[0]) <= 2):
        suspicious_singles.append(partner)
        cursor.execute("""
            UPDATE videos 
            SET is_collab = 0, collab_partner = NULL, collab_category = NULL
            WHERE collab_partner = ?
        """, (partner,))
        total_fixed += 1

if suspicious_singles:
    print(f"  Removed {len(suspicious_singles)} suspicious single-video partners")

conn.commit()

# 결과 확인
cursor.execute("SELECT COUNT(*) FROM videos WHERE is_collab = 1")
collab_count = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(DISTINCT collab_partner) FROM videos WHERE is_collab = 1")
partner_count = cursor.fetchone()[0]

print(f"\n  Total removed: {total_fixed}")
print(f"  Remaining collabs: {collab_count}")
print(f"  Remaining partners: {partner_count}")

conn.close()
print("\nDeep cleanup complete!")
