import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("=== ALL REMAINING PARTNERS ===\n")

cursor.execute("""
    SELECT collab_partner, collab_category, COUNT(*), SUM(view_count)
    FROM videos 
    WHERE is_collab = 1 AND collab_partner IS NOT NULL
    GROUP BY collab_partner 
    ORDER BY SUM(view_count) DESC
""")

partners = cursor.fetchall()
print(f"Total unique partners: {len(partners)}\n")

print(f"{'#':<4} {'Partner':<40} {'Cat':<8} {'Cnt':>4} {'Views':>15}")
print("-" * 80)

suspicious = []
for i, (partner, cat, cnt, views) in enumerate(partners, 1):
    views = views or 0
    
    # 의심스러운 패턴 체크
    is_suspicious = False
    if (partner[0].islower() or 
        partner.startswith('-') or
        len(partner) <= 3 or
        'PUBG' in partner.upper() or
        partner.endswith('...') or
        '  ' in partner):
        is_suspicious = True
        suspicious.append((partner, cnt, views))
    
    flag = " *" if is_suspicious else ""
    print(f"{i:<4} {partner[:40]:<40} {(cat or 'N/A'):<8} {cnt:>4} {views:>15,}{flag}")

if suspicious:
    print(f"\n\n=== SUSPICIOUS PARTNERS (marked with *) ===")
    print(f"Found {len(suspicious)} suspicious partners:")
    for p, c, v in suspicious:
        print(f"  - '{p}' ({c} videos, {v:,} views)")

conn.close()
