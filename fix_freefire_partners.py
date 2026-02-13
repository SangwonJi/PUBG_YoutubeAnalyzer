"""
Fix incorrectly classified Free Fire partners
"""
import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
c = conn.cursor()

# List of invalid partner names to remove
INVALID_PARTNERS = [
    'you',
    'you today',
    'Free Fire',
    'Garena Free Fire',
    'free fire',
    'FF',
]

print("Fixing Free Fire partner classifications...")
print()

for partner in INVALID_PARTNERS:
    c.execute('''UPDATE videos SET 
        is_collab = 0,
        collab_partner = NULL,
        collab_category = NULL,
        collab_summary = NULL,
        collab_confidence = 0
    WHERE source_channel = 'freefire' AND collab_partner = ?''', (partner,))
    
    if c.rowcount > 0:
        print(f"  Removed '{partner}': {c.rowcount} videos")

conn.commit()

# Also merge duplicate partners (case sensitivity)
print()
print("Merging duplicate partners...")

# Naruto variations
c.execute('''UPDATE videos SET collab_partner = 'Naruto Shippuden' 
             WHERE source_channel = 'freefire' AND collab_partner = 'NARUTO SHIPPUDEN' ''')
if c.rowcount > 0:
    print(f"  Merged NARUTO SHIPPUDEN -> Naruto Shippuden: {c.rowcount} videos")

c.execute('''UPDATE videos SET collab_partner = 'Naruto Shippuden' 
             WHERE source_channel = 'freefire' AND collab_partner = 'NARUTO SHIPPUDEN Chapter 2' ''')
if c.rowcount > 0:
    print(f"  Merged NARUTO SHIPPUDEN Chapter 2 -> Naruto Shippuden: {c.rowcount} videos")

# Demon Slayer variations
c.execute('''UPDATE videos SET collab_partner = 'Demon Slayer' 
             WHERE source_channel = 'freefire' AND collab_partner = 'DemonSlayer' ''')
if c.rowcount > 0:
    print(f"  Merged DemonSlayer -> Demon Slayer: {c.rowcount} videos")

conn.commit()

# Show updated results
print()
print("=== Updated Free Fire Top 15 Partners ===")
c.execute('''SELECT collab_partner, COUNT(*), SUM(view_count) FROM videos 
             WHERE source_channel='freefire' AND is_collab=1 AND collab_partner IS NOT NULL 
             GROUP BY collab_partner ORDER BY SUM(view_count) DESC LIMIT 15''')
for r in c.fetchall():
    print(f"  {r[1]:3} videos | {r[2]:>12,} views | {r[0]}")

# Summary
c.execute("SELECT COUNT(*) FROM videos WHERE source_channel='freefire' AND is_collab=1")
total_collabs = c.fetchone()[0]
c.execute("SELECT COUNT(DISTINCT collab_partner) FROM videos WHERE source_channel='freefire' AND is_collab=1")
unique_partners = c.fetchone()[0]

print()
print(f"Total Free Fire collabs: {total_collabs}")
print(f"Unique partners: {unique_partners}")

conn.close()
