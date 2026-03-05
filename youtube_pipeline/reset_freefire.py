"""
Reset Free Fire classifications for GPT reclassification
"""
import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
c = conn.cursor()

print("Resetting Free Fire classifications...")

# Reset Free Fire classifications
c.execute('''UPDATE videos SET 
    is_collab = 0,
    collab_partner = NULL,
    collab_category = NULL,
    collab_region = NULL,
    collab_summary = NULL,
    collab_confidence = 0,
    classification_method = NULL
WHERE source_channel = 'freefire' ''')

affected = c.rowcount
conn.commit()
print(f'Reset {affected} Free Fire videos for reclassification')

# Verify
c.execute("SELECT COUNT(*) FROM videos WHERE source_channel = 'freefire' AND is_collab = 1")
print(f'Free Fire collabs remaining: {c.fetchone()[0]}')

conn.close()
print("Done! Now run: python main.py classify --channel freefire")
