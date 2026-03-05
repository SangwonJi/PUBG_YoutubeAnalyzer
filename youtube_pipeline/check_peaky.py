import sqlite3
conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("=== Peaky Blinders in collab_partner ===")
cursor.execute("SELECT video_id, title, collab_partner FROM videos WHERE collab_partner LIKE '%Peaky%'")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1][:50]}... | Partner: {row[2]}")

print("\n=== Peaky Blinders in title (but not partner) ===")
cursor.execute("""
    SELECT video_id, title, collab_partner, is_collab 
    FROM videos 
    WHERE title LIKE '%Peaky%' AND (collab_partner NOT LIKE '%Peaky%' OR collab_partner IS NULL)
""")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1][:50]}...")
    print(f"    Partner: {row[2]}, is_collab: {row[3]}")

conn.close()
