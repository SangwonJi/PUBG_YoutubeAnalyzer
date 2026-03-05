import sqlite3
conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

print("=== Sample 'Other' Category Videos ===\n")
cursor.execute("SELECT title FROM videos WHERE content_type = 'Other' LIMIT 50")
for i, row in enumerate(cursor.fetchall(), 1):
    print(f"{i:2}. {row[0][:90]}")

print("\n\n=== Word Frequency in 'Other' ===\n")
cursor.execute("SELECT title FROM videos WHERE content_type = 'Other'")
titles = [row[0] for row in cursor.fetchall()]

# 단어 빈도 분석
from collections import Counter
words = []
for title in titles:
    words.extend(title.upper().split())

common = Counter(words).most_common(40)
for word, count in common:
    if len(word) > 2 and word not in ['PUBG', 'MOBILE', 'THE', 'AND', 'FOR', 'NEW', 'NOW', 'YOU', 'YOUR', 'WITH', 'OUT', 'ARE', 'ALL']:
        print(f"{word}: {count}")

conn.close()
