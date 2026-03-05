import sqlite3

conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()
cursor.execute('SELECT title FROM videos WHERE is_collab = 0')
titles = [row[0] for row in cursor.fetchall()]

print(f"Total non-collab videos: {len(titles)}\n")

# 키워드 분석
keywords = [
    'PMGC', 'PDP', 'WOW', 'PASS', 'DRAW', 
    'UPDATE', 'PATCH', 'SEASON', 'ROYALE', 
    'MODE', 'MAP', 'EVENT', 'TUTORIAL', 'GUIDE', 
    'ESPORTS', 'TOURNAMENT', 'CHAMPIONSHIP', 'LEAGUE', 
    'LIVE', 'HIGHLIGHTS', 'TRAILER', 'TEASER',
    'PUBG', 'MOBILE', 'NEW', 'GAMEPLAY', 'OFFICIAL'
]

print("=== Keyword Analysis ===")
results = []
for kw in keywords:
    count = sum(1 for t in titles if kw.upper() in t.upper())
    if count > 0:
        results.append((kw, count))

results.sort(key=lambda x: x[1], reverse=True)
for kw, count in results:
    print(f"{kw}: {count}")

# 샘플 타이틀 출력
print("\n=== Sample Titles by Category ===")
categories = {
    'PMGC': [],
    'PDP': [],
    'WOW': [],
    'PASS': [],
    'DRAW': [],
    'UPDATE': [],
    'ESPORTS': [],
    'TRAILER': []
}

for title in titles:
    for cat in categories:
        if cat.upper() in title.upper():
            if len(categories[cat]) < 3:
                categories[cat].append(title[:80])

for cat, samples in categories.items():
    if samples:
        print(f"\n[{cat}]")
        for s in samples:
            print(f"  - {s}")

conn.close()
