import sqlite3
conn = sqlite3.connect('data/pubg_collab.db')
cursor = conn.cursor()

# 추가 정리
invalid = [
    'OPENING PUBG MOBILE', 
    'PUBG MOBILE Interview', 
    'PUBGMOBILE Battlefields in Real Life',
    'citement peaked during the PUBG MOBILE', 
    'PUBG MOBILE x Porsche Trailer'
]
for p in invalid:
    cursor.execute('UPDATE videos SET is_collab = 0, collab_partner = NULL WHERE collab_partner = ?', (p,))
    if cursor.rowcount > 0:
        print(f'Fixed: {p}')

# LINE FRIENDS는 실제 콜라보이므로 파트너명만 정리
cursor.execute("UPDATE videos SET collab_partner = 'LINE FRIENDS' WHERE collab_partner = 'PUBG MOBILE and LINE FRIENDS'")
print(f'LINE FRIENDS: {cursor.rowcount}')

conn.commit()
cursor.execute('SELECT COUNT(*) FROM videos WHERE is_collab = 1')
print(f'Total collabs: {cursor.fetchone()[0]}')
conn.close()
