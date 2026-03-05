"""
Classify Free Fire non-collab videos by content type
"""
import sqlite3
import re
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/pubg_collab.db')
c = conn.cursor()

print("Classifying Free Fire non-collab content types...")
print()

# Content type rules (order matters - first match wins)
CONTENT_RULES = [
    # Shorts / Meme content
    ('Shorts', [
        r'#shorts',
        r'#freefireofficial\s*#',
        r'\?\s*\|',  # Question format titles
        r'be like\s*\|',
        r'which one',
        r'guess who',
        r'do you (think|know|have|remember)',
        r'have you ever',
        r'has this ever',
    ]),
    
    # Update / Patch
    ('Update', [
        r'new patch',
        r'update',
        r'now available',
        r'is here!',
        r'coming soon',
        r'unlocked',
        r'upgraded',
        r'awakened',
        r'new ability',
        r'check out new',
        r'new looks',
        r'mechadrake',
        r'new dawn',
    ]),
    
    # Booyah Pass
    ('Pass', [
        r'booyah pass',
        r'elite pass',
        r'pass s\d',
    ]),
    
    # Story / Lore
    ('Story', [
        r'free fire story',
        r'teaser',
        r'trailer',
        r'cinematic',
        r'animation',
        r'rampage',
    ]),
    
    # Event
    ('Event', [
        r'event',
        r'rewards',
        r'guild',
        r'tournament',
        r'competition',
        r'contest',
    ]),
    
    # Esports
    ('Esports', [
        r'esports',
        r'ffws',
        r'ffac',
        r'champion',
        r'finals',
        r'world series',
        r'league',
        r'pro league',
    ]),
    
    # Craftland
    ('Craftland', [
        r'craftland',
    ]),
    
    # Gameplay / Guide
    ('Guide', [
        r'guide',
        r'tutorial',
        r'tips',
        r'how to',
        r'gameplay',
    ]),
    
    # Character content (Kelly, Maxim, Andrew, etc.)
    ('Character', [
        r'\b(kelly|maxim|andrew|hayato|alok|moco|tatsuya|chrono|skyler|dimitri)\b',
    ]),
]

def classify_video(title, description):
    """Classify a video based on title and description."""
    text = f"{title} {description or ''}".lower()
    
    for content_type, patterns in CONTENT_RULES:
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return content_type
    
    return 'Other'

# Get all Free Fire non-collab videos
c.execute('''SELECT video_id, title, description FROM videos 
             WHERE source_channel = 'freefire' AND is_collab = 0''')
videos = c.fetchall()

print(f"Found {len(videos)} Free Fire non-collab videos to classify")
print()

# Classify each video
counts = {}
for video_id, title, description in videos:
    content_type = classify_video(title, description)
    
    c.execute('UPDATE videos SET content_type = ? WHERE video_id = ?', 
              (content_type, video_id))
    
    counts[content_type] = counts.get(content_type, 0) + 1

conn.commit()

# Print results
print("=== Classification Results ===")
for content_type, count in sorted(counts.items(), key=lambda x: -x[1]):
    print(f"  {content_type:15} : {count:4} videos")

print()
print(f"Total classified: {sum(counts.values())} videos")

# Show samples for each type
print()
print("=== Sample Titles by Type ===")
for content_type in sorted(counts.keys()):
    c.execute('''SELECT title FROM videos 
                 WHERE source_channel = 'freefire' AND content_type = ?
                 ORDER BY view_count DESC LIMIT 3''', (content_type,))
    print(f"\n{content_type}:")
    for r in c.fetchall():
        title = r[0].encode('ascii', 'ignore').decode()[:60]
        print(f"  - {title}")

conn.close()
print()
print("Classification complete!")
