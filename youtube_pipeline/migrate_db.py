"""
Database migration script: Add source_channel column
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import sqlite3
from pathlib import Path

DB_PATH = Path('./data/pubg_collab.db')

def migrate():
    """Add source_channel column to videos and collab_agg tables."""
    print("Database Migration: Adding source_channel column")
    print("=" * 60)
    
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if column already exists in videos table
    cursor.execute("PRAGMA table_info(videos)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if 'source_channel' not in columns:
        print("Adding source_channel to videos table...")
        cursor.execute("ALTER TABLE videos ADD COLUMN source_channel TEXT DEFAULT 'pubgm'")
        cursor.execute("UPDATE videos SET source_channel = 'pubgm' WHERE source_channel IS NULL")
        print("  - Column added and set to 'pubgm' for existing videos")
    else:
        print("  - source_channel already exists in videos table")
    
    # Check if column already exists in collab_agg table
    cursor.execute("PRAGMA table_info(collab_agg)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if 'source_channel' not in columns:
        print("Adding source_channel to collab_agg table...")
        cursor.execute("ALTER TABLE collab_agg ADD COLUMN source_channel TEXT DEFAULT 'pubgm'")
        cursor.execute("UPDATE collab_agg SET source_channel = 'pubgm' WHERE source_channel IS NULL")
        print("  - Column added and set to 'pubgm' for existing aggregations")
    else:
        print("  - source_channel already exists in collab_agg table")
    
    # Create index if not exists
    print("Creating indexes...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_source_channel ON videos(source_channel)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_collab_agg_source_channel ON collab_agg(source_channel)")
    print("  - Indexes created")
    
    conn.commit()
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM videos WHERE source_channel = 'pubgm'")
    pubgm_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM videos WHERE source_channel = 'freefire'")
    freefire_count = cursor.fetchone()[0]
    
    print("\n" + "=" * 60)
    print("Migration Complete!")
    print(f"  - PUBGM videos: {pubgm_count}")
    print(f"  - Free Fire videos: {freefire_count}")
    print("=" * 60)
    
    conn.close()

if __name__ == '__main__':
    migrate()
