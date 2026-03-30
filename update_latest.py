"""
증분 업데이트 스크립트: 마지막 수집일 이후의 새 영상만 가져와서
GPT 분류 후 대시보드 JSON을 재생성합니다.

사용법:
    cd pubg_collab_pipeline_v2
    python update_latest.py              # 모든 리전 업데이트
    python update_latest.py --region Turkey  # 특정 리전만
    python update_latest.py --dry-run    # 실제 수집 없이 신규 영상 수만 확인
"""
import argparse
import json
import os
import sqlite3
import time
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(r"c:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\.env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("update")

# ── Paths ──
DB_PATH = Path(r"C:\Users\sangwon.ji\Desktop\Google_PUBG\data\pubgm_youtube_regional.db")
DOCS = Path(r"c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs")
CSV_OUT = Path(r"c:\Users\sangwon.ji\Downloads\pubg_weibo_analyzer\pubg_weibo_analyzer\output\yt_regional_all_videos.csv")

# ── API Keys ──
YT_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
GPT_API_KEY = os.getenv("GPT_API_KEY", "")

# ── Channels ──
CHANNELS = {
    "Global":    "@PUBGMOBILE",
    "MENA":      "@PUBGMOBILEMENAOFFICIAL",
    "Turkey":    "@PUBGMOBILETürkiye",
    "Indonesia": "@pubgmobileofficialindonesia",
    "LATAM":     "@PUBGMOBILELATAM",
    "CIS":       "@pubgmobile_cis",
    "India":     "@BattlegroundsMobile_IN",
    "Malaysia":  "@pubgmobilemy",
    "Pakistan":  "@PUBGMOBILEPakistan",
    "Taiwan":    "@PUBGMOBILEMTW",
    "Thailand":  "@pubgmth",
    "Korea":     "@pubgm_kr",
    "Japan":     "@pubgmobile_japan",
}

REGION_META = {
    'Global':    {'name': 'PUBG MOBILE',            'dateRange': '2018 - 2026'},
    'MENA':      {'name': 'PUBG MOBILE MENA',      'dateRange': '2019 - 2026'},
    'Turkey':    {'name': 'PUBG MOBILE Turkey',     'dateRange': '2019 - 2026'},
    'Indonesia': {'name': 'PUBG MOBILE Indonesia',  'dateRange': '2018 - 2026'},
    'LATAM':     {'name': 'PUBG MOBILE LATAM',      'dateRange': '2020 - 2026'},
    'CIS':       {'name': 'PUBG MOBILE CIS',        'dateRange': '2018 - 2026'},
    'India':     {'name': 'PUBG MOBILE India',       'dateRange': '2021 - 2026'},
    'Malaysia':  {'name': 'PUBG MOBILE Malaysia',    'dateRange': '2019 - 2026'},
    'Pakistan':  {'name': 'PUBG MOBILE Pakistan',    'dateRange': '2020 - 2026'},
    'Taiwan':    {'name': 'PUBG MOBILE Taiwan',      'dateRange': '2019 - 2026'},
    'Thailand':  {'name': 'PUBG MOBILE Thailand',    'dateRange': '2019 - 2026'},
    'Korea':     {'name': 'PUBG MOBILE Korea',       'dateRange': '2018 - 2026'},
    'Japan':     {'name': 'PUBG MOBILE Japan',       'dateRange': '2018 - 2026'},
}


# ═══════════════════════════════════════════════
# STEP 1: YouTube API - 새 영상만 수집
# ═══════════════════════════════════════════════

def get_youtube():
    from googleapiclient.discovery import build
    if not YT_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set")
    return build("youtube", "v3", developerKey=YT_API_KEY)


def get_last_date_per_region(conn):
    """DB에서 리전별 가장 최신 published_at을 가져온다."""
    rows = conn.execute(
        "SELECT region, MAX(published_at) FROM videos GROUP BY region"
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1]}


def get_existing_video_ids(conn, region):
    """해당 리전의 기존 video_id 세트."""
    rows = conn.execute(
        "SELECT video_id FROM videos WHERE region = ?", (region,)
    ).fetchall()
    return {r[0] for r in rows}


def resolve_channel(youtube, handle):
    """핸들 → channel_id + uploads playlist."""
    clean = handle.lstrip("@")
    resp = youtube.channels().list(
        part="id,contentDetails,snippet,statistics", forHandle=clean
    ).execute()
    if not resp.get("items"):
        return None, None, None
    item = resp["items"][0]
    ch_id = item["id"]
    uploads = item["contentDetails"]["relatedPlaylists"]["uploads"]
    title = item["snippet"]["title"]
    return ch_id, uploads, title


def fetch_new_video_ids(youtube, uploads_playlist, existing_ids, max_pages=200):
    """
    Playlist를 최신부터 순회하며, 이미 있는 video_id가 연속으로 나오면 중단.
    새 video_id 리스트 반환.
    """
    new_ids = []
    next_page = None
    consecutive_old = 0

    for page_num in range(max_pages):
        resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist,
            maxResults=50,
            pageToken=next_page,
        ).execute()

        for item in resp.get("items", []):
            vid = item["contentDetails"]["videoId"]
            if vid in existing_ids:
                consecutive_old += 1
                if consecutive_old >= 10:
                    log.info(f"  → 기존 영상 10개 연속 발견, 수집 중단 (총 {len(new_ids)}개 신규)")
                    return new_ids
            else:
                consecutive_old = 0
                new_ids.append(vid)

        next_page = resp.get("nextPageToken")
        if not next_page:
            break

    return new_ids


def fetch_video_details(youtube, video_ids, region):
    """video_ids → 상세 정보 리스트."""
    all_videos = []
    now = datetime.now(timezone.utc).isoformat()

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        resp = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch),
        ).execute()

        for item in resp.get("items", []):
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})
            thumbs = snippet.get("thumbnails", {})
            thumb_url = (
                thumbs.get("maxres", {}).get("url")
                or thumbs.get("high", {}).get("url")
                or thumbs.get("medium", {}).get("url")
                or thumbs.get("default", {}).get("url", "")
            )
            all_videos.append({
                "video_id": item["id"],
                "channel_id": snippet["channelId"],
                "region": region,
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "published_at": snippet.get("publishedAt", ""),
                "thumbnail_url": thumb_url,
                "duration": content.get("duration", ""),
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
                "tags": json.dumps(snippet.get("tags", []), ensure_ascii=False),
                "fetched_at": now,
                "updated_at": now,
            })
    return all_videos


def save_videos_to_db(conn, videos):
    """DB에 upsert."""
    for v in videos:
        conn.execute("""
            INSERT INTO videos (
                video_id, channel_id, region, title, description,
                published_at, thumbnail_url, duration,
                view_count, like_count, comment_count, tags,
                fetched_at, updated_at
            ) VALUES (
                :video_id, :channel_id, :region, :title, :description,
                :published_at, :thumbnail_url, :duration,
                :view_count, :like_count, :comment_count, :tags,
                :fetched_at, :updated_at
            )
            ON CONFLICT(video_id) DO UPDATE SET
                view_count = excluded.view_count,
                like_count = excluded.like_count,
                comment_count = excluded.comment_count,
                updated_at = excluded.updated_at
        """, v)
    conn.commit()


# ═══════════════════════════════════════════════
# STEP 2: GPT 분류 (새 영상만)
# ═══════════════════════════════════════════════

def classify_new_videos(new_videos):
    """GPT로 콜라보 여부 + 비콜라보 카테고리 분류."""
    if not new_videos:
        return {}, {}

    from openai import OpenAI
    client = OpenAI(api_key=GPT_API_KEY)

    COLLAB_SYS = """You classify PUBG Mobile YouTube video titles as collaboration or not.
A collaboration video features a specific external brand, IP, celebrity, artist, game, or creator.
Regular updates, events, esports, tutorials, patch notes, seasonal content are NOT collaborations.
Reply JSON: {"results": [{"collab": true/false, "partner": "Name or empty", "category": "Brand/Anime/Artist/Game/Movie/IP/Entertainment/Creator/Other or empty"}]}
Only mark collab if there's a clear external partner."""

    NC_SYS = """Classify PUBG Mobile YouTube videos into one category:
Update, Esports, Event, Promotion, Creative, Tutorial, Livestream, Shorts, Community, Announcement, Festive, Other.
Reply JSON: {"c":["cat1","cat2",...]}"""

    BATCH = 60
    collab_map = {}
    nc_map = {}

    # Collab classification
    log.info(f"  GPT 콜라보 분류 중... ({len(new_videos)}개)")
    for i in range(0, len(new_videos), BATCH):
        batch = new_videos[i:i + BATCH]
        titles = [f"{j+1}. [{v['region']}] {v['title'][:100]}" for j, v in enumerate(batch)]

        for attempt in range(3):
            try:
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": COLLAB_SYS},
                        {"role": "user", "content": "\n".join(titles)},
                    ],
                    temperature=0.1, max_tokens=3000,
                    response_format={"type": "json_object"},
                )
                result = json.loads(resp.choices[0].message.content)
                results = result.get("results", list(result.values())[0] if result else [])
                if not isinstance(results, list):
                    results = []

                for j, v in enumerate(batch):
                    if j < len(results) and isinstance(results[j], dict):
                        collab_map[v['video_id']] = {
                            'is_collab': results[j].get('collab', False),
                            'partner': results[j].get('partner', ''),
                            'category': results[j].get('category', ''),
                        }
                    else:
                        collab_map[v['video_id']] = {'is_collab': False, 'partner': '', 'category': ''}
                break
            except Exception as e:
                if attempt == 2:
                    log.warning(f"  GPT 오류 (배치 {i}): {e}")
                    for v in batch:
                        collab_map[v['video_id']] = {'is_collab': False, 'partner': '', 'category': ''}
                else:
                    time.sleep(2)

        done = min(i + BATCH, len(new_videos))
        if done % 300 == 0 or done == len(new_videos):
            log.info(f"  콜라보 [{done}/{len(new_videos)}]")

    # Non-collab content category
    nc_videos = [v for v in new_videos if not collab_map.get(v['video_id'], {}).get('is_collab')]
    if nc_videos:
        log.info(f"  GPT 비콜라보 분류 중... ({len(nc_videos)}개)")
        for i in range(0, len(nc_videos), BATCH):
            batch = nc_videos[i:i + BATCH]
            titles = [f"{j+1}. {v['title'][:80]}" for j, v in enumerate(batch)]

            for attempt in range(3):
                try:
                    resp = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": NC_SYS},
                            {"role": "user", "content": "\n".join(titles)},
                        ],
                        temperature=0.1, max_tokens=1500,
                        response_format={"type": "json_object"},
                    )
                    result = json.loads(resp.choices[0].message.content)
                    cats = list(result.values())[0] if isinstance(result, dict) else []
                    if not isinstance(cats, list):
                        cats = []
                    while len(cats) < len(batch):
                        cats.append("Other")
                    for j, v in enumerate(batch):
                        nc_map[v['video_id']] = str(cats[j]) if j < len(cats) else "Other"
                    break
                except Exception as e:
                    if attempt == 2:
                        for v in batch:
                            nc_map[v['video_id']] = "Other"
                    else:
                        time.sleep(1)

    return collab_map, nc_map


# ═══════════════════════════════════════════════
# STEP 3: DB 분류 결과 저장
# ═══════════════════════════════════════════════

def save_classifications(conn, collab_map, nc_map):
    """DB에 분류 결과 업데이트."""
    for vid, info in collab_map.items():
        conn.execute("""
            UPDATE videos SET
                is_collab = ?,
                collab_partner = ?,
                collab_category = ?,
                classified_by = 'gpt-4o-mini'
            WHERE video_id = ?
        """, (1 if info['is_collab'] else 0, info.get('partner', ''), info.get('category', ''), vid))
    for vid, cat in nc_map.items():
        conn.execute("""
            UPDATE videos SET content_category = ? WHERE video_id = ?
        """, (cat, vid))
    conn.commit()


# ═══════════════════════════════════════════════
# STEP 4: JSON 재생성
#   Global: DB 전체에서 생성 (DB에 분류 데이터 있음)
#   리전: 기존 JSON에 신규 분류 영상만 병합 (기존 분류는 JSON에만 존재)
# ═══════════════════════════════════════════════

def _rebuild_global_json(conn):
    """Global은 DB에서 전체 재생성."""
    rows = conn.execute("""
        SELECT video_id, title, published_at, thumbnail_url,
               view_count, like_count, comment_count,
               is_collab, collab_partner, collab_category, content_category
        FROM videos WHERE region = 'Global'
    """).fetchall()

    collab_partners = defaultdict(list)
    noncollab_posts = []

    for r in rows:
        post = {
            'video_id': r[0], 'url': f"https://www.youtube.com/watch?v={r[0]}",
            'title': (r[1] or '')[:120], 'published_at': (r[2] or '')[:10],
            'view_count': r[4] or 0, 'like_count': r[5] or 0,
            'comment_count': r[6] or 0, 'thumbnail': r[3] or '',
        }
        if r[7] == 1 and r[8]:
            collab_partners[r[8]].append({**post, 'category': r[9] or 'Other'})
        else:
            post['content_category'] = r[10] or 'Other'
            noncollab_posts.append(post)

    data_list = []
    for partner, posts in collab_partners.items():
        cat = posts[0].get('category', 'Other')
        data_list.append({
            'name': partner, 'category': cat,
            'post_count': len(posts), 'video_count': len(posts),
            'total_views': sum(p['view_count'] for p in posts),
            'total_likes': sum(p['like_count'] for p in posts),
            'total_comments': sum(p['comment_count'] for p in posts),
            'first_collab': min((p['published_at'] for p in posts if p['published_at']), default=''),
            'videos': sorted(posts, key=lambda x: x.get('view_count', 0), reverse=True),
        })
    data_list.sort(key=lambda x: x['total_views'], reverse=True)

    cat_groups = defaultdict(list)
    for p in noncollab_posts:
        cat_groups[p.get('content_category', 'Other')].append(p)
    content_types = []
    for cat, videos in sorted(cat_groups.items(), key=lambda x: -len(x[1])):
        content_types.append({
            'name': cat, 'video_count': len(videos),
            'total_views': sum(v['view_count'] for v in videos),
            'videos': sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True),
        })
    others = {
        'video_count': len(noncollab_posts),
        'total_views': sum(p['view_count'] for p in noncollab_posts),
        'total_likes': sum(p['like_count'] for p in noncollab_posts),
        'total_comments': sum(p['comment_count'] for p in noncollab_posts),
        'content_types': content_types,
    }

    with open(DOCS / 'data.json', 'w', encoding='utf-8') as f:
        json.dump(data_list, f, ensure_ascii=False)
    with open(DOCS / 'others.json', 'w', encoding='utf-8') as f:
        json.dump(others, f, ensure_ascii=False)

    collab_count = sum(p['video_count'] for p in data_list)
    log.info(f"  Global: {len(data_list)} 파트너, {collab_count} 콜라보, {len(noncollab_posts)} 일반")


def _merge_region_json(conn, region):
    """리전은 기존 JSON에 신규 GPT 분류 영상만 병합."""
    rk = region.lower()
    data_file = DOCS / f'yt_{rk}_data.json'
    others_file = DOCS / f'yt_{rk}_others.json'

    with open(data_file, 'r', encoding='utf-8') as f:
        partners = json.load(f)
    with open(others_file, 'r', encoding='utf-8') as f:
        others = json.load(f)

    existing_ids = set()
    for p in partners:
        for v in p.get('videos', []):
            existing_ids.add(v.get('video_id', ''))
    for ct in others.get('content_types', []):
        for v in ct.get('videos', []):
            existing_ids.add(v.get('video_id', ''))

    new_rows = conn.execute("""
        SELECT video_id, title, published_at, thumbnail_url,
               view_count, like_count, comment_count,
               is_collab, collab_partner, collab_category, content_category
        FROM videos
        WHERE region = ? AND classified_by = 'gpt-4o-mini'
    """, (region,)).fetchall()

    added = 0
    for r in new_rows:
        vid = r[0]
        if vid in existing_ids:
            continue
        post = {
            'video_id': vid, 'url': f"https://www.youtube.com/watch?v={vid}",
            'title': (r[1] or '')[:120], 'published_at': (r[2] or '')[:10],
            'view_count': r[4] or 0, 'like_count': r[5] or 0,
            'comment_count': r[6] or 0, 'thumbnail': r[3] or '',
        }

        if r[7] == 1 and r[8]:
            partner_name = r[8]
            found = False
            for p in partners:
                if p['name'].upper() == partner_name.upper():
                    p['videos'].append(post)
                    p['video_count'] = len(p['videos'])
                    p['post_count'] = len(p['videos'])
                    p['total_views'] = sum(v['view_count'] for v in p['videos'])
                    p['total_likes'] = sum(v['like_count'] for v in p['videos'])
                    p['total_comments'] = sum(v['comment_count'] for v in p['videos'])
                    p['videos'].sort(key=lambda x: x.get('view_count', 0), reverse=True)
                    found = True
                    break
            if not found:
                partners.append({
                    'name': partner_name, 'category': r[9] or 'Other',
                    'post_count': 1, 'video_count': 1,
                    'total_views': post['view_count'], 'total_likes': post['like_count'],
                    'total_comments': post['comment_count'],
                    'first_collab': post['published_at'], 'videos': [post],
                })
        else:
            cat = r[10] or 'Other'
            post['content_category'] = cat
            found = False
            for ct in others.get('content_types', []):
                if ct['name'] == cat:
                    ct['videos'].append(post)
                    ct['video_count'] = len(ct['videos'])
                    ct['total_views'] = sum(v['view_count'] for v in ct['videos'])
                    ct['videos'].sort(key=lambda x: x.get('view_count', 0), reverse=True)
                    found = True
                    break
            if not found:
                others.setdefault('content_types', []).append({
                    'name': cat, 'video_count': 1,
                    'total_views': post['view_count'], 'videos': [post],
                })
            others['video_count'] = others.get('video_count', 0) + 1
            others['total_views'] = others.get('total_views', 0) + post['view_count']
            others['total_likes'] = others.get('total_likes', 0) + post['like_count']
            others['total_comments'] = others.get('total_comments', 0) + post['comment_count']
        added += 1

    partners.sort(key=lambda x: x.get('total_views', 0), reverse=True)

    with open(data_file, 'w', encoding='utf-8') as f:
        json.dump(partners, f, ensure_ascii=False)
    with open(others_file, 'w', encoding='utf-8') as f:
        json.dump(others, f, ensure_ascii=False)

    collab_count = sum(p.get('video_count', 0) for p in partners)
    nc_count = others.get('video_count', 0)
    log.info(f"  {region}: {len(partners)} 파트너, {collab_count} 콜라보, {nc_count} 일반 (+{added} new)")


def regenerate_json(conn):
    """Global은 DB에서 재생성, 리전은 기존 JSON에 병합."""
    log.info("JSON 재생성 중...")
    _rebuild_global_json(conn)
    for region in REGION_META:
        if region == 'Global':
            continue
        _merge_region_json(conn, region)


# ═══════════════════════════════════════════════
# STEP 5: CSV 재생성
# ═══════════════════════════════════════════════

def regenerate_csv(conn):
    """DB에서 전체 CSV를 재생성."""
    import csv

    rows = conn.execute("""
        SELECT video_id, region, title, published_at, duration,
               view_count, like_count, comment_count,
               thumbnail_url, is_collab, collab_partner,
               collab_category, content_category,
               SUBSTR(description, 1, 200)
        FROM videos ORDER BY published_at DESC
    """).fetchall()

    CSV_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_OUT, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "video_id", "region", "title", "published_at", "duration",
            "views", "likes", "comments", "thumbnail_url",
            "is_collab", "partner", "collab_category", "content_category",
            "description_preview"
        ])
        for r in rows:
            writer.writerow(r)

    log.info(f"CSV 재생성: {CSV_OUT} ({len(rows)}개)")


# ═══════════════════════════════════════════════
# STEP 6: CSV → DB 시드 (Korea/Japan 등 DB에 없는 리전)
# ═══════════════════════════════════════════════

def seed_missing_regions_from_csv(conn):
    """CSV에는 있지만 DB에 없는 리전의 영상을 DB에 시드."""
    import csv as csv_mod

    if not CSV_OUT.exists():
        log.warning(f"CSV 파일 없음: {CSV_OUT}")
        return 0

    db_regions = {r[0] for r in conn.execute("SELECT DISTINCT region FROM videos").fetchall()}
    log.info(f"  DB 기존 리전: {sorted(db_regions)}")

    with open(CSV_OUT, "r", encoding="utf-8-sig") as f:
        rows = list(csv_mod.DictReader(f))

    missing_rows = [r for r in rows if r["region"] not in db_regions]
    if not missing_rows:
        log.info("  CSV → DB 시드: 해당 없음 (모든 리전이 DB에 존재)")
        return 0

    missing_regions = sorted({r["region"] for r in missing_rows})
    log.info(f"  DB에 없는 리전 발견: {missing_regions}")

    now = datetime.now(timezone.utc).isoformat()
    seeded = 0
    for r in missing_rows:
        conn.execute("""
            INSERT OR IGNORE INTO videos (
                video_id, channel_id, region, title, description,
                published_at, thumbnail_url, duration,
                view_count, like_count, comment_count, tags,
                fetched_at, updated_at,
                is_collab, collab_partner, collab_category, content_category, classified_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r["video_id"], "", r["region"], r["title"],
            r.get("description_preview", ""),
            r["published_at"], r.get("thumbnail_url", ""), r.get("duration", ""),
            int(r.get("views", 0) or 0),
            int(r.get("likes", 0) or 0),
            int(r.get("comments", 0) or 0),
            "", now, now,
            int(r.get("is_collab", 0) or 0),
            r.get("partner", ""),
            r.get("collab_category", ""),
            r.get("content_category", ""),
            "csv_seed" if (r.get("is_collab") == "1" or r.get("content_category")) else None,
        ))
        seeded += 1
    conn.commit()

    for region in missing_regions:
        count = sum(1 for r in missing_rows if r["region"] == region)
        log.info(f"    {region}: {count}개 시드 완료")

    log.info(f"  총 {seeded}개 시드 완료")
    return seeded


# ═══════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="YouTube 리전 데이터 증분 업데이트")
    parser.add_argument("--region", help="특정 리전만 업데이트 (예: Turkey)")
    parser.add_argument("--dry-run", action="store_true", help="실제 수집 없이 신규 수만 확인")
    parser.add_argument("--skip-classify", action="store_true", help="GPT 분류 건너뛰기")
    parser.add_argument("--json-only", action="store_true", help="JSON만 재생성 (fetch/classify 스킵)")
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("  YouTube Regional 증분 업데이트")
    log.info("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # DB에 없는 리전(Korea/Japan 등)을 CSV에서 시드
    seed_missing_regions_from_csv(conn)

    if args.json_only:
        regenerate_json(conn)
        regenerate_csv(conn)
        conn.close()
        log.info("완료!")
        return

    youtube = get_youtube()
    last_dates = get_last_date_per_region(conn)

    regions_to_update = [args.region] if args.region else list(CHANNELS.keys())
    all_new_videos = []

    for region in regions_to_update:
        handle = CHANNELS.get(region)
        if not handle:
            log.warning(f"알 수 없는 리전: {region}")
            continue

        last = last_dates.get(region, "없음")
        log.info(f"\n{'─'*45}")
        log.info(f"  [{region}] {handle}")
        log.info(f"  마지막 수집: {last}")
        log.info(f"{'─'*45}")

        # Resolve channel
        ch_id, uploads, title = resolve_channel(youtube, handle)
        if not ch_id:
            log.error(f"  채널 해석 실패: {handle}")
            continue
        log.info(f"  채널: {title} ({ch_id})")

        # Get existing IDs
        existing_ids = get_existing_video_ids(conn, region)
        log.info(f"  기존 영상: {len(existing_ids)}개")

        # Fetch new video IDs
        try:
            new_ids = fetch_new_video_ids(youtube, uploads, existing_ids)
        except Exception as e:
            log.error(f"  영상 ID 수집 실패: {e}")
            continue
        log.info(f"  신규 영상: {len(new_ids)}개")

        if args.dry_run or not new_ids:
            continue

        # Fetch details
        log.info(f"  상세 정보 수집 중...")
        try:
            new_details = fetch_video_details(youtube, new_ids, region)
        except Exception as e:
            log.error(f"  상세 정보 수집 실패: {e}")
            continue
        log.info(f"  {len(new_details)}개 수집 완료")

        # Save to DB
        save_videos_to_db(conn, new_details)
        all_new_videos.extend(new_details)

    if args.dry_run:
        conn.close()
        log.info("\n[dry-run] 실제 수집 없이 종료")
        return

    # Classify new videos
    if all_new_videos and not args.skip_classify:
        log.info(f"\n총 {len(all_new_videos)}개 신규 영상 GPT 분류 시작...")
        collab_map, nc_map = classify_new_videos(all_new_videos)
        save_classifications(conn, collab_map, nc_map)

        collab_count = sum(1 for v in collab_map.values() if v.get('is_collab'))
        log.info(f"  분류 완료: 콜라보 {collab_count}개, 비콜라보 {len(all_new_videos) - collab_count}개")

    # Regenerate JSON & CSV
    regenerate_json(conn)
    regenerate_csv(conn)

    conn.close()

    # Summary
    total = sum(1 for _ in sqlite3.connect(DB_PATH).execute("SELECT 1 FROM videos"))
    log.info(f"\n{'='*55}")
    log.info(f"  업데이트 완료!")
    log.info(f"  신규 수집: {len(all_new_videos)}개")
    log.info(f"  DB 전체: {total}개")
    log.info(f"{'='*55}")


if __name__ == "__main__":
    main()
