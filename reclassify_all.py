"""
전체 영상 GPT 재분류 스크립트.
youtube_pipeline DB + Regional DB의 모든 영상을 GPT-4o-mini로 일괄 재분류.

Phase A: 콜라보 판별 (제목+리전 → collab여부, 파트너명, 카테고리)
Phase B: 비콜라보 콘텐츠 분류 (제목 → 콘텐츠 카테고리)

사용법:
    python reclassify_all.py --db youtube     # youtube_pipeline DB만
    python reclassify_all.py --db regional    # Regional DB만
    python reclassify_all.py --db all         # 둘 다
    python reclassify_all.py --db youtube --resume   # 중단 지점부터 이어서
"""
import argparse
import json
import os
import sqlite3
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "youtube_pipeline" / ".env")

from openai import OpenAI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("reclassify")

CLIENT = OpenAI(api_key=os.getenv("GPT_API_KEY", ""))
MODEL = "gpt-4o-mini"
BATCH_COLLAB = 30
BATCH_CONTENT = 30
CHECKPOINT_DIR = Path(__file__).parent / "_reclassify_checkpoints"

YT_DB = Path(__file__).parent / "youtube_pipeline" / "data" / "pubg_collab.db"
REGIONAL_DB = Path(r"C:\Users\sangwon.ji\Desktop\Google_PUBG\data\pubgm_youtube_regional.db")

# ── Prompts ──

COLLAB_SYSTEM = """You are classifying PUBG Mobile / Free Fire YouTube video titles as collaboration or not.

A **collaboration** video features a specific EXTERNAL partner:
- Brand (car, fashion, food, tech): e.g. Lamborghini, McLaren, Apollo, Samsung
- Artist/Musician: e.g. BLACKPINK, Alan Walker, OneRepublic, BamBam
- Anime/Manga IP: e.g. Jujutsu Kaisen, Dragon Ball, Evangelion, Naruto
- Movie/TV IP: e.g. Godzilla, The Walking Dead, Arcane, The Boys
- Game crossover: e.g. Resident Evil, Metro Exodus
- Creator/Influencer: e.g. MrBeast, specific YouTuber names
- Sports personality: e.g. Messi, Richarlison

**NOT a collaboration** (do NOT mark as collab):
- In-game events, updates, patches, seasons, anniversary celebrations
- Esports tournaments, competitive content
- Original characters (Chicko, Victor, Sara, Andy, Carlo)
- In-game features: modes, maps, weapons, skins, Royale Pass, WOW, Craftland
- Music created specifically for the game without a named external artist
- Promotional/marketing content without external partner
- Community content, memes, tutorials

For each title, respond JSON:
{"results": [{"collab": true/false, "partner": "ExactPartnerName or empty", "category": "Brand/Artist/Anime/Movie/Game/Creator/Sports/Other or empty"}]}

Be STRICT: only mark collab if there is a clearly identifiable external partner name."""

CONTENT_SYSTEM = """Classify PUBG Mobile / Free Fire YouTube video titles into exactly ONE content category.

Categories:
- Update: version updates, patch notes, new features, optimization
- Esports: tournaments, competitive events, PMPL, PMGC, scrims
- Mode: game modes (Metro, Payload, Arena, TDM, Infection, Zombie)
- Map: map updates, new areas, map guides (Erangel, Miramar, Livik, Nusa)
- Skin: outfit previews, skin showcases, lucky draws, crate openings
- Pass: Royale Pass, season pass, RP rewards
- Weapon: gun guides, weapon comparisons, new weapons
- Vehicle: vehicle showcases (not brand collabs)
- Shorts: short-form comedy clips, memes, funny moments (typically <60s)
- Chicko: content featuring Chicko character
- WOW: World of Wonder / creative mode content
- Craftland: Craftland related content
- Anniversary: birthday/anniversary celebrations
- Season: new season announcements, season themes
- Event: in-game events, limited-time activities
- Promotion: marketing, trailers, cinematic ads
- Community: fan content, community highlights, user submissions
- Tutorial: guides, tips, how-to content
- Livestream: live broadcasts, live events
- Music: original music, OST, theme songs (no external artist collab)
- Story: lore, backstory, character stories, animations
- Festive: holiday-themed (Ramadan, Christmas, Lunar New Year, Diwali)
- Other: doesn't fit above categories

Reply JSON: {"c":["cat1","cat2",...]}
One category per title, in order matching the input list."""


def gpt_call(system, user_prompt, max_tokens=4096):
    for attempt in range(3):
        try:
            resp = CLIENT.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            if attempt == 2:
                log.warning("GPT error (3 retries failed): %s", e)
                return None
            time.sleep(2 ** attempt)
    return None


# ── Checkpoint helpers ──

def _ckpt_path(db_name, phase):
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    return CHECKPOINT_DIR / f"{db_name}_{phase}.json"


def save_checkpoint(db_name, phase, results):
    path = _ckpt_path(db_name, phase)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False)


def load_checkpoint(db_name, phase):
    path = _ckpt_path(db_name, phase)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def clear_checkpoints(db_name):
    for phase in ("collab", "content"):
        path = _ckpt_path(db_name, phase)
        if path.exists():
            path.unlink()


# ── Phase A: Collab classification ──

def classify_collabs(videos, db_name, resume=False):
    """Batch classify collab/non-collab. Returns {video_id: {collab, partner, category}}"""
    existing = load_checkpoint(db_name, "collab") if resume else {}
    to_process = [v for v in videos if v["video_id"] not in existing]

    if existing:
        log.info("  Phase A: %d already done, %d remaining", len(existing), len(to_process))
    else:
        log.info("  Phase A: %d videos to classify", len(to_process))

    results = dict(existing)

    for i in range(0, len(to_process), BATCH_COLLAB):
        batch = to_process[i:i + BATCH_COLLAB]
        titles = [
            "%d. [%s] %s" % (j + 1, v.get("region", v.get("source_channel", "")), v["title"][:100])
            for j, v in enumerate(batch)
        ]

        data = gpt_call(COLLAB_SYSTEM, "\n".join(titles))
        if data:
            raw = data.get("results", [])
            if not isinstance(raw, list):
                raw = list(data.values())[0] if data else []
                if not isinstance(raw, list):
                    raw = []
            for j, v in enumerate(batch):
                if j < len(raw) and isinstance(raw[j], dict):
                    results[v["video_id"]] = {
                        "collab": bool(raw[j].get("collab", False)),
                        "partner": raw[j].get("partner", ""),
                        "category": raw[j].get("category", ""),
                    }
                else:
                    results[v["video_id"]] = {"collab": False, "partner": "", "category": ""}
        else:
            for v in batch:
                results[v["video_id"]] = {"collab": False, "partner": "", "category": ""}

        done = min(i + BATCH_COLLAB, len(to_process))
        total_done = len(existing) + done
        total_all = len(videos)
        if done % 300 == 0 or done == len(to_process):
            collab_count = sum(1 for r in results.values() if r.get("collab"))
            log.info("  Phase A [%d/%d] collab=%d", total_done, total_all, collab_count)
            save_checkpoint(db_name, "collab", results)

    save_checkpoint(db_name, "collab", results)
    collab_count = sum(1 for r in results.values() if r.get("collab"))
    log.info("  Phase A complete: %d collab, %d non-collab", collab_count, len(results) - collab_count)
    return results


# ── Phase B: Non-collab content classification ──

def classify_content(videos, collab_results, db_name, resume=False):
    """Batch classify content categories for non-collab videos."""
    non_collab_videos = [v for v in videos if not collab_results.get(v["video_id"], {}).get("collab", False)]
    existing = load_checkpoint(db_name, "content") if resume else {}
    to_process = [v for v in non_collab_videos if v["video_id"] not in existing]

    if existing:
        log.info("  Phase B: %d already done, %d remaining", len(existing), len(to_process))
    else:
        log.info("  Phase B: %d non-collab videos to classify", len(to_process))

    results = dict(existing)

    for i in range(0, len(to_process), BATCH_CONTENT):
        batch = to_process[i:i + BATCH_CONTENT]
        titles = ["%d. %s" % (j + 1, v["title"][:80]) for j, v in enumerate(batch)]

        data = gpt_call(CONTENT_SYSTEM, "\n".join(titles), max_tokens=4096)
        if data:
            cats = list(data.values())[0] if isinstance(data, dict) else []
            if not isinstance(cats, list):
                cats = []
            while len(cats) < len(batch):
                cats.append("Other")
            for j, v in enumerate(batch):
                results[v["video_id"]] = str(cats[j]) if j < len(cats) else "Other"
        else:
            for v in batch:
                results[v["video_id"]] = "Other"

        done = min(i + BATCH_CONTENT, len(to_process))
        total_done = len(existing) + done
        total_all = len(non_collab_videos)
        if done % 300 == 0 or done == len(to_process):
            log.info("  Phase B [%d/%d]", total_done, total_all)
            save_checkpoint(db_name, "content", results)

    save_checkpoint(db_name, "content", results)
    log.info("  Phase B complete: %d videos classified", len(results))
    return results


# ── DB-specific handlers ──

def fix_others_youtube():
    """Re-classify only 'Other' content_type videos in youtube_pipeline DB."""
    log.info("=" * 55)
    log.info("  youtube_pipeline DB: fix 'Other' content types")
    log.info("=" * 55)

    conn = sqlite3.connect(str(YT_DB))
    rows = conn.execute(
        "SELECT video_id, title, source_channel FROM videos "
        "WHERE is_collab=0 AND (content_type='Other' OR content_type IS NULL) ORDER BY published_at"
    ).fetchall()
    videos = [{"video_id": r[0], "title": r[1], "source_channel": r[2] or "pubgm"} for r in rows]
    log.info("  Loaded %d 'Other' videos to reclassify", len(videos))

    existing = load_checkpoint("youtube_fix", "content") if True else {}
    to_process = [v for v in videos if v["video_id"] not in existing]
    results = dict(existing)

    if existing:
        log.info("  Checkpoint: %d done, %d remaining", len(existing), len(to_process))

    for i in range(0, len(to_process), BATCH_CONTENT):
        batch = to_process[i:i + BATCH_CONTENT]
        titles = ["%d. %s" % (j + 1, v["title"][:80]) for j, v in enumerate(batch)]
        data = gpt_call(CONTENT_SYSTEM, "\n".join(titles), max_tokens=4096)
        if data:
            cats = list(data.values())[0] if isinstance(data, dict) else []
            if not isinstance(cats, list):
                cats = []
            while len(cats) < len(batch):
                cats.append("Other")
            for j, v in enumerate(batch):
                results[v["video_id"]] = str(cats[j]) if j < len(cats) else "Other"
        else:
            for v in batch:
                results[v["video_id"]] = "Other"

        done = min(i + BATCH_CONTENT, len(to_process))
        if done % 300 == 0 or done == len(to_process):
            log.info("  Fix [%d/%d]", len(existing) + done, len(videos))
            save_checkpoint("youtube_fix", "content", results)

    save_checkpoint("youtube_fix", "content", results)

    now = datetime.now(timezone.utc).isoformat()
    for vid, cat in results.items():
        conn.execute(
            "UPDATE videos SET content_type=?, updated_at=? WHERE video_id=?",
            (cat, now, vid)
        )
    conn.commit()
    conn.close()
    clear_checkpoints("youtube_fix")

    still_other = sum(1 for c in results.values() if c == "Other")
    log.info("  Fix done: %d reclassified, %d still Other", len(results) - still_other, still_other)


def fix_others_regional():
    """Re-classify only 'Other' content_category videos in regional DB."""
    log.info("=" * 55)
    log.info("  Regional DB: fix 'Other' content categories")
    log.info("=" * 55)

    conn = sqlite3.connect(str(REGIONAL_DB))
    rows = conn.execute(
        "SELECT video_id, title, region FROM videos "
        "WHERE is_collab=0 AND (content_category='Other' OR content_category IS NULL) ORDER BY published_at"
    ).fetchall()
    videos = [{"video_id": r[0], "title": r[1], "region": r[2] or "Unknown"} for r in rows]
    log.info("  Loaded %d 'Other' videos to reclassify", len(videos))

    existing = load_checkpoint("regional_fix", "content") if True else {}
    to_process = [v for v in videos if v["video_id"] not in existing]
    results = dict(existing)

    if existing:
        log.info("  Checkpoint: %d done, %d remaining", len(existing), len(to_process))

    for i in range(0, len(to_process), BATCH_CONTENT):
        batch = to_process[i:i + BATCH_CONTENT]
        titles = ["%d. %s" % (j + 1, v["title"][:80]) for j, v in enumerate(batch)]
        data = gpt_call(CONTENT_SYSTEM, "\n".join(titles), max_tokens=4096)
        if data:
            cats = list(data.values())[0] if isinstance(data, dict) else []
            if not isinstance(cats, list):
                cats = []
            while len(cats) < len(batch):
                cats.append("Other")
            for j, v in enumerate(batch):
                results[v["video_id"]] = str(cats[j]) if j < len(cats) else "Other"
        else:
            for v in batch:
                results[v["video_id"]] = "Other"

        done = min(i + BATCH_CONTENT, len(to_process))
        if done % 300 == 0 or done == len(to_process):
            log.info("  Fix [%d/%d]", len(existing) + done, len(videos))
            save_checkpoint("regional_fix", "content", results)

    save_checkpoint("regional_fix", "content", results)

    now = datetime.now(timezone.utc).isoformat()
    for vid, cat in results.items():
        conn.execute(
            "UPDATE videos SET content_category=?, updated_at=? WHERE video_id=?",
            (cat, now, vid)
        )
    conn.commit()
    conn.close()
    clear_checkpoints("regional_fix")

    still_other = sum(1 for c in results.values() if c == "Other")
    log.info("  Fix done: %d reclassified, %d still Other", len(results) - still_other, still_other)


def process_youtube_db(resume=False):
    log.info("=" * 55)
    log.info("  youtube_pipeline DB reclassification")
    log.info("=" * 55)

    conn = sqlite3.connect(str(YT_DB))
    rows = conn.execute(
        "SELECT video_id, title, source_channel FROM videos ORDER BY published_at"
    ).fetchall()
    videos = [{"video_id": r[0], "title": r[1], "source_channel": r[2] or "pubgm"} for r in rows]
    log.info("  Loaded %d videos", len(videos))

    collab_results = classify_collabs(videos, "youtube", resume)
    content_results = classify_content(videos, collab_results, "youtube", resume)

    log.info("  Saving to DB...")
    now = datetime.now(timezone.utc).isoformat()
    for vid, info in collab_results.items():
        conn.execute(
            "UPDATE videos SET is_collab=?, collab_partner=?, collab_category=?, "
            "collab_confidence=?, classification_method=?, updated_at=? WHERE video_id=?",
            (1 if info["collab"] else 0, info.get("partner", ""), info.get("category", ""),
             0.9, "gpt_reclassify", now, vid)
        )
    for vid, cat in content_results.items():
        conn.execute(
            "UPDATE videos SET content_type=?, updated_at=? WHERE video_id=?",
            (cat, now, vid)
        )
    conn.commit()
    conn.close()

    clear_checkpoints("youtube")
    log.info("  youtube_pipeline DB done!")


def process_regional_db(resume=False):
    log.info("=" * 55)
    log.info("  Regional DB reclassification")
    log.info("=" * 55)

    conn = sqlite3.connect(str(REGIONAL_DB))
    rows = conn.execute(
        "SELECT video_id, title, region FROM videos ORDER BY published_at"
    ).fetchall()
    videos = [{"video_id": r[0], "title": r[1], "region": r[2] or "Unknown"} for r in rows]
    log.info("  Loaded %d videos", len(videos))

    collab_results = classify_collabs(videos, "regional", resume)
    content_results = classify_content(videos, collab_results, "regional", resume)

    log.info("  Saving to DB...")
    now = datetime.now(timezone.utc).isoformat()
    for vid, info in collab_results.items():
        conn.execute(
            "UPDATE videos SET is_collab=?, collab_partner=?, collab_category=?, "
            "classified_by=?, updated_at=? WHERE video_id=?",
            (1 if info["collab"] else 0, info.get("partner", ""), info.get("category", ""),
             "gpt_reclassify", now, vid)
        )
    for vid, cat in content_results.items():
        conn.execute(
            "UPDATE videos SET content_category=?, updated_at=? WHERE video_id=?",
            (cat, now, vid)
        )
    conn.commit()
    conn.close()

    clear_checkpoints("regional")
    log.info("  Regional DB done!")


def main():
    parser = argparse.ArgumentParser(description="전체 영상 GPT 재분류")
    parser.add_argument("--db", required=True, choices=["youtube", "regional", "all"])
    parser.add_argument("--resume", action="store_true", help="체크포인트에서 이어서 진행")
    parser.add_argument("--fix-others", action="store_true",
                        help="'Other'로 분류된 영상만 재처리")
    args = parser.parse_args()

    log.info("GPT model: %s, Collab batch: %d, Content batch: %d", MODEL, BATCH_COLLAB, BATCH_CONTENT)

    if args.fix_others:
        if args.db in ("youtube", "all"):
            fix_others_youtube()
        if args.db in ("regional", "all"):
            fix_others_regional()
    else:
        if args.db in ("youtube", "all"):
            process_youtube_db(args.resume)
        if args.db in ("regional", "all"):
            process_regional_db(args.resume)

    log.info("All done!")


if __name__ == "__main__":
    main()
