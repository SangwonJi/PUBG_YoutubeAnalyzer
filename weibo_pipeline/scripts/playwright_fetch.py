"""
Playwright 기반 웨이보 스크래퍼 v3
브라우저 세션 내에서 직접 API를 호출하여 전체 게시물을 수집합니다.
스크롤 방식이 아닌, 브라우저의 인증된 세션으로 API를 직접 호출합니다.

사용법:
    pip install playwright
    playwright install chromium
    python scripts/playwright_fetch.py
"""
import re
import json
import time
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Settings ──────────────────────────────────
WEIBO_UID = "7095404909"
DB_PATH = Path("data/pubg_weibo.db")
REQUEST_DELAY = 4          # 요청 간 대기 (초)
SAVE_EVERY = 100           # N개마다 DB 저장
MAX_EMPTY_PAGES = 5        # 연속 빈 페이지 시 중단
# ──────────────────────────────────────────────


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    schema_path = Path("db/schema.sql")
    if schema_path.exists():
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(schema_path.read_text(encoding="utf-8"))
    logger.info(f"[DB] Ready: {DB_PATH}")


def clean_html(text: str) -> str:
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", "", text)
    clean = clean.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    clean = clean.replace("&nbsp;", " ").replace("&quot;", '"')
    return clean.strip()


def save_posts(posts: list[dict]):
    with sqlite3.connect(DB_PATH) as conn:
        for p in posts:
            conn.execute("""
                INSERT INTO posts (
                    mid, bid, created_at, text_raw, text_clean, source,
                    reposts_count, comments_count, attitudes_count,
                    has_video, has_image, media_type,
                    video_url, thumbnail_url, video_duration,
                    page_type, page_title, page_url,
                    fetched_at, updated_at
                ) VALUES (
                    :mid, :bid, :created_at, :text_raw, :text_clean, :source,
                    :reposts_count, :comments_count, :attitudes_count,
                    :has_video, :has_image, :media_type,
                    :video_url, :thumbnail_url, :video_duration,
                    :page_type, :page_title, :page_url,
                    :fetched_at, :updated_at
                )
                ON CONFLICT(mid) DO UPDATE SET
                    reposts_count = excluded.reposts_count,
                    comments_count = excluded.comments_count,
                    attitudes_count = excluded.attitudes_count,
                    updated_at = excluded.updated_at
            """, p)
        conn.commit()


def parse_mblog(mblog: dict) -> dict | None:
    try:
        mid = str(mblog.get("id", mblog.get("mid", "")))
        if not mid:
            return None

        text_raw = mblog.get("text", "")
        text_clean = clean_html(text_raw)

        has_video = 0
        has_image = 0
        media_type = "text"
        video_url = None
        thumbnail_url = None
        video_duration = 0
        page_type = None
        page_title = None
        page_url = None

        page_info = mblog.get("page_info") or {}
        if page_info:
            page_type = page_info.get("type", "")
            page_title = page_info.get("page_title", "")
            page_url = page_info.get("page_url", "")
            if page_type == "video":
                has_video = 1
                media_type = "video"
                urls = page_info.get("urls") or page_info.get("media_info") or {}
                if isinstance(urls, dict):
                    video_url = (
                        urls.get("mp4_720p_mp4")
                        or urls.get("mp4_hd_mp4")
                        or urls.get("mp4_sd_mp4")
                        or urls.get("mp4_ld_mp4")
                        or urls.get("stream_url")
                    )
                pic = page_info.get("page_pic") or {}
                thumbnail_url = pic.get("url", "") if isinstance(pic, dict) else ""
                mi = page_info.get("media_info") or {}
                dur = mi.get("duration", 0)
                try:
                    video_duration = int(float(dur)) if dur else 0
                except (ValueError, TypeError):
                    video_duration = 0

        pics = mblog.get("pics") or []
        if pics and not has_video:
            has_image = 1
            media_type = "image"

        created_at = mblog.get("created_at", "")
        try:
            dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            created_at = dt.astimezone(timezone.utc).isoformat()
        except (ValueError, TypeError):
            pass

        now = datetime.now(timezone.utc).isoformat()
        return {
            "mid": mid,
            "bid": mblog.get("bid", ""),
            "created_at": created_at,
            "text_raw": text_raw,
            "text_clean": text_clean,
            "source": clean_html(mblog.get("source", "")),
            "reposts_count": mblog.get("reposts_count", 0),
            "comments_count": mblog.get("comments_count", 0),
            "attitudes_count": mblog.get("attitudes_count", 0),
            "has_video": has_video,
            "has_image": has_image,
            "media_type": media_type,
            "video_url": video_url,
            "thumbnail_url": thumbnail_url,
            "video_duration": video_duration,
            "page_type": page_type,
            "page_title": page_title,
            "page_url": page_url,
            "fetched_at": now,
            "updated_at": now,
        }
    except Exception as e:
        logger.error(f"[Parse] Error: {e}")
        return None


def fetch_api_via_browser(page, container_id: str, since_id: str = None) -> dict | None:
    """브라우저 컨텍스트 내에서 API를 직접 호출 (쿠키 자동 포함)."""
    url = f"https://m.weibo.cn/api/container/getIndex?type=uid&value={WEIBO_UID}&containerid={container_id}"
    if since_id:
        url += f"&since_id={since_id}"

    try:
        result = page.evaluate(f"""
            async () => {{
                try {{
                    const resp = await fetch("{url}", {{
                        credentials: 'include',
                        headers: {{
                            'Accept': 'application/json',
                            'X-Requested-With': 'XMLHttpRequest'
                        }}
                    }});
                    const text = await resp.text();
                    return {{ status: resp.status, body: text }};
                }} catch(e) {{
                    return {{ status: 0, body: e.toString() }};
                }}
            }}
        """)

        if result["status"] != 200:
            logger.warning(f"[API] Status {result['status']}")
            return None

        data = json.loads(result["body"])
        if data.get("ok") != 1:
            msg = data.get("msg", "unknown")
            logger.warning(f"[API] Not OK: {msg}")
            return None

        return data

    except Exception as e:
        logger.error(f"[API] Error: {e}")
        return None


def main():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("=" * 50)
        print("  Playwright가 설치되지 않았습니다.")
        print("    pip install playwright")
        print("    playwright install chromium")
        print("=" * 50)
        return

    init_db()

    with sqlite3.connect(DB_PATH) as conn:
        existing_mids = set(
            row[0] for row in conn.execute("SELECT mid FROM posts").fetchall()
        )
    logger.info(f"[DB] Existing posts: {len(existing_mids)}")

    collected = []
    seen_mids = set(existing_mids)
    total_new = 0

    with sync_playwright() as p:
        logger.info("[Browser] Launching Chromium...")
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/17.0 Mobile/15E148 Safari/604.1"
            ),
            viewport={"width": 375, "height": 812},
            locale="zh-CN",
        )
        page = context.new_page()

        # ── Step 1: 로그인 ──
        logger.info("=" * 50)
        logger.info("  브라우저가 열립니다.")
        logger.info("  1. 웨이보에 로그인하세요")
        logger.info("  2. 로그인 완료 후 이 터미널에서 Enter를 누르세요")
        logger.info("=" * 50)

        page.goto("https://m.weibo.cn/login", timeout=60000)
        time.sleep(3)
        input("\n>>> 로그인 완료 후 Enter 키를 누르세요... ")

        # ── Step 2: 프로필 페이지 이동 (세션 활성화) ──
        logger.info("[Browser] Navigating to profile...")
        time.sleep(2)
        try:
            page.goto(f"https://m.weibo.cn/u/{WEIBO_UID}", timeout=60000)
        except Exception:
            time.sleep(3)
            page.goto(f"https://m.weibo.cn/u/{WEIBO_UID}", timeout=60000)
        time.sleep(5)

        logger.info(f"[Browser] Current URL: {page.url}")

        # ── Step 3: API 직접 호출로 수집 ──
        container_id = f"107603{WEIBO_UID}"
        since_id = None
        empty_count = 0
        request_count = 0

        logger.info("[Fetch] Starting API-based collection via browser...")
        logger.info("[Fetch] Press Ctrl+C to stop safely")

        try:
            while empty_count < MAX_EMPTY_PAGES:
                time.sleep(REQUEST_DELAY)
                request_count += 1

                data = fetch_api_via_browser(page, container_id, since_id)
                if not data:
                    empty_count += 1
                    logger.warning(f"[Fetch] Empty response ({empty_count}/{MAX_EMPTY_PAGES})")
                    time.sleep(5)  # 추가 대기
                    continue

                cards = data.get("data", {}).get("cards", [])
                if not cards:
                    empty_count += 1
                    logger.info(f"[Fetch] No cards ({empty_count}/{MAX_EMPTY_PAGES})")
                    continue

                new_in_batch = 0
                for card in cards:
                    if card.get("card_type") != 9:
                        continue
                    mblog = card.get("mblog")
                    if not mblog:
                        continue
                    post = parse_mblog(mblog)
                    if post and post["mid"] not in seen_mids:
                        collected.append(post)
                        seen_mids.add(post["mid"])
                        total_new += 1
                        new_in_batch += 1

                if new_in_batch > 0:
                    empty_count = 0
                    db_total = len(existing_mids) + total_new
                    if total_new % 20 < new_in_batch or total_new <= new_in_batch:
                        logger.info(
                            f"[Fetch] +{new_in_batch} new "
                            f"(session: {total_new}, DB total: {db_total}, "
                            f"requests: {request_count})"
                        )
                else:
                    empty_count += 1
                    logger.info(f"[Fetch] No new posts ({empty_count}/{MAX_EMPTY_PAGES})")

                # 페이지네이션
                cardlist_info = data.get("data", {}).get("cardlistInfo", {})
                new_since_id = cardlist_info.get("since_id")
                if new_since_id:
                    since_id = str(new_since_id)
                else:
                    logger.info("[Fetch] No more since_id, reached end of timeline")
                    break

                # 주기적 저장
                if len(collected) >= SAVE_EVERY:
                    save_posts(collected)
                    logger.info(f"[DB] Saved {len(collected)} posts")
                    collected.clear()

                # 50번마다 추가 대기
                if request_count % 50 == 0:
                    logger.info(f"[Throttle] Extended pause after {request_count} requests")
                    time.sleep(10)

        except KeyboardInterrupt:
            logger.info("\n[Stop] Ctrl+C detected. Saving...")

        # 남은 데이터 저장
        if collected:
            save_posts(collected)
            logger.info(f"[DB] Saved remaining {len(collected)} posts")

        browser.close()

    # 최종 통계
    with sqlite3.connect(DB_PATH) as conn:
        final_count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        date_range = conn.execute(
            "SELECT MIN(created_at), MAX(created_at) FROM posts"
        ).fetchone()

    logger.info("=" * 50)
    logger.info(f"  완료!")
    logger.info(f"  이번 세션 신규: {total_new}개")
    logger.info(f"  DB 총 게시물: {final_count}개")
    logger.info(f"  기간: {date_range[0]} ~ {date_range[1]}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
