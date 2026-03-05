"""
Pipeline Step 2: Classify
룰 기반 + GPT를 활용하여 콜라보/联动 콘텐츠를 분류합니다.
"""
import re
import logging
from typing import Optional

from db.models import get_conn, get_unclassified_posts, update_classification

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Known Partners (알려진 콜라보 파트너 매핑)
# ──────────────────────────────────────────────
KNOWN_PARTNERS = {
    # Brand
    "兰博基尼": ("Lamborghini", "Brand"),
    "lamborghini": ("Lamborghini", "Brand"),
    "保时捷": ("Porsche", "Brand"),
    "porsche": ("Porsche", "Brand"),
    "特斯拉": ("Tesla", "Brand"),
    "tesla": ("Tesla", "Brand"),
    "玛莎拉蒂": ("Maserati", "Brand"),
    "maserati": ("Maserati", "Brand"),
    "迈凯伦": ("McLaren", "Brand"),
    "mclaren": ("McLaren", "Brand"),
    "阿斯顿马丁": ("Aston Martin", "Brand"),
    "aston martin": ("Aston Martin", "Brand"),
    "布加迪": ("Bugatti", "Brand"),
    "bugatti": ("Bugatti", "Brand"),
    "ducati": ("Ducati", "Brand"),
    "杜卡迪": ("Ducati", "Brand"),
    "bmw": ("BMW", "Brand"),
    "奔驰": ("Mercedes-Benz", "Brand"),
    "mercedes": ("Mercedes-Benz", "Brand"),
    "山德士": ("KFC", "Brand"),
    "肯德基": ("KFC", "Brand"),

    # Anime
    "龙珠": ("Dragon Ball", "Anime"),
    "dragon ball": ("Dragon Ball", "Anime"),
    "咒术回战": ("Jujutsu Kaisen", "Anime"),
    "jujutsu kaisen": ("Jujutsu Kaisen", "Anime"),
    "进击的巨人": ("Attack on Titan", "Anime"),
    "新世纪福音战士": ("Evangelion", "Anime"),
    "eva": ("Evangelion", "Anime"),
    "初音未来": ("Hatsune Miku", "Anime"),
    "neon genesis": ("Evangelion", "Anime"),
    "鬼灭之刃": ("Demon Slayer", "Anime"),
    "火影忍者": ("Naruto", "Anime"),
    "naruto": ("Naruto", "Anime"),
    "喜羊羊": ("Pleasant Goat", "Anime"),
    "灰太狼": ("Pleasant Goat", "Anime"),

    # Artist
    "blackpink": ("BLACKPINK", "Artist"),
    "alan walker": ("Alan Walker", "Artist"),
    "周杰伦": ("Jay Chou", "Artist"),
    "jay chou": ("Jay Chou", "Artist"),
    "田曦薇": ("Tian Xiwei", "Artist"),

    # Game
    "生化危机": ("Resident Evil", "Game"),
    "resident evil": ("Resident Evil", "Game"),
    "仙剑奇侠传": ("Chinese Paladin", "Game"),
    "仙剑": ("Chinese Paladin", "Game"),
    "metro": ("Metro Exodus", "Game"),
    "地铁": ("Metro Exodus", "Game"),

    # Movie
    "哥斯拉": ("Godzilla", "Movie"),
    "godzilla": ("Godzilla", "Movie"),
    "哪吒": ("Ne Zha", "Movie"),
    "变形金刚": ("Transformers", "Movie"),
    "transformers": ("Transformers", "Movie"),
    "蜘蛛侠": ("Spider-Man", "Movie"),
    "spider-man": ("Spider-Man", "Movie"),

    # IP
    "hello kitty": ("Hello Kitty", "IP"),
    "小黄人": ("Minions", "IP"),
    "minions": ("Minions", "IP"),
    "米洛卡卡": ("Milokaka", "IP"),
}

# Collab signal keywords (联动 신호 키워드)
COLLAB_KEYWORDS = [
    "联动", "合作", "联名", "跨界",
    "×",  # multiplication sign used in collabs
    "✕", "✖",
    "collab", "collaboration",
    "携手", "牵手", "强强联合",
    "梦幻联动", "重磅联动", "深度联动",
    "IP联动", "品牌联动",
    "正版授权", "独家合作",
]

# Anti-keywords: terms that indicate NOT a collab
NON_COLLAB_KEYWORDS = [
    "更新公告", "维护公告", "热更公告",
    "赛事", "比赛", "冠军", "联赛", "PEL", "PEI",
    "Bug修复", "版本更新",
    "签到", "登录奖励",
]


def run_classify(no_gpt: bool = False, reclassify: bool = False) -> dict:
    """
    Classify posts as collab or non-collab.

    Args:
        no_gpt: If True, only use rule-based classification
        reclassify: If True, re-classify all posts (not just unclassified)

    Returns:
        Summary dict
    """
    stats = {"total": 0, "collab_rule": 0, "collab_gpt": 0,
             "non_collab": 0, "uncertain": 0, "errors": 0}

    with get_conn() as conn:
        if reclassify:
            # Reset all classifications
            conn.execute("UPDATE posts SET classified_at = NULL")
            conn.commit()

        posts = get_unclassified_posts(conn)
        stats["total"] = len(posts)
        logger.info(f"[Classify] {len(posts)} posts to classify")

        # Load GPT client if needed
        gpt_client = None
        if not no_gpt:
            try:
                from clients.gpt_client import GPTClient
                gpt_client = GPTClient()
                logger.info("[Classify] GPT client loaded")
            except Exception as e:
                logger.warning(f"[Classify] GPT unavailable: {e}. Using rules only.")

        for post in posts:
            try:
                result = _classify_by_rules(post)

                if result and result["confidence"] >= 0.7:
                    # High confidence rule match
                    update_classification(
                        conn,
                        mid=post["mid"],
                        is_collab=result["is_collab"],
                        partner=result["partner"],
                        category=result["category"],
                        region=result.get("region", "CN"),
                        classified_by="rule",
                        confidence=result["confidence"],
                    )
                    if result["is_collab"]:
                        stats["collab_rule"] += 1
                    else:
                        stats["non_collab"] += 1

                elif gpt_client:
                    # Use GPT for uncertain cases
                    gpt_result = gpt_client.classify_collab(
                        text=post["text_clean"] or post["text_raw"] or "",
                        page_title=post.get("page_title", "") or "",
                        mid=post["mid"],
                    )
                    if gpt_result:
                        update_classification(
                            conn,
                            mid=post["mid"],
                            is_collab=gpt_result["is_collab"],
                            partner=gpt_result["partner"],
                            category=gpt_result["category"],
                            region=gpt_result.get("region", "CN"),
                            classified_by="gpt",
                            confidence=gpt_result["confidence"],
                        )
                        if gpt_result["is_collab"]:
                            stats["collab_gpt"] += 1
                        else:
                            stats["non_collab"] += 1
                    else:
                        stats["errors"] += 1
                else:
                    # Rule-based with low confidence, no GPT
                    if result:
                        update_classification(
                            conn,
                            mid=post["mid"],
                            is_collab=result["is_collab"],
                            partner=result["partner"],
                            category=result["category"],
                            region=result.get("region", "CN"),
                            classified_by="rule",
                            confidence=result["confidence"],
                        )
                        if result["is_collab"]:
                            stats["collab_rule"] += 1
                        else:
                            stats["non_collab"] += 1
                    else:
                        stats["uncertain"] += 1

            except Exception as e:
                logger.error(f"[Classify] Error on mid={post['mid']}: {e}")
                stats["errors"] += 1

    logger.info(f"[Classify] Done. {stats}")
    return stats


def _classify_by_rules(post: dict) -> Optional[dict]:
    """
    Rule-based collab classification.

    Returns dict with is_collab, partner, category, confidence.
    """
    text = (post.get("text_clean") or post.get("text_raw") or "").lower()
    page_title = (post.get("page_title") or "").lower()
    combined = f"{text} {page_title}"

    # Step 1: Check anti-keywords first
    for kw in NON_COLLAB_KEYWORDS:
        if kw in combined:
            # Could still be a collab if also has collab keywords
            # but reduce confidence
            pass

    # Step 2: Check known partners
    for keyword, (partner_name, category) in KNOWN_PARTNERS.items():
        if keyword.lower() in combined:
            # Found a known partner
            has_collab_signal = any(kw in combined for kw in COLLAB_KEYWORDS)
            confidence = 0.95 if has_collab_signal else 0.75
            return {
                "is_collab": True,
                "partner": partner_name,
                "category": category,
                "region": "CN",
                "confidence": confidence,
            }

    # Step 3: Check collab keywords without known partner
    collab_signals = [kw for kw in COLLAB_KEYWORDS if kw in combined]
    if collab_signals:
        # Has collab signal but unknown partner → low confidence
        partner = _extract_partner_from_text(combined)
        return {
            "is_collab": True,
            "partner": partner or "Unknown",
            "category": "Other",
            "region": "CN",
            "confidence": 0.5 if partner else 0.3,
        }

    # Step 4: Check for "×" pattern (A × B format common in Chinese collabs)
    x_match = re.search(r"和平精英\s*[×✕✖xX]\s*(.+?)(?:\s|$|[,，。!！])", combined)
    if x_match:
        partner_raw = x_match.group(1).strip()
        return {
            "is_collab": True,
            "partner": partner_raw,
            "category": "Other",
            "region": "CN",
            "confidence": 0.85,
        }

    # Step 5: Check hashtag patterns (#和平精英×Partner#)
    hashtag_match = re.search(
        r"#和平精英[×✕xX](.+?)#", combined
    )
    if hashtag_match:
        partner_raw = hashtag_match.group(1).strip()
        return {
            "is_collab": True,
            "partner": partner_raw,
            "category": "Other",
            "region": "CN",
            "confidence": 0.9,
        }

    # No collab detected
    is_definitely_not = any(kw in combined for kw in NON_COLLAB_KEYWORDS)
    return {
        "is_collab": False,
        "partner": "",
        "category": "",
        "region": "CN",
        "confidence": 0.85 if is_definitely_not else 0.5,
    }


def _extract_partner_from_text(text: str) -> str:
    """Try to extract a partner name from text using patterns."""
    # Pattern: 「XXX」 or 《XXX》 (common in Chinese for titles/names)
    patterns = [
        r"[「『《](.+?)[」』》]",
        r"【(.+?)】",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text)
        # Filter out common non-partner matches
        for m in matches:
            if m not in ("和平精英", "游戏", "活动", "公告"):
                return m
    return ""
