#!/usr/bin/env python3
"""
CRITICAL AUDIT: Cross-validate numbers in all_context.txt against JSON source files.
"""
import json
import re
from pathlib import Path

DOCS = Path(r"c:\Users\sangwon.ji\pubg_collab_pipeline_v2\docs")

def parse_int(s):
    """Parse number with commas: '137,643,873' -> 137643873"""
    if s is None: return None
    return int(str(s).replace(",", "")) if str(s).replace(",", "").isdigit() else None

def extract_partner_line(line):
    """Parse: #1 BLACKPINK [Artist] 23 videos | 137,643,873 views | 1,180,236 likes | 39,391 comments"""
    m = re.match(r'#\d+\s+(.+?)\s+\[.+?\]\s+(\d+)\s+videos\s+\|\s+([\d,]+)\s+views\s+\|\s+([\d,]+)\s+likes\s+\|\s+([\d,]+)\s+comments', line)
    if m:
        return {
            "name": m.group(1).strip(),
            "video_count": int(m.group(2)),
            "total_views": parse_int(m.group(3)),
            "total_likes": parse_int(m.group(4)),
            "total_comments": parse_int(m.group(5))
        }
    return None

def find_in_context(context_lines, partner_name, region_hint=None):
    """Find partner entry in context - match by name (case-insensitive, partial)"""
    for i, line in enumerate(context_lines):
        if line.startswith("#") and partner_name.upper() in line.upper():
            parsed = extract_partner_line(line)
            if parsed:
                return parsed, i+1
    return None, None

def main():
    with open(DOCS / "all_context.txt", "r", encoding="utf-8") as f:
        context_lines = f.readlines()

    results = []

    # 1. YouTube Global - BLACKPINK (data.json)
    with open(DOCS / "data.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    bp = next((p for p in data if p["name"] == "BLACKPINK"), None)
    ctx, _ = find_in_context(context_lines, "BLACKPINK")
    if bp and ctx:
        r = {"test": "1. YouTube Global - BLACKPINK", "source": "data.json"}
        r["total_views"] = ("PASS" if bp["total_views"] == ctx["total_views"] else f"FAIL (JSON:{bp['total_views']} vs ctx:{ctx['total_views']})")
        r["total_likes"] = ("PASS" if bp["total_likes"] == ctx["total_likes"] else f"FAIL (JSON:{bp['total_likes']} vs ctx:{ctx['total_likes']})")
        r["total_comments"] = ("PASS" if bp["total_comments"] == ctx["total_comments"] else f"FAIL (JSON:{bp['total_comments']} vs ctx:{ctx['total_comments']})")
        r["video_count"] = ("PASS" if bp["video_count"] == ctx["video_count"] else f"FAIL (JSON:{bp['video_count']} vs ctx:{ctx['video_count']})")
        r["video_count_vs_array"] = "PASS" if bp["video_count"] == len(bp["videos"]) else f"FAIL (count:{bp['video_count']} vs array:{len(bp['videos'])})"
        results.append(r)

    # 2. YouTube Global - JUJUTSU KAISEN (data.json) - rank #23
    jjk = next((p for p in data if "Jujutsu" in p["name"] or "JUJUTSU" in p["name"]), None)
    if not jjk:
        jjk = next((p for p in data if "jujutsu" in p["name"].lower()), None)
    ctx, _ = find_in_context(context_lines, "Jujutsu Kaisen")
    if jjk and ctx:
        r = {"test": "2. YouTube Global - JUJUTSU KAISEN", "source": "data.json"}
        r["total_views"] = ("PASS" if jjk["total_views"] == ctx["total_views"] else f"FAIL (JSON:{jjk['total_views']} vs ctx:{ctx['total_views']})")
        r["total_likes"] = ("PASS" if jjk["total_likes"] == ctx["total_likes"] else f"FAIL (JSON:{jjk['total_likes']} vs ctx:{ctx['total_likes']})")
        r["total_comments"] = ("PASS" if jjk["total_comments"] == ctx["total_comments"] else f"FAIL (JSON:{jjk['total_comments']} vs ctx:{ctx['total_comments']})")
        r["video_count"] = ("PASS" if jjk["video_count"] == ctx["video_count"] else f"FAIL (JSON:{jjk['video_count']} vs ctx:{ctx['video_count']})")
        results.append(r)
    else:
        results.append({"test": "2. YouTube Global - JUJUTSU KAISEN", "source": "data.json", "error": f"jjk={jjk is not None}, ctx={ctx is not None}"})

    # 3. YouTube MENA - DRAGON BALL SUPER (yt_mena_data.json)
    with open(DOCS / "yt_mena_data.json", "r", encoding="utf-8") as f:
        mena = json.load(f)
    dbs_mena = next((p for p in mena if "Dragon" in p["name"] and "Ball" in p["name"]), None)
    ctx, _ = find_in_context(context_lines, "DRAGON BALL SUPER")
    if dbs_mena and ctx:
        r = {"test": "3. YouTube MENA - DRAGON BALL SUPER", "source": "yt_mena_data.json"}
        r["total_views"] = ("PASS" if dbs_mena["total_views"] == ctx["total_views"] else f"FAIL (JSON:{dbs_mena['total_views']} vs ctx:{ctx['total_views']})")
        r["total_likes"] = ("PASS" if dbs_mena["total_likes"] == ctx["total_likes"] else f"FAIL (JSON:{dbs_mena['total_likes']} vs ctx:{ctx['total_likes']})")
        r["total_comments"] = ("PASS" if dbs_mena["total_comments"] == ctx["total_comments"] else f"FAIL (JSON:{dbs_mena['total_comments']} vs ctx:{ctx['total_comments']})")
        r["video_count"] = ("PASS" if dbs_mena["video_count"] == ctx["video_count"] else f"FAIL (JSON:{dbs_mena['video_count']} vs ctx:{ctx['video_count']})")
        results.append(r)

    # 4. YouTube India - LOCO (yt_india_data.json)
    with open(DOCS / "yt_india_data.json", "r", encoding="utf-8") as f:
        india = json.load(f)
    loco = next((p for p in india if p["name"] == "LOCO"), None)
    ctx, _ = find_in_context(context_lines, "LOCO")
    if loco and ctx:
        r = {"test": "4. YouTube India - LOCO", "source": "yt_india_data.json"}
        r["total_views"] = ("PASS" if loco["total_views"] == ctx["total_views"] else f"FAIL (JSON:{loco['total_views']} vs ctx:{ctx['total_views']})")
        r["total_likes"] = ("PASS" if loco["total_likes"] == ctx["total_likes"] else f"FAIL (JSON:{loco['total_likes']} vs ctx:{ctx['total_likes']})")
        r["total_comments"] = ("PASS" if loco["total_comments"] == ctx["total_comments"] else f"FAIL (JSON:{loco['total_comments']} vs ctx:{ctx['total_comments']})")
        r["video_count"] = ("PASS" if loco["video_count"] == ctx["video_count"] else f"FAIL (JSON:{loco['video_count']} vs ctx:{ctx['video_count']})")
        r["video_count_vs_array"] = "PASS" if loco["video_count"] == len(loco["videos"]) else f"FAIL"
        results.append(r)

    # 5. YouTube Indonesia - VIVO (yt_indonesia_data.json)
    with open(DOCS / "yt_indonesia_data.json", "r", encoding="utf-8") as f:
        indo = json.load(f)
    vivo = next((p for p in indo if p["name"] == "VIVO"), None)
    ctx, _ = find_in_context(context_lines, "VIVO")
    if vivo and ctx:
        r = {"test": "5. YouTube Indonesia - VIVO", "source": "yt_indonesia_data.json"}
        r["total_views"] = ("PASS" if vivo["total_views"] == ctx["total_views"] else f"FAIL (JSON:{vivo['total_views']} vs ctx:{ctx['total_views']})")
        r["total_likes"] = ("PASS" if vivo["total_likes"] == ctx["total_likes"] else f"FAIL (JSON:{vivo['total_likes']} vs ctx:{ctx['total_likes']})")
        r["total_comments"] = ("PASS" if vivo["total_comments"] == ctx["total_comments"] else f"FAIL (JSON:{vivo['total_comments']} vs ctx:{ctx['total_comments']})")
        r["video_count"] = ("PASS" if vivo["video_count"] == ctx["video_count"] else f"FAIL (JSON:{vivo['video_count']} vs ctx:{ctx['video_count']})")
        results.append(r)
    else:
        results.append({"test": "5. YouTube Indonesia - VIVO", "error": f"vivo={vivo is not None}, ctx={ctx is not None}"})

    # 6. YouTube Korea - JUJUTSU KAISEN (yt_korea_data.json)
    with open(DOCS / "yt_korea_data.json", "r", encoding="utf-8") as f:
        korea = json.load(f)
    jjk_kr = next((p for p in korea if "Jujutsu" in p["name"] or "JUJUTSU" in p["name"]), None)
    ctx, _ = find_in_context(context_lines, "JUJUTSU KAISEN")
    if jjk_kr and ctx:
        r = {"test": "6. YouTube Korea - JUJUTSU KAISEN", "source": "yt_korea_data.json"}
        r["total_views"] = ("PASS" if jjk_kr["total_views"] == ctx["total_views"] else f"FAIL (JSON:{jjk_kr['total_views']} vs ctx:{ctx['total_views']})")
        r["total_likes"] = ("PASS" if jjk_kr["total_likes"] == ctx["total_likes"] else f"FAIL (JSON:{jjk_kr['total_likes']} vs ctx:{ctx['total_likes']})")
        r["total_comments"] = ("PASS" if jjk_kr["total_comments"] == ctx["total_comments"] else f"FAIL (JSON:{jjk_kr['total_comments']} vs ctx:{ctx['total_comments']})")
        r["video_count"] = ("PASS" if jjk_kr["video_count"] == ctx["video_count"] else f"FAIL (JSON:{jjk_kr['video_count']} vs ctx:{ctx['video_count']})")
        results.append(r)
    else:
        results.append({"test": "6. YouTube Korea - JUJUTSU KAISEN", "error": f"jjk_kr={jjk_kr is not None}, ctx={ctx is not None}"})

    # 7. YouTube Japan - ATTACK ON TITAN (yt_japan_data.json)
    with open(DOCS / "yt_japan_data.json", "r", encoding="utf-8") as f:
        japan = json.load(f)
    aot_jp = next((p for p in japan if "進撃" in p["name"] or "Attack" in p["name"] or "Titan" in p["name"]), None)
    ctx, _ = find_in_context(context_lines, "ATTACK ON TITAN")
    if aot_jp and ctx:
        r = {"test": "7. YouTube Japan - ATTACK ON TITAN", "source": "yt_japan_data.json"}
        r["total_views"] = ("PASS" if aot_jp["total_views"] == ctx["total_views"] else f"FAIL (JSON:{aot_jp['total_views']} vs ctx:{ctx['total_views']})")
        r["total_likes"] = ("PASS" if aot_jp["total_likes"] == ctx["total_likes"] else f"FAIL (JSON:{aot_jp['total_likes']} vs ctx:{ctx['total_likes']})")
        r["total_comments"] = ("PASS" if aot_jp["total_comments"] == ctx["total_comments"] else f"FAIL (JSON:{aot_jp['total_comments']} vs ctx:{ctx['total_comments']})")
        r["video_count"] = ("PASS" if aot_jp["video_count"] == ctx["video_count"] else f"FAIL (JSON:{aot_jp['video_count']} vs ctx:{ctx['video_count']})")
        results.append(r)
    else:
        results.append({"test": "7. YouTube Japan - ATTACK ON TITAN", "error": f"aot_jp={aot_jp is not None}, ctx={ctx is not None}"})

    # 8. Instagram - ATTACK ON TITAN (ig_data.json)
    with open(DOCS / "ig_data.json", "r", encoding="utf-8") as f:
        ig = json.load(f)
    aot_ig = next((p for p in ig if "Attack" in p["name"] or "Titan" in p["name"] or "進撃" in p.get("name","")), None)
    ctx, _ = find_in_context(context_lines, "ATTACK ON TITAN")
    if aot_ig and ctx:
        r = {"test": "8. Instagram - ATTACK ON TITAN", "source": "ig_data.json"}
        r["total_views"] = ("PASS" if aot_ig["total_views"] == ctx["total_views"] else f"FAIL (JSON:{aot_ig['total_views']} vs ctx:{ctx['total_views']})")
        r["total_likes"] = ("PASS" if aot_ig.get("total_likes") == ctx["total_likes"] else f"FAIL (JSON:{aot_ig.get('total_likes')} vs ctx:{ctx['total_likes']})")
        r["total_comments"] = ("PASS" if aot_ig.get("total_comments") == ctx["total_comments"] else f"FAIL (JSON:{aot_ig.get('total_comments')} vs ctx:{ctx['total_comments']})")
        r["video_count"] = ("PASS" if aot_ig["video_count"] == ctx["video_count"] else f"FAIL (JSON:{aot_ig['video_count']} vs ctx:{ctx['video_count']})")
        results.append(r)
    else:
        results.append({"test": "8. Instagram - ATTACK ON TITAN", "error": f"aot_ig={aot_ig is not None}, ctx={ctx is not None}"})

    # 9. Weibo - top partner (weibo_data.json) - total_reposts, total_attitudes
    with open(DOCS / "weibo_data.json", "r", encoding="utf-8") as f:
        weibo = json.load(f)
    top_weibo = weibo[0] if weibo else None
    # Find Weibo section in context - first partner
    weibo_ctx = None
    in_weibo = False
    for line in context_lines:
        if "Weibo" in line and "partners" in line:
            in_weibo = True
            continue
        if in_weibo and line.startswith("#") and "[" in line:
            weibo_ctx = extract_partner_line(line)
            break
    if top_weibo and weibo_ctx:
        r = {"test": "9. Weibo - top partner", "source": "weibo_data.json", "partner": top_weibo.get("name")}
        r["total_reposts"] = ("PASS" if top_weibo.get("total_reposts") == weibo_ctx.get("total_views") else f"FAIL (check schema)")
        r["total_attitudes"] = ("PASS" if top_weibo.get("total_attitudes") == weibo_ctx.get("total_likes") else f"FAIL (check schema)")
        results.append(r)
    else:
        results.append({"test": "9. Weibo - top partner", "error": "Schema may differ"})

    # 10. Free Fire - top partner (freefire_data.json)
    with open(DOCS / "freefire_data.json", "r", encoding="utf-8") as f:
        ff = json.load(f)
    top_ff = ff[0] if ff else None
    ff_ctx = None
    in_ff = False
    for line in context_lines:
        if "Free Fire" in line and "partners" in line:
            in_ff = True
            continue
        if in_ff and line.startswith("#") and "[" in line:
            ff_ctx = extract_partner_line(line)
            break
    if top_ff and ff_ctx:
        r = {"test": "10. Free Fire - top partner", "source": "freefire_data.json", "partner": top_ff.get("name")}
        r["total_views"] = ("PASS" if top_ff["total_views"] == ff_ctx["total_views"] else f"FAIL (JSON:{top_ff['total_views']} vs ctx:{ff_ctx['total_views']})")
        r["total_likes"] = ("PASS" if top_ff["total_likes"] == ff_ctx["total_likes"] else f"FAIL (JSON:{top_ff['total_likes']} vs ctx:{ff_ctx['total_likes']})")
        r["total_comments"] = ("PASS" if top_ff["total_comments"] == ff_ctx["total_comments"] else f"FAIL (JSON:{top_ff['total_comments']} vs ctx:{ff_ctx['total_comments']})")
        r["video_count"] = ("PASS" if top_ff["video_count"] == ff_ctx["video_count"] else f"FAIL (JSON:{top_ff['video_count']} vs ctx:{ff_ctx['video_count']})")
        results.append(r)
    else:
        results.append({"test": "10. Free Fire - top partner", "error": f"top_ff={top_ff is not None}, ff_ctx={ff_ctx is not None}"})

    # Print report
    print("=" * 80)
    print("CRITICAL AUDIT REPORT: all_context.txt vs JSON Sources")
    print("=" * 80)
    for r in results:
        print(f"\n{r['test']} ({r.get('source','')})")
        if "partner" in r:
            print(f"  Partner: {r['partner']}")
        for k, v in r.items():
            if k not in ("test", "source", "partner", "error"):
                print(f"  {k}: {v}")
        if "error" in r:
            print(f"  ERROR: {r['error']}")

if __name__ == "__main__":
    main()
