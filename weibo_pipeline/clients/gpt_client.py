"""
OpenAI GPT client for collab classification.
YouTube 파이프라인과 동일한 구조, 웨이보 컨텍스트에 맞게 프롬프트 조정.
"""
import json
import logging
from typing import Optional

from openai import OpenAI

import config

logger = logging.getLogger(__name__)


class GPTClient:
    """GPT API client for collab classification."""

    def __init__(self):
        if not config.GPT_API_KEY:
            raise ValueError("GPT_API_KEY not set in environment")
        self.client = OpenAI(api_key=config.GPT_API_KEY)
        self.model = config.GPT_MODEL
        self._cache: dict[str, dict] = {}

    def classify_collab(self, text: str, page_title: str = "",
                        mid: str = "") -> Optional[dict]:
        """
        Use GPT to classify whether a Weibo post is a collab.

        Returns:
            dict with keys: is_collab, partner, category, region, confidence
            or None on failure
        """
        # Check cache
        cache_key = f"{mid}:{text[:100]}"
        if cache_key in self._cache:
            logger.debug(f"[GPT] Cache hit for mid={mid}")
            return self._cache[cache_key]

        prompt = self._build_prompt(text, page_title)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self._system_prompt()},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=500,
                response_format={"type": "json_object"},
            )

            content = response.choices[0].message.content
            result = json.loads(content)

            # Validate required fields
            validated = {
                "is_collab": bool(result.get("is_collab", False)),
                "partner": result.get("partner", ""),
                "category": result.get("category", "Other"),
                "region": result.get("region", "CN"),
                "confidence": float(result.get("confidence", 0.5)),
            }

            self._cache[cache_key] = validated
            return validated

        except Exception as e:
            logger.error(f"[GPT] Classification failed for mid={mid}: {e}")
            return None

    @staticmethod
    def _system_prompt() -> str:
        return """你是一个游戏市场分析专家，专门分析和平精英(Game for Peace / PUBG Mobile中国版)的官方微博内容。
你的任务是判断一条微博是否是联动/合作(collaboration)内容，并提取相关信息。

分类规则:
1. 联动/合作包括: IP联动、品牌合作、艺人合作、游戏联动、动漫联动、影视联动等
2. 非联动内容包括: 纯游戏更新、赛事信息、日常运营活动、节日活动(非IP联动)、Bug修复等
3. 如果是联动，需要识别合作伙伴名称和类别

分类类别(category):
- IP: 知识产权/角色授权 (如:Hello Kitty, 小黄人)
- Brand: 商业品牌合作 (如:兰博基尼, 保时捷, 特斯拉)
- Artist: 艺人/音乐人合作 (如:周杰伦, BLACKPINK)
- Game: 游戏联动 (如:生化危机, 仙剑奇侠传)
- Anime: 动漫/动画联动 (如:龙珠, 咒术回战, 喜羊羊)
- Movie: 电影/电视剧联动 (如:哥斯拉, 哪吒)
- Other: 其他类型联动

地区(region): 主要目标为CN(中国大陆)

请始终以JSON格式回复:
{
    "is_collab": true/false,
    "partner": "合作伙伴名称",
    "category": "IP/Brand/Artist/Game/Anime/Movie/Other",
    "region": "CN",
    "confidence": 0.0-1.0
}"""

    @staticmethod
    def _build_prompt(text: str, page_title: str = "") -> str:
        parts = [f"微博正文:\n{text[:1000]}"]
        if page_title:
            parts.append(f"\n视频/页面标题:\n{page_title}")
        parts.append("\n请分析这条微博是否是联动/合作内容。")
        return "\n".join(parts)
