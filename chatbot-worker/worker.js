function getSystemPrompt() {
  const today = new Date().toISOString().slice(0, 10);
  return `You are a senior data analyst for PUBG MOBILE's IP Licensing (IPL) team.
You analyze collaboration partnership data from YouTube, Instagram, and Weibo across global regions.
Today is ${today}.

PARTNER NAME ALIASES (Korean → English in data):
주술회전/JJK = JUJUTSU KAISEN, 드래곤볼 = DRAGON BALL SUPER, 진격의거인 = ATTACK ON TITAN, 블랙핑크 = BLACKPINK, 베이비몬스터 = BABYMONSTER, 고질라 = GODZILLA, 스파이더맨 = SPIDER-MAN, 트랜스포머 = TRANSFORMERS, 부가티 = BUGATTI, 포르쉐 = PORSCHE, 메시 = LIONEL MESSI, 소닉 = SONIC, 아케인 = ARCANE, 피키블라인더스 = PEAKY BLINDERS, 맥라렌 = MCLAREN, 브루스리 = BRUCE LEE, 발렌시아가 = BALENCIAGA, 알란워커 = ALAN WALKER, 이소룡 = BRUCE LEE, 카이주넘버8 = KAIJU NO.8, 원펀맨 = ONE-PUNCH MAN, 베어브릭 = BE@RBRICK, 파가니 = PAGANI

CRITICAL RULES — NEVER VIOLATE:
1. ONLY state facts that exist in the provided "CURRENT DASHBOARD DATA". NEVER invent dates, percentages, engagement rates, durations, or any number not explicitly in the data.
2. When the user mentions a partner in Korean, ALWAYS look up the English name using the alias list above, then find that partner in the data. The data uses ENGLISH partner names.
3. If a collaboration partner has videos across different time periods (months/years apart), identify them as separate waves with actual dates.
4. NEVER say "콜라보 기간: X~Y" or "참여율" or "전환율". Only report what is in the data: video count, views, likes, comments, and actual video dates.
5. Use ## for section headers. Use **bold** for numbers. Use markdown tables for comparisons.
6. Be concise and data-driven. NO generic filler. Only state what the numbers show.
7. Always respond in Korean unless the user writes in another language.
8. When comparing, ALWAYS use a markdown table.
9. For "보고서": 핵심 요약 (3줄) → 상세 데이터 테이블 → 인사이트 → 권장사항.
10. If information is not in the data, say "해당 데이터가 없습니다".
11. ALWAYS list actual video titles with dates and view counts when analyzing a partner.`;
}

const rateLimitMap = new Map();

function checkRateLimit(ip, maxPerMin) {
  const now = Date.now();
  const windowMs = 60_000;
  let entry = rateLimitMap.get(ip);
  if (!entry || now - entry.start > windowMs) {
    entry = { start: now, count: 0 };
    rateLimitMap.set(ip, entry);
  }
  entry.count++;
  return entry.count <= maxPerMin;
}

function corsHeaders(origin, allowedOrigin) {
  const allowed = allowedOrigin === '*' || origin === allowedOrigin
    || origin?.startsWith('http://localhost') || origin?.startsWith('http://127.0.0.1');
  return {
    'Access-Control-Allow-Origin': allowed ? origin : allowedOrigin,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
}

// ===== YouTube Data API v3 Functions =====

async function ytSearch(apiKey, query, maxResults = 10) {
  const params = new URLSearchParams({
    part: 'snippet',
    q: query,
    type: 'video',
    maxResults: String(maxResults),
    order: 'relevance',
    key: apiKey,
  });
  const res = await fetch(`https://www.googleapis.com/youtube/v3/search?${params}`);
  if (!res.ok) return { error: `YouTube Search API error: ${res.status}` };
  return res.json();
}

async function ytVideoDetails(apiKey, videoIds) {
  const params = new URLSearchParams({
    part: 'snippet,statistics',
    id: videoIds.join(','),
    key: apiKey,
  });
  const res = await fetch(`https://www.googleapis.com/youtube/v3/videos?${params}`);
  if (!res.ok) return { error: `YouTube Videos API error: ${res.status}` };
  return res.json();
}

async function ytComments(apiKey, videoId, maxResults = 30) {
  const params = new URLSearchParams({
    part: 'snippet',
    videoId: videoId,
    maxResults: String(maxResults),
    order: 'relevance',
    textFormat: 'plainText',
    key: apiKey,
  });
  const res = await fetch(`https://www.googleapis.com/youtube/v3/commentThreads?${params}`);
  if (!res.ok) return { error: `YouTube Comments API error: ${res.status}` };
  return res.json();
}

async function ytChannelSearch(apiKey, channelId, query, maxResults = 10) {
  const params = new URLSearchParams({
    part: 'snippet',
    channelId: channelId,
    q: query,
    type: 'video',
    maxResults: String(maxResults),
    order: 'date',
    key: apiKey,
  });
  const res = await fetch(`https://www.googleapis.com/youtube/v3/search?${params}`);
  if (!res.ok) return { error: `YouTube Channel Search API error: ${res.status}` };
  return res.json();
}

async function fetchYouTubeData(apiKey, ytRequest) {
  const { action, query, videoId, videoIds, channelId, maxResults } = ytRequest;
  let result = '';

  try {
    if (action === 'search' && query) {
      const data = await ytSearch(apiKey, query, maxResults || 10);
      if (data.error) return `[YouTube 검색 오류] ${data.error}`;

      const ids = data.items?.map(i => i.id.videoId).filter(Boolean) || [];
      if (ids.length === 0) return `[YouTube 검색] "${query}" 결과 없음`;

      const details = await ytVideoDetails(apiKey, ids);
      if (details.error) return `[YouTube 상세 오류] ${details.error}`;

      result = `[YouTube 실시간 검색: "${query}" — ${details.items?.length || 0}개 결과]\n`;
      (details.items || []).forEach((v, i) => {
        const s = v.statistics || {};
        const pub = v.snippet?.publishedAt?.substring(0, 10) || '';
        result += `${i + 1}. "${v.snippet?.title}" (${v.snippet?.channelTitle})\n`;
        result += `   조회수: ${Number(s.viewCount || 0).toLocaleString()}, 좋아요: ${Number(s.likeCount || 0).toLocaleString()}, 댓글: ${Number(s.commentCount || 0).toLocaleString()}\n`;
        result += `   게시일: ${pub} | https://youtube.com/watch?v=${v.id}\n`;
      });

    } else if (action === 'comments' && videoId) {
      const data = await ytComments(apiKey, videoId, maxResults || 30);
      if (data.error) return `[YouTube 댓글 오류] ${data.error}`;

      result = `[YouTube 댓글: ${videoId} — ${data.items?.length || 0}개]\n`;
      (data.items || []).forEach((c, i) => {
        const s = c.snippet?.topLevelComment?.snippet;
        if (!s) return;
        const likes = s.likeCount || 0;
        const text = (s.textDisplay || '').substring(0, 200);
        result += `${i + 1}. [좋아요 ${likes}] ${text}\n`;
      });

    } else if (action === 'video_details' && (videoId || videoIds)) {
      const ids = videoIds || [videoId];
      const data = await ytVideoDetails(apiKey, ids);
      if (data.error) return `[YouTube 상세 오류] ${data.error}`;

      result = `[YouTube 영상 상세 — ${data.items?.length || 0}개]\n`;
      (data.items || []).forEach((v, i) => {
        const s = v.statistics || {};
        result += `${i + 1}. "${v.snippet?.title}" (${v.snippet?.channelTitle})\n`;
        result += `   조회수: ${Number(s.viewCount || 0).toLocaleString()}, 좋아요: ${Number(s.likeCount || 0).toLocaleString()}, 댓글: ${Number(s.commentCount || 0).toLocaleString()}\n`;
        result += `   게시일: ${v.snippet?.publishedAt?.substring(0, 10)} | 설명: ${(v.snippet?.description || '').substring(0, 150)}\n`;
      });

    } else if (action === 'channel_search' && channelId && query) {
      const data = await ytChannelSearch(apiKey, channelId, query, maxResults || 10);
      if (data.error) return `[YouTube 채널 검색 오류] ${data.error}`;

      const ids = data.items?.map(i => i.id.videoId).filter(Boolean) || [];
      if (ids.length === 0) return `[YouTube 채널 검색] "${query}" 결과 없음`;

      const details = await ytVideoDetails(apiKey, ids);
      result = `[YouTube 채널 내 검색: "${query}" — ${details.items?.length || 0}개]\n`;
      (details.items || []).forEach((v, i) => {
        const s = v.statistics || {};
        result += `${i + 1}. "${v.snippet?.title}"\n`;
        result += `   조회수: ${Number(s.viewCount || 0).toLocaleString()}, 좋아요: ${Number(s.likeCount || 0).toLocaleString()}\n`;
        result += `   게시일: ${v.snippet?.publishedAt?.substring(0, 10)} | https://youtube.com/watch?v=${v.id}\n`;
      });
    }
  } catch (err) {
    return `[YouTube API 오류] ${err.message}`;
  }

  return result || '[YouTube] 요청된 데이터 없음';
}

// ===== Main Handler =====

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin') || '';
    const allowedOrigin = env.ALLOWED_ORIGIN || '*';
    const headers = corsHeaders(origin, allowedOrigin);

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers });
    }

    if (request.method !== 'POST') {
      return new Response(JSON.stringify({ error: 'Method not allowed' }), {
        status: 405, headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
    const maxPerMin = parseInt(env.RATE_LIMIT_PER_MIN || '30');
    if (!checkRateLimit(ip, maxPerMin)) {
      return new Response(JSON.stringify({ error: '요청 한도 초과. 잠시 후 다시 시도해주세요.' }), {
        status: 429, headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response(JSON.stringify({ error: 'Invalid JSON' }), {
        status: 400, headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    const { messages, context } = body;
    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      return new Response(JSON.stringify({ error: 'messages array required' }), {
        status: 400, headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    const maxCtx = parseInt(env.MAX_CONTEXT_CHARS || '30000');
    let fullContext = context ? context.substring(0, maxCtx) : '';

    const systemContent = getSystemPrompt() + (fullContext
      ? `\n\n--- CURRENT DASHBOARD DATA ---\n${fullContext}`
      : '');

    const apiMessages = [
      { role: 'system', content: systemContent },
      ...messages.slice(-10),
    ];

    // Try OpenAI via AI Gateway if configured, otherwise Workers AI
    if (env.OPENAI_API_KEY && env.AI_GATEWAY_URL) {
      return callOpenAI(env.OPENAI_API_KEY, env.AI_GATEWAY_URL, apiMessages, headers);
    }

    if (!env.AI) {
      return new Response(JSON.stringify({ error: 'AI binding not configured' }), {
        status: 500, headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    try {
      const stream = await env.AI.run('@cf/meta/llama-3.3-70b-instruct-fp8-fast', {
        messages: apiMessages,
        stream: true,
        max_tokens: 2048,
        temperature: 0.2,
      });

      return new Response(stream, {
        status: 200,
        headers: { ...headers, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: `Workers AI error: ${err.message}` }), {
        status: 502, headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }
  },
};

async function callOpenAI(apiKey, gatewayUrl, apiMessages, cors) {
  try {
    const res = await fetch(gatewayUrl, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini', messages: apiMessages,
        stream: true, max_tokens: 2048, temperature: 0.3,
      }),
    });
    if (!res.ok) {
      const errText = await res.text();
      let errMsg = `OpenAI error: ${res.status}`;
      try { errMsg = JSON.parse(errText).error?.message || errMsg; } catch {}
      return new Response(JSON.stringify({ error: errMsg }), {
        status: 502, headers: { ...cors, 'Content-Type': 'application/json' },
      });
    }
    return new Response(res.body, {
      status: 200,
      headers: { ...cors, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Failed to reach OpenAI' }), {
      status: 502, headers: { ...cors, 'Content-Type': 'application/json' },
    });
  }
}
