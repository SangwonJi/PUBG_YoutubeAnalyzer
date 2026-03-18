function getSystemPrompt() {
  const today = new Date().toISOString().slice(0, 10);
  return `You are a senior IPL (IP Licensing) data analyst specializing in PUBG MOBILE and Free Fire collaboration performance across ALL global regions. You write like a professional consultant — precise, data-driven, insightful. Today is ${today}.

DATA SCOPE:
The dashboard data covers ALL regions and platforms:
- YouTube: Global, MENA, India, Indonesia, LATAM, Malaysia, Pakistan, Taiwan, Thailand, Turkey, CIS, Korea, Japan
- Instagram: Global
- Weibo: China (和平精英 / Game For Peace)
- Free Fire YouTube: Global
Each section is labeled [YouTube Global], [YouTube MENA], [YouTube Korea], [YouTube Japan], etc. When analyzing a partner, check ALL regions for that partner name to provide cross-regional insights.

CATEGORY SYSTEM: Animation(애니), Artist(아티스트), Character(캐릭터), Fashion(패션), Film(영화), Game(게임), Vehicle(자동차), Other(기타)

REGION ALIASES: 글로벌=Global, 중동=MENA, 동남아=Indonesia+Malaysia+Thailand, 남아시아=India+Pakistan, 중남미=LATAM, 대만=Taiwan, 터키=Turkey, 러시아=CIS, 중국=Weibo, 인스타=Instagram, 프리파이어=Free Fire, 한국=Korea, 일본=Japan

PARTNER ALIASES (Korean→English): 주술회전/JJK=JUJUTSU KAISEN, 드래곤볼=DRAGON BALL SUPER, 진격의거인=ATTACK ON TITAN, 블랙핑크=BLACKPINK, 베이비몬스터=BABYMONSTER, 고질라=GODZILLA, 스파이더맨=SPIDER-MAN, 트랜스포머=TRANSFORMERS, 부가티=BUGATTI, 포르쉐=PORSCHE, 메시=LIONEL MESSI, 소닉=SONIC, 아케인=ARCANE, 맥라렌=MCLAREN, 브루스리/이소룡=BRUCE LEE, 발렌시아가=BALENCIAGA, 알란워커=ALAN WALKER, 카이주넘버8=KAIJU NO.8, 원펀맨=ONE-PUNCH MAN, 베어브릭=BE@RBRICK, 파가니=PAGANI, 다잉라이트=DYING LIGHT, 메트로엑소더스=METRO EXODUS, 나루토=NARUTO

RULES:
1. ONLY use data from "DASHBOARD DATA". NEVER invent numbers, dates, or titles.
2. Korean partner/region names → look up alias above → find in data.
3. **YEAR FILTERING (CRITICAL)**: If user mentions a year (e.g., "2026년 주술회전"), ONLY include videos from that year. Do NOT mix other years.
4. When a partner has videos across multiple years, separate into waves: "Wave 1 (2022)", "Wave 2 (2026)".
5. **CROSS-REGION ANALYSIS**: When asked about a partner's performance, search ALL regions. Show a region comparison table if the partner appears in multiple regions.
6. **PLATFORM COMPARISON**: When asked to compare platforms (YouTube vs Instagram vs Weibo), use the relevant section for each.
7. NEVER fabricate: "콜라보 기간", "참여율", "전환율", "DAU", "매출".
8. If data doesn't exist, say so. NEVER guess.
9. Respond in Korean unless user writes in English/Chinese.
10. For partners ranked 51+ per region, only summary stats are available (no individual video titles). Note this when relevant.
11. **NUMBERS**: Data contains exact numbers with commas (e.g., 1,400,000). Copy them directly into tables. Do NOT attempt to convert or reformat numbers.

FORMAT:
- Use ## for sections, **bold** for key numbers
- ALWAYS show markdown tables for data
- For partner analysis: 요약 → 리전별 성과 테이블 → 영상 상세 → 인사이트
- For regional comparison: 개요 → 리전별 비교 테이블 → 핵심 차이점
- For cross-platform: 플랫폼별 비교 → 종합 평가

QUALITY:
- Write like a senior consultant briefing a VP.
- Every claim must cite a specific number from the data.
- For cross-region questions, ALWAYS show a region-by-region breakdown.
- Identify outliers and explain why they matter.
- When comparing, highlight which regions over/under-performed and hypothesize why.`;
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
    'Access-Control-Allow-Origin': allowed ? (origin || '*') : allowedOrigin,
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

    const maxCtx = parseInt(env.MAX_CONTEXT_CHARS || '80000');
    let fullContext = context ? context.substring(0, maxCtx) : '';

    const systemContent = getSystemPrompt() + (fullContext
      ? `\n\n--- CURRENT DASHBOARD DATA ---\n${fullContext}`
      : '');

    const apiMessages = [
      { role: 'system', content: systemContent },
      ...messages.slice(-10),
    ];

    // Always try Claude first (single attempt), instant Llama fallback
    const anthropicKey = (env.ANTHROPIC_API_KEY || '').trim();
    if (anthropicKey) {
      const claudeRes = await callClaude(anthropicKey, systemContent, messages.slice(-10), headers);
      if (claudeRes.status === 200) return claudeRes;
    }

    if (env.OPENAI_API_KEY && env.AI_GATEWAY_URL) {
      return callOpenAI(env.OPENAI_API_KEY, env.AI_GATEWAY_URL, apiMessages, headers);
    }

    // Workers AI fallback (Llama) — cap context for 24K token limit
    const llamaCtx = fullContext ? fullContext.substring(0, 35000) : '';
    const llamaSystem = getSystemPrompt() + (llamaCtx
      ? `\n\n--- CURRENT DASHBOARD DATA ---\n${llamaCtx}`
      : '');
    const llamaMessages = [
      { role: 'system', content: llamaSystem },
      ...messages.slice(-10),
    ];

    if (!env.AI) {
      return new Response(JSON.stringify({ error: 'AI binding not configured' }), {
        status: 500, headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    try {
      const stream = await env.AI.run('@cf/meta/llama-3.3-70b-instruct-fp8-fast', {
        messages: llamaMessages,
        stream: true,
        max_tokens: 3000,
        temperature: 0.15,
      });

      return new Response(stream, {
        status: 200,
        headers: { ...headers, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'X-AI-Model': 'llama-3.3-70b' },
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: `Workers AI error: ${err.message}` }), {
        status: 502, headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }
  },
};

async function callClaude(apiKey, systemContent, userMessages, cors) {
  const systemPromptOnly = getSystemPrompt();
  const dataContext = systemContent.includes('--- CURRENT DASHBOARD DATA ---')
    ? systemContent.split('--- CURRENT DASHBOARD DATA ---')[1] || ''
    : '';

  const systemBlocks = [
    { type: 'text', text: systemPromptOnly, cache_control: { type: 'ephemeral' } },
  ];

  const messagesWithContext = [];
  if (dataContext.trim()) {
    messagesWithContext.push({
      role: 'user',
      content: [
        { type: 'text', text: `[DASHBOARD DATA]\n${dataContext}`, cache_control: { type: 'ephemeral' } },
      ],
    });
    messagesWithContext.push({
      role: 'assistant',
      content: '데이터를 확인했습니다. 질문해 주세요.',
    });
  }
  messagesWithContext.push(...userMessages);

  const ACCOUNT_ID = 'c0851d705f263467c1648cccbf45fcc1';
  const GATEWAY = 'pubgm-chatbot';
  const endpoint = `https://gateway.ai.cloudflare.com/v1/${ACCOUNT_ID}/${GATEWAY}/anthropic/v1/messages`;

  try {
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'anthropic-beta': 'prompt-caching-2024-07-31',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4096,
        temperature: 0.1,
        system: systemBlocks,
        messages: messagesWithContext,
        stream: true,
      }),
    });

    if (res.ok) return streamClaudeResponse(res, cors);

    const errText = await res.text();
    return new Response(JSON.stringify({ error: `Claude ${res.status}: ${errText.substring(0, 200)}` }), {
      status: 502, headers: { ...cors, 'Content-Type': 'application/json' },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: `Claude unreachable: ${err.message}` }), {
      status: 502, headers: { ...cors, 'Content-Type': 'application/json' },
    });
  }
}

function streamClaudeResponse(res, cors) {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();

  (async () => {
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const data = line.slice(6);
          if (data === '[DONE]') continue;
          try {
            const parsed = JSON.parse(data);
            if (parsed.type === 'content_block_delta' && parsed.delta?.text) {
              await writer.write(encoder.encode(`data: ${JSON.stringify({ response: parsed.delta.text })}\n\n`));
            }
          } catch {}
        }
      }
      await writer.write(encoder.encode('data: [DONE]\n\n'));
    } catch (err) {
      await writer.write(encoder.encode(`data: ${JSON.stringify({ error: err.message })}\n\n`));
    } finally {
      await writer.close();
    }
  })();

  return new Response(readable, {
    status: 200,
    headers: { ...cors, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'X-AI-Model': 'claude-sonnet' },
  });
}

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
