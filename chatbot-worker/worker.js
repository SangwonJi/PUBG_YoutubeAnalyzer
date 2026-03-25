// ==========================================
// PUBG MOBILE Collab Chatbot Worker
// with Smart Context Retrieval (RAG)
// ==========================================

// ===== Partner & Region lookup maps for RAG =====
const PARTNER_ALIASES = {
  '주술회전': 'JUJUTSU KAISEN', 'jjk': 'JUJUTSU KAISEN', 'jujutsu': 'JUJUTSU KAISEN',
  '드래곤볼': 'DRAGON BALL', 'dragon ball': 'DRAGON BALL',
  '진격의거인': 'ATTACK ON TITAN', '진격의 거인': 'ATTACK ON TITAN', 'aot': 'ATTACK ON TITAN', 'attack on titan': 'ATTACK ON TITAN',
  '블랙핑크': 'BLACKPINK', 'blackpink': 'BLACKPINK',
  '베이비몬스터': 'BABYMONSTER', 'babymonster': 'BABYMONSTER',
  '고질라': 'GODZILLA', 'godzilla': 'GODZILLA',
  '스파이더맨': 'SPIDER-MAN', 'spider-man': 'SPIDER-MAN', 'spiderman': 'SPIDER-MAN',
  '트랜스포머': 'TRANSFORMERS', 'transformers': 'TRANSFORMERS',
  '부가티': 'BUGATTI', 'bugatti': 'BUGATTI',
  '포르쉐': 'PORSCHE', 'porsche': 'PORSCHE',
  '메시': 'LIONEL MESSI', 'messi': 'LIONEL MESSI',
  '소닉': 'SONIC', 'sonic': 'SONIC',
  '아케인': 'ARCANE', 'arcane': 'ARCANE',
  '맥라렌': 'MCLAREN', 'mclaren': 'MCLAREN',
  '브루스리': 'BRUCE LEE', '이소룡': 'BRUCE LEE', 'bruce lee': 'BRUCE LEE',
  '발렌시아가': 'BALENCIAGA', 'balenciaga': 'BALENCIAGA',
  '알란워커': 'ALAN WALKER', 'alan walker': 'ALAN WALKER',
  '카이주넘버8': 'KAIJU NO.8', 'kaiju': 'KAIJU NO.8',
  '원펀맨': 'ONE-PUNCH MAN', 'one punch': 'ONE-PUNCH MAN',
  '베어브릭': 'BE@RBRICK', 'bearbrick': 'BE@RBRICK',
  '파가니': 'PAGANI', 'pagani': 'PAGANI',
  '나루토': 'NARUTO', 'naruto': 'NARUTO',
  '원피스': 'ONE PIECE', 'one piece': 'ONE PIECE',
  '람보르기니': 'LAMBORGHINI', 'lamborghini': 'LAMBORGHINI',
  '다잉라이트': 'DYING LIGHT', 'dying light': 'DYING LIGHT',
  '메트로': 'METRO EXODUS', 'metro': 'METRO EXODUS',
  '제노버스': 'ZENOVERSE',
  '뱅드림': 'BANG DREAM', 'bang dream': 'BANG DREAM',
  '에반게리온': 'EVANGELION', 'evangelion': 'EVANGELION',
  '네온 제네시스': 'EVANGELION',
  '워킹데드': 'THE WALKING DEAD', 'walking dead': 'THE WALKING DEAD',
  '레지던트이블': 'RESIDENT EVIL', 'resident evil': 'RESIDENT EVIL',
  '미션임파서블': 'MISSION: IMPOSSIBLE', 'mission impossible': 'MISSION: IMPOSSIBLE',
  '킹오브파이터즈': 'THE KING OF FIGHTERS', 'kof': 'THE KING OF FIGHTERS',
};

const REGION_ALIASES = {
  '글로벌': 'Global', 'global': 'Global', '전체': 'Global',
  '인도': 'India', 'india': 'India',
  '인도네시아': 'Indonesia', 'indonesia': 'Indonesia', '인니': 'Indonesia',
  '라틴': 'LATAM', 'latam': 'LATAM', '중남미': 'LATAM', '남미': 'LATAM',
  '말레이시아': 'Malaysia', 'malaysia': 'Malaysia',
  '파키스탄': 'Pakistan', 'pakistan': 'Pakistan',
  '대만': 'Taiwan', 'taiwan': 'Taiwan',
  '태국': 'Thailand', 'thailand': 'Thailand',
  '터키': 'Turkey', 'turkey': 'Turkey', '튀르키예': 'Turkey',
  'cis': 'CIS', '러시아': 'CIS', '독립국가연합': 'CIS',
  '한국': 'Korea', 'korea': 'Korea',
  '일본': 'Japan', 'japan': 'Japan',
  '인스타': 'Instagram', 'instagram': 'Instagram', '인스타그램': 'Instagram',
  '웨이보': 'Weibo', 'weibo': 'Weibo', '중국': 'Weibo', '차이나': 'Weibo',
  '프리파이어': 'Free Fire', 'freefire': 'Free Fire', 'free fire': 'Free Fire',
  '동남아': 'SEA', '동남아시아': 'SEA',
  '남아시아': 'SouthAsia',
  '동아시아': 'EastAsia', '동북아': 'EastAsia', '동북아시아': 'EastAsia',
  '중동': 'MiddleEast', '아랍': 'MiddleEast', 'mena': 'MENA',
};

const CATEGORY_ALIASES = {
  '애니': 'Animation', '애니메이션': 'Animation', 'animation': 'Animation',
  '아티스트': 'Artist', '가수': 'Artist', 'artist': 'Artist',
  '캐릭터': 'Character', 'character': 'Character',
  '패션': 'Fashion', 'fashion': 'Fashion',
  '영화': 'Film', 'film': 'Film',
  '게임': 'Game', 'game': 'Game',
  '자동차': 'Vehicle', '차량': 'Vehicle', '차': 'Vehicle', 'vehicle': 'Vehicle',
};

// ===== Smart Context Retrieval (RAG) =====

function parseContextSections(fullContext) {
  const sections = [];
  const headerRe = /\n\[([^\]]+)\]/g;
  const starts = [];
  let m;
  while ((m = headerRe.exec(fullContext)) !== null) {
    starts.push({ name: m[1], idx: m.index });
  }
  for (let i = 0; i < starts.length; i++) {
    const end = i < starts.length - 1 ? starts[i + 1].idx : fullContext.length;
    sections.push({
      name: starts[i].name,
      content: fullContext.substring(starts[i].idx, end),
    });
  }
  const overviewEnd = starts.length > 0 ? starts[0].idx : fullContext.length;
  return { overview: fullContext.substring(0, overviewEnd), sections };
}

function analyzeQuery(messages) {
  const allText = messages.map(m => m.content || '').join(' ').toLowerCase();
  const partners = new Set();
  const regions = new Set();
  const categories = new Set();

  for (const [alias, canonical] of Object.entries(PARTNER_ALIASES)) {
    if (allText.includes(alias)) partners.add(canonical.toUpperCase());
  }
  for (const [alias, canonical] of Object.entries(REGION_ALIASES)) {
    if (allText.includes(alias)) regions.add(canonical);
  }
  for (const [alias, canonical] of Object.entries(CATEGORY_ALIASES)) {
    if (allText.includes(alias)) categories.add(canonical);
  }

  const regionGroups = {
    SEA: ['Indonesia', 'Malaysia', 'Thailand'],
    SouthAsia: ['India', 'Pakistan'],
    EastAsia: ['Korea', 'Japan', 'Taiwan'],
    MiddleEast: ['MENA', 'Turkey'],
  };
  for (const [group, list] of Object.entries(regionGroups)) {
    if (regions.has(group)) {
      regions.delete(group);
      list.forEach(r => regions.add(r));
    }
  }

  const isGeneral = partners.size === 0 && regions.size === 0 && categories.size === 0;
  const isTopN = /top\s*\d|상위|1위|순위|랭킹|ranking/i.test(allText);
  const isOverview = /전체|overview|총|요약|summary|개요/i.test(allText);

  return { partners: [...partners], regions: [...regions], categories: [...categories], isGeneral, isTopN, isOverview };
}

function smartRetrieve(fullContext, messages, maxChars) {
  if (!fullContext) return { context: '', info: 'no-context' };

  const { overview, sections } = parseContextSections(fullContext);
  const query = analyzeQuery(messages);

  const hasSpecificFilter = query.partners.length > 0 || query.regions.length > 0 || query.categories.length > 0;
  if ((query.isGeneral || query.isOverview || query.isTopN) && !hasSpecificFilter) {
    let ctx = overview;
    for (const s of sections) {
      if (ctx.length + s.content.length <= maxChars) {
        ctx += s.content;
      } else {
        const remaining = maxChars - ctx.length;
        if (remaining > 500) ctx += s.content.substring(0, remaining);
        break;
      }
    }
    return { context: ctx, info: `general(${sections.length} sections)` };
  }

  let ctx = overview;
  const usedSections = new Set();

  function extractByCategory(sectionContent, cats) {
    const lines = sectionContent.split('\n');
    const headerLine = lines[0] || '';
    const extracted = [];
    const partnerSummaries = [];
    let capturing = false;
    for (const line of lines) {
      if (line.startsWith('#')) {
        capturing = cats.some(c => line.includes(`[${c}]`));
        if (capturing) {
          extracted.push(line);
          partnerSummaries.push(line);
        }
      } else if (capturing && (line.startsWith('  ') || line.startsWith('\t'))) {
        extracted.push(line);
      } else if (capturing) {
        capturing = false;
      }
    }
    if (extracted.length > 0) {
      const catName = cats.join('/');
      const countNote = `\n[CATEGORY FILTER: ${catName} — ${partnerSummaries.length} partners found. YOU MUST LIST ALL ${partnerSummaries.length} IN YOUR TABLE.]`;
      return countNote + '\n' + headerLine + '\n' + extracted.join('\n') + '\n';
    }
    return null;
  }

  if (query.regions.length > 0 && query.partners.length === 0) {
    for (const s of sections) {
      const sLower = s.name.toLowerCase();
      if (query.regions.some(r => sLower.includes(r.toLowerCase()))) {
        if (query.categories.length > 0) {
          const filtered = extractByCategory(s.content, query.categories);
          if (filtered) {
            ctx += filtered;
            usedSections.add(s.name);
          }
        } else {
          ctx += s.content;
          usedSections.add(s.name);
        }
      }
    }
  }

  if (query.partners.length > 0) {
    for (const s of sections) {
      const sContentLower = s.content.toLowerCase();
      const hasPartner = query.partners.some(p => sContentLower.includes(p.toLowerCase()));
      if (!hasPartner) continue;
      if (usedSections.has(s.name)) continue;

      if (query.regions.length === 0) {
        const lines = s.content.split('\n');
        const headerLine = lines[0] || '';
        const extracted = [];
        let capturing = false;
        for (const line of lines) {
          if (line.startsWith('#')) {
            const ll = line.toLowerCase();
            capturing = query.partners.some(p => ll.includes(p.toLowerCase()));
            if (capturing) extracted.push(line);
          } else if (capturing && (line.startsWith('  ') || line.startsWith('\t'))) {
            extracted.push(line);
          } else if (capturing) {
            capturing = false;
          }
        }
        if (extracted.length > 0) {
          ctx += '\n' + headerLine + '\n' + extracted.join('\n') + '\n';
          usedSections.add(s.name);
        }
      } else {
        ctx += s.content;
        usedSections.add(s.name);
      }
    }
  }

  if (query.categories.length > 0 && usedSections.size === 0) {
    for (const s of sections) {
      if (query.categories.some(c => s.content.includes(`[${c}]`))) {
        const filtered = extractByCategory(s.content, query.categories);
        if (filtered) {
          ctx += filtered;
          usedSections.add(s.name);
        }
      }
    }
  }

  if (usedSections.size === 0) {
    return smartRetrieve(fullContext, [{ content: '' }], maxChars);
  }

  if (ctx.length > maxChars) {
    ctx = ctx.substring(0, maxChars - 50) + '\n[context trimmed]';
  }

  const pInfo = query.partners.length > 0 ? `partners=[${query.partners.join(',')}]` : '';
  const rInfo = query.regions.length > 0 ? `regions=[${query.regions.join(',')}]` : '';
  return {
    context: ctx,
    info: `focused: ${usedSections.size}/${sections.length} sections | ${pInfo} ${rInfo} | ${ctx.length.toLocaleString()} chars`,
  };
}

// ===== System Prompt =====

function getSystemPrompt() {
  const today = new Date().toISOString().slice(0, 10);
  return `You are a senior IPL (IP Licensing) data analyst specializing in PUBG MOBILE and Free Fire collaboration performance. You write like a professional consultant — precise, data-driven, insightful. Today is ${today}.

DATA SCOPE:
The dashboard covers ALL regions and platforms:
- YouTube: Global, MENA, India, Indonesia, LATAM, Malaysia, Pakistan, Taiwan, Thailand, Turkey, CIS, Korea, Japan
- Instagram: Global
- Weibo: China (和平精英 / Game For Peace)
- Free Fire YouTube: Global
Each section is labeled [YouTube Global], [YouTube MENA], etc. The system pre-filters relevant sections for your query.

CATEGORY SYSTEM: Animation(애니), Artist(아티스트), Character(캐릭터), Fashion(패션), Film(영화), Game(게임), Vehicle(자동차), Other(기타)

REGION GROUPING (권역 vs 국가):
- 동아시아(East Asia) = Korea + Japan + Taiwan
- 동남아시아(Southeast Asia) = Indonesia + Malaysia + Thailand
- 남아시아(South Asia) = India + Pakistan
- 중동(Middle East) = MENA + Turkey
- 남미(Latin America) = LATAM
- CIS/유라시아 = CIS
When user asks about a region group (e.g., "동아시아"), aggregate data from ALL constituent countries. When comparing, do NOT mix region-level (동아시아, 동남아) with country-level (한국, 일본) in the same table. Use consistent granularity.

REGION ALIASES: 글로벌=Global, 중동=MENA+Turkey, 동남아=Indonesia+Malaysia+Thailand, 남아시아=India+Pakistan, 동아시아=Korea+Japan+Taiwan, 중남미=LATAM, 대만=Taiwan, 터키=Turkey, 러시아=CIS, 중국=Weibo, 인스타=Instagram, 프리파이어=Free Fire, 한국=Korea, 일본=Japan

PARTNER ALIASES (Korean→English): 주술회전/JJK=JUJUTSU KAISEN, 드래곤볼=DRAGON BALL SUPER, 진격의거인=ATTACK ON TITAN, 블랙핑크=BLACKPINK, 베이비몬스터=BABYMONSTER, 고질라=GODZILLA, 스파이더맨=SPIDER-MAN, 트랜스포머=TRANSFORMERS, 부가티=BUGATTI, 포르쉐=PORSCHE, 메시=LIONEL MESSI, 소닉=SONIC, 아케인=ARCANE, 맥라렌=MCLAREN, 브루스리/이소룡=BRUCE LEE, 발렌시아가=BALENCIAGA, 알란워커=ALAN WALKER, 카이주넘버8=KAIJU NO.8, 원펀맨=ONE-PUNCH MAN, 나루토=NARUTO

CRITICAL RULES:
1. **DATA ONLY**: ONLY cite data from "DASHBOARD DATA". NEVER invent numbers, dates, titles, or partners. If a partner does NOT appear in a region section, do NOT include that region.
2. Korean partner/region names → look up alias → find in data.
3. **YEAR FILTERING**: If user mentions a year (e.g., "2026년 주술회전"), ONLY include videos from that year.
4. **CROSS-REGION THOROUGHNESS**: The system has pre-filtered sections containing the asked partner. You MUST check EVERY section provided. Partner names may vary in case ("ATTACK ON TITAN" vs "Attack on Titan") — match case-insensitively. List ALL regions where found.
5. **NO FABRICATION**: NEVER add regions where the partner was NOT found. NEVER fabricate 참여율, 전환율, DAU, 매출.
6. If data doesn't exist, say so explicitly.
7. Respond in Korean unless user writes in English/Chinese.
9. **CATEGORY COMPLETENESS**: When user asks about a specific category (e.g., "Vehicle"), list ALL partners in that category from the provided data. NEVER skip partners. If data has 20 Vehicle partners, show all 20 in the table. Count them before responding.
10. **REGION vs COUNTRY CONSISTENCY**: In comparison tables, NEVER mix region-level labels (동아시아, 동남아시아, 중동) with country-level labels (한국, 일본, 터키) in the same table. If user asks to compare regions, aggregate country data into region totals. If user asks to compare countries, use country-level data only.
8. **NUMBERS — ABSOLUTE RULE**: Every number MUST be copied character-by-character from the data. NEVER drop leading digits.
   - Data: "23,001,580 views" → Write: "23,001,580" (CORRECT)
   - WRONG: ",001,580" or "3,001,580" (leading digits dropped!)
   - Data: "7,672,649 views" → Write: "7,672,649" (CORRECT)
   - WRONG: "7,,649" (middle digits dropped!)
   - Data: "3,536,736 views" → Write: "3,536,736" (CORRECT)
   - WRONG: ",536,736" (leading digit dropped!)
   - In tables, ALWAYS double-check every number has ALL digits before submitting.
   - In summary text, write full numbers: "23,001,580 조회수" — NEVER "억천만", "~2천3백만", or abbreviations.
   - If you are unsure of a number, look it up again in the data rather than guessing.

FORMAT:
- ## for sections, **bold** for key numbers and partner names
- ALWAYS use markdown tables for data comparisons
- In markdown tables, write complete numbers with all digits (e.g., | 23,001,580 | not | ,001,580 |)
- Partner analysis: 요약 → 리전별 성과 테이블 → 영상 상세 → 인사이트
- Regional comparison: 개요 → 리전별 비교 테이블 → 핵심 차이점
- Cross-platform: 플랫폼별 비교 → 종합 평가

ANALYSIS QUALITY:
- Write like a senior consultant briefing a VP.
- Every claim MUST cite a specific, complete number from the data. Double-check before writing.
- For cross-region questions, show a region-by-region breakdown table.
- Calculate derived metrics (평균 조회수/영상, 좋아요율) when useful.
- Identify outliers and explain why they matter.
- When comparing, highlight over/under-performers with hypotheses.
- For timeline/trend questions, note collaboration waves by year.
- In summary/핵심 sections, write the FULL number: e.g., "BUGATTI가 23,001,580 조회수로 1위".`;
}

// ===== Rate Limiting =====

const rateLimitMap = new Map();

function checkRateLimit(ip, maxPerMin) {
  const now = Date.now();
  let entry = rateLimitMap.get(ip);
  if (!entry || now - entry.start > 60_000) {
    entry = { start: now, count: 0 };
    rateLimitMap.set(ip, entry);
  }
  entry.count++;
  return entry.count <= maxPerMin;
}

// ===== CORS =====

function corsHeaders(origin, allowedOrigin) {
  const allowed = allowedOrigin === '*' || origin === allowedOrigin
    || origin?.startsWith('http://localhost') || origin?.startsWith('http://127.0.0.1');
  return {
    'Access-Control-Allow-Origin': allowed ? (origin || '*') : allowedOrigin,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
}

// ===== YouTube Data API v3 =====

async function ytSearch(apiKey, query, maxResults = 10) {
  const params = new URLSearchParams({ part: 'snippet', q: query, type: 'video', maxResults: String(maxResults), order: 'relevance', key: apiKey });
  const res = await fetch(`https://www.googleapis.com/youtube/v3/search?${params}`);
  if (!res.ok) return { error: `YouTube Search API error: ${res.status}` };
  return res.json();
}

async function ytVideoDetails(apiKey, videoIds) {
  const params = new URLSearchParams({ part: 'snippet,statistics', id: videoIds.join(','), key: apiKey });
  const res = await fetch(`https://www.googleapis.com/youtube/v3/videos?${params}`);
  if (!res.ok) return { error: `YouTube Videos API error: ${res.status}` };
  return res.json();
}

async function ytComments(apiKey, videoId, maxResults = 30) {
  const params = new URLSearchParams({ part: 'snippet', videoId, maxResults: String(maxResults), order: 'relevance', textFormat: 'plainText', key: apiKey });
  const res = await fetch(`https://www.googleapis.com/youtube/v3/commentThreads?${params}`);
  if (!res.ok) return { error: `YouTube Comments API error: ${res.status}` };
  return res.json();
}

async function fetchYouTubeData(apiKey, ytRequest) {
  const { action, query, videoId, videoIds, maxResults } = ytRequest;
  try {
    if (action === 'search' && query) {
      const data = await ytSearch(apiKey, query, maxResults || 10);
      if (data.error) return `[YouTube 검색 오류] ${data.error}`;
      const ids = data.items?.map(i => i.id.videoId).filter(Boolean) || [];
      if (ids.length === 0) return `[YouTube 검색] "${query}" 결과 없음`;
      const details = await ytVideoDetails(apiKey, ids);
      if (details.error) return `[YouTube 상세 오류] ${details.error}`;
      let result = `[YouTube 실시간 검색: "${query}" — ${details.items?.length || 0}개 결과]\n`;
      (details.items || []).forEach((v, i) => {
        const s = v.statistics || {};
        result += `${i + 1}. "${v.snippet?.title}" (${v.snippet?.channelTitle})\n`;
        result += `   조회수: ${Number(s.viewCount || 0).toLocaleString()}, 좋아요: ${Number(s.likeCount || 0).toLocaleString()}, 댓글: ${Number(s.commentCount || 0).toLocaleString()}\n`;
        result += `   게시일: ${v.snippet?.publishedAt?.substring(0, 10)} | https://youtube.com/watch?v=${v.id}\n`;
      });
      return result;
    } else if (action === 'comments' && videoId) {
      const data = await ytComments(apiKey, videoId, maxResults || 30);
      if (data.error) return `[YouTube 댓글 오류] ${data.error}`;
      let result = `[YouTube 댓글: ${videoId} — ${data.items?.length || 0}개]\n`;
      (data.items || []).forEach((c, i) => {
        const s = c.snippet?.topLevelComment?.snippet;
        if (!s) return;
        result += `${i + 1}. [좋아요 ${s.likeCount || 0}] ${(s.textDisplay || '').substring(0, 200)}\n`;
      });
      return result;
    } else if (action === 'video_details' && (videoId || videoIds)) {
      const ids = videoIds || [videoId];
      const data = await ytVideoDetails(apiKey, ids);
      if (data.error) return `[YouTube 상세 오류] ${data.error}`;
      let result = `[YouTube 영상 상세 — ${data.items?.length || 0}개]\n`;
      (data.items || []).forEach((v, i) => {
        const s = v.statistics || {};
        result += `${i + 1}. "${v.snippet?.title}" (${v.snippet?.channelTitle})\n`;
        result += `   조회수: ${Number(s.viewCount || 0).toLocaleString()}, 좋아요: ${Number(s.likeCount || 0).toLocaleString()}\n`;
      });
      return result;
    }
  } catch (err) {
    return `[YouTube API 오류] ${err.message}`;
  }
  return '[YouTube] 요청된 데이터 없음';
}

// ===== Main Handler =====

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin') || '';
    const allowedOrigin = env.ALLOWED_ORIGIN || '*';
    const headers = corsHeaders(origin, allowedOrigin);

    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers });
    if (request.method !== 'POST') {
      return new Response(JSON.stringify({ error: 'Method not allowed' }), { status: 405, headers: { ...headers, 'Content-Type': 'application/json' } });
    }

    const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
    if (!checkRateLimit(ip, parseInt(env.RATE_LIMIT_PER_MIN || '50'))) {
      return new Response(JSON.stringify({ error: '요청 한도 초과. 잠시 후 다시 시도해주세요.' }), { status: 429, headers: { ...headers, 'Content-Type': 'application/json' } });
    }

    let body;
    try { body = await request.json(); } catch {
      return new Response(JSON.stringify({ error: 'Invalid JSON' }), { status: 400, headers: { ...headers, 'Content-Type': 'application/json' } });
    }

    const { messages, context } = body;
    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      return new Response(JSON.stringify({ error: 'messages array required' }), { status: 400, headers: { ...headers, 'Content-Type': 'application/json' } });
    }

    const maxCtx = parseInt(env.MAX_CONTEXT_CHARS || '900000');
    const fullContext = context ? context.substring(0, maxCtx) : '';

    // ===== Smart Retrieval: filter context per query =====
    const CLAUDE_MAX_CHARS = 350_000;
    const LLAMA_MAX_CHARS = 30_000;

    const { context: claudeContext, info: retrievalInfo } = smartRetrieve(fullContext, messages.slice(-10), CLAUDE_MAX_CHARS);
    const claudeSystemContent = getSystemPrompt() + (claudeContext ? `\n\n--- CURRENT DASHBOARD DATA (${retrievalInfo}) ---\n${claudeContext}` : '');

    // Try Claude first
    let claudeError = '';
    const anthropicKey = (env.ANTHROPIC_API_KEY || '').trim();
    if (anthropicKey) {
      try {
        const claudeRes = await callClaude(anthropicKey, claudeSystemContent, messages.slice(-10), headers, retrievalInfo);
        if (claudeRes.status === 200) return claudeRes;
        claudeError = `status=${claudeRes.status}`;
        try { const b = await claudeRes.clone().text(); claudeError += ` ${b.substring(0, 200)}`; } catch {}
      } catch (e) {
        claudeError = `exception=${e.message}`;
      }
    }

    // Llama fallback with smaller filtered context
    const { context: llamaContext } = smartRetrieve(fullContext, messages.slice(-10), LLAMA_MAX_CHARS);
    const llamaSystem = getSystemPrompt() + (llamaContext ? `\n\n--- CURRENT DASHBOARD DATA ---\n${llamaContext}` : '');
    const llamaMessages = [{ role: 'system', content: llamaSystem }, ...messages.slice(-10)];

    if (!env.AI) {
      return new Response(JSON.stringify({ error: 'AI binding not configured' }), { status: 500, headers: { ...headers, 'Content-Type': 'application/json' } });
    }

    try {
      const stream = await env.AI.run('@cf/meta/llama-3.3-70b-instruct-fp8-fast', {
        messages: llamaMessages, stream: true, max_tokens: 4000, temperature: 0.15,
      });
      return new Response(stream, {
        status: 200,
        headers: { ...headers, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'X-AI-Model': 'llama-3.3-70b', 'X-Claude-Error': claudeError || 'none', 'X-Retrieval': retrievalInfo },
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: `Workers AI error: ${err.message}` }), { status: 502, headers: { ...headers, 'Content-Type': 'application/json' } });
    }
  },
};

// ===== Claude API via AI Gateway =====

async function callClaude(apiKey, systemContent, userMessages, cors, retrievalInfo) {
  const systemPromptOnly = getSystemPrompt();
  const dataContext = systemContent.includes('--- CURRENT DASHBOARD DATA')
    ? systemContent.split(/--- CURRENT DASHBOARD DATA[^-]*---/)[1] || ''
    : '';

  const systemBlocks = [
    { type: 'text', text: systemPromptOnly, cache_control: { type: 'ephemeral' } },
  ];

  const messagesWithContext = [];
  if (dataContext.trim()) {
    messagesWithContext.push({
      role: 'user',
      content: [{ type: 'text', text: `[DASHBOARD DATA]\n${dataContext}`, cache_control: { type: 'ephemeral' } }],
    });
    messagesWithContext.push({ role: 'assistant', content: '데이터를 확인했습니다. 질문해 주세요.' });
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
        max_tokens: 8192,
        temperature: 0.1,
        system: systemBlocks,
        messages: messagesWithContext,
        stream: true,
      }),
    });

    if (res.ok) return streamClaudeResponse(res, cors, retrievalInfo);

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

function streamClaudeResponse(res, cors, retrievalInfo) {
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
    headers: { ...cors, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'X-AI-Model': 'claude-sonnet', 'X-Retrieval': retrievalInfo || '' },
  });
}

async function callOpenAI(apiKey, gatewayUrl, apiMessages, cors) {
  try {
    const res = await fetch(gatewayUrl, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'gpt-4o-mini', messages: apiMessages, stream: true, max_tokens: 2048, temperature: 0.3 }),
    });
    if (!res.ok) {
      const errText = await res.text();
      let errMsg = `OpenAI error: ${res.status}`;
      try { errMsg = JSON.parse(errText).error?.message || errMsg; } catch {}
      return new Response(JSON.stringify({ error: errMsg }), { status: 502, headers: { ...cors, 'Content-Type': 'application/json' } });
    }
    return new Response(res.body, { status: 200, headers: { ...cors, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' } });
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Failed to reach OpenAI' }), { status: 502, headers: { ...cors, 'Content-Type': 'application/json' } });
  }
}
