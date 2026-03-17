const SYSTEM_PROMPT = `You are an expert data analyst for PUBG MOBILE collaboration partnerships.
You analyze YouTube, Instagram, and Weibo collaboration data across multiple global regions.

Guidelines:
- Always respond in Korean unless the user writes in another language
- Cite specific numbers from the provided data context
- When asked for reports, use structured format with headers, bullets, and tables
- When comparing, highlight key differences and provide actionable insights
- If asked about data you don't have, say so clearly
- Keep responses concise but data-rich
- Use markdown formatting for readability`;

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
        status: 405,
        headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
    const maxPerMin = parseInt(env.RATE_LIMIT_PER_MIN || '10');
    if (!checkRateLimit(ip, maxPerMin)) {
      return new Response(JSON.stringify({ error: 'Rate limit exceeded. Please wait a moment.' }), {
        status: 429,
        headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response(JSON.stringify({ error: 'Invalid JSON' }), {
        status: 400,
        headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    const { messages, context } = body;
    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      return new Response(JSON.stringify({ error: 'messages array required' }), {
        status: 400,
        headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    const maxCtx = parseInt(env.MAX_CONTEXT_CHARS || '12000');
    const trimmedContext = context ? context.substring(0, maxCtx) : '';

    const systemContent = SYSTEM_PROMPT + (trimmedContext
      ? `\n\n--- CURRENT DASHBOARD DATA ---\n${trimmedContext}`
      : '');

    const apiMessages = [
      { role: 'system', content: systemContent },
      ...messages.slice(-10),
    ];

    // Try OpenAI first (if API key and gateway URL are set), fall back to Workers AI
    const apiKey = env.OPENAI_API_KEY;
    const gatewayUrl = env.AI_GATEWAY_URL;

    if (apiKey && gatewayUrl) {
      return callOpenAI(apiKey, gatewayUrl, apiMessages, headers);
    }

    // Workers AI (default — no geo-restrictions)
    if (!env.AI) {
      return new Response(JSON.stringify({ error: 'AI binding not configured' }), {
        status: 500,
        headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }

    try {
      const stream = await env.AI.run('@cf/meta/llama-3.3-70b-instruct-fp8-fast', {
        messages: apiMessages,
        stream: true,
        max_tokens: 2048,
        temperature: 0.3,
      });

      return new Response(stream, {
        status: 200,
        headers: {
          ...headers,
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
        },
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: `Workers AI error: ${err.message}` }), {
        status: 502,
        headers: { ...headers, 'Content-Type': 'application/json' },
      });
    }
  },
};

async function callOpenAI(apiKey, gatewayUrl, apiMessages, corsHeaders) {
  try {
    const res = await fetch(gatewayUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: apiMessages,
        stream: true,
        max_tokens: 2048,
        temperature: 0.3,
      }),
    });

    if (!res.ok) {
      const errText = await res.text();
      let errMsg = `OpenAI error: ${res.status}`;
      try { errMsg = JSON.parse(errText).error?.message || errMsg; } catch {}
      return new Response(JSON.stringify({ error: errMsg }), {
        status: 502,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }

    return new Response(res.body, {
      status: 200,
      headers: {
        ...corsHeaders,
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
      },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Failed to reach OpenAI' }), {
      status: 502,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
}
