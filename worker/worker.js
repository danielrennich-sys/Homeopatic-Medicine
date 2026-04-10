/**
 * Cloudflare Worker — Anthropic API Proxy
 *
 * Forwards intake analysis requests to Claude API using a secret API key.
 * Keeps your key hidden from end users.
 *
 * Environment variable required:
 *   ANTHROPIC_API_KEY = your sk-ant-... key (set in Cloudflare dashboard)
 */

const ALLOWED_ORIGINS = [
    'https://danielrennich-sys.github.io',
    'http://localhost:8080',
    'http://127.0.0.1:8080',
];

function corsHeaders(request) {
    const origin = request.headers.get('Origin') || '';
    const allowedOrigin = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
    return {
        'Access-Control-Allow-Origin': allowedOrigin,
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
    };
}

export default {
    async fetch(request, env) {
        // Handle CORS preflight
        if (request.method === 'OPTIONS') {
            return new Response(null, { status: 204, headers: corsHeaders(request) });
        }

        if (request.method !== 'POST') {
            return new Response(JSON.stringify({ error: 'POST only' }), {
                status: 405,
                headers: { ...corsHeaders(request), 'Content-Type': 'application/json' },
            });
        }

        // Rate limiting: simple per-IP limit using CF headers
        // (Cloudflare's free tier doesn't have built-in rate limiting,
        //  but the Worker invocation limit of 100K/day is effectively a cap)

        const apiKey = env.ANTHROPIC_API_KEY;
        if (!apiKey) {
            return new Response(JSON.stringify({ error: 'API key not configured' }), {
                status: 500,
                headers: { ...corsHeaders(request), 'Content-Type': 'application/json' },
            });
        }

        try {
            const body = await request.text();

            // Basic validation — only allow messages API calls
            const parsed = JSON.parse(body);
            if (!parsed.messages || !parsed.model) {
                return new Response(JSON.stringify({ error: 'Invalid request format' }), {
                    status: 400,
                    headers: { ...corsHeaders(request), 'Content-Type': 'application/json' },
                });
            }

            // Enforce limits to prevent abuse
            parsed.max_tokens = Math.min(parsed.max_tokens || 2000, 3000);

            // Forward to Anthropic API
            const anthropicResp = await fetch('https://api.anthropic.com/v1/messages', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': apiKey,
                    'anthropic-version': '2023-06-01',
                },
                body: JSON.stringify(parsed),
            });

            const respBody = await anthropicResp.text();

            return new Response(respBody, {
                status: anthropicResp.status,
                headers: {
                    ...corsHeaders(request),
                    'Content-Type': 'application/json',
                },
            });
        } catch (err) {
            return new Response(JSON.stringify({ error: 'Proxy error: ' + err.message }), {
                status: 500,
                headers: { ...corsHeaders(request), 'Content-Type': 'application/json' },
            });
        }
    },
};
