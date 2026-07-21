// HackFire's Panel — Installer Relay Worker
// Deployed once on your own Cloudflare account. Browsers can't call
// api.cloudflare.com directly (no CORS), so this worker bridges that gap:
// the browser sends the user's *own* CF token here, this worker uses it
// server-side to provision their panel, then forwards the result back.
// The token is never stored or logged — used in-memory for one request only.

const CF_API = 'https://api.cloudflare.com/client/v4';
const SCRIPT_NAME = 'hackfires-panel';
const SOURCE_URL = 'https://raw.githubusercontent.com/clashhackfire-dot/HackFirePanel/main/worker.js';

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

export default {
  async fetch(request) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }

    if (request.method !== 'POST') {
      return json({ error: 'POST only' }, 405);
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return json({ error: 'invalid JSON body' }, 400);
    }

    const { token, uuid } = body;
    if (!token || !uuid) {
      return json({ error: 'token and uuid are required' }, 400);
    }

    try {
      await verifyToken(token);
      const accountId = await getAccountId(token);
      const workerSource = await fetchWorkerSource();

      await uploadWorker(token, accountId, workerSource, uuid);
      await enableSubdomain(token, accountId);
      const subdomain = await getSubdomainPrefix(token, accountId);

      const workerUrl = `https://${SCRIPT_NAME}.${subdomain}.workers.dev`;
      const vlessLink = `vless://${uuid}@${SCRIPT_NAME}.${subdomain}.workers.dev:443?encryption=none&security=tls&sni=${SCRIPT_NAME}.${subdomain}.workers.dev&type=ws&host=${SCRIPT_NAME}.${subdomain}.workers.dev&path=%2F#HackFiresPanel`;

      return json({ ok: true, url: workerUrl, uuid, vlessLink });
    } catch (err) {
      return json({ error: err.message }, 500);
    }
  },
};

async function verifyToken(token) {
  const res = await cfFetch(`${CF_API}/user/tokens/verify`, token);
  if (!res.success) throw new Error('Invalid or expired Cloudflare token');
}

async function getAccountId(token) {
  const res = await cfFetch(`${CF_API}/accounts`, token);
  if (!res.success || !res.result?.length) throw new Error('No Cloudflare account found for this token');
  return res.result[0].id;
}

async function fetchWorkerSource() {
  const res = await fetch(SOURCE_URL);
  if (!res.ok) throw new Error('Could not fetch worker source from GitHub');
  return await res.text();
}

async function uploadWorker(token, accountId, source, uuid) {
  const metadata = {
    main_module: 'worker.js',
    compatibility_date: '2026-07-01',
    compatibility_flags: ['nodejs_compat'],
    bindings: [{ type: 'plain_text', name: 'USER_ID', text: uuid }],
  };

  const form = new FormData();
  form.append('metadata', JSON.stringify(metadata));
  form.append('worker.js', new Blob([source], { type: 'application/javascript+module' }), 'worker.js');

  const res = await fetch(`${CF_API}/accounts/${accountId}/workers/scripts/${SCRIPT_NAME}`, {
    method: 'PUT',
    headers: { Authorization: `Bearer ${token}` },
    body: form,
  });
  const data = await res.json();
  if (!data.success) throw new Error(data.errors?.[0]?.message || 'Worker upload failed');
}

async function enableSubdomain(token, accountId) {
  const res = await fetch(`${CF_API}/accounts/${accountId}/workers/scripts/${SCRIPT_NAME}/subdomain`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled: true }),
  });
  const data = await res.json();
  if (!data.success) throw new Error(data.errors?.[0]?.message || 'Could not enable workers.dev subdomain');
}

async function getSubdomainPrefix(token, accountId) {
  const res = await cfFetch(`${CF_API}/accounts/${accountId}/workers/subdomain`, token);
  if (!res.success) throw new Error('Could not read workers.dev subdomain prefix');
  return res.result.subdomain;
}

async function cfFetch(url, token) {
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  return await res.json();
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
  });
}
