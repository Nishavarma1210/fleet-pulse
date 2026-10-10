// Fleet Pulse mock backend. Implements the API contract from the assignment brief.
// Kept in one file on purpose: it is not scored, it just has to behave as specified.
// Run: npm install && npm start   (API on :4000, secondary listener on :4001)

const http = require('http');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const { WebSocketServer } = require('ws');

// ---------- Constants ----------
const PORT = 4000;
const PORT_SECONDARY = 4001; // same data, no HTTP chaos (circuit-breaker stretch goal)
const SERVICE_COUNT = 2000;
const PING_MS = 5000;
const TICK_MS = 20;
const SHUFFLE_FLUSH_MS = 250; // shuffled events are held at most this long (< 1 s)
const LOG_KEEP_MS = 6 * 60 * 1000; // replay keeps at least 5 minutes
const INCIDENT_CHANCE = 0.0002; // chance that an event starts an incident on its service
const PASSWORD = 'pulse123';
const USERS = {
  'operator@pulse.dev': { id: 'u-1', email: 'operator@pulse.dev', role: 'operator' },
  'viewer@pulse.dev': { id: 'u-2', email: 'viewer@pulse.dev', role: 'viewer' },
};
const METRICS = ['p95', 'errRate', 'rps'];
const OPERATORS = ['>', '>=', '<', '<='];
const DEFAULT_CHAOS = { rate: 500 };

// ---------- Chaos switches ----------
let chaos = { ...DEFAULT_CHAOS };

const tokens = (v) =>
  v === undefined || v === null ? [] : String(v).split(',').map((s) => s.trim()).filter(Boolean);
// "drop:20" -> 20
const numAfter = (list, name) => {
  const t = list.find((x) => x.startsWith(name + ':'));
  return t ? Number(t.split(':')[1]) || 0 : 0;
};
// Only these keys can be overridden per connection through the query string.
const pick = (query = {}) => {
  const out = {};
  for (const k of ['ws', 'sse', 'chaos', 'kill']) {
    if (typeof query[k] === 'string') out[k] = query[k];
  }
  return out;
};
function effective(overrides = {}) {
  const sw = { ...chaos, ...overrides };
  const ws = tokens(sw.ws);
  const sse = tokens(sw.sse);
  const c = tokens(sw.chaos);
  return {
    wsDown: ws.includes('down'),
    wsSilent: ws.includes('silent'),
    sseDown: sse.includes('down'),
    drop: numAfter(ws, 'drop'),
    shuffle: c.includes('shuffle'),
    dupe: c.includes('dupe'),
    garbage: numAfter(c, 'garbage'),
    kill: Number(sw.kill) || 0,
  };
}
function httpFx() {
  const h = tokens(chaos.http);
  return { err500: numAfter(h, '500'), slow: numAfter(h, 'slow'), limited: h.includes('429') };
}

// ---------- Services and metric generation ----------
const TEAMS = ['payments', 'checkout', 'search', 'auth', 'inventory', 'shipping', 'notifications', 'billing', 'catalog', 'analytics', 'media', 'messaging'];
const KINDS = ['api', 'worker', 'gateway', 'db-proxy', 'cache', 'scheduler', 'ingest', 'indexer'];

// Deterministic pseudo-random number in [0, 1) so history is stable between calls.
const hash = (i, k) => {
  const x = Math.sin(i * 12.9898 + k * 78.233) * 43758.5453;
  return x - Math.floor(x);
};

const services = [];
const base = [];
const state = [];
for (let i = 0; i < SERVICE_COUNT; i++) {
  const team = TEAMS[i % TEAMS.length];
  const kind = KINDS[Math.floor(i / TEAMS.length) % KINDS.length];
  const n = Math.floor(i / (TEAMS.length * KINDS.length)) + 1;
  services.push({ id: 'svc-' + String(i + 1).padStart(4, '0'), name: `${team}-${kind}-${String(n).padStart(2, '0')}`, team });
  base.push({ p95: 40 + hash(i, 1) * 360, err: 0.1 + hash(i, 2) * 1.4, rps: 50 + hash(i, 3) * 2950, phase: hash(i, 4) * 6.28 });
  state.push({ seq: 1000 + Math.floor(hash(i, 5) * 9000), incidentUntil: 0, p95Mult: 1, errAdd: 0, last: null });
}

// Smooth per-service curves, used for both live events and history so they match.
function metricAt(i, sec) {
  const b = base[i];
  const w = Math.sin(sec / 90 + b.phase);
  const noise = hash(i, sec) - 0.5;
  return {
    p95: Math.max(5, b.p95 * (1 + 0.15 * w) + noise * b.p95 * 0.05),
    err: Math.max(0, b.err * (1 + 0.3 * w)),
    rps: Math.max(1, b.rps * (1 + 0.2 * Math.sin(sec / 120 + b.phase * 2))),
  };
}
const statusOf = (p95, err) => (err >= 20 ? 'down' : err >= 3 || p95 >= 600 ? 'degraded' : 'healthy');

let offset = 1000000; // global stream offset
const log = []; // every generated event, in offset order (used by replay)

function generateEvent(i, ts) {
  const s = state[i];
  const m = metricAt(i, Math.floor(ts / 1000));
  let p95 = m.p95 * (0.95 + Math.random() * 0.1);
  let err = m.err * (0.85 + Math.random() * 0.3);
  const rps = m.rps * (0.95 + Math.random() * 0.1);
  if (ts < s.incidentUntil) {
    p95 *= s.p95Mult;
    err += s.errAdd;
  } else if (Math.random() < INCIDENT_CHANCE) {
    s.incidentUntil = ts + (20 + Math.random() * 40) * 1000;
    s.p95Mult = 2 + Math.random() * 3;
    s.errAdd = 4 + Math.random() * 36;
  }
  const ev = {
    type: 'metric',
    offset: ++offset,
    serviceId: services[i].id,
    seq: ++s.seq,
    ts,
    p95: Math.round(p95),
    errRate: Math.min(100, Math.round(err * 10) / 10),
    rps: Math.round(rps),
    status: statusOf(p95, err),
  };
  s.last = ev;
  log.push(ev);
  return ev;
}
// One event per service so /metrics/snapshot is complete from the start.
for (let i = 0; i < SERVICE_COUNT; i++) generateEvent(i, Date.now());

// ---------- Stream clients (WebSocket and SSE share this) ----------
const clients = new Set();

function makeClient(transport, query, send, close) {
  const c = { transport, query, send, close, fx: effective(pick(query)), openedAt: Date.now(), buf: [] };
  clients.add(c);
  return c;
}

// Replace a frame with broken data: invalid JSON or an event with a missing field.
function mutate(ev, json) {
  if (Math.random() < 0.5) return json.slice(0, Math.max(5, Math.floor(json.length / 2)));
  const copy = { ...ev };
  const keys = ['serviceId', 'seq', 'ts', 'p95', 'errRate', 'rps', 'status'];
  delete copy[keys[Math.floor(Math.random() * keys.length)]];
  return JSON.stringify(copy);
}
function emit(c, ev, json) {
  const out = c.fx.garbage && Math.random() * 100 < c.fx.garbage ? mutate(ev, json) : json;
  if (c.fx.shuffle) c.buf.push([ev.offset, out]);
  else c.send('metric', out, ev.offset);
}
function deliver(c, ev, json) {
  const fx = c.fx;
  if (fx.wsSilent && c.transport === 'ws') return;
  if (fx.drop && Math.random() * 100 < fx.drop) return; // offset/seq are used up -> gap
  emit(c, ev, json);
  if (fx.dupe && Math.random() < 0.1) {
    setTimeout(() => clients.has(c) && emit(c, ev, json), Math.random() * 700);
  }
}
function refreshClients() {
  for (const c of clients) {
    c.fx = effective(pick(c.query));
    if ((c.transport === 'ws' && c.fx.wsDown) || (c.transport === 'sse' && c.fx.sseDown)) c.close();
  }
}

// Generator: produces `rate` events per second, spread over small ticks.
let lastTick = Date.now();
let budget = 0;
setInterval(() => {
  const now = Date.now();
  const dt = Math.min(now - lastTick, 200);
  lastTick = now;
  budget += ((Number(chaos.rate) || 500) * dt) / 1000;
  const n = Math.floor(budget);
  budget -= n;
  for (let k = 0; k < n; k++) {
    const ts = now - Math.round((dt * (n - 1 - k)) / n);
    const ev = generateEvent(Math.floor(Math.random() * SERVICE_COUNT), ts);
    if (clients.size) {
      const json = JSON.stringify(ev);
      for (const c of clients) deliver(c, ev, json);
    }
  }
}, TICK_MS);

// Release held (shuffled) events in random order.
setInterval(() => {
  for (const c of clients) {
    if (!c.buf.length) continue;
    const items = c.buf;
    c.buf = [];
    for (let i = items.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [items[i], items[j]] = [items[j], items[i]];
    }
    for (const [off, out] of items) c.send('metric', out, off);
  }
}, SHUFFLE_FLUSH_MS);

// Ping every 5 s on both transports (a silent WebSocket gets nothing at all).
setInterval(() => {
  const json = JSON.stringify({ type: 'ping', ts: Date.now() });
  for (const c of clients) {
    if (c.fx.wsSilent && c.transport === 'ws') continue;
    c.send('ping', json);
  }
}, PING_MS);

// kill=<s>: close every connection that has been open that long.
setInterval(() => {
  const now = Date.now();
  for (const c of clients) if (c.fx.kill && now - c.openedAt >= c.fx.kill * 1000) c.close();
}, 1000);

// Drop old events (keep ~6 min) and long-expired tokens.
setInterval(() => {
  const cutoff = Date.now() - LOG_KEEP_MS;
  let k = 0;
  while (k < log.length && log[k].ts < cutoff) k++;
  if (k > 0) log.splice(0, k);
  for (const [t, v] of accessTokens) if (v.exp < Date.now() - 10 * 60 * 1000) accessTokens.delete(t);
}, 10000);

// ---------- Auth ----------
const accessTokens = new Map(); // token -> { user, exp }
const refreshTokens = new Map(); // token -> { user, used }

function issueTokens(user) {
  const ttl = Number(chaos.token_ttl) || 60;
  const accessToken = 'at_' + crypto.randomBytes(16).toString('hex');
  const refreshToken = 'rt_' + crypto.randomBytes(16).toString('hex');
  accessTokens.set(accessToken, { user, exp: Date.now() + ttl * 1000 });
  refreshTokens.set(refreshToken, { user, used: false });
  return { accessToken, refreshToken, expiresIn: ttl, user };
}
function checkToken(token) {
  const t = token && accessTokens.get(token);
  if (!t) return { code: 'token_invalid', message: 'Access token invalid' };
  if (t.exp < Date.now()) return { code: 'token_expired', message: 'Access token expired' };
  return { user: t.user };
}

// ---------- Rules ----------
const rules = [];
const idempotency = new Map(); // Idempotency-Key -> { status, body }
let ruleSeq = 0;
function seedRule(name, conditions) {
  const t = Date.now();
  rules.push({ id: 'r-' + ++ruleSeq, name, conditions, createdAt: t, updatedAt: t });
}
seedRule('Checkout latency', [{ metric: 'p95', operator: '>', threshold: 500, durationSec: 30 }]);
seedRule('High error rate', [{ metric: 'errRate', operator: '>=', threshold: 5, durationSec: 20 }]);

function validateRule(b) {
  if (!b || typeof b !== 'object' || Array.isArray(b)) return [{ field: 'body', message: 'Body must be an object' }];
  const d = [];
  if (typeof b.name !== 'string' || !b.name.trim()) d.push({ field: 'name', message: 'Name is required' });
  if (!Array.isArray(b.conditions) || b.conditions.length === 0) {
    d.push({ field: 'conditions', message: 'At least one condition is required' });
  } else {
    b.conditions.forEach((c, i) => {
      const f = `conditions[${i}]`;
      if (!c || typeof c !== 'object') return d.push({ field: f, message: 'Must be an object' });
      if (!METRICS.includes(c.metric)) d.push({ field: f + '.metric', message: 'Must be p95, errRate or rps' });
      if (!OPERATORS.includes(c.operator)) d.push({ field: f + '.operator', message: 'Must be >, >=, < or <=' });
      if (typeof c.threshold !== 'number' || !isFinite(c.threshold)) d.push({ field: f + '.threshold', message: 'Must be a number' });
      if (typeof c.durationSec !== 'number' || !(c.durationSec >= 0)) d.push({ field: f + '.durationSec', message: 'Must be a number >= 0' });
    });
  }
  return d;
}
const nameTaken = (name, excludeId) => {
  const n = String(name).trim().toLowerCase();
  return rules.some((r) => r.id !== excludeId && r.name.trim().toLowerCase() === n);
};

// ---------- HTTP app ----------
const err = (res, status, code, message, details = []) => res.status(status).json({ error: { code, message, details } });
const STREAM_PATHS = ['/stream', '/stream/sse', '/assistant/stream'];

function makeApp(withHttpChaos) {
  const app = express();
  app.use(cors({ origin: true, exposedHeaders: ['Retry-After'] }));
  app.use(express.json({ limit: '1mb' }));

  // --- No auth, no HTTP chaos ---
  app.get('/health', (req, res) => res.json({ status: 'ok' }));

  app.get('/chaos', (req, res) => res.json(chaos));
  app.post('/chaos', (req, res) => {
    const b = req.body;
    if (!b || typeof b !== 'object' || Array.isArray(b)) return err(res, 400, 'bad_request', 'Body must be an object');
    for (const [k, v] of Object.entries(b)) {
      if (v === null || v === '') delete chaos[k];
      else chaos[k] = v;
    }
    chaos.rate = Math.min(5000, Math.max(1, Number(chaos.rate) || 500));
    refreshClients();
    res.json(chaos);
  });
  app.delete('/chaos', (req, res) => {
    chaos = { ...DEFAULT_CHAOS };
    refreshClients();
    res.json(chaos);
  });

  app.post('/auth/login', (req, res) => {
    const { email, password } = req.body || {};
    if (typeof email !== 'string' || typeof password !== 'string') return err(res, 400, 'bad_request', 'Email and password are required');
    const user = USERS[email.toLowerCase()];
    if (!user || password !== PASSWORD) return err(res, 401, 'invalid_credentials', 'Wrong email or password');
    res.json(issueTokens(user));
  });
  app.post('/auth/refresh', (req, res) => {
    const entry = refreshTokens.get((req.body || {}).refreshToken);
    if (!entry || entry.used) return err(res, 401, 'refresh_invalid', 'Refresh token invalid');
    entry.used = true; // rotated: the old one can never be used again
    res.json(issueTokens(entry.user));
  });

  // --- HTTP chaos (port 4000 only). Streams are not REST, so they are skipped. ---
  if (withHttpChaos) {
    app.use(async (req, res, next) => {
      if (STREAM_PATHS.includes(req.path)) return next();
      const fx = httpFx();
      if (fx.slow) await new Promise((r) => setTimeout(r, fx.slow));
      if (fx.limited) {
        res.set('Retry-After', '2');
        return err(res, 429, 'rate_limited', 'Too many requests');
      }
      if (fx.err500 && Math.random() * 100 < fx.err500) return err(res, 500, 'server_error', 'Injected server error');
      next();
    });
  }

  // SSE refuses new connections when sse=down (checked before auth).
  app.use('/stream/sse', (req, res, next) => {
    if (effective(pick(req.query)).sseDown) return err(res, 503, 'unavailable', 'SSE is down');
    next();
  });

  // --- Everything below needs a valid access token ---
  app.use((req, res, next) => {
    const header = req.get('Authorization') || '';
    let token = header.startsWith('Bearer ') ? header.slice(7) : '';
    if (!token && STREAM_PATHS.includes(req.path)) token = req.query.access_token;
    const r = checkToken(token);
    if (!r.user) return err(res, 401, r.code, r.message);
    req.user = r.user;
    next();
  });
  const needOperator = (req, res, next) =>
    req.user.role === 'operator' ? next() : err(res, 403, 'forbidden', 'Operators only');

  app.get('/services', (req, res) => res.json(services.map((s, i) => ({ ...s, status: state[i].last.status }))));

  app.get('/metrics/snapshot', (req, res) => res.json({ offset, ts: Date.now(), metrics: state.map((s) => s.last) }));

  app.get('/metrics/history', (req, res) => {
    const m = /^svc-(\d{4})$/.exec(String(req.query.serviceId || ''));
    const idx = m ? Number(m[1]) - 1 : -1;
    if (idx < 0 || idx >= SERVICE_COUNT) return err(res, 404, 'not_found', 'Unknown service');
    const minutes = req.query.minutes === undefined ? 15 : Number(req.query.minutes);
    if (!Number.isInteger(minutes) || minutes < 1 || minutes > 60) return err(res, 400, 'bad_request', 'minutes must be 1-60');
    const nowSec = Math.floor(Date.now() / 1000);
    const points = [];
    for (let sec = nowSec - minutes * 60 + 1; sec <= nowSec; sec++) {
      const v = metricAt(idx, sec);
      points.push({ ts: sec * 1000, p95: Math.round(v.p95), errRate: Math.round(v.err * 10) / 10, rps: Math.round(v.rps) });
    }
    res.json({ serviceId: services[idx].id, points });
  });

  app.get('/metrics/replay', (req, res) => {
    const since = Number(req.query.since);
    if (!Number.isInteger(since)) return err(res, 400, 'bad_request', 'since must be an integer offset');
    if (log.length === 0 || since < log[0].offset - 1) return err(res, 410, 'replay_too_old', 'Offset is too old, reload the snapshot');
    res.json({ events: log.slice(since + 1 - log[0].offset), offset });
  });

  // --- Rules (check-name must come before /:id) ---
  app.get('/rules/check-name', (req, res) => res.json({ available: !nameTaken(req.query.name || '', req.query.excludeId) }));
  app.get('/rules', (req, res) => res.json(rules));
  app.get('/rules/:id', (req, res) => {
    const r = rules.find((x) => x.id === req.params.id);
    return r ? res.json(r) : err(res, 404, 'not_found', 'Rule not found');
  });
  app.post('/rules', needOperator, (req, res) => {
    const key = req.get('Idempotency-Key');
    if (key && idempotency.has(key)) {
      const saved = idempotency.get(key);
      return res.status(saved.status).json(saved.body); // same key -> same answer, no second rule
    }
    const d = validateRule(req.body);
    if (d.length) return err(res, 422, 'validation', 'Invalid rule', d);
    if (nameTaken(req.body.name)) return err(res, 409, 'name_taken', 'Rule name already exists');
    const t = Date.now();
    const rule = { ...req.body, id: 'r-' + ++ruleSeq, createdAt: t, updatedAt: t };
    rules.push(rule);
    if (key) idempotency.set(key, { status: 201, body: rule });
    res.status(201).json(rule);
  });
  app.put('/rules/:id', needOperator, (req, res) => {
    const idx = rules.findIndex((x) => x.id === req.params.id);
    if (idx < 0) return err(res, 404, 'not_found', 'Rule not found');
    const d = validateRule(req.body);
    if (d.length) return err(res, 422, 'validation', 'Invalid rule', d);
    if (nameTaken(req.body.name, req.params.id)) return err(res, 409, 'name_taken', 'Rule name already exists');
    const old = rules[idx];
    rules[idx] = { ...req.body, id: old.id, createdAt: old.createdAt, updatedAt: Date.now() };
    res.json(rules[idx]);
  });
  app.delete('/rules/:id', needOperator, (req, res) => {
    const idx = rules.findIndex((x) => x.id === req.params.id);
    if (idx < 0) return err(res, 404, 'not_found', 'Rule not found');
    rules.splice(idx, 1);
    res.status(204).end();
  });

  // --- Telemetry and assistant (stretch goals) ---
  app.post('/telemetry', (req, res) => {
    const n = Array.isArray(req.body && req.body.errors) ? req.body.errors.length : 0;
    console.log(`[telemetry] received ${n} error report(s)`);
    res.status(202).json({});
  });
  app.get('/assistant/stream', (req, res) => {
    const q = String(req.query.q || '').trim();
    if (!q) return err(res, 400, 'bad_request', 'q is required');
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    const worst = state.map((s) => s.last).sort((a, b) => b.errRate - a.errRate).slice(0, 3);
    const text =
      `**Ops assistant (mock)**\n\nYou asked: "${q}"\n\nHighest error rates right now:\n` +
      worst.map((e) => `- ${e.serviceId}: ${e.errRate}% errors, p95 ${e.p95} ms`).join('\n');
    const words = text.split(/(\s+)/);
    let n = 0;
    const timer = setInterval(() => {
      if (n >= words.length) {
        res.write('event: done\ndata: {}\n\n');
        clearInterval(timer);
        return res.end();
      }
      res.write(`event: token\ndata: ${JSON.stringify({ text: words[n++] })}\n\n`);
    }, 40);
    req.on('close', () => clearInterval(timer));
  });

  // --- SSE metric stream ---
  app.get('/stream/sse', (req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
    res.write(': connected\n\n');
    const c = makeClient(
      'sse',
      req.query,
      (type, json, off) => res.write(`event: ${type}\n${off !== undefined ? `id: ${off}\n` : ''}data: ${json}\n\n`),
      () => res.end(),
    );
    req.on('close', () => clients.delete(c));
  });

  app.use((req, res) => err(res, 404, 'not_found', 'Route not found'));
  // eslint-disable-next-line no-unused-vars
  app.use((e, req, res, next) => {
    if (e && e.type === 'entity.parse.failed') return err(res, 400, 'bad_request', 'Invalid JSON body');
    console.error(e);
    err(res, 500, 'server_error', 'Unexpected error');
  });
  return app;
}

// ---------- WebSocket stream ----------
const wss = new WebSocketServer({ noServer: true });

function onUpgrade(req, socket, head) {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname !== '/stream') return socket.destroy();
  const query = Object.fromEntries(url.searchParams);
  if (effective(pick(query)).wsDown) {
    socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
    return socket.destroy();
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    const header = req.headers.authorization || '';
    const token = url.searchParams.get('access_token') || (header.startsWith('Bearer ') ? header.slice(7) : '');
    if (!checkToken(token).user) return ws.close(4401, 'unauthorized'); // token is only checked at open
    const c = makeClient(
      'ws',
      query,
      (type, json) => ws.readyState === 1 && ws.bufferedAmount < 8e6 && ws.send(json),
      () => ws.close(1001, 'closed by server'),
    );
    ws.on('close', () => clients.delete(c));
    ws.on('error', () => clients.delete(c));
  });
}

function listen(port, withHttpChaos) {
  const server = http.createServer(makeApp(withHttpChaos));
  server.on('upgrade', onUpgrade);
  server.listen(port, () => console.log(`Fleet Pulse mock listening on :${port}${withHttpChaos ? '' : ' (no HTTP chaos)'}`));
}
listen(PORT, true);
listen(PORT_SECONDARY, false);
