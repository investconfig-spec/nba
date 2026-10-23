// NBA Command Hub — zero-dependency backend (mirrors C:\UFC\server.js / C:\SNOOKER\server.js)
// Serves nba.html + proxies free NBA data with:
//   1. in-memory TTL cache   (avoid re-hitting upstream within TTL)
//   2. SQLite staging table  (mirror every success; fall back to last-known on failure)
// Data sources (all free, no key):
//   - ESPN scoreboard / standings / news  (site.api.espn.com / site.web.api.espn.com)
//   - The Odds API NBA odds               (shared ODDS_KEY, free 500 req/month — long TTL)
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');

const PORT = 8092;
const ROOT = __dirname;

const ESPN = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba';
const ESPN_WEB = 'https://site.web.api.espn.com/apis/v2/sports/basketball/nba';
const ODDS_KEY = 'f7e06f4d27c1bb5e70d6f4713b8d2f8e';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36';

// ---- per-source TTL (ms) ----
const TTL = {
  scoreboard: 30 * 1000,       // live scores change fast
  standings: 6 * 3600 * 1000,  // tables move slowly
  news: 15 * 60 * 1000,
  odds: 30 * 60 * 1000,        // shared free key — be conservative
};

// ---- staging DB (SQLite) ----
const DB_PATH = path.join(ROOT, 'nba_staging.db');
const db = new DatabaseSync(DB_PATH);
db.exec(`CREATE TABLE IF NOT EXISTS nba_cache (
  source TEXT NOT NULL, key TEXT NOT NULL, payload TEXT NOT NULL,
  status INTEGER, fetched_at TEXT NOT NULL, PRIMARY KEY (source, key))`);
const upsertStaging = db.prepare(
  `INSERT INTO nba_cache (source,key,payload,status,fetched_at) VALUES (?,?,?,?,?)
   ON CONFLICT(source,key) DO UPDATE SET payload=excluded.payload, status=excluded.status, fetched_at=excluded.fetched_at`);
const getStaging = db.prepare(`SELECT payload, fetched_at FROM nba_cache WHERE source=? AND key=?`);
const stagingCount = () => db.prepare(`SELECT count(*) n FROM nba_cache`).get().n;

// ---- in-memory TTL cache ----
const mem = new Map();
function memGet(key, ttl) { const e = mem.get(key); return e && (Date.now() - e.ts) < ttl ? e : null; }
function memSet(key, payload, status) {
  mem.set(key, { payload, status, ts: Date.now() });
  if (mem.size > 500) { const k = mem.keys().next().value; mem.delete(k); }
}

// ---- single-flight ----
const inflight = new Map();

function send(res, status, body, headers = {}) {
  const h = { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', ...headers };
  res.writeHead(status, h);
  res.end(body);
}

async function fetchUpstream(url, headers = {}) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 15000);
  try {
    const r = await fetch(url, { headers: { 'User-Agent': UA, ...headers }, signal: ctl.signal });
    const text = await r.text();
    return { ok: r.ok, status: r.status, text, headers: r.headers };
  } catch (e) {
    return { ok: false, status: 0, text: '', error: String(e && e.message || e) };
  } finally { clearTimeout(t); }
}

// core proxy: source -> upstream URL builder, TTL
async function proxied(source, key, url, extraHeaders) {
  const cacheKey = source + ':' + key;
  const hit = memGet(cacheKey, TTL[source] || 60000);
  if (hit) return { payload: hit.payload, status: 200, stale: 0, cache: 'memory' };

  if (inflight.has(cacheKey)) return inflight.get(cacheKey);
  const p = (async () => {
    const up = await fetchUpstream(url, extraHeaders);
    if (up.ok) {
      upsertStaging.run(source, key, up.text, up.status, new Date().toISOString());
      memSet(cacheKey, up.text, up.status);
      return { payload: up.text, status: up.status, stale: 0, cache: 'upstream' };
    }
    const row = getStaging.get(source, key);
    if (row) return { payload: row.payload, status: 200, stale: 1, cache: 'staging', fetchedAt: row.fetched_at };
    return { payload: JSON.stringify({ error: 'upstream_failed', detail: up.error || ('HTTP ' + up.status) }), status: 502, stale: 0, cache: 'none' };
  })().finally(() => inflight.delete(cacheKey));
  inflight.set(cacheKey, p);
  return p;
}

// ---- ESPN scoreboard normalisation ----
function normRecord(team) {
  try {
    const rec = (team && team.record && team.record[0]) || {};
    return rec.summary || rec.displayValue || '';
  } catch (e) { return ''; }
}
function normTeam(comp) {
  const home = (comp.homeAway === 'home');
  const logo = comp.team && comp.team.logo;
  return {
    name: (comp.team && (comp.team.displayName || comp.team.name)) || '',
    abbr: (comp.team && comp.team.abbreviation) || '',
    logo: logo || '',
    score: comp.score != null ? String(comp.score) : '',
    record: normRecord(comp.team),
    winner: !!comp.winner,
  };
}
function normEvent(ev) {
  const comp = (ev.competitions && ev.competitions[0]) || {};
  const comps = comp.competitors || [];
  const home = comps.find(c => c.homeAway === 'home');
  const away = comps.find(c => c.homeAway === 'away');
  const status = (ev.status && ev.status.type) || {};
  const stName = (status.name || '').toLowerCase();
  const state = stName.includes('final') ? 'final' : stName.includes('progress') || stName.includes('in-progress') ? 'live' : 'scheduled';
  return {
    id: ev.id,
    date: ev.date,
    name: ev.name || '',
    shortName: ev.shortName || '',
    status: state,
    statusDetail: (ev.status && (ev.status.type && (ev.status.type.detail || ev.status.type.shortDetail))) || (comp.status && comp.status.type && comp.status.type.shortDetail) || '',
    period: ev.status && ev.status.period,
    clock: ev.status && ev.status.displayClock,
    home: home ? normTeam(home) : null,
    away: away ? normTeam(away) : null,
    venue: comp.venue ? [comp.venue.fullName, comp.venue.address && comp.venue.address.city].filter(Boolean).join(' · ') : '',
  };
}

function datesWindow(n, startOffset = 0) {
  const out = [];
  const base = new Date();
  for (let i = 0; i < n; i++) {
    const d = new Date(base.getTime() + (i + startOffset) * 86400 * 1000);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const day = String(d.getUTCDate()).padStart(2, '0');
    out.push(String(y) + m + day);
  }
  return out;
}

// shape a raw ESPN scoreboard payload into the client shape
function shapeScoreboard(raw) {
  const data = JSON.parse(raw);
  const events = (data.events || []).map(normEvent);
  return {
    season: data.season, events,
    live: events.filter(e => e.status === 'live'),
    upcoming: events.filter(e => e.status === 'scheduled'),
    recent: events.filter(e => e.status === 'final'),
  };
}

async function scoreboard(qs) {
  const p = new URLSearchParams(qs || '');
  // single-day mode (explicit ?dates=YYYYMMDD)
  if (p.get('dates')) {
    const key = 'scoreboard:' + qs;
    const r = await proxied('scoreboard', key, ESPN + '/scoreboard?' + qs);
    if (r.status !== 200 || r.stale) return r;
    try { return { payload: JSON.stringify(shapeScoreboard(r.payload)), status: 200, stale: 0, cache: 'upstream' }; }
    catch (e) { return r; }
  }
  // multi-day window (yesterday + today + next 3 days) so upcoming games and
  // last night's finals both populate the dashboard (ESPN groups by US-Eastern date)
  const days = datesWindow(5, -1);
  const seen = new Set();
  let season = null;
  const events = [];
  for (const d of days) {
    const r = await proxied('scoreboard', 'scoreboard:' + d, ESPN + '/scoreboard?dates=' + d);
    if (r.status !== 200) continue;
    try {
      const data = JSON.parse(r.payload);
      if (!season) season = data.season;
      for (const ev of (data.events || [])) {
        if (!seen.has(ev.id)) { seen.add(ev.id); events.push(normEvent(ev)); }
      }
    } catch (e) {}
  }
  events.sort((a, b) => (a.date || '').localeCompare(b.date || ''));
  const payload = JSON.stringify({
    season, events,
    live: events.filter(e => e.status === 'live'),
    upcoming: events.filter(e => e.status === 'scheduled'),
    recent: events.filter(e => e.status === 'final'),
  });
  return { payload, status: 200, stale: 0, cache: 'upstream' };
}

// ---- standings normalisation (group by conference) ----
async function standings() {
  const r = await proxied('standings', 'standings', ESPN_WEB + '/standings');
  if (r.status !== 200 || r.stale) return r;
  try {
    const data = JSON.parse(r.payload);
    const groups = (data.children || []).map(conf => ({
      name: conf.name,
      abbreviation: conf.abbreviation,
      entries: ((conf.standings && conf.standings.entries) || []).map(en => ({
        team: en.team ? { id: en.team.id, name: en.team.displayName, abbr: en.team.abbreviation, logo: en.team.logos && en.team.logos[0] && en.team.logos[0].href } : {},
        stats: (en.stats || []).reduce((a, s) => { a[s.name] = s.displayValue != null ? s.displayValue : s.value; return a; }, {}),
      })),
    }));
    return { payload: JSON.stringify({ season: data.season && data.season.displayName, groups }), status: 200, stale: 0, cache: 'upstream' };
  } catch (e) {
    return r;
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');

  // ---- scoreboard (live + upcoming + recent) ----
  if (url.pathname === '/api/scoreboard') {
    const r = await scoreboard(url.searchParams.toString());
    return send(res, r.status, r.payload, { 'X-Stale': String(r.stale), 'X-Cache': r.cache, ...(r.fetchedAt ? { 'X-Fetched-At': r.fetchedAt } : {}) });
  }

  // ---- standings ----
  if (url.pathname === '/api/standings') {
    const r = await standings();
    return send(res, r.status, r.payload, { 'X-Stale': String(r.stale), 'X-Cache': r.cache, ...(r.fetchedAt ? { 'X-Fetched-At': r.fetchedAt } : {}) });
  }

  // ---- news ----
  if (url.pathname === '/api/news') {
    const r = await proxied('news', 'news', ESPN + '/news');
    return send(res, r.status, r.payload, { 'X-Stale': String(r.stale), 'X-Cache': r.cache, ...(r.fetchedAt ? { 'X-Fetched-At': r.fetchedAt } : {}) });
  }

  // ---- odds (h2h + spreads + totals) ----
  if (url.pathname === '/api/odds') {
    const markets = url.searchParams.get('markets') || 'h2h,spreads,totals';
    const key = 'odds:' + markets;
    const url2 = `https://api.the-odds-api.com/v4/sports/basketball_nba/odds/?regions=us&markets=${encodeURIComponent(markets)}&oddsFormat=decimal&apiKey=${ODDS_KEY}`;
    const r = await proxied('odds', key, url2);
    return send(res, r.status, r.payload, { 'X-Stale': String(r.stale), 'X-Cache': r.cache, ...(r.fetchedAt ? { 'X-Fetched-At': r.fetchedAt } : {}) });
  }

  // ---- health ----
  if (url.pathname === '/api/health') {
    return send(res, 200, JSON.stringify({ ok: true, staging_rows: stagingCount() }));
  }

  // ---- static files ----
  let file = url.pathname === '/' ? '/nba.html' : url.pathname;
  const filePath = path.join(ROOT, file);
  if (filePath.startsWith(ROOT) && fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath).toLowerCase();
    const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml' };
    res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream' });
    return res.end(fs.readFileSync(filePath));
  }
  send(res, 404, JSON.stringify({ error: 'not_found' }));
});

server.listen(PORT, () => {
  console.log(`NBA Hub backend on http://localhost:${PORT}  (staging: ${DB_PATH}, rows=${stagingCount()})`);
});
