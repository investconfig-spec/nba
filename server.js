// NBA Command Hub — zero-dependency backend (mirrors C:\UFC\server.js / C:\SNOOKER\server.js)
// Serves nba.html + proxies free NBA data with:
//   1. in-memory TTL cache   (avoid re-hitting upstream within TTL)
//   2. SQLite staging table  (mirror every success; fall back to last-known on failure)
//   3. single-flight coalescing (concurrent requests share one upstream fetch)
//   4. background refresher  (keeps a warm local snapshot of everything, so the whole
//      site can serve from staging with ZERO upstream requests if ESPN/odds go down)
// Data sources (all free, no key):
//   - ESPN scoreboard / standings / teams / rosters / stats / news / box scores
//   - The Odds API NBA odds (shared ODDS_KEY, free 500 req/month — long TTL)
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');

const PORT = 8092;
const ROOT = __dirname;

const ESPN = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba';
const ESPN_WEB = 'https://site.web.api.espn.com/apis/v2/sports/basketball/nba';
const ESPN_STATS = 'https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/statistics/byathlete?region=us&lang=en&contentorigin=espn&limit=500';
const ODDS_KEY = 'f7e06f4d27c1bb5e70d6f4713b8d2f8e';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36';

// ---- per-source TTL (ms) ----
const TTL = {
  scoreboard: 30 * 1000,       // live scores change fast
  standings: 6 * 3600 * 1000,
  news: 15 * 60 * 1000,
  odds: 30 * 60 * 1000,        // shared free key — be conservative
  teams: 12 * 3600 * 1000,
  roster: 12 * 3600 * 1000,
  stats: 6 * 3600 * 1000,      // league-wide player stats
  summary: 60 * 60 * 1000,     // box scores (final games don't change)
};

// ---- staging DB (SQLite) — the snapshot/backup table ----
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
const stagingRows = () => db.prepare(`SELECT source, key, fetched_at FROM nba_cache ORDER BY source`).all();

// ---- in-memory TTL cache ----
const mem = new Map();
function memGet(key, ttl) { const e = mem.get(key); return e && (Date.now() - e.ts) < ttl ? e : null; }
function memSet(key, payload, status) {
  mem.set(key, { payload, status, ts: Date.now() });
  if (mem.size > 800) { const k = mem.keys().next().value; mem.delete(k); }
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
  const t = setTimeout(() => ctl.abort(), 20000);
  try {
    const r = await fetch(url, { headers: { 'User-Agent': UA, ...headers }, signal: ctl.signal });
    const text = await r.text();
    return { ok: r.ok, status: r.status, text };
  } catch (e) {
    return { ok: false, status: 0, text: '', error: String(e && e.message || e) };
  } finally { clearTimeout(t); }
}

// core proxy: in-memory TTL -> upstream -> staging fallback (snapshot)
async function proxied(source, key, url) {
  const cacheKey = source + ':' + key;
  const hit = memGet(cacheKey, TTL[source] || 60000);
  if (hit) return { payload: hit.payload, status: 200, stale: 0, cache: 'memory' };

  if (inflight.has(cacheKey)) return inflight.get(cacheKey);
  const p = (async () => {
    const up = await fetchUpstream(url);
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

// ---- normalisers ----
function normRecord(team) {
  try { const rec = (team && team.record && team.record[0]) || {}; return rec.summary || rec.displayValue || ''; }
  catch (e) { return ''; }
}
function normTeamSide(comp) {
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
  const state = stName.includes('final') ? 'final' : (stName.includes('progress') || stName.includes('in-progress')) ? 'live' : 'scheduled';
  return {
    id: ev.id,
    date: ev.date,
    name: ev.name || '',
    shortName: ev.shortName || '',
    status: state,
    statusDetail: (ev.status && ev.status.type && (ev.status.type.detail || ev.status.type.shortDetail)) || '',
    period: ev.status && ev.status.period,
    clock: ev.status && ev.status.displayClock,
    home: home ? normTeamSide(home) : null,
    away: away ? normTeamSide(away) : null,
    venue: comp.venue ? [comp.venue.fullName, comp.venue.address && comp.venue.address.city].filter(Boolean).join(' · ') : '',
  };
}
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
function datesWindow(n, startOffset = 0) {
  const out = [];
  const base = new Date();
  for (let i = 0; i < n; i++) {
    const d = new Date(base.getTime() + (i + startOffset) * 86400 * 1000);
    out.push(String(d.getUTCFullYear()) + String(d.getUTCMonth() + 1).padStart(2, '0') + String(d.getUTCDate()).padStart(2, '0'));
  }
  return out;
}

async function scoreboard(qs) {
  const p = new URLSearchParams(qs || '');
  if (p.get('dates')) {
    const r = await proxied('scoreboard', 'scoreboard:' + qs, ESPN + '/scoreboard?' + qs);
    if (r.status !== 200 || r.stale) return r;
    try { return { payload: JSON.stringify(shapeScoreboard(r.payload)), status: 200, stale: 0, cache: 'upstream' }; }
    catch (e) { return r; }
  }
  // multi-day window (yesterday + today + next 3 days); ESPN groups by US-Eastern date
  const days = datesWindow(5, -1);
  const seen = new Set(); let season = null; const events = [];
  for (const d of days) {
    const r = await proxied('scoreboard', 'scoreboard:' + d, ESPN + '/scoreboard?dates=' + d);
    if (r.status !== 200) continue;
    try {
      const data = JSON.parse(r.payload);
      if (!season) season = data.season;
      for (const ev of (data.events || [])) if (!seen.has(ev.id)) { seen.add(ev.id); events.push(normEvent(ev)); }
    } catch (e) {}
  }
  events.sort((a, b) => (a.date || '').localeCompare(b.date || ''));
  const out = {
    season, events,
    live: events.filter(e => e.status === 'live'),
    upcoming: events.filter(e => e.status === 'scheduled'),
    recent: events.filter(e => e.status === 'final'),
  };
  return { payload: JSON.stringify(out), status: 200, stale: 0, cache: 'upstream' };
}

async function standings() {
  const r = await proxied('standings', 'standings', ESPN_WEB + '/standings');
  if (r.status !== 200 || r.stale) return r;
  try {
    const data = JSON.parse(r.payload);
    const groups = (data.children || []).map(conf => ({
      name: conf.name, abbreviation: conf.abbreviation,
      entries: ((conf.standings && conf.standings.entries) || []).map(en => ({
        team: en.team ? { id: en.team.id, name: en.team.displayName, abbr: en.team.abbreviation, logo: en.team.logos && en.team.logos[0] && en.team.logos[0].href } : {},
        stats: (en.stats || []).reduce((a, s) => { a[s.name] = s.displayValue != null ? s.displayValue : s.value; return a; }, {}),
      })),
    }));
    return { payload: JSON.stringify({ season: data.season && data.season.displayName, groups }), status: 200, stale: 0, cache: 'upstream' };
  } catch (e) { return r; }
}

// ---- teams (30) with record merged from standings ----
async function teams() {
  const r = await proxied('teams', 'teams', ESPN + '/teams');
  if (r.status !== 200 || r.stale) return r;
  try {
    const data = JSON.parse(r.payload);
    const list = data.sports[0].leagues[0].teams.map(t => {
      const tm = t.team || {};
      return {
        id: tm.id, name: tm.displayName, abbr: tm.abbreviation,
        color: tm.color ? '#' + tm.color : '', altColor: tm.alternateColor ? '#' + tm.alternateColor : '',
        logo: (tm.logos && tm.logos[0] && tm.logos[0].href) || ('https://a.espncdn.com/i/teamlogos/nba/500/' + (tm.abbreviation || '').toLowerCase() + '.png'),
      };
    });
    // merge W/L from standings if cached
    const s = getStaging.get('standings', 'standings');
    if (s) {
      try {
        const sd = JSON.parse(s.payload);
        const rec = {};
        for (const g of (sd.children || [])) for (const en of (g.standings && g.standings.entries) || []) {
          if (en.team && en.team.abbreviation) rec[en.team.abbreviation] = en.stats || [];
        }
        for (const t of list) {
          const st = rec[t.abbr] || [];
          const byName = st.reduce((a, x) => { a[x.name] = x.displayValue != null ? x.displayValue : x.value; return a; }, {});
          t.wins = byName.wins; t.losses = byName.losses; t.winPercent = byName.winPercent;
        }
      } catch (e) {}
    }
    return { payload: JSON.stringify({ teams: list, count: list.length }), status: 200, stale: 0, cache: 'upstream' };
  } catch (e) { return r; }
}

// ---- team roster ----
async function roster(abbr) {
  const r = await proxied('roster', 'roster:' + abbr, ESPN + '/teams/' + abbr.toLowerCase() + '/roster');
  if (r.status !== 200 || r.stale) return r;
  try {
    const data = JSON.parse(r.payload);
    const athletes = (data.athletes || []).map(a => ({
      id: a.id, name: a.displayName || a.fullName, shortName: a.shortName,
      jersey: a.jersey, position: a.position && a.position.abbreviation,
      height: a.displayHeight, weight: a.displayWeight, age: a.age,
      headshot: a.headshot && a.headshot.href, college: a.college, experience: a.experience && a.experience.years,
    }));
    return { payload: JSON.stringify({ team: { name: data.team && data.team.displayName, abbr: data.team && data.team.abbreviation }, roster: athletes, count: athletes.length }), status: 200, stale: 0, cache: 'upstream' };
  } catch (e) { return r; }
}

// ---- league-wide player stats (322 players) -> powers Players + Leaders ----
const LEADER_CATS = { points: 'avgPoints', rebounds: 'avgRebounds', assists: 'avgAssists', steals: 'avgSteals', blocks: 'avgBlocks' };

async function playerStats() {
  const r = await proxied('stats', 'stats', ESPN_STATS);
  if (r.status !== 200 || r.stale) return r;
  try {
    const data = JSON.parse(r.payload);
    // flat ordered stat names from top-level categories
    const names = [];
    for (const cat of (data.categories || [])) for (const n of (cat.names || [])) names.push(n);
    const players = (data.athletes || []).map(a => {
      const ath = a.athlete || {};
      const vals = [];
      for (const cat of (a.categories || [])) for (const v of (cat.values || [])) vals.push(v);
      const stats = {};
      for (let i = 0; i < names.length; i++) stats[names[i]] = vals[i] != null ? vals[i] : null;
      return {
        id: ath.id, name: ath.displayName, shortName: ath.shortName,
        headshot: ath.headshot && ath.headshot.href,
        position: ath.position && ath.position.abbreviation,
        age: ath.age, team: ath.teamName, teamShortName: ath.teamShortName, teamId: ath.teamId,
        logo: ath.teamLogos && ath.teamLogos[0] && ath.teamLogos[0].href,
        stats,
      };
    });
    return { payload: JSON.stringify({ players, count: players.length }), status: 200, stale: 0, cache: 'upstream' };
  } catch (e) { return r; }
}

// ---- box score ----
async function gameSummary(eventId) {
  const r = await proxied('summary', 'summary:' + eventId, ESPN + '/summary?event=' + eventId);
  if (r.status !== 200 || r.stale) return r;
  try {
    const d = JSON.parse(r.payload);
    const box = d.boxscore || {};
    // team summary stats
    const teams = (box.teams || []).map(t => ({
      team: t.team ? { id: t.team.id, name: t.team.displayName, abbr: t.team.abbreviation, logo: t.team.logo } : {},
      stats: (t.statistics || []).reduce((a, s) => { a[s.label || s.name] = s.displayValue || s.value; return a; }, {}),
    }));
    // player lines live under boxscore.players[].statistics[0].athletes[]
    const players = (box.players || []).map(pe => {
      const s = (pe.statistics && pe.statistics[0]) || {};
      const names = s.names || s.labels || [];
      return {
        team: pe.team ? { id: pe.team.id, name: pe.team.displayName, abbr: pe.team.abbreviation, logo: pe.team.logo } : {},
        players: (s.athletes || []).map(a => {
          const stats = {};
          (a.stats || []).forEach((v, i) => { stats[names[i]] = v; });
          return {
            name: a.athlete && a.athlete.displayName, shortName: a.athlete && a.athlete.shortName,
            jersey: a.athlete && a.athlete.jersey, position: a.athlete && a.athlete.position && a.athlete.position.abbreviation,
            starter: !!a.starter, didNotPlay: !!a.didNotPlay, ejected: !!a.ejected,
            stats,
          };
        }),
      };
    });
    return { payload: JSON.stringify({ id: d.id, date: d.date, header: d.header, teams, players, winProbability: d.winprobability }), status: 200, stale: 0, cache: 'upstream' };
  } catch (e) { return r; }
}

// ---- background refresher: keep a warm local snapshot so the site serves offline ----
const WARM = [
  { source: 'scoreboard', key: 'scoreboard', fn: () => scoreboard(''), every: 3 * 60e3 },
  { source: 'standings', key: 'standings', fn: () => standings(), every: 6 * 3600e3 },
  { source: 'teams', key: 'teams', fn: () => teams(), every: 6 * 3600e3 },
  { source: 'stats', key: 'stats', fn: () => playerStats(), every: 6 * 3600e3 },
  { source: 'news', key: 'news', fn: async () => proxied('news', 'news', ESPN + '/news'), every: 30 * 60e3 },
];
const lastRun = new Map();
async function tick() {
  const now = Date.now();
  for (const w of WARM) {
    if (now - (lastRun.get(w.key) || 0) < w.every) continue;
    lastRun.set(w.key, now);
    try { await w.fn(); } catch (e) {}
    break; // one warm task per tick
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const rp = async (r) => send(res, r.status, r.payload, { 'X-Stale': String(r.stale), 'X-Cache': r.cache, ...(r.fetchedAt ? { 'X-Fetched-At': r.fetchedAt } : {}) });

  if (url.pathname === '/api/scoreboard') return rp(await scoreboard(url.searchParams.toString()));
  if (url.pathname === '/api/standings') return rp(await standings());
  if (url.pathname === '/api/teams') return rp(await teams());
  if (url.pathname === '/api/players') {
    // full player list (same snapshot as leaders)
    const r = await playerStats();
    if (r.status === 200 && !r.stale && url.searchParams.get('q')) {
      try {
        const q = url.searchParams.get('q').toLowerCase();
        const d = JSON.parse(r.payload);
        d.players = d.players.filter(p => (p.name || '').toLowerCase().includes(q));
        return rp({ ...r, payload: JSON.stringify(d) });
      } catch (e) {}
    }
    return rp(r);
  }
  if (url.pathname === '/api/leaders') {
    const cat = url.searchParams.get('cat') || 'points';
    const field = LEADER_CATS[cat] || 'avgPoints';
    const limit = Math.min(100, +(url.searchParams.get('limit') || 25));
    const r = await playerStats();
    if (r.status !== 200 || r.stale) return rp(r);
    try {
      const d = JSON.parse(r.payload);
      const sorted = d.players.slice().sort((a, b) => ((b.stats && b.stats[field]) || 0) - ((a.stats && a.stats[field]) || 0));
      return send(res, 200, JSON.stringify({ category: cat, field, leaders: sorted.slice(0, limit) }), { 'X-Stale': String(r.stale), 'X-Cache': r.cache });
    } catch (e) { return rp(r); }
  }
  if (url.pathname.startsWith('/api/team/')) {
    const abbr = url.pathname.slice('/api/team/'.length).replace(/[^a-zA-Z]/g, '');
    if (!abbr) return send(res, 400, JSON.stringify({ error: 'team required' }));
    return rp(await roster(abbr));
  }
  if (url.pathname.startsWith('/api/game/')) {
    const id = url.pathname.slice('/api/game/'.length);
    if (!/^\d+$/.test(id)) return send(res, 400, JSON.stringify({ error: 'event id required' }));
    return rp(await gameSummary(id));
  }
  if (url.pathname === '/api/news') return rp(await proxied('news', 'news', ESPN + '/news'));
  if (url.pathname === '/api/odds') {
    const markets = url.searchParams.get('markets') || 'h2h,spreads,totals';
    return rp(await proxied('odds', 'odds:' + markets, `https://api.the-odds-api.com/v4/sports/basketball_nba/odds/?regions=us&markets=${encodeURIComponent(markets)}&oddsFormat=decimal&apiKey=${ODDS_KEY}`));
  }
  if (url.pathname === '/api/health') {
    return send(res, 200, JSON.stringify({ ok: true, staging_rows: stagingCount(), staging: stagingRows() }));
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
  setInterval(tick, 60 * 1000);
  setTimeout(tick, 1500);
  console.log(`NBA Hub backend on http://localhost:${PORT}  (staging: ${DB_PATH}, rows=${stagingCount()})`);
});
