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
const ODDS_URL = `https://api.the-odds-api.com/v4/sports/basketball_nba/odds/?regions=us&markets=h2h,spreads,totals&oddsFormat=decimal&apiKey=${ODDS_KEY}`;
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36';
// TeamRankings odds (free scrape, no quota) — fallback when The Odds API is exhausted/empty
const TR_ODDS = 'https://www.teamrankings.com/nba/odds/';
const TR_AJAX = 'https://www.teamrankings.com/ajax/league/v3/odds_controller.php';
const TR_TTL = 60 * 60 * 1000; // 1h — free source, next-day lines move slowly

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
const DATA_DIR = path.join(ROOT, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const DB_PATH = path.join(DATA_DIR, 'nba_staging.db');
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

// ---- results archive (pre-map upcoming + snapshot odds BEFORE final, then merge results) ----
db.exec(`CREATE TABLE IF NOT EXISTS nba_results (
  id TEXT PRIMARY KEY, date TEXT, home_abbr TEXT, away_abbr TEXT, winner_abbr TEXT,
  payload TEXT NOT NULL, archived_at TEXT NOT NULL)`);
const upsertResult = db.prepare(`INSERT INTO nba_results (id,date,home_abbr,away_abbr,winner_abbr,payload,archived_at)
  VALUES (?,?,?,?,?,?,?)
  ON CONFLICT(id) DO UPDATE SET
    date=excluded.date, home_abbr=excluded.home_abbr, away_abbr=excluded.away_abbr,
    winner_abbr=excluded.winner_abbr, payload=excluded.payload, archived_at=excluded.archived_at`);
const getResult = db.prepare(`SELECT payload FROM nba_results WHERE id=?`);
const listResults = db.prepare(`SELECT payload FROM nba_results ORDER BY date DESC, id DESC`);
const resultCount = () => db.prepare(`SELECT count(*) n FROM nba_results`).get().n;

// team-name normalisation (mirrors the frontend normName) for odds matching
function normName(n) { return (n || '').replace(/^Los Angeles\b/, 'LA').trim(); }

// odds snapshot: match one event's home/away to The Odds API, return the
// spread (home/away point+price) and total, or null when no line exists.
function snapshotOdds(ev, oddsArr) {
  const h = ev.home, a = ev.away;
  if (!h || !a || !h.name || !a.name || !oddsArr || !oddsArr.length) return null;
  const g = oddsArr.find(x =>
    (normName(x.home_team) === normName(h.name) && normName(x.away_team) === normName(a.name)) ||
    (normName(x.home_team) === normName(a.name) && normName(x.away_team) === normName(h.name)));
  if (!g) return null;
  const bk = (g.bookmakers || [])[0] || {};
  const sp = ((bk.markets || []).find(x => x.key === 'spreads') || {}).outcomes || [];
  const to = ((bk.markets || []).find(x => x.key === 'totals') || {}).outcomes || [];
  const hh = sp.find(o => normName(o.name) === normName(h.name)) || null;
  const aa = sp.find(o => normName(o.name) === normName(a.name)) || null;
  const ov = to.find(o => o.name === 'Over') || null;
  const total = ov && ov.point != null ? ov.point : null;
  if (!hh && !aa && total == null) return null;
  return {
    h: hh && hh.point != null ? { point: hh.point, price: hh.price } : null,
    a: aa && aa.point != null ? { point: aa.point, price: aa.price } : null,
    total,
  };
}

// Pre-map EVERY event (scheduled/live/final) into nba_results with an odds
// snapshot. When a game goes final, the result merges in via UPSERT while the
// pre-snapshotted odds are preserved (bookmakers pull the line post-game).
function snapshotResults(events, oddsArr) {
  let written = 0;
  const now = new Date().toISOString();
  for (const ev of events || []) {
    const h = ev.home || {}, a = ev.away || {};
    const w = h.winner ? 'home' : (a.winner ? 'away' : '');
    const wAbbr = w === 'home' ? (h.abbr || '') : (w === 'away' ? (a.abbr || '') : '');
    let odds = snapshotOdds(ev, oddsArr);
    if ((!odds || odds.total == null) && ev.status === 'final') {
      const prior = getResult.get(ev.id);
      if (prior) { try { odds = JSON.parse(prior.payload).odds || null; } catch (e) {} }
    }
    const payload = JSON.stringify({ ...ev, odds });
    const r = upsertResult.run(ev.id, ev.date || '', h.abbr || '', a.abbr || '', wAbbr, payload, now);
    if (r.changes > 0) written++;
  }
  return written;
}

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

// ---- TeamRankings odds (free scrape fallback for The Odds API) ----
// slug -> display name (e.g. "los-angeles-clippers" -> "Los Angeles Clippers");
// normName() on both sides later reconciles Clippers/Lakers with ESPN's "LA Clippers".
function teamNameFromSlug(slug) {
  return (slug || '').split('-').map(w => w ? w[0].toUpperCase() + w.slice(1) : w).join(' ').trim();
}

// Parse the TeamRankings AJAX HTML (tables of [away row][home row][footer]) into the
// SAME shape The Odds API returns, so the frontend spreadFor()/snapshotOdds() read it unchanged.
function parseTeamRankings(html) {
  const games = [];
  const tables = html.match(/<table[^>]*tr-table[^>]*>[\s\S]*?<\/table>/g) || [];
  const sp = n => { const x = parseFloat(n); return Number.isFinite(x) ? x : null; };
  const ml = n => { const x = parseInt(n, 10); return Number.isFinite(x) ? x : null; };
  for (const tbl of tables) {
    const tb = tbl.match(/<tbody>([\s\S]*?)<\/tbody>/);
    if (!tb) continue;
    const rows = (tb[1].match(/<tr[^>]*>[\s\S]*?<\/tr>/g) || []).filter(r => !/score-footer/.test(r));
    if (rows.length < 2) continue;
    const teams = [];
    for (const r of rows) {
      const slug = (r.match(/href="\/nba\/team\/([^"]+)"/) || [])[1] || '';
      const tds = r.match(/<td[^>]*>[\s\S]*?<\/td>/g) || [];
      const cell = i => tds[i] ? tds[i].replace(/<[^>]+>/g, '').trim() : '';
      // td order: [0]=team name, [1]=empty, [2]=spread, [3]=total, [4]=money line
      teams.push({ name: teamNameFromSlug(slug), spread: cell(2), total: cell(3), moneyline: cell(4) });
    }
    const away = teams[0], home = teams[1];
    const total = sp(away.total || home.total);
    const awayPt = sp(away.spread), homePt = sp(home.spread);
    if (awayPt == null && homePt == null && total == null) continue;
    const markets = [];
    const h2h = [];
    const aml = ml(away.moneyline), hml = ml(home.moneyline);
    if (aml != null) h2h.push({ name: away.name, price: aml });
    if (hml != null) h2h.push({ name: home.name, price: hml });
    if (h2h.length) markets.push({ key: 'h2h', outcomes: h2h });
    const spreads = [];
    if (awayPt != null) spreads.push({ name: away.name, point: awayPt });
    if (homePt != null) spreads.push({ name: home.name, point: homePt });
    if (spreads.length) markets.push({ key: 'spreads', outcomes: spreads });
    if (total != null) markets.push({ key: 'totals', outcomes: [{ name: 'Over', point: total }, { name: 'Under', point: total }] });
    if (!markets.length) continue;
    games.push({ home_team: home.name, away_team: away.name, bookmakers: [{ key: 'teamrankings', markets }] });
  }
  return games;
}

let _seasonId = null;
async function getSeasonId() {
  if (_seasonId) return _seasonId;
  const r = await fetchUpstream(TR_ODDS);
  if (r.ok) {
    const m = r.text.match(/name="season_id"\s+value="(\d+)"/i);
    if (m) _seasonId = m[1];
  }
  return _seasonId || '224';
}

async function teamRankingsOdds() {
  const cacheKey = 'troods:odds';
  const hit = memGet(cacheKey, TR_TTL);
  if (hit) return { payload: hit.payload, status: 200, stale: 0, cache: 'memory' };
  if (inflight.has(cacheKey)) return inflight.get(cacheKey);
  const p = (async () => {
    let up = { ok: false, status: 0, text: '' };
    try {
      const seasonId = await getSeasonId();
      const body = `league=nba&view_type=odds&view=latest&season_id=${seasonId}&div=0&team=0`;
      const ctl = new AbortController();
      const t = setTimeout(() => ctl.abort(), 20000);
      const resp = await fetch(TR_AJAX, {
        method: 'POST',
        headers: { 'User-Agent': UA, 'Content-Type': 'application/x-www-form-urlencoded' },
        body, signal: ctl.signal,
      });
      const text = await resp.text();
      clearTimeout(t);
      up = { ok: resp.ok, status: resp.status, text };
    } catch (e) { up = { ok: false, status: 0, text: '', error: String(e && e.message || e) }; }

    if (up.ok) {
      const payload = JSON.stringify(parseTeamRankings(up.text));
      upsertStaging.run('troods', 'odds', payload, up.status, new Date().toISOString());
      memSet(cacheKey, payload, up.status);
      return { payload, status: 200, stale: 0, cache: 'upstream' };
    }
    const row = getStaging.get('troods', 'odds');
    if (row) return { payload: row.payload, status: 200, stale: 1, cache: 'staging', fetchedAt: row.fetched_at };
    return { payload: '[]', status: 502, stale: 0, cache: 'none' };
  })().finally(() => inflight.delete(cacheKey));
  inflight.set(cacheKey, p);
  return p;
}

// The Odds API and TeamRankings merged. When The Odds API is FRESH (live) it wins;
// when it is stale/empty (e.g. monthly quota exhausted -> staging fallback) TeamRankings
// is fresher and wins instead. Either way the other source fills any missing games.
async function mergedOdds() {
  let oddsArr = [], oddsFresh = false;
  try {
    const o = await proxied('odds', 'odds:h2h,spreads,totals', ODDS_URL);
    if (o.status === 200) { oddsArr.push(...JSON.parse(o.payload)); oddsFresh = !o.stale; }
  } catch (e) {}
  let trArr = [];
  try {
    const t = await teamRankingsOdds();
    if (t.status === 200) trArr.push(...JSON.parse(t.payload));
  } catch (e) {}
  const key = g => [normName(g.home_team), normName(g.away_team)].sort().join('|');
  const primary = oddsFresh ? oddsArr : trArr;
  const secondary = oddsFresh ? trArr : oddsArr;
  const seen = new Set(primary.map(key));
  const merged = primary.slice();
  for (const g of secondary) {
    if (seen.has(key(g))) continue;
    seen.add(key(g));
    merged.push(g);
  }
  return merged;
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
  // pre-map upcoming + snapshot odds BEFORE final, merge results after
  let oddsArr = null;
  try { oddsArr = await mergedOdds(); } catch (e) {}
  snapshotResults(events, oddsArr);
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
          t.stats = byName; // full standings stats (PPG, OPP PPG, DIFF, HOME, ROAD, CONF, streak, last10, …)
        }
      } catch (e) {}
    }
    return { payload: JSON.stringify({ teams: list, count: list.length }), status: 200, stale: 0, cache: 'upstream' };
  } catch (e) { return r; }
}

// ---- box-score fallback: aggregate a team's per-player season stats from played games ----
// (covers teams/players absent from the byathlete season-stats feed — e.g. right after
// opening night, when ESPN's per-player stats feed lags the live scoreboard)
async function teamBoxStats(abbr) {
  const sb = await scoreboard('');
  let events = [];
  try { const d = JSON.parse(sb.payload); events = (d.recent || []).concat(d.live || []); } catch (e) {}
  const games = events.filter(g => g.home && g.away && (g.home.abbr === abbr || g.away.abbr === abbr));
  if (!games.length) return {};
  const agg = {};
  const n = v => { const x = Number(v); return Number.isFinite(x) ? x : 0; };
  const split = s => { const p = String(s == null ? '' : s).split('-'); return [n(p[0]), n(p[1])]; };
  for (const g of games) {
    const r = await gameSummary(g.id);
    if (r.status !== 200) continue;
    let d; try { d = JSON.parse(r.payload); } catch (e) { continue; }
    for (const pe of (d.players || [])) for (const p of (pe.players || [])) {
      if (p.didNotPlay) continue;
      const key = p.name || p.shortName;
      if (!key) continue;
      const st = p.stats || {};
      const fg = split(st.FG), tp = split(st['3PT']), ft = split(st.FT);
      const a = agg[key] || (agg[key] = { gp: 0, min: 0, pts: 0, reb: 0, ast: 0, fgm: 0, fga: 0, tpm: 0, tpa: 0, ftm: 0, fta: 0 });
      a.gp += 1; a.min += n(st.MIN); a.pts += n(st.PTS); a.reb += n(st.REB); a.ast += n(st.AST);
      a.fgm += fg[0]; a.fga += fg[1]; a.tpm += tp[0]; a.tpa += tp[1]; a.ftm += ft[0]; a.fta += ft[1];
    }
  }
  const out = {};
  for (const [key, a] of Object.entries(agg)) {
    if (!a.gp) continue;
    const tsDen = 2 * (a.fga + 0.44 * a.fta);
    out[key] = {
      gamesPlayed: a.gp,
      avgMinutes: +(a.min / a.gp).toFixed(1),
      avgPoints: +(a.pts / a.gp).toFixed(1),
      avgRebounds: +(a.reb / a.gp).toFixed(1),
      avgAssists: +(a.ast / a.gp).toFixed(1),
      fieldGoalPct: a.fga ? +(100 * a.fgm / a.fga).toFixed(1) : null,
      threePointFieldGoalPct: a.tpa ? +(100 * a.tpm / a.tpa).toFixed(1) : null,
      freeThrowPct: a.fta ? +(100 * a.ftm / a.fta).toFixed(1) : null,
      tsPct: tsDen > 0 ? +(100 * a.pts / tsDen).toFixed(1) : null,
    };
  }
  return out;
}

// ---- team detail: roster + coach + full standings stats + team leaders + per-player stats ----
async function teamDetail(abbr) {
  const r = await proxied('roster', 'roster:' + abbr, ESPN + '/teams/' + abbr.toLowerCase() + '/roster');
  if (r.status !== 200 || r.stale) return r;
  try {
    const data = JSON.parse(r.payload);
    const team = data.team || {};
    const coach = (data.coach && data.coach[0]) || {};

    // full standings stats for this team (PPG / OPP PPG / DIFF / HOME / ROAD / CONF / streak / last10 …)
    let st = {};
    const sRow = getStaging.get('standings', 'standings');
    if (sRow) {
      try {
        const sd = JSON.parse(sRow.payload);
        for (const g of (sd.children || [])) for (const en of (g.standings && g.standings.entries) || []) {
          if (en.team && en.team.abbreviation === team.abbreviation) {
            st = (en.stats || []).reduce((a, x) => { a[x.name] = x.displayValue != null ? x.displayValue : x.value; return a; }, {});
          }
        }
      } catch (e) {}
    }

    // per-player season stats from the byathlete snapshot, joined by athlete id
    const psRow = getStaging.get('stats', 'stats');
    let players = [];
    if (psRow) { try { players = parsePlayerStats(psRow.payload); } catch (e) {} }
    const pmap = {};
    for (const p of players) pmap[p.id] = p;

    const athletes = (data.athletes || []).map(a => {
      const ps = pmap[a.id] || {};
      const s2 = ps.stats || {};
      return {
        id: a.id, name: a.displayName || a.fullName, shortName: a.shortName,
        jersey: a.jersey, position: a.position && a.position.abbreviation,
        height: a.displayHeight, weight: a.displayWeight, age: a.age,
        exp: a.experience && a.experience.years, headshot: a.headshot && a.headshot.href, college: a.college,
        stats: {
          gp: s2.gamesPlayed, min: s2.avgMinutes, pts: s2.avgPoints, reb: s2.avgRebounds,
          ast: s2.avgAssists, fgPct: s2.fieldGoalPct, threePct: s2.threePointFieldGoalPct,
          tsPct: s2.tsPct, efgPct: s2.efgPct,
        },
      };
    });

    // box-score fallback for players absent from the season-stats feed (fresh/lagged games)
    if (athletes.some(a => a.stats.gp == null)) {
      let box = null;
      try { box = await teamBoxStats(team.abbreviation); } catch (e) {}
      if (box) for (const a of athletes) {
        if (a.stats.gp != null) continue;
        const bs = box[a.name] || box[a.shortName];
        if (bs) a.stats = {
          gp: bs.gamesPlayed, min: bs.avgMinutes, pts: bs.avgPoints, reb: bs.avgRebounds,
          ast: bs.avgAssists, fgPct: bs.fieldGoalPct, threePct: bs.threePointFieldGoalPct,
          tsPct: bs.tsPct, efgPct: null,
        };
      }
    }

    // team leaders (formulated from merged roster stats — byathlete + box-score fallback)
    const top = f => athletes.reduce((best, a) => {
      const v = num(a.stats && a.stats[f]);
      return (v > 0 && (!best || v > best.v)) ? { v, name: a.name, headshot: a.headshot } : best;
    }, null);
    const lPts = top('pts'), lReb = top('reb'), lAst = top('ast');
    const leaders = {
      points: lPts ? { name: lPts.name, headshot: lPts.headshot, val: lPts.v } : null,
      rebounds: lReb ? { name: lReb.name, headshot: lReb.headshot, val: lReb.v } : null,
      assists: lAst ? { name: lAst.name, headshot: lAst.headshot, val: lAst.v } : null,
    };

    return { payload: JSON.stringify({
      team: { id: team.id, name: team.displayName, abbr: team.abbreviation, logo: team.logo, color: team.color ? '#' + team.color : '' },
      season: team.seasonSummary || '', record: team.recordSummary || '',
      coach: coach.firstName && coach.lastName ? coach.firstName + ' ' + coach.lastName : '',
      standings: st,
      leaders,
      roster: athletes, count: athletes.length,
    }), status: 200, stale: 0, cache: 'upstream' };
  } catch (e) { return r; }
}

// ---- league-wide player stats (322 players) -> powers Players + Leaders ----
const LEADER_CATS = { points: 'avgPoints', rebounds: 'avgRebounds', assists: 'avgAssists', steals: 'avgSteals', blocks: 'avgBlocks' };

const num = v => { const n = Number(v); return Number.isFinite(n) ? n : 0; };

// shared: parse raw ESPN byathlete JSON -> normalized players[] (adds FORMULATED columns)
function parsePlayerStats(text) {
  const data = JSON.parse(text);
  const names = [];
  for (const cat of (data.categories || [])) for (const n of (cat.names || [])) names.push(n);
  return (data.athletes || []).map(a => {
    const ath = a.athlete || {};
    const vals = [];
    for (const cat of (a.categories || [])) for (const v of (cat.values || [])) vals.push(v);
    const stats = {};
    for (let i = 0; i < names.length; i++) stats[names[i]] = vals[i] != null ? vals[i] : null;
    // ---- formulated columns (derived, not in ESPN feed) ----
    // TS% = PTS / (2 * (FGA + 0.44 * FTA)) * 100
    const pts = num(stats.points), fga = num(stats.fieldGoalsAttempted), fta = num(stats.freeThrowsAttempted);
    const tsDen = 2 * (fga + 0.44 * fta);
    stats.tsPct = tsDen > 0 ? +(100 * pts / tsDen).toFixed(1) : null;
    // eFG% = (FGM + 0.5 * 3PM) / FGA * 100
    const fgm = num(stats.fieldGoalsMade), tpm = num(stats.threePointFieldGoalsMade);
    stats.efgPct = fga > 0 ? +(100 * (fgm + 0.5 * tpm) / fga).toFixed(1) : null;
    return {
      id: ath.id, name: ath.displayName, shortName: ath.shortName,
      headshot: ath.headshot && ath.headshot.href,
      position: ath.position && ath.position.abbreviation,
      age: ath.age, team: ath.teamName, teamShortName: ath.teamShortName, teamId: ath.teamId,
      logo: ath.teamLogos && ath.teamLogos[0] && ath.teamLogos[0].href,
      stats,
    };
  });
}

async function playerStats() {
  const r = await proxied('stats', 'stats', ESPN_STATS);
  if (r.status !== 200 || r.stale) return r;
  try {
    const players = parsePlayerStats(r.payload);
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

// ---- Hidden Markov Regression team pillars ----
// 5 formulated team metrics (STAMINA / SHARPNESS / EFFICIENCY / SPEED / TEMPO GRIP) + RANK,
// computed from per-game team aggregates and persisted into staging (source='pillars').
// Sort key per pillar maps to the ranking stat used for "top-4 sorted rows".
const PILLAR_TTL = 6 * 3600 * 1000;
const PILLAR_SORT = { stamina: 'reb', sharpness: 'ast', efficiency: 'pts', speed: 'tsPct' };
const PILLAR_LABEL = { stamina: 'Stamina', sharpness: 'Sharpness', efficiency: 'Efficiency', speed: 'Speed', tempoGrip: 'Tempo Grip' };

async function computePillars() {
  const tRes = await teams();
  let teamsList = [];
  try { teamsList = JSON.parse(tRes.payload).teams || []; } catch (e) {}
  if (!teamsList.length) return null;

  // fetch all team rosters (merged byathlete + box-score fallback) in parallel
  const details = await Promise.all(teamsList.map(async (t) => {
    const r = await teamDetail(t.abbr);
    try { return { t, d: JSON.parse(r.payload) }; } catch (e) { return { t, d: null }; }
  }));

  const rows = [];
  for (const { t, d } of details) {
    if (!d) continue;
    const roster = (d.roster || []).filter(a => a.stats && num(a.stats.gp) > 0);
    if (!roster.length) continue;

    const sum = f => roster.reduce((s, a) => s + num(a.stats && a.stats[f]), 0);
    const pts = sum('pts'), reb = sum('reb'), ast = sum('ast'), min = sum('min');
    // minutes-weighted averages (shooting %s + age — weighted by minutes played)
    const minW = f => {
      let w = 0, tot = 0;
      for (const a of roster) { const m = num(a.stats && a.stats.min); const v = num(a.stats && a.stats[f]); w += m; tot += m * v; }
      return w > 0 ? tot / w : null;
    };
    const fgPct = minW('fgPct'), threePct = minW('threePct'), tsPct = minW('tsPct');
    let age = null;
    { let w = 0, s = 0; for (const a of roster) { const m = num(a.stats && a.stats.min); w += m; s += m * num(a.age); } age = w > 0 ? s / w : null; }

    if (!age || !min) continue;

    // ---- 5 pillars (formulas as specified) ----
    const stamina = (fgPct != null && age > 0 && min > 0)
      ? (fgPct * pts / age) * ((ast + 0.23) * 100 / min) : null;
    const sharpness = (tsPct != null && fgPct > 0 && threePct > 0)
      ? (tsPct * reb) / (threePct / fgPct) * (ast + 0.213) : null;
    const efficiency = (age > 0 && min > 0 && tsPct != null)
      ? ((ast * pts / age) / min) * tsPct : null;
    const speed = (fgPct > 0 && age > 0)
      ? (reb * min * (pts / fgPct)) / age : null;
    const tempoGrip = (stamina != null && sharpness != null && efficiency && speed)
      ? (stamina * sharpness) / (efficiency * speed) : null;

    rows.push({
      abbr: t.abbr, name: t.name, logo: t.logo, color: t.color,
      pts: +pts.toFixed(1), reb: +reb.toFixed(1), ast: +ast.toFixed(1), min: +min.toFixed(1),
      age: age != null ? +age.toFixed(1) : null,
      fgPct: fgPct != null ? +fgPct.toFixed(1) : null,
      threePct: threePct != null ? +threePct.toFixed(1) : null,
      tsPct: tsPct != null ? +tsPct.toFixed(1) : null,
      stamina, sharpness, efficiency, speed, tempoGrip,
    });
  }

  // ---- normalize each pillar so its top-4 (by sort key) sum equals the target ----
  const PILLAR_TARGET = { stamina: 50, sharpness: 7, efficiency: 5, speed: 1.35, tempoGrip: 10 };
  const PILLAR_SORT_KEY = { stamina: 'reb', sharpness: 'ast', efficiency: 'pts', speed: 'tsPct', tempoGrip: 'tempoGrip' };
  const normalization = {};
  for (const pillar of Object.keys(PILLAR_SORT_KEY)) {
    const target = PILLAR_TARGET[pillar];
    if (!target) continue; // pillar without a target (e.g. efficiency) stays raw
    const key = PILLAR_SORT_KEY[pillar];
    const top4 = [...rows].sort((a, b) => (b[key] || 0) - (a[key] || 0)).slice(0, 4);
    const sum = top4.reduce((s, x) => s + (x[pillar] != null ? x[pillar] : 0), 0);
    const factor = sum > 0 ? target / sum : null;
    if (factor == null) continue;
    for (const x of rows) if (x[pillar] != null) x[pillar] = x[pillar] * factor;
    normalization[pillar] = { target, sortKey: key, top4Sum: +sum.toFixed(3), factor };
  }

  // cap: any pillar > 5 -> 5 + value/10 (guaranteed < 10; applies to all teams = home & away)
  for (const x of rows) for (const k of ['stamina', 'sharpness', 'efficiency', 'speed', 'tempoGrip'])
    if (x[k] != null && x[k] > 5) x[k] = 5 + x[k] / 10;

  // round pillar values for display
  for (const x of rows) for (const k of ['stamina', 'sharpness', 'efficiency', 'speed', 'tempoGrip'])
    if (x[k] != null && Number.isFinite(x[k])) x[k] = +x[k].toFixed(3);

  // top-4 per pillar (sorted by that pillar's sort key), then assign pillars to teams
  const top4 = {};
  for (const [pillar, key] of Object.entries(PILLAR_SORT)) {
    const sorted = [...rows].sort((a, b) => (b[key] || 0) - (a[key] || 0));
    top4[pillar] = sorted.slice(0, 4).map(x => x.abbr);
  }
  // overall rank by TEMPO GRIP (desc)
  [...rows].sort((a, b) => (b.tempoGrip || -Infinity) - (a.tempoGrip || -Infinity)).forEach((x, i) => { x.rank = i + 1; });
  for (const x of rows) x.top4 = Object.keys(top4).filter(p => top4[p].includes(x.abbr));

  const result = { generated: new Date().toISOString(), top4, normalization, teams: rows };
  upsertStaging.run('pillars', 'pillars', JSON.stringify(result), 200, new Date().toISOString());
  return result;
}

async function pillars() {
  const key = 'pillars';
  const hit = memGet(key, PILLAR_TTL);
  if (hit) return { payload: hit.payload, status: 200, stale: 0, cache: 'memory' };
  const staged = getStaging.get('pillars', 'pillars');
  if (staged && (Date.now() - new Date(staged.fetched_at).getTime()) < PILLAR_TTL) {
    return { payload: staged.payload, status: 200, stale: 0, cache: 'staging', fetchedAt: staged.fetched_at };
  }
  const result = await computePillars();
  if (!result) {
    if (staged) return { payload: staged.payload, status: 200, stale: 1, cache: 'staging', fetchedAt: staged.fetched_at };
    return { payload: JSON.stringify({ error: 'pillars_unavailable' }), status: 502, stale: 0, cache: 'none' };
  }
  const payload = JSON.stringify(result);
  memSet(key, payload, 200);
  return { payload, status: 200, stale: 0, cache: 'computed' };
}

async function results() {
  const rows = listResults.all();
  const out = [];
  for (const r of rows) { try { out.push(JSON.parse(r.payload)); } catch (e) {} }
  return { payload: JSON.stringify({ results: out, count: out.length }), status: 200, stale: 0, cache: 'db' };
}

// ---- background refresher: keep a warm local snapshot so the site serves offline ----
const WARM = [
  { source: 'scoreboard', key: 'scoreboard', fn: () => scoreboard(''), every: 3 * 60e3 },
  { source: 'standings', key: 'standings', fn: () => standings(), every: 6 * 3600e3 },
  { source: 'teams', key: 'teams', fn: () => teams(), every: 6 * 3600e3 },
  { source: 'stats', key: 'stats', fn: () => playerStats(), every: 6 * 3600e3 },
  { source: 'pillars', key: 'pillars', fn: () => pillars(), every: 6 * 3600e3 },
  { source: 'troods', key: 'troods:odds', fn: () => teamRankingsOdds(), every: 60 * 60e3 },
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
  if (url.pathname === '/api/pillars') return rp(await pillars());
  if (url.pathname === '/api/results') return rp(await results());
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
    return rp(await teamDetail(abbr));
  }
  if (url.pathname.startsWith('/api/game/')) {
    const id = url.pathname.slice('/api/game/'.length);
    if (!/^\d+$/.test(id)) return send(res, 400, JSON.stringify({ error: 'event id required' }));
    return rp(await gameSummary(id));
  }
  if (url.pathname === '/api/news') return rp(await proxied('news', 'news', ESPN + '/news'));
  if (url.pathname === '/api/odds') {
    const merged = await mergedOdds();
    return send(res, 200, JSON.stringify(merged));
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
