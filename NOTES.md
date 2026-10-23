# NBA Command Hub — Architecture

Single-file dashboard (`nba.html`) + zero-dependency Node backend (`server.js` on **:8092`),
mirroring the C:\UFC / C:\SNOOKER design: dark theme (orange accent), tabbed nav
(Dashboard / Standings / News / Odds / Admin), staging fallback (UI never blanks on
quota/outage), team-logos-with-initials-fallback.

## Data sources (all free, no key)
- **ESPN scoreboard** — `site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard`
  (supports `?dates=YYYYMMDD`). The `/api/scoreboard` endpoint fetches a **5-day window
  (yesterday → +3 days)** and merges, so both last night's finals and the next few days of
  fixtures populate the dashboard. ESPN groups scores by **US-Eastern date**, so "yesterday"
  (UTC −1 offset) is included to catch overnight finals.
- **ESPN standings** — `site.web.api.espn.com/apis/v2/sports/basketball/nba/standings`
  (East + West children, 15 teams each). Stats keys: wins/losses/winPercent/gamesBehind/
  streak/pointsFor/pointsAgainst/pointDifferential (no home/away record keys on this endpoint).
- **ESPN news** — `site.api.espn.com/apis/site/v2/sports/basketball/nba/news`
  (`articles[].links.href` — NOT `links.web.href`).
- **The Odds API NBA** — `api.the-odds-api.com/v4/sports/basketball_nba/odds/` (h2h+spreads+
  totals). Reuses the shared `ODDS_KEY` also used by C:\UFC. **Free tier is 500 req/month
  shared across modules**, so `/api/odds` TTL is 30 min (not 60s).

## Files
- `nba.html` — single-file app, auto-redirects `file://` → `http://localhost:8092/nba.html`.
- `server.js` — Node proxy: in-memory TTL cache + SQLite staging (`nba_staging.db`) +
  single-flight. Endpoints: `/api/scoreboard`, `/api/standings`, `/api/news`, `/api/odds`,
  `/api/health`. Scoreboard + standings are normalised server-side into a clean client shape.
- `Dockerfile` — node:22-slim, `COPY server.js nba.html nba_staging.db ./`, `EXPOSE 8092`.

## Pitfalls
- **ESPN date grouping is US-Eastern.** `?dates=` takes a single `YYYYMMDD`, no range — hence
  the multi-request window merge in `scoreboard()`. Don't try `?dates=A-B`.
- **`node:sqlite` needs Node ≥22** (`DatabaseSync`) — matches the `node:22-slim` runtime.
- **Odds key is shared with UFC** — keep the TTL long; don't lower it below ~15 min.
- **Team logos** live at `team.logos[0].href` (scoreboard) and `entries[].team.logos[0].href`
  (standings) — render with `onerror` → initials fallback.

## Verify
```
node server.js                       # :8092
curl http://localhost:8092/api/health
curl http://localhost:8092/api/scoreboard
curl http://localhost:8092/api/standings
curl http://localhost:8092/api/news
curl http://localhost:8092/api/odds
```
