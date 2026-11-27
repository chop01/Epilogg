<p align="center"><img src="static/icon.svg" width="96" alt=""></p>

# Epilogg

**A self-hosted log of the games you finished.** For each game you keep the title, platform, completion
date, a 0–10 rating and notes. Covers, official names and release years come from IGDB.

Epilogg can also look through your **Steam, Xbox, PlayStation, Epic, GOG, RetroAchievements and Nintendo Switch** accounts and suggest games you have
probably finished. It reads the achievements and trophies with their dates (story-ending achievement,
platinum, 100%) and shows the suggestions in a list. You confirm or dismiss each one: nothing goes into
your log automatically.

- Grid of covers with filters (year, platform, rating) and sorting
- Hours played and achievements for every game, plus a platinum badge when everything is unlocked
- Tabs: **Completed** (your log), **Not completed** (platform games you dismissed), **Platforms** (games to review),
  **Stats**
- **Stats** (year in review): one year or all time, with:
  - games finished per month;
  - platforms and the rating distribution;
  - your top-rated games;
  - highlights (longest, quickest, first and last finished, oldest and newest release)
- Multi-user: each user has their own log and platform accounts, and an admin creates the users
- English and Italian interface
- Read-only API token for dashboards (e.g. a [Homepage](https://gethomepage.dev) widget)
- Works on phones
- One small container: FastAPI, SQLite, no external database

## Quick start

The image is `ghcr.io/chop01/epilogg`, built for amd64 and arm64.

```bash
mkdir epilogg && cd epilogg
curl -O https://raw.githubusercontent.com/chop01/Epilogg/main/docker-compose.yml
curl -o .env https://raw.githubusercontent.com/chop01/Epilogg/main/.env.example
# edit .env (IGDB keys at least), then:
docker compose up -d
```

Open `http://<host>:8000`. On the first visit Epilogg asks you to create the **admin account**. From
⚙ Settings the admin can then add more users.

Epilogg has no TLS of its own: put it behind a reverse proxy (Traefik, Caddy, nginx…) if you reach it
from outside your LAN. The session cookie is marked `Secure` whenever the proxy forwards HTTPS.

## Configuration

| Variable | Required | What it is |
|---|---|---|
| `IGDB_CLIENT_ID`, `IGDB_CLIENT_SECRET` | for covers | Twitch app credentials: create an app at [dev.twitch.tv/console](https://dev.twitch.tv/console) (category "Application Integration", any OAuth redirect such as `http://localhost`). |
| `STEAM_API_KEY` | for Steam | One key for every user, from [steamcommunity.com/dev/apikey](https://steamcommunity.com/dev/apikey). Users can also enter their own key. |
| `TZ` | no | Time zone, e.g. `Europe/Zurich`. |
| `STEAM_ID`, `XBL_API_KEY`, `PSN_NPSSO` | no | Only read on the first start, when they are copied to the admin's account. |

### Platform accounts

Each user fills in their own accounts in ⚙ Settings → Platform accounts. Saved values are never shown
again.

| Platform | What to enter |
|---|---|
| Steam | SteamID64, custom profile name or profile URL. The profile and "Game details" must be **public**. |
| Xbox / PC Game Pass | An [OpenXBL](https://xbl.io) API key: sign in with your Microsoft account, then "API Keys". The free plan allows 150 requests/hour, so the first full sync can take a few hours. |
| RetroAchievements | Username and the "Web API Key" from your [settings](https://retroachievements.org/controlpanel.php). "Beaten" awards are used as story-end hints, with their date. |
| Epic Games | "Sign in", then paste the `authorizationCode` Epic shows. You get play time and achievements. Unofficial API, the same one Legendary/Heroic and Playnite use. |
| Nintendo Switch | Play time from the **Parental Controls** app. Pair the console with the app on your phone first; no restrictions are needed. Then click "Sign in", right-click "Select this person" → copy link, and paste it. Nintendo keeps only the last few weeks, so Epilogg stores every day it sees and the history grows from the pairing day on. Unofficial API, as nxapi uses. |
| GOG | "Sign in", then paste the address of the page you land on (it contains `code=`). You get achievements but no play time, so only games with at least one achievement are listed. Unofficial Galaxy API. |
| PlayStation | The `npsso` value from [ca.account.sony.com/api/v1/ssocookie](https://ca.account.sony.com/api/v1/ssocookie), read while signed in on playstation.com. It lasts about two months. |

Syncs run on their own: incrementally every day, and every hour when an API rate limit interrupted the
last run. Use "Sync" to start one right away.

### How a game is suggested as finished

The platforms record unlocks, not "completed". For each game Epilogg looks, in this order, for:

1. **Story end**: the first unlocked achievement whose text talks about finishing the game ("complete
   the story", "credits", "final boss"…).
2. **Platinum** trophy.
3. **100%** of the achievements.
4. **Last achievement** (a weak hint).

The date it proposes is the unlock date. Games whose title matches one already in your log are linked
to it, and lend it their hours and achievements.

## Import and export

⚙ Settings → *Your data* exports your log as CSV, or imports one: an Epilogg export, or any sheet with at
least `title` and `platform` columns.

- **Optional columns**: `completed_on` (or `date`), `rating`, `hours`, `notes`, `igdb_id`.
- **Format**: comma or semicolon separated; dates such as `2026-11-26` or `26/11/2026`.
- **Duplicates**: games already in the log (same title and date) are skipped.
- **Covers**: they are matched in the background. When the file has an `igdb_id`, that game is used as is.

## Homepage widget

In ⚙ Settings → "Generate token", then:

```yaml
- Epilogg:
    href: https://epilogg.example.com
    icon: https://epilogg.example.com/static/icon.svg
    widget:
      type: customapi
      url: http://epilogg:8000/api/stats
      headers:
        Authorization: "Bearer {{HOMEPAGE_VAR_EPILOGG_TOKEN}}"
      mappings:
        - { field: year, label: This year }
        - { field: total, label: Total }
        - { field: to_review, label: To review }
```

## Backup

Everything is in the `/data` volume:

- `games.db`;
- `covers/`, which can be downloaded again;
- `psn_token_*.json`, which is regenerated from the NPSSO.

For a consistent copy while the app runs:

```bash
docker compose exec epilogg python -c "import sqlite3; print('\n'.join(sqlite3.connect('/data/games.db').iterdump()))" | gzip > epilogg.sql.gz
```

## Forgotten password

```bash
docker compose exec -it epilogg python store.py reset-password <username>
```

## Development

```bash
docker build -t epilogg .
docker run --rm -p 8000:8000 -v "$PWD/data:/data" --env-file .env epilogg
```

The code is small:

- `app.py`: API, users and IGDB matching;
- `connectors.py`: Steam, Xbox and PSN;
- `store.py`: the database;
- `static/index.html`: the whole interface, in plain JS.

## License

[MIT](LICENSE)
