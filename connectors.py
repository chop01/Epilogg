"""Steam, Xbox (OpenXBL) and PSN connectors: they fill the candidates table.

Platforms do not say "completed": they record achievements/trophies with dates. For each game we look for
an ending hint, in this order:
  story    - first unlocked achievement whose text talks about finishing the story (complete the game, credits…)
  platinum - PSN platinum
  100      - all achievements
  last     - last unlocked achievement (weak)
Candidates are confirmed or dismissed in the app; the user adds the rating.

Syncs are incremental: a game's achievements are downloaded again only when its "fingerprint"
(last played, achievements unlocked) has changed.
"""
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone

import httpx

from store import DATA, db, norm, now

log = logging.getLogger("connectors")

# Per-user credentials live in the credentials table (app settings). The Steam key in .env is shared by
# everyone (any key can read public profiles); the other .env variables are only used on the first start,
# when they are copied to the first admin (see app.setup).
ENV_STEAM_KEY = os.environ.get("STEAM_API_KEY", "")
ENV_CREDS = {"steam_id": os.environ.get("STEAM_ID", ""), "xbl_key": os.environ.get("XBL_API_KEY", ""),
             "psn_npsso": os.environ.get("PSN_NPSSO", "")}
CRED_KEYS = ("steam_id", "steam_key", "xbl_key", "psn_npsso")


def psn_token_file(uid: int) -> str:   # PSN session token (refreshes itself)
    return os.path.join(DATA, f"psn_token_{uid}.json")


class SyncError(Exception):
    """Error shown in the page: translated text key + parameters."""
    def __init__(self, key, **params):
        super().__init__(key)
        self.key, self.params = key, params


def msg(key, **params) -> str:
    return json.dumps({"k": key, "p": params})


def creds(uid: int) -> dict:
    with db() as con:
        c = {r["key"]: r["value"] for r in con.execute("SELECT key, value FROM credentials WHERE user_id=?", (uid,))}
    c.setdefault("steam_key", ENV_STEAM_KEY)
    return c

MIN_MINUTES = 30   # below this, a Steam game without achievements is not even a candidate

# Achievement text that means the story is over (the APIs are queried in English).
# Verb and object must be adjacent: "completed every mini-game" or "completed the X Island story" do not count.
_VERB = r"(complete[ds]?|completing|finish(e[ds])?|beat(en)?|clear(ed)?)"
END_RE = re.compile(
    rf"\b{_VERB} (the )?(main |entire |whole )?(game|story|campaign|adventure|journey|main quest|final chapter|last chapter)\b"
    r"|\b(see|saw|seen|reach(ed)?|witness(ed)?|watch(ed)?|view(ed)?|unlock(ed)?|get|got)( \w+){0,3} ending\b"
    r"|\bcredits\b|^the end$|\bepilogue\b|\bfinal boss\b|\bend boss\b|\bmission complete\b|\bgame complete\b"
    r"|\bstory complete\b"
    rf"|\b{_VERB} (all|every) (the )?(main )?(chapters|episodes|acts)\b",
    re.I)
NOT_END_RE = re.compile(r"new game ?(\+|plus)|mini-?games?", re.I)


def is_end(name: str | None, desc: str | None, title: str | None = None) -> bool:
    name, desc = (name or "").strip(), (desc or "").strip()
    text = f"{name}. {desc}"
    if NOT_END_RE.search(text):
        return False
    if any(END_RE.search(x) for x in (name, desc)):
        return True
    # "Complete <game title>" / 'Cleared "Ghost Trick"'
    t = norm(title or "")
    m = re.search(rf"\b{_VERB} (?P<rest>.+)", norm(desc))
    rest = m and m.group("rest").strip()
    return bool(t and rest and len(rest) >= 4 and (t.startswith(rest) or rest.startswith(t)))


def day(ts) -> str | None:
    """Unix timestamp, datetime or ISO string -> YYYY-MM-DD (UTC)."""
    if not ts:
        return None
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, timezone.utc).date().isoformat()
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc).date().isoformat()
    return str(ts)[:10]


def end_signal(achs: list[dict], title: str, total: int = 0, platinum: dict | None = None):
    """achs: [{name, desc, date}] of the unlocked ones. Returns (signal, label, date), most reliable first:
    story (first ending achievement unlocked), platinum, 100 (all), last (last unlocked: weak hint, right
    ~9 times out of 10 in testing, wrong when the game was picked up again later for the post-game)."""
    dated = [a for a in achs if a.get("date")]
    story = sorted((a for a in dated if is_end(a["name"], a["desc"], title)), key=lambda a: a["date"])
    if story:
        return "story", story[0]["name"], story[0]["date"]
    if platinum and platinum.get("date"):
        return "platinum", platinum["name"], platinum["date"]
    if not dated:
        return None, None, None
    last = max(dated, key=lambda a: a["date"])
    if total and len(achs) >= total:
        return "100", None, last["date"]
    return "last", last["name"], last["date"]


# --- state and writes ---------------------------------------------------------

_locks: dict = {}   # (user, source) -> Lock
_locks_guard = threading.Lock()


class RateLimited(SyncError):
    """Rate limit reached: the sync resumes on the next round."""


def configured(uid: int) -> dict:
    c = creds(uid)
    return {"steam": bool(c.get("steam_key") and c.get("steam_id")), "xbox": bool(c.get("xbl_key")),
            "psn": bool(c.get("psn_npsso"))}


def set_state(uid, source, status, message=None, ok=False):
    with db() as con:
        con.execute("INSERT INTO sync_state (user_id, source, last_run, status, message) VALUES (?,?,?,?,?) "
                    "ON CONFLICT(user_id, source) DO UPDATE SET last_run=excluded.last_run, status=excluded.status, "
                    "message=excluded.message", (uid, source, now(), status, message))
        if ok:
            con.execute("UPDATE sync_state SET last_ok=? WHERE user_id=? AND source=?", (now(), uid, source))


def known(uid, source) -> dict:
    """ext_id -> fingerprint of the candidates already downloaded."""
    with db() as con:
        return {r["ext_id"]: r["fingerprint"] for r in
                con.execute("SELECT ext_id, fingerprint FROM candidates WHERE user_id=? AND source=?", (uid, source))}


def upsert(uid, c: dict):
    """Insert/update a candidate; link it to a game already in the log when the title matches."""
    c = {k: c.get(k) for k in ("source", "ext_id", "title", "platform", "image_url", "hours", "ach_done",
                               "ach_total", "first_played", "last_played", "signal", "signal_label",
                               "signal_date", "fingerprint")}
    with db() as con:
        con.execute(
            "INSERT INTO candidates (user_id, source, ext_id, title, platform, image_url, hours, ach_done, ach_total,"
            " first_played, last_played, signal, signal_label, signal_date, fingerprint, updated_at)"
            " VALUES (:uid,:source,:ext_id,:title,:platform,:image_url,:hours,:ach_done,:ach_total,:first_played,"
            " :last_played,:signal,:signal_label,:signal_date,:fingerprint,:now)"
            " ON CONFLICT(user_id, source, ext_id) DO UPDATE SET title=excluded.title, image_url=excluded.image_url,"
            " hours=excluded.hours, ach_done=excluded.ach_done, ach_total=excluded.ach_total,"
            " first_played=excluded.first_played, last_played=excluded.last_played, signal=excluded.signal,"
            " signal_label=excluded.signal_label, signal_date=excluded.signal_date,"
            " fingerprint=excluded.fingerprint, updated_at=excluded.updated_at",
            {**c, "now": now(), "uid": uid})
    link_existing(uid)


def update_light(uid, source, ext_id, **fields):
    """Update only hours/last played, leaving the rest alone (fingerprint unchanged)."""
    if not fields:
        return
    sets = ", ".join(f"{k}=?" for k in fields)
    with db() as con:
        con.execute(f"UPDATE candidates SET {sets}, updated_at=? WHERE user_id=? AND source=? AND ext_id=?",
                    (*fields.values(), now(), uid, source, ext_id))


SUBTITLE_RE = re.compile(r"\s*(:|&|\s[-\u2013\u2014]\s|\(|\u2122|\u00ae)")


def head(title: str) -> str:
    """Title without subtitle: "Yakuza Kiwami 3 & Dark Ties" -> "yakuza kiwami 3"."""
    return norm(SUBTITLE_RE.split(title, maxsplit=1)[0])


def same_game(cand: str, known: str, loose: bool = False) -> bool:
    """Same normalized title. With loose=True the platform title may also carry an extra subtitle after
    ":", "&", " - " ("Yakuza Kiwami 3 & Dark Ties" ~ "yakuza kiwami 3"). Never the other way round, and never
    without a separator: "The Stanley Parable" != "...: Ultra Deluxe", "God of War" != "God of War Ragnarök"."""
    a, b = norm(cand), norm(known)
    if not a or not b:
        return False
    return a == b or (loose and len(b) >= 6 and head(cand) == b)


def link_existing(uid: int):
    """New candidates that match a game in the log become "already in the log" (the log reads hours and
    achievements from them, see app.platform_stats).
    Exact matches first (title or IGDB name); the subtitle rule only applies to games that do not already have
    an exact match from the same platform (MGS V: The Phantom Pain yes, Ground Zeroes no)."""
    with db() as con:
        exact, loose = {}, {}   # normalized title -> game id (the first one inserted wins)
        for g in con.execute("SELECT id, title, igdb_name FROM games WHERE user_id=? ORDER BY id", (uid,)):
            for t in (g["title"], g["igdb_name"]):
                if t:
                    n = norm(t)
                    exact.setdefault(n, g["id"])
                    if len(n) >= 6:
                        loose.setdefault(n, g["id"])
        taken = {(r["game_id"], r["source"]) for r in
                 con.execute("SELECT game_id, source FROM candidates WHERE user_id=? AND game_id IS NOT NULL", (uid,))}
        todo = con.execute("SELECT id, title, source FROM candidates WHERE user_id=? AND status='new' AND game_id IS NULL",
                           (uid,)).fetchall()
        links = []
        for c in todo:   # first pass: identical title
            gid = exact.get(norm(c["title"]))
            if gid:
                links.append((gid, c["id"]))
                taken.add((gid, c["source"]))
        linked = {cid for _, cid in links}
        for c in todo:   # second pass: extra subtitle, if that game has no candidate from this source yet
            gid = c["id"] not in linked and loose.get(head(c["title"]))
            if gid and (gid, c["source"]) not in taken:
                links.append((gid, c["id"]))
        for gid, cid in links:
            con.execute("UPDATE candidates SET status='confirmed', game_id=? WHERE id=?", (gid, cid))


# --- Steam ----------------------------------------------------------------------

def steam_get(client, key, path, **params):
    r = client.get(f"https://api.steampowered.com/{path}", params={"key": key, **params})
    if r.status_code == 429:
        raise RateLimited("rate_steam")
    if r.status_code == 403:
        raise SyncError("steam_badkey")
    return r


def steam_id(client, key, sid) -> str:
    sid = sid.strip().rstrip("/").split("/")[-1]   # also accepts the profile URL
    if sid.isdigit() and len(sid) == 17:
        return sid
    r = steam_get(client, key, "ISteamUser/ResolveVanityURL/v1/", vanityurl=sid).json()["response"]
    if r.get("success") != 1:
        raise SyncError("steam_noid", id=sid)
    return r["steamid"]


def sync_steam(uid, cr):
    key = cr["steam_key"]
    with httpx.Client(timeout=30) as client:
        sid = steam_id(client, key, cr["steam_id"])
        r = steam_get(client, key, "IPlayerService/GetOwnedGames/v1/", steamid=sid, include_appinfo=1,
                      include_played_free_games=1)
        r.raise_for_status()
        owned = r.json().get("response", {}).get("games")
        if owned is None:
            raise SyncError("steam_private")
        have = known(uid, "steam")
        fetched = 0
        for g in owned:
            minutes = g.get("playtime_forever", 0)
            if minutes == 0:
                continue
            ext = str(g["appid"])
            hours = round(minutes / 60, 1)
            last = day(g.get("rtime_last_played"))
            fp = f"{g.get('rtime_last_played', 0)}"
            if have.get(ext) == fp:
                continue
            achs, total = [], 0
            a = steam_get(client, key, "ISteamUserStats/GetPlayerAchievements/v1/", steamid=sid, appid=ext, l="english")
            if a.status_code == 200:
                lst = a.json().get("playerstats", {}).get("achievements") or []
                total = len(lst)
                achs = [{"name": x.get("name"), "desc": x.get("description"), "date": day(x.get("unlocktime"))}
                        for x in lst if x.get("achieved")]
            fetched += 1
            time.sleep(0.2)
            if total == 0 and minutes < MIN_MINUTES:
                continue
            sig, label, sdate = end_signal(achs, g.get("name") or "", total)
            upsert(uid, {"source": "steam", "ext_id": ext, "title": g.get("name") or ext, "platform": "Steam",
                    "image_url": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{ext}/library_600x900.jpg",
                    "hours": hours, "ach_done": len(achs), "ach_total": total,
                    "first_played": min((a["date"] for a in achs), default=None), "last_played": last,
                    "signal": sig, "signal_label": label, "signal_date": sdate, "fingerprint": fp})
    return msg("steam_ok", n=len(owned), f=fetched)


# --- Xbox (OpenXBL, free plan: 150 requests/hour) --------------------------------

def xbl_get(client, key, path):
    r = client.get(f"https://xbl.io/api/v2/{path}", headers={"X-Authorization": key, "Accept": "application/json",
                                                              "Accept-Language": "en-US"})
    if r.status_code == 429:
        raise RateLimited("rate_xbox")
    if r.status_code in (401, 403):
        raise SyncError("xbox_badkey")
    r.raise_for_status()
    rem = r.headers.get("X-RateLimit-Remaining")
    if rem is not None and rem.isdigit() and int(rem) < 3:
        raise RateLimited("rate_xbox")
    j = r.json()
    return j.get("content", j) if isinstance(j, dict) else j   # OpenXBL wraps responses in {"content", "code"}


def xbox_platform(devices) -> str:
    """devices = where the game can run, not where it was played: a suggestion the user can change."""
    devices = set(devices or [])
    if devices & {"PC", "Win32"}:
        return "Game Pass"
    return "Xbox 360" if devices == {"Xbox360"} else "Xbox"


def xbox_minutes(client, key, xuid: str, title_ids: list[str]) -> dict:
    """titleId -> minutes played (MinutesPlayed stat), 100 titles per request."""
    out = {}
    for i in range(0, len(title_ids), 100):
        body = {"xuids": [xuid], "groups": [],
                "stats": [{"name": "MinutesPlayed", "titleId": t} for t in title_ids[i:i + 100]]}
        r = client.post("https://xbl.io/api/v2/player/stats", json=body,
                        headers={"X-Authorization": key, "Accept": "application/json"})
        if r.status_code == 429:
            raise RateLimited("rate_xbox")
        r.raise_for_status()
        j = r.json()
        for coll in (j.get("content", j) or {}).get("statlistscollection", []):
            for st in coll.get("stats", []):
                if st.get("name") == "MinutesPlayed" and str(st.get("value", "")).isdigit():
                    out[str(st["titleid"])] = int(st["value"])
    return out


def sync_xbox(uid, cr):
    key = cr["xbl_key"]
    with httpx.Client(timeout=30) as client:
        summary = xbl_get(client, key, "achievements")
        titles = summary.get("titles", [])
        have = known(uid, "xbox")
        # hours for every title in a few requests, before the achievements (which may stop at the hourly limit)
        minutes = xbox_minutes(client, key, summary.get("xuid"), [str(t.get("titleId")) for t in titles if t.get("titleId")])
        hours_of = lambda ext: round(minutes[ext] / 60, 1) if ext in minutes else None
        for ext in have:
            update_light(uid, "xbox", ext, hours=hours_of(ext))
        fetched = 0
        for t in titles:
            if t.get("type") not in (None, "Game"):
                continue
            devices = t.get("devices") or []
            if devices and set(devices) <= {"Mobile"}:
                continue
            ext = str(t.get("titleId") or t.get("modernTitleId"))
            prog = t.get("achievement") or {}
            done, total = prog.get("currentAchievements", 0), prog.get("totalAchievements", 0)
            full = bool(prog.get("totalGamerscore")) and prog.get("currentGamerscore") == prog.get("totalGamerscore")
            last = (t.get("titleHistory") or {}).get("lastTimePlayed")
            fp = f"{done}|{day(last)}"
            if have.get(ext) == fp:
                continue
            achs = []
            if done:
                token, n = None, 0
                while True:
                    j = xbl_get(client, key, f"achievements/title/{ext}" + (f"/{token}" if token else ""))
                    lst = j.get("achievements", [])
                    if n == 0 and lst:
                        total = max(total, (j.get("pagingInfo") or {}).get("totalRecords") or len(lst))
                    for a in lst:
                        if a.get("progressState") == "Achieved":
                            achs.append({"name": a.get("name"), "desc": a.get("description"),
                                         "date": day((a.get("progression") or {}).get("timeUnlocked"))})
                    token = (j.get("pagingInfo") or {}).get("continuationToken")
                    n += 1
                    if not token or n > 5:
                        break
            fetched += 1
            sig, label, sdate = end_signal(achs, t.get("name") or "", total)
            if full and sig in (None, "last"):
                sig, label = "100", None
            upsert(uid, {"source": "xbox", "ext_id": ext, "title": t.get("name") or ext, "platform": xbox_platform(devices),
                    "image_url": t.get("displayImage"), "hours": hours_of(ext), "ach_done": done, "ach_total": total,
                    "first_played": min((a["date"] for a in achs if a["date"]), default=None), "last_played": day(last),
                    "signal": sig, "signal_label": label, "signal_date": sdate, "fingerprint": fp})
    return msg("xbox_ok", n=len(titles), f=fetched)


# --- PSN (psnawp: NPSSO -> self-refreshing token; 1 request every 3 s) -----------

PSN_PLATFORMS = ["PS5", "PS4", "PS3", "PSVITA", "PSPC"]
PSN_LABEL = {"PSVITA": "PS Vita", "PSPC": "PC"}


def psn_client(uid, npsso):
    from psnawp_api import PSNAWP
    token, path = None, psn_token_file(uid)
    if os.path.exists(path):
        with open(path) as f:
            token = json.load(f)
        if token.get("npsso_hash") != hash_npsso(npsso):
            token = None          # NPSSO changed in the settings: start again from it
    return PSNAWP(npsso, token_response=token and token["token"])


def hash_npsso(npsso) -> str:
    import hashlib
    return hashlib.sha256(npsso.encode()).hexdigest()[:16]


def save_psn_token(uid, npsso, psn):
    tr = psn.authenticator.token_response
    if tr:
        path = psn_token_file(uid)
        tmp = path + ".tmp"
        with open(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
            json.dump({"npsso_hash": hash_npsso(npsso), "token": dict(tr)}, f)
        os.replace(tmp, path)


def sync_psn(uid, cr):
    from psnawp_api.core.psnawp_exceptions import PSNAWPAuthenticationError, PSNAWPTooManyRequestsError
    from psnawp_api.models.trophies.trophy_constants import TrophyType
    try:
        npsso = cr["psn_npsso"]
        psn = psn_client(uid, npsso)
        me = psn.me()
        # play time (PS4/PS5 only), matched by title
        stats = {}
        for s in me.title_stats():
            if s.name:
                stats.setdefault(norm(s.name), s)
        have = known(uid, "psn")
        titles = list(me.trophy_titles())
        fetched = 0
        for t in titles:
            ext = t.np_communication_id
            plats = {p.value for p in t.title_platform}
            plat = next((p for p in PSN_PLATFORMS if p in plats), "PS")
            tsum = lambda ts: (ts.bronze + ts.silver + ts.gold + ts.platinum) if ts else 0
            earned, total = tsum(t.earned_trophies), tsum(t.defined_trophies)
            st = stats.get(norm(t.title_name or ""))
            hours = round(st.play_duration.total_seconds() / 3600, 1) if st and st.play_duration else None
            fp = f"{earned}|{day(t.last_updated_datetime)}"
            if have.get(ext) == fp:
                update_light(uid, "psn", ext, hours=hours)
                continue
            achs, plat_t = [], None
            if earned:
                from psnawp_api.models.trophies.trophy_constants import PlatformType
                for tr in me.trophies(ext, PlatformType(plat), include_progress=True, trophy_group_id="all"):
                    if tr.earned:
                        a = {"name": tr.trophy_name, "desc": tr.trophy_detail, "date": day(tr.earned_date_time)}
                        if tr.trophy_type == TrophyType.PLATINUM:
                            plat_t = a
                        elif tr.trophy_group_id in (None, "default"):   # no DLC for the ending hint
                            achs.append(a)
            fetched += 1
            sig, label, sdate = end_signal(achs, t.title_name or "", 0, plat_t)
            if sig in (None, "last") and total and earned >= total:
                sig, label, sdate = "100", None, day(t.last_updated_datetime)
            upsert(uid, {"source": "psn", "ext_id": ext, "title": t.title_name or ext, "platform": PSN_LABEL.get(plat, plat),
                    "image_url": t.title_icon_url, "hours": hours, "ach_done": earned, "ach_total": total,
                    "first_played": day(st.first_played_date_time) if st else None,
                    "last_played": day(st.last_played_date_time) if st else day(t.last_updated_datetime),
                    "signal": sig, "signal_label": label, "signal_date": sdate, "fingerprint": fp})
            save_psn_token(uid, npsso, psn)
        save_psn_token(uid, npsso, psn)
        return msg("psn_ok", n=len(titles), f=fetched)
    except PSNAWPAuthenticationError as e:
        raise SyncError("psn_expired") from e
    except PSNAWPTooManyRequestsError as e:
        raise RateLimited("rate_psn") from e



# --- orchestration -------------------------------------------------------------

SYNC = {"steam": sync_steam, "xbox": sync_xbox, "psn": sync_psn}
SCHEDULED = ("steam", "xbox", "psn")


def run(uid: int, source: str) -> bool:
    """Run a sync unless one is already running. Returns False if it was already running."""
    with _locks_guard:
        lock = _locks.setdefault((uid, source), threading.Lock())
    if not lock.acquire(blocking=False):
        return False
    try:
        set_state(uid, source, "running")
        set_state(uid, source, "ok", SYNC[source](uid, creds(uid)), ok=True)
    except RateLimited as e:
        set_state(uid, source, "partial", msg(e.key, **e.params))
    except SyncError as e:
        set_state(uid, source, "error", msg(e.key, **e.params))
    except Exception as e:   # noqa: BLE001 - show the error in the app, never stop the scheduler
        log.exception("sync %s user %s", source, uid)
        set_state(uid, source, "error", msg("raw", text=str(e)[:500]))
    finally:
        lock.release()
    return True


def run_async(uid: int, source: str):
    threading.Thread(target=run, args=(uid, source), daemon=True).start()


def scheduler():
    """Every hour, for every user: resume sources stopped halfway (API limit), and run all of them once a day."""
    time.sleep(60)
    while True:
        with db() as con:
            users = [r["id"] for r in con.execute("SELECT id FROM users")]
            state = {(r["user_id"], r["source"]): dict(r) for r in con.execute("SELECT * FROM sync_state")}
        for uid in users:
            for src, ok in configured(uid).items():
                if not ok or src not in SCHEDULED:
                    continue
                st = state.get((uid, src)) or {}
                last_ok = st.get("last_ok")
                stale = not last_ok or (datetime.now() - datetime.fromisoformat(last_ok)).total_seconds() > 20 * 3600
                if st.get("status") == "partial" or stale:
                    run(uid, src)
        time.sleep(3600)


def start_scheduler():
    with db() as con:   # a restart during a sync leaves "running" behind: no longer true
        con.execute("UPDATE sync_state SET status='error', message=? WHERE status='running'", (msg("restart"),))
    threading.Thread(target=scheduler, daemon=True).start()
