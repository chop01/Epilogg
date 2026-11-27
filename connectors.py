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
CRED_KEYS = ("steam_id", "steam_key", "xbl_key", "psn_npsso", "ra_user", "ra_key")   # typed in the settings
LINKED = ("gog_token", "epic_token", "nintendo_token", "ubisoft_token")   # set by signing in (gog_link / epic_link)


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
            "psn": bool(c.get("psn_npsso")), "ra": bool(c.get("ra_user") and c.get("ra_key")),
            "gog": bool(c.get("gog_token")), "epic": bool(c.get("epic_token")),
            "nintendo": bool(c.get("nintendo_token")), "ubisoft": bool(c.get("ubisoft_token"))}


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



# --- RetroAchievements (official Web API: username + web API key) ----------------

RA_API = "https://retroachievements.org/API/"
# RA console name -> platform name in the log (others are kept as RA names them)
RA_PLATFORM = {"NES/Famicom": "NES", "SNES/Super Famicom": "SNES", "Nintendo 64": "N64", "Game Boy": "Game Boy",
               "Game Boy Color": "GBC", "Game Boy Advance": "GBA", "Nintendo DS": "NDS", "GameCube": "GameCube",
               "Mega Drive / Genesis": "Mega Drive", "Master System": "Master System", "PlayStation": "PS1",
               "PlayStation 2": "PS2", "PlayStation Portable": "PSP", "Saturn": "Saturn", "Dreamcast": "Dreamcast"}


def ra_get(client, cr, endpoint, **params):
    r = client.get(RA_API + endpoint, params={"y": cr["ra_key"], "u": cr["ra_user"], **params})
    if r.status_code == 429:
        raise RateLimited("rate_ra")
    if r.status_code in (401, 403):
        raise SyncError("ra_badkey")
    r.raise_for_status()
    return r.json()


def sync_ra(uid, cr):
    """Every game with progress, plus the "beaten" / "mastered" awards with their dates (two kinds of request)."""
    with httpx.Client(timeout=30) as client:
        games, o = [], 0
        while True:
            j = ra_get(client, cr, "API_GetUserCompletionProgress.php", c=500, o=o)
            games += j.get("Results") or []
            o += 500
            if o >= (j.get("Total") or 0):
                break
        beaten, mastered = {}, {}   # game id -> earliest award date
        for a in ra_get(client, cr, "API_GetUserAwards.php").get("VisibleUserAwards") or []:
            target = {"Game Beaten": beaten, "Mastery/Completion": mastered}.get(a.get("AwardType"))
            if target is not None and a.get("AwardData"):
                d = day(a.get("AwardedAt"))
                gid = str(a["AwardData"])
                target[gid] = min(filter(None, (target.get(gid), d)), default=None)
        for g in games:
            ext = str(g["GameID"])
            done = max(g.get("NumAwarded") or 0, g.get("NumAwardedHardcore") or 0)
            last = day(g.get("MostRecentAwardedDate"))
            kind = g.get("HighestAwardKind") or ""
            if beaten.get(ext) or kind.startswith("beaten"):
                sig, label, sdate = "story", "Beaten", beaten.get(ext) or day(g.get("HighestAwardDate"))
            elif mastered.get(ext) or kind in ("mastered", "completed"):
                sig, label, sdate = "100", None, mastered.get(ext) or day(g.get("HighestAwardDate"))
            elif done:
                sig, label, sdate = "last", None, last
            else:
                sig = label = sdate = None
            upsert(uid, {"source": "ra", "ext_id": ext, "title": g.get("Title") or ext,
                         "platform": RA_PLATFORM.get(g.get("ConsoleName"), g.get("ConsoleName") or "Retro"),
                         "image_url": "https://media.retroachievements.org" + g["ImageIcon"] if g.get("ImageIcon") else None,
                         "hours": None, "ach_done": done, "ach_total": g.get("MaxPossible") or 0,
                         "first_played": None, "last_played": last,
                         "signal": sig, "signal_label": label, "signal_date": sdate,
                         "fingerprint": f"{done}|{kind}|{last}"})
    return msg("ra_ok", n=len(games))


# --- shared by GOG and Epic: OAuth tokens kept in the credentials table ----------

def set_cred(uid, key, value):
    with db() as con:
        if value is None:
            con.execute("DELETE FROM credentials WHERE user_id=? AND key=?", (uid, key))
        else:
            con.execute("INSERT INTO credentials (user_id, key, value) VALUES (?,?,?) ON CONFLICT(user_id, key)"
                        " DO UPDATE SET value=excluded.value", (uid, key, value))


def token_of(cr, key) -> dict:
    try:
        return json.loads(cr.get(key) or "{}")
    except ValueError:
        return {}


# --- GOG (Galaxy client login; unofficial API: library + achievements, no play time) ---

GOG_CLIENT = "46899977096215655"
GOG_SECRET = "9d85c43b1482497dbbce61f6e4aa173a433796eeae2ca8c5f6129f2dc4de46d9"   # public, from the Galaxy client
GOG_REDIRECT = "https://embed.gog.com/on_login_success?origin=client"


def gog_login_url(uid=None) -> str:
    from urllib.parse import urlencode
    return "https://auth.gog.com/auth?" + urlencode({"client_id": GOG_CLIENT, "redirect_uri": GOG_REDIRECT,
                                                     "response_type": "code", "layout": "client2"})


def gog_token(params) -> dict:
    r = httpx.get("https://auth.gog.com/token", timeout=30,
                  params={"client_id": GOG_CLIENT, "client_secret": GOG_SECRET, **params})
    if r.status_code != 200 or "refresh_token" not in r.text:
        raise SyncError("gog_expired")
    return r.json()


def gog_link(uid, pasted: str):
    """pasted = the address of the page GOG shows after signing in (…on_login_success?…&code=XXX) or the code."""
    from urllib.parse import parse_qs, urlparse
    p = pasted.strip()
    code = (parse_qs(urlparse(p).query).get("code") or [p])[0] if "code=" in p else p
    t = gog_token({"grant_type": "authorization_code", "code": code, "redirect_uri": GOG_REDIRECT})
    set_cred(uid, "gog_token", json.dumps({"refresh_token": t["refresh_token"], "user_id": t.get("user_id")}))


def sync_gog(uid, cr):
    t = gog_token({"grant_type": "refresh_token", "refresh_token": token_of(cr, "gog_token").get("refresh_token", "")})
    set_cred(uid, "gog_token", json.dumps({"refresh_token": t["refresh_token"], "user_id": t.get("user_id")}))
    h = {"Authorization": f"Bearer {t['access_token']}"}
    with httpx.Client(timeout=30, headers=h) as client:
        owned = client.get("https://embed.gog.com/user/data/games").json().get("owned") or []
        info = {}
        for i in range(0, len(owned), 50):   # titles and covers, 50 products per request (public endpoint)
            r = client.get("https://api.gog.com/products", params={"ids": ",".join(map(str, owned[i:i + 50]))})
            if r.status_code == 200:
                info.update({p["id"]: p for p in r.json() if p.get("game_type", "game") == "game"})
        fetched = 0
        for pid, p in info.items():
            r = client.get(f"https://gameplay.gog.com/clients/{pid}/users/{t['user_id']}/achievements")
            if r.status_code == 429:
                raise RateLimited("rate_gog")
            items = r.json().get("items", []) if r.status_code == 200 else []
            achs = [{"name": a.get("name"), "desc": a.get("description"), "date": day(a.get("date_unlocked"))}
                    for a in items if a.get("date_unlocked")]
            fetched += 1
            time.sleep(0.2)
            if not achs:   # no play time on GOG: without unlocks we cannot tell a started game from an unplayed one
                continue
            title = p.get("title") or str(pid)
            sig, label, sdate = end_signal(achs, title, len(items))
            img = (p.get("images") or {}).get("logo2x") or (p.get("images") or {}).get("icon")
            upsert(uid, {"source": "gog", "ext_id": str(pid), "title": title, "platform": "GOG",
                         "image_url": ("https:" + img) if img and img.startswith("//") else img,
                         "hours": None, "ach_done": len(achs), "ach_total": len(items),
                         "first_played": min(a["date"] for a in achs), "last_played": max(a["date"] for a in achs),
                         "signal": sig, "signal_label": label, "signal_date": sdate,
                         "fingerprint": str(len(achs))})
    return msg("gog_ok", n=len(info), f=fetched)


# --- Epic (launcher login as Legendary/Heroic do; unofficial API: library, play time, achievements) ---

EPIC_CLIENT = ("34a02cf8f4414e29b15921876da36f9a", "daafbccc737745039dffe53d94fc76cf")   # public launcher client
EPIC_OAUTH = "https://account-public-service-prod03.ol.epicgames.com/account/api/oauth/token"
EPIC_LIBRARY = "https://library-service.live.use1a.on.epicgames.com/library/api/public"
EPIC_GQL = "https://launcher.store.epicgames.com/graphql"
EPIC_UA = "EpicGamesLauncher/17.0.0-unknown"
EPIC_LOGIN = ("https://www.epicgames.com/id/login?redirectUrl=https%3A%2F%2Fwww.epicgames.com%2Fid%2Fapi%2Fredirect"
              "%3FclientId%3D34a02cf8f4414e29b15921876da36f9a%26responseType%3Dcode")
EPIC_ACH_DEFS = """query Achievement($sandboxId: String!, $locale: String!) { Achievement {
  productAchievementsRecordBySandbox(sandboxId: $sandboxId, locale: $locale) { totalAchievements
    achievements { achievement { name unlockedDisplayName unlockedDescription } } } } }"""
EPIC_ACH_USER = """query PlayerAchievement($epicAccountId: String!, $sandboxId: String!) { PlayerAchievement {
  playerAchievementGameRecordsBySandbox(epicAccountId: $epicAccountId, sandboxId: $sandboxId) { records {
    totalUnlocked playerAchievements { playerAchievement { unlocked unlockDate achievementName } } } } } }"""


def epic_token(data) -> dict:
    r = httpx.post(EPIC_OAUTH, data={**data, "token_type": "eg1"}, auth=EPIC_CLIENT, timeout=30,
                   headers={"User-Agent": EPIC_UA})
    if r.status_code != 200 or "refresh_token" not in r.text:
        raise SyncError("epic_expired")
    return r.json()


def epic_save(uid, t):
    set_cred(uid, "epic_token", json.dumps({"refresh_token": t["refresh_token"], "account_id": t["account_id"],
                                            "refresh_expires_at": t.get("refresh_expires_at")}))


def epic_link(uid, pasted: str):
    """pasted = the JSON Epic shows after signing in, or just its "authorizationCode"."""
    p = pasted.strip()
    m = re.search(r'"authorizationCode"\s*:\s*"([0-9a-f]+)"', p)
    t = epic_token({"grant_type": "authorization_code", "code": m.group(1) if m else p.strip('"')})
    epic_save(uid, t)


def epic_keepalive(uid):
    """Refresh the Epic token every hour: an unused refresh token expires after a few hours."""
    t = epic_token({"grant_type": "refresh_token", "refresh_token": token_of(creds(uid), "epic_token")["refresh_token"]})
    epic_save(uid, t)
    return t


def epic_gql(client, query, **variables):
    r = client.post(EPIC_GQL, json={"query": query, "variables": variables})
    return r.json().get("data") or {} if r.status_code == 200 else {}


def sync_epic(uid, cr):
    t = epic_keepalive(uid)
    acc = t["account_id"]
    h = {"Authorization": f"bearer {t['access_token']}", "User-Agent": EPIC_UA}
    with httpx.Client(timeout=30, headers=h) as client:
        records, cursor = [], None
        while True:
            r = client.get(f"{EPIC_LIBRARY}/items", params={"includeMetadata": "true", **({"cursor": cursor} if cursor else {})})
            r.raise_for_status()
            j = r.json()
            records += [x for x in j.get("records", []) if x.get("sandboxType") != "PRIVATE"]
            cursor = (j.get("responseMetadata") or {}).get("nextCursor")
            if not cursor:
                break
        played = {}   # appName -> seconds
        r = client.get(f"{EPIC_LIBRARY}/playtime/account/{acc}/all")
        if r.status_code == 200:
            for p in r.json():
                played[p.get("artifactId")] = p.get("totalTime") or 0
        have = known(uid, "epic")
        fetched = 0
        for rec in records:
            ns, item, app = rec.get("namespace"), rec.get("catalogItemId"), rec.get("appName")
            secs = played.get(app, 0)
            if played and not secs:
                continue   # never started (Epic gives away many games)
            hours = round(secs / 3600, 1) if secs else None
            fp = f"{secs // 600}"   # every 10 minutes of play counts as a change
            if have.get(item) == fp:
                continue
            c = client.get(f"https://catalog-public-service-prod06.ol.epicgames.com/catalog/api/shared/namespace/{ns}"
                           "/bulk/items", params={"id": item, "includeDLCDetails": "false", "country": "US", "locale": "en-US"})
            meta = (c.json() or {}).get(item, {}) if c.status_code == 200 else {}
            if meta.get("mainGameItem") or any(x.get("path") == "addons" for x in meta.get("categories", [])):
                continue   # DLC
            title = meta.get("title") or app
            img = next((i["url"] for i in meta.get("keyImages", []) if i.get("type") in ("DieselGameBoxTall", "DieselGameBox")), None)
            defs = epic_gql(client, EPIC_ACH_DEFS, sandboxId=ns, locale="en-US")
            rec_defs = (defs.get("Achievement") or {}).get("productAchievementsRecordBySandbox") or {}
            names = {a["achievement"]["name"]: a["achievement"] for a in rec_defs.get("achievements") or []}
            user = epic_gql(client, EPIC_ACH_USER, epicAccountId=acc, sandboxId=ns)
            urecs = ((user.get("PlayerAchievement") or {}).get("playerAchievementGameRecordsBySandbox") or {}).get("records") or []
            achs = []
            for pa in (urecs[0].get("playerAchievements") or []) if urecs else []:
                a = pa.get("playerAchievement") or {}
                if a.get("unlocked"):
                    d = names.get(a.get("achievementName"), {})
                    achs.append({"name": d.get("unlockedDisplayName") or a.get("achievementName"),
                                 "desc": d.get("unlockedDescription"), "date": day(a.get("unlockDate"))})
            total = rec_defs.get("totalAchievements") or len(names)
            fetched += 1
            time.sleep(0.2)
            if not played and not achs:
                continue   # no play time from Epic: without unlocks we cannot tell a started game from an unplayed one
            sig, label, sdate = end_signal(achs, title, total)
            upsert(uid, {"source": "epic", "ext_id": item, "title": title, "platform": "Epic",
                         "image_url": img, "hours": hours, "ach_done": len(achs), "ach_total": total,
                         "first_played": min((a["date"] for a in achs if a["date"]), default=None),
                         "last_played": max((a["date"] for a in achs if a["date"]), default=None),
                         "signal": sig, "signal_label": label, "signal_date": sdate, "fingerprint": fp})
    return msg("epic_ok", n=len(records), f=fetched)

# --- Nintendo Switch (Parental Controls app API, as nxapi does) -------------------
# Only play time from the day the console is paired with the Parental Controls app: Nintendo keeps the daily
# summaries for a few weeks, so every sync stores them in nintendo_play and the history builds up here.

NX_CLIENT = "54789befb391a838"
NX_SCOPE = ("openid user user.mii moonUser:administration moonDevice:create moonOwnedDevice:administration "
            "moonParentalControlSetting moonParentalControlSetting:update moonParentalControlSettingState "
            "moonPairingState moonSmartDevice:administration moonDailySummary moonMonthlySummary")
NX_MOON = "https://api-lp1.pctl.srv.nintendo.net/moon"
# Nintendo refuses old app builds ("update_required"); the check is on the build number only. If it happens
# again, raise NINTENDO_PCTL_BUILD in the .env (the error shows in the Platforms tab).
NX_VERSION = os.environ.get("NINTENDO_PCTL_VERSION", "2.6.1")
NX_BUILD = os.environ.get("NINTENDO_PCTL_BUILD", "9999")
NX_HEADERS = {"Cache-Control": "no-store", "X-Moon-App-Id": "com.nintendo.znma", "X-Moon-Os": "ANDROID",
              "X-Moon-Os-Version": "26", "X-Moon-Model": "", "X-Moon-TimeZone": os.environ.get("TZ", "Europe/London"),
              "X-Moon-Os-Language": "en-GB", "X-Moon-App-Language": "en-GB",
              "X-Moon-App-Display-Version": NX_VERSION, "X-Moon-App-Internal-Version": NX_BUILD,
              "User-Agent": f"moon_ANDROID/{NX_VERSION} (com.nintendo.znma; build:{NX_BUILD}; ANDROID 26)"}


def _b64(b: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def nintendo_login_url(uid) -> str:
    """Nintendo account sign-in for the Parental Controls app (PKCE); the verifier waits in the credentials."""
    import hashlib
    import secrets
    from urllib.parse import urlencode
    state, verifier = _b64(secrets.token_bytes(36)), _b64(secrets.token_bytes(32))
    set_cred(uid, "nintendo_pending", json.dumps({"state": state, "verifier": verifier}))
    return "https://accounts.nintendo.com/connect/1.0.0/authorize?" + urlencode({
        "state": state, "redirect_uri": f"npf{NX_CLIENT}://auth", "client_id": NX_CLIENT, "scope": NX_SCOPE,
        "response_type": "session_token_code", "session_token_code_challenge": _b64(hashlib.sha256(verifier.encode()).digest()),
        "session_token_code_challenge_method": "S256", "theme": "login_form"})


def nintendo_link(uid, pasted: str):
    """pasted = the "Select this person" link (npf54789befb391a838://auth#session_token_code=…&state=…)."""
    from urllib.parse import parse_qs
    pending = token_of(creds(uid), "nintendo_pending")
    frag = parse_qs(pasted.strip().split("#", 1)[-1])
    code = (frag.get("session_token_code") or [None])[0]
    if not pending or not code or (frag.get("state") or [None])[0] != pending.get("state"):
        raise SyncError("nintendo_link")
    r = httpx.post("https://accounts.nintendo.com/connect/1.0.0/api/session_token", timeout=30,
                   headers={"Accept": "application/json", "User-Agent": "NASDKAPI; Android"},
                   data={"client_id": NX_CLIENT, "session_token_code": code, "session_token_code_verifier": pending["verifier"]})
    if r.status_code != 200 or "session_token" not in r.text:
        raise SyncError("nintendo_link")
    set_cred(uid, "nintendo_token", json.dumps({"session_token": r.json()["session_token"]}))
    set_cred(uid, "nintendo_pending", None)


def nintendo_access(client, cr) -> tuple[str, str]:
    """session token -> (access token, Nintendo account id)."""
    r = client.post("https://accounts.nintendo.com/connect/1.0.0/api/token",
                    headers={"Accept": "application/json", "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 8.0.0)"},
                    json={"client_id": NX_CLIENT, "session_token": token_of(cr, "nintendo_token").get("session_token"),
                          "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer-session-token"})
    if r.status_code != 200:
        raise SyncError("nintendo_expired")
    tok = r.json()["access_token"]
    me = client.get("https://api.accounts.nintendo.com/2.0.0/users/me", headers={"Authorization": f"Bearer {tok}"})
    me.raise_for_status()
    return tok, me.json()["id"]


def sync_nintendo(uid, cr):
    with httpx.Client(timeout=30) as client:
        tok, na_id = nintendo_access(client, cr)
        h = {**NX_HEADERS, "Authorization": f"Bearer {tok}"}

        def moon(path):
            r = client.get(NX_MOON + path, headers=h)
            if r.status_code == 429:
                raise RateLimited("rate_nintendo")
            if r.status_code == 400 and "update_required" in r.text:
                raise SyncError("nintendo_update")
            r.raise_for_status()
            return r.json()

        devices = moon(f"/v1/users/{na_id}/devices").get("items") or []
        if not devices:
            raise SyncError("nintendo_nodevice")
        titles = {}   # applicationId -> Title (name, image, first play: the earliest across consoles)

        def add_title(t):
            old = titles.get(t["applicationId"])
            if not old or (t.get("firstPlayDate") or "9") < (old.get("firstPlayDate") or "9"):
                titles[t["applicationId"]] = {**(old or {}), **t}

        with db() as con:
            for d in devices:
                dev = d["deviceId"]
                for s in moon(f"/v1/devices/{dev}/daily_summaries").get("items") or []:
                    for t in s.get("playedApps") or []:
                        add_title(t)
                    secs = {}
                    for p in (s.get("devicePlayers") or []) + ([s["anonymousPlayer"]] if s.get("anonymousPlayer") else []):
                        for a in p.get("playedApps") or []:
                            secs[a["applicationId"]] = secs.get(a["applicationId"], 0) + (a.get("playingTime") or 0)
                    for app_id, sec in secs.items():
                        con.execute("INSERT INTO nintendo_play (user_id, device_id, day, app_id, seconds) VALUES (?,?,?,?,?)"
                                    " ON CONFLICT(user_id, device_id, day, app_id) DO UPDATE SET seconds=excluded.seconds",
                                    (uid, dev, s["date"], app_id, sec))
                daily_months = {x[0] for x in con.execute(
                    "SELECT DISTINCT substr(day, 1, 7) FROM nintendo_play WHERE user_id=? AND device_id=?"
                    " AND substr(day, 9, 2) != '00'", (uid, dev))}
                # Monthly summaries: Nintendo lists the last 12 months but keeps older ones too (since the console was
                # paired): walk back until it has no more. Months already stored are not fetched again, except the
                # last two. Each fetched month leaves a marker row (app "", 0 s) so that empty months count as stored.
                stored = {x[0] for x in con.execute(
                    "SELECT DISTINCT substr(day, 1, 7) FROM nintendo_play WHERE user_id=? AND device_id=?"
                    " AND substr(day, 9, 2) = '00'", (uid, dev))}
                listed = sorted(moon(f"/v1/devices/{dev}/monthly_summaries").get("indexes") or [], reverse=True)

                def month_summary(month):
                    r = client.get(f"{NX_MOON}/v1/devices/{dev}/monthly_summaries/{month}", headers=h)
                    return r.json() if r.status_code == 200 else None

                def store_month(month, m):
                    for t in m.get("playedApps") or []:
                        add_title(t)
                    con.execute("INSERT OR IGNORE INTO nintendo_play (user_id, device_id, day, app_id, seconds)"
                                " VALUES (?,?,?,'',0)", (uid, dev, f"{month}-00"))
                    if month in daily_months:
                        return
                    # per-game seconds from the time ranking, stored on day "YYYY-MM-00"
                    rank = ((m.get("insights") or {}).get("rankings") or {}).get("byTime") or []
                    for x in rank:
                        if x.get("units"):
                            con.execute("INSERT INTO nintendo_play (user_id, device_id, day, app_id, seconds) VALUES (?,?,?,?,?)"
                                        " ON CONFLICT(user_id, device_id, day, app_id) DO UPDATE SET seconds=excluded.seconds",
                                        (uid, dev, f"{month}-00", x["applicationId"], x["units"]))

                for i, month in enumerate(listed):
                    if i < 2 or month not in stored:
                        m = month_summary(month)
                        if m:
                            store_month(month, m)
                month, misses = (listed[-1] if listed else None), 0
                while month and misses < 3:
                    y, mo = map(int, month.split("-"))
                    month = f"{y - (mo == 1)}-{(mo - 2) % 12 + 1:02d}"
                    if month in stored:
                        break
                    m = month_summary(month)
                    if m:
                        store_month(month, m)
                        misses = 0
                    else:
                        misses += 1
        with db() as con:
            # a month total ("-00") counts only while that month has no daily rows
            rows = con.execute(
                "SELECT app_id, SUM(seconds) secs, MIN(day) first, MAX(day) last FROM nintendo_play p"
                " WHERE user_id=? AND seconds > 0 AND NOT (substr(day, 9, 2) = '00' AND EXISTS ("
                "   SELECT 1 FROM nintendo_play q WHERE q.user_id = p.user_id AND q.device_id = p.device_id"
                "   AND substr(q.day, 1, 7) = substr(p.day, 1, 7) AND substr(q.day, 9, 2) != '00'))"
                " GROUP BY app_id", (uid,)).fetchall()
            names = {r["ext_id"]: r for r in con.execute(
                "SELECT ext_id, title, image_url, first_played FROM candidates WHERE user_id=? AND source='nintendo'", (uid,))}
        for r in rows:
            last = r["last"].replace("-00", "-01") if r["last"].endswith("-00") else r["last"]   # month total only
            t = titles.get(r["app_id"]) or {}
            old = names.get(r["app_id"])
            title = t.get("title") or (old and old["title"]) or r["app_id"]
            img = (t.get("imageUri") or {}).get("large") or (old and old["image_url"])
            # Switch 2 games have ids starting with 04 (a Switch 1 game played on a Switch 2 stays "Switch")
            plat = "Switch 2" if r["app_id"].startswith("04") else "Switch"
            upsert(uid, {"source": "nintendo", "ext_id": r["app_id"], "title": title, "platform": plat,
                         "image_url": img, "hours": round(r["secs"] / 3600, 1), "ach_done": None, "ach_total": None,
                         # exact date from Nintendo when known (stored months are not fetched again), else the month
                         "first_played": min(filter(None, (t.get("firstPlayDate"), old and old["first_played"])),
                                             default=r["first"].replace("-00", "-01")),
                         "last_played": last,
                         "signal": "played", "signal_label": None, "signal_date": last,
                         "fingerprint": f"{r['secs']}|{r['last']}"})
    return msg("nintendo_ok", n=len(rows), d=len(devices))

# --- Ubisoft Connect (session copied from the browser, as the GOG Galaxy Uplay integration does) ---
# No OAuth code page for third parties: the user signs in on connect.ubisoft.com (with "Remember me") and pastes
# the session the site keeps in localStorage. Epilogg never sees the password: it renews the ticket on its own
# (PUT with the ticket, or POST with the rememberMe ticket when the ticket has expired).

# app id of the Ubisoft Connect web site (the old Galaxy one, b8fde481…, no longer reaches the games list)
UBI_APPID = "f35adcb5-1911-440c-b1c9-48fdc1701c68"
UBI_LOGIN_APPID, UBI_GENOME = "b8fde481-327d-4031-85ce-7c10a202a700", "fbd6791c-a6c6-4206-a75e-77234080b87b"
UBI_SESSIONS = "https://public-ubiservices.ubi.com/v3/profiles/sessions"
UBI_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/131.0.0.0 Safari/537.36")
UBI_LOGIN = (f"https://connect.ubisoft.com/login?appId={UBI_LOGIN_APPID}&genomeId={UBI_GENOME}&lang=en-US"
             "&nextUrl=https:%2F%2Fconnect.ubisoft.com%2Fready")
UBI_GAMES_QUERY = """query AllGames { viewer { id ownedGames: games(filterBy: {isOwned: true}) {
  totalCount nodes { id spaceId name } } } }"""
# where a game was played -> platform in the log ("crossplay" = cross-platform space, usually PC here)
UBI_PLATFORM = {"PC": "Ubisoft", "crossplay": "Ubisoft", "ORBIS": "PS4", "PROSPERO": "PS5", "DURANGO": "Xbox",
                "XboxScarlett": "Xbox", "NX": "Switch", "SWITCH2": "Switch 2"}


def ubi_headers(t=None) -> dict:
    h = {"Ubi-AppId": UBI_APPID, "User-Agent": UBI_UA, "Content-Type": "application/json",
         "Origin": "https://connect.ubisoft.com", "Referer": "https://connect.ubisoft.com/"}
    if t:
        h |= {"Authorization": f"Ubi_v1 t={t['ticket']}", "Ubi-SessionId": t.get("sessionId", "")}
    return h


def ubi_save(uid, j: dict, old: dict | None = None):
    old = old or {}
    set_cred(uid, "ubisoft_token", json.dumps({
        "ticket": j["ticket"], "sessionId": j.get("sessionId"), "userId": j.get("userId") or old.get("userId"),
        "rememberMeTicket": j.get("rememberMeTicket") or old.get("rememberMeTicket")}))


def ubi_link(uid, pasted: str):
    """pasted = what the browser console prints for PRODloginData and PRODrememberMe (one or more JSON objects)."""
    found = {}

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k in ("ticket", "sessionId", "userId", "rememberMeTicket") and isinstance(v, str) and v:
                    found.setdefault(k, v)
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
        elif isinstance(x, str) and x.strip()[:1] in "{[":
            try:
                walk(json.loads(x))
            except ValueError:
                pass

    dec, text, i = json.JSONDecoder(), pasted.strip(), 0
    while i < len(text):   # the paste may hold several JSON values one after the other
        if text[i] in "{[\"":
            try:
                obj, end = dec.raw_decode(text, i)
                walk(obj)
                i = end
                continue
            except ValueError:
                pass
        i += 1
    if not found.get("ticket") and not found.get("rememberMeTicket"):
        # what arrived, without values: length and the key names of any JSON in it
        names = sorted(set(re.findall(r'"([A-Za-z_][\w.-]{0,40})"\s*:', text)))
        log.warning("ubisoft link: no ticket in %d chars, starts with %r, keys %s", len(text),
                    re.sub(r"[A-Za-z0-9]", "x", text[:12]), names[:40])
        raise SyncError("ubisoft_link")
    log.warning("ubisoft link: found %s", sorted(found))   # names only
    t = ubi_refresh(found)   # checks right away that Ubisoft accepts the session
    ubi_save(uid, t, found)
    if not found.get("rememberMeTicket"):
        log.warning("ubisoft: no rememberMe ticket, the session will expire unless renewed every hour")


def ubi_refresh(t: dict) -> dict:
    """New ticket: from the current one (PUT), or from the rememberMe ticket (POST) when it has expired."""
    if t.get("ticket"):
        r = httpx.put(UBI_SESSIONS, headers=ubi_headers() | {"Authorization": f"Ubi_v1 t={t['ticket']}"}, timeout=30)
        if r.status_code == 200 and "ticket" in r.text:
            return r.json()
        log.warning("ubisoft ticket refresh: %s %s", r.status_code, ubi_err(r))
    if t.get("rememberMeTicket"):
        r = httpx.post(UBI_SESSIONS, headers=ubi_headers() | {"Authorization": f"rm_v1 t={t['rememberMeTicket']}"},
                       json={"rememberMe": True}, timeout=30)
        if r.status_code == 200 and "ticket" in r.text:
            return r.json()
        log.warning("ubisoft rememberMe refresh: %s %s", r.status_code, ubi_err(r))
    raise SyncError("ubisoft_expired")


def ubi_err(r) -> str:
    """Error fields of a Ubisoft response, never tickets."""
    try:
        j = r.json()
        return str({k: j.get(k) for k in ("errorCode", "message", "httpCode") if k in j})
    except ValueError:
        return r.text[:120] if "ticket" not in r.text else "(body hidden)"


def ubi_keepalive(uid) -> dict:
    old = token_of(creds(uid), "ubisoft_token")
    j = ubi_refresh(old)
    ubi_save(uid, j, old)
    return token_of(creds(uid), "ubisoft_token")


def ubi_minutes(cards) -> float | None:
    """Play time from the stats card, as the Galaxy integration reads it: a LongTimespan stat called playtime
    (or the one that looks like a total)."""
    factor = {"Hours": 1 / 60, "Minutes": 1, "Seconds": 60, "Miliseconds": 60000, "Milliseconds": 60000}
    spans = [c for c in cards if c.get("format") == "LongTimespan" and c.get("unit") in factor]
    if not spans:
        return None
    named = [c for c in spans if (c.get("displayName") or "").strip().lower() in
             ("playtime", "time played", "play time", "total play time", "total playtime")]
    best = named[0] if named else max(spans, key=lambda c: sum(w in (c.get("displayName") or "").lower()
                                                                  for w in ("all", "total", "absolute")))
    try:
        return float(best["value"]) / factor[best["unit"]]
    except (TypeError, ValueError):
        return None


def sync_ubisoft(uid, cr):
    """Games played (first/last played), names from the owned games and the application catalogue, play time from
    the stats card when the game publishes one."""
    t = ubi_keepalive(uid)
    with httpx.Client(timeout=30, headers=ubi_headers(t)) as client:
        def get(url, **params):
            r = client.get(url, params=params)
            if r.status_code in (401, 403):
                log.warning("ubisoft %s: %s %s", url.rsplit("/", 1)[-1], r.status_code, ubi_err(r))
                raise SyncError("ubisoft_expired")
            if r.status_code == 429:
                raise RateLimited("rate_ubisoft")
            return r
        played = get(f"https://public-ubiservices.ubi.com/v1/profiles/{t['userId']}/gamesplayed").json().get("gamesPlayed") or []
        names, owned = {}, 0
        r = client.post("https://public-ubiservices.ubi.com/v1/profiles/me/uplay/graphql",
                        json={"operationName": "AllGames", "query": UBI_GAMES_QUERY, "variables": {}})
        if r.status_code == 200:
            for g in ((((r.json().get("data") or {}).get("viewer") or {}).get("ownedGames") or {}).get("nodes") or []):
                names[g.get("spaceId")] = g.get("name")
                owned += 1
        spaces = [g["spaceId"] for g in played if g.get("spaceId")]
        for i in range(0, len(spaces), 50):   # catalogue names for games played but not owned (Ubisoft+, consoles)
            r = client.get("https://api-ubiservices.ubi.com/v2/applications", params={"spaceIds": ",".join(spaces[i:i + 50])})
            if r.status_code == 200:
                for a in r.json().get("applications") or []:
                    names.setdefault(a.get("spaceId"), a.get("displayName") or a.get("name"))
        have = known(uid, "ubisoft")
        for g in played:
            space = g.get("spaceId")
            first = day((g.get("firstPlayed") or {}).get("createdAt"))
            last = day((g.get("lastPlayed") or {}).get("updatedAt"))
            if not space or not last or not names.get(space):
                continue   # no name anywhere: the Ubisoft Connect launcher itself, not a game
            fp = f"{last}|{g.get('sessionsCount')}"
            if have.get(space) == fp:
                continue
            s = client.get(f"https://public-ubiservices.ubi.com/v1/profiles/{t['userId']}/statscard",
                           params={"spaceId": space}, headers={"Ubi-RequestedPlatformType": "uplay", "Ubi-LocaleCode": "en-GB"})
            cards = (s.json() or {}).get("Statscards") or [] if s.status_code == 200 else []   # 412: no stats
            minutes = ubi_minutes(cards)
            time.sleep(0.2)
            upsert(uid, {"source": "ubisoft", "ext_id": space, "title": names.get(space) or space,
                         "platform": UBI_PLATFORM.get(g.get("spacePlatformType"), "Ubisoft"),
                         "image_url": None, "hours": round(minutes / 60, 1) if minutes else None,
                         "ach_done": None, "ach_total": None, "first_played": first, "last_played": last,
                         "signal": "played", "signal_label": None, "signal_date": last, "fingerprint": fp})
    return msg("ubisoft_ok", n=owned, f=len(played))


# --- orchestration -------------------------------------------------------------

SYNC = {"steam": sync_steam, "xbox": sync_xbox, "psn": sync_psn, "ra": sync_ra, "gog": sync_gog, "epic": sync_epic,
        "nintendo": sync_nintendo, "ubisoft": sync_ubisoft}
SCHEDULED = tuple(SYNC)


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
            # Epic and Ubisoft sessions expire after a few hours without use: renew them every hour
            for src, keep in (("epic", epic_keepalive), ("ubisoft", ubi_keepalive)):
                if configured(uid)[src]:
                    try:
                        keep(uid)
                    except SyncError as e:
                        set_state(uid, src, "error", msg(e.key))
                    except Exception:   # noqa: BLE001 - network hiccup: try again next hour
                        log.warning("%s keepalive failed for user %s", src, uid)
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
