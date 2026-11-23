"""Connettori Steam, Xbox (OpenXBL) e PSN: riempiono la tabella candidates.

Le piattaforme non dicono "completato": registrano achievement/trofei con data. Per ogni gioco si cerca un
indizio di fine, in quest'ordine:
  story    - primo achievement sbloccato il cui testo parla di fine storia (complete the game, credits, ending…)
  platinum - platino PSN
  100      - tutti gli achievement
I candidati si confermano o scartano nell'app; il voto lo mette l'utente.

La sincronizzazione è incrementale: il dettaglio achievement di un gioco si riscarica solo se la sua
"impronta" (ultimo avvio, achievement sbloccati) è cambiata.
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

STEAM_KEY = os.environ.get("STEAM_API_KEY", "")
STEAM_ID = os.environ.get("STEAM_ID", "")
XBL_KEY = os.environ.get("XBL_API_KEY", "")
PSN_NPSSO = os.environ.get("PSN_NPSSO", "")
PSN_TOKEN_FILE = os.path.join(DATA, "psn_token.json")   # token di sessione PSN (si rinnova da solo)

MIN_MINUTES = 30   # sotto questa soglia un gioco Steam senza achievement non è nemmeno candidato

# Testo di un achievement che indica la fine della storia (le API sono interrogate in inglese).
# Verbo e oggetto devono essere attaccati: "completed every mini-game" o "completed the X Island story" non valgono.
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
    # "Complete <titolo del gioco>" / 'Cleared "Ghost Trick"'
    t = norm(title or "")
    m = re.search(rf"\b{_VERB} (?P<rest>.+)", norm(desc))
    rest = m and m.group("rest").strip()
    return bool(t and rest and len(rest) >= 4 and (t.startswith(rest) or rest.startswith(t)))


def day(ts) -> str | None:
    """Unix timestamp, datetime o stringa ISO -> YYYY-MM-DD (UTC)."""
    if not ts:
        return None
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, timezone.utc).date().isoformat()
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc).date().isoformat()
    return str(ts)[:10]


def end_signal(achs: list[dict], title: str, total: int = 0, platinum: dict | None = None):
    """achs: [{name, desc, date}] dei soli sbloccati. Ritorna (signal, label, date), dal più al meno affidabile:
    story (primo achievement di fine sbloccato), platinum, 100 (tutti), last (ultimo sbloccato: indizio debole,
    giusto ~9 volte su 10 sui giochi del registro, sbaglia se il gioco è stato ripreso dopo per il post-game)."""
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
        return "100", "Tutti gli achievement", last["date"]
    return "last", last["name"], last["date"]


# --- stato e scrittura --------------------------------------------------------

_locks = {s: threading.Lock() for s in ("steam", "xbox", "psn")}


class RateLimited(Exception):
    """Limite di richieste raggiunto: la sincronizzazione riprende al giro successivo."""


def configured() -> dict:
    return {"steam": bool(STEAM_KEY and STEAM_ID), "xbox": bool(XBL_KEY), "psn": bool(PSN_NPSSO)}


def set_state(source, status, message=None, ok=False):
    with db() as con:
        con.execute("INSERT INTO sync_state (source, last_run, status, message) VALUES (?,?,?,?) "
                    "ON CONFLICT(source) DO UPDATE SET last_run=excluded.last_run, status=excluded.status, "
                    "message=excluded.message", (source, now(), status, message))
        if ok:
            con.execute("UPDATE sync_state SET last_ok=? WHERE source=?", (now(), source))


def known(source) -> dict:
    """ext_id -> fingerprint dei candidati già scaricati."""
    with db() as con:
        return {r["ext_id"]: r["fingerprint"] for r in
                con.execute("SELECT ext_id, fingerprint FROM candidates WHERE source=?", (source,))}


def upsert(c: dict):
    """Inserisce/aggiorna un candidato; lo collega a un gioco già nel registro se il titolo coincide."""
    c = {k: c.get(k) for k in ("source", "ext_id", "title", "platform", "image_url", "hours", "ach_done",
                               "ach_total", "first_played", "last_played", "signal", "signal_label",
                               "signal_date", "fingerprint")}
    with db() as con:
        con.execute(
            "INSERT INTO candidates (source, ext_id, title, platform, image_url, hours, ach_done, ach_total,"
            " first_played, last_played, signal, signal_label, signal_date, fingerprint, updated_at)"
            " VALUES (:source,:ext_id,:title,:platform,:image_url,:hours,:ach_done,:ach_total,:first_played,"
            " :last_played,:signal,:signal_label,:signal_date,:fingerprint,:now)"
            " ON CONFLICT(source, ext_id) DO UPDATE SET title=excluded.title, image_url=excluded.image_url,"
            " hours=excluded.hours, ach_done=excluded.ach_done, ach_total=excluded.ach_total,"
            " first_played=excluded.first_played, last_played=excluded.last_played, signal=excluded.signal,"
            " signal_label=excluded.signal_label, signal_date=excluded.signal_date,"
            " fingerprint=excluded.fingerprint, updated_at=excluded.updated_at",
            {**c, "now": now()})
    link_existing()


def update_light(source, ext_id, **fields):
    """Aggiorna solo ore/ultimo avvio senza toccare il resto (impronta invariata)."""
    if not fields:
        return
    sets = ", ".join(f"{k}=?" for k in fields)
    with db() as con:
        con.execute(f"UPDATE candidates SET {sets}, updated_at=? WHERE source=? AND ext_id=?",
                    (*fields.values(), now(), source, ext_id))


SUBTITLE_RE = re.compile(r"\s*(:|&|\s[-\u2013\u2014]\s|\(|\u2122|\u00ae)")


def head(title: str) -> str:
    """Titolo senza sottotitolo: "Yakuza Kiwami 3 & Dark Ties" -> "yakuza kiwami 3"."""
    return norm(SUBTITLE_RE.split(title, maxsplit=1)[0])


def same_game(cand: str, known: str, loose: bool = False) -> bool:
    """Stesso titolo normalizzato. Con loose=True vale anche il titolo della piattaforma con un sottotitolo in più
    separato da ":", "&", " - " ("Yakuza Kiwami 3 & Dark Ties" ~ "yakuza kiwami 3"). Mai il contrario, né senza
    separatore: "The Stanley Parable" ≠ "...: Ultra Deluxe", "God of War" ≠ "God of War Ragnarök"."""
    a, b = norm(cand), norm(known)
    if not a or not b:
        return False
    return a == b or (loose and len(b) >= 6 and head(cand) == b)


def link_existing():
    """Candidati nuovi che corrispondono a un gioco del registro: passano a "già nel registro" e gli danno le ore.
    Prima le corrispondenze esatte (titolo o nome IGDB); il sottotitolo vale solo per i giochi che dalla stessa
    piattaforma non ne hanno già una esatta (MGS V: The Phantom Pain sì, Ground Zeroes no)."""
    with db() as con:
        games = [(g["id"], t) for g in con.execute("SELECT id, title, igdb_name FROM games")
                 for t in (g["title"], g["igdb_name"]) if t]
        for loose in (False, True):
            taken = {(r["game_id"], r["source"]) for r in
                     con.execute("SELECT game_id, source FROM candidates WHERE game_id IS NOT NULL")}
            for c in con.execute("SELECT id, title, source FROM candidates WHERE status='new' AND game_id IS NULL").fetchall():
                gid = next((gid for gid, t in games if same_game(c["title"], t, loose)
                            and not (loose and (gid, c["source"]) in taken)), None)
                if gid:
                    con.execute("UPDATE candidates SET status='confirmed', game_id=? WHERE id=?", (gid, c["id"]))
                    con.execute("UPDATE games SET hours=COALESCE(hours, (SELECT hours FROM candidates WHERE id=?))"
                                " WHERE id=?", (c["id"], gid))


# --- Steam ----------------------------------------------------------------------

def steam_get(client, path, **params):
    r = client.get(f"https://api.steampowered.com/{path}", params={"key": STEAM_KEY, **params})
    if r.status_code == 429:
        raise RateLimited("Steam: troppe richieste")
    return r


def steam_id(client) -> str:
    if STEAM_ID.isdigit() and len(STEAM_ID) == 17:
        return STEAM_ID
    r = steam_get(client, "ISteamUser/ResolveVanityURL/v1/", vanityurl=STEAM_ID).json()["response"]
    if r.get("success") != 1:
        raise RuntimeError(f"STEAM_ID '{STEAM_ID}' non trovato (serve lo SteamID64 o il nome del profilo)")
    return r["steamid"]


def sync_steam():
    with httpx.Client(timeout=30) as client:
        sid = steam_id(client)
        r = steam_get(client, "IPlayerService/GetOwnedGames/v1/", steamid=sid, include_appinfo=1,
                      include_played_free_games=1)
        r.raise_for_status()
        owned = r.json().get("response", {}).get("games")
        if owned is None:
            raise RuntimeError("Steam non restituisce la libreria: profilo e 'Dettagli di gioco' devono essere pubblici")
        have = known("steam")
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
            a = steam_get(client, "ISteamUserStats/GetPlayerAchievements/v1/", steamid=sid, appid=ext, l="english")
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
            upsert({"source": "steam", "ext_id": ext, "title": g.get("name") or ext, "platform": "Steam",
                    "image_url": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{ext}/library_600x900.jpg",
                    "hours": hours, "ach_done": len(achs), "ach_total": total,
                    "first_played": min((a["date"] for a in achs), default=None), "last_played": last,
                    "signal": sig, "signal_label": label, "signal_date": sdate, "fingerprint": fp})
    return f"{len(owned)} giochi in libreria, {fetched} aggiornati"


# --- Xbox (OpenXBL, piano gratuito: 150 richieste/ora) ---------------------------

def xbl_get(client, path):
    r = client.get(f"https://xbl.io/api/v2/{path}", headers={"X-Authorization": XBL_KEY, "Accept": "application/json",
                                                              "Accept-Language": "en-US"})
    if r.status_code == 429:
        raise RateLimited("OpenXBL: limite orario raggiunto")
    r.raise_for_status()
    rem = r.headers.get("X-RateLimit-Remaining")
    if rem is not None and rem.isdigit() and int(rem) < 3:
        raise RateLimited("OpenXBL: limite orario quasi raggiunto")
    j = r.json()
    return j.get("content", j) if isinstance(j, dict) else j   # OpenXBL incapsula in {"content", "code"}


def xbox_platform(devices) -> str:
    """devices = dove il gioco può girare, non dove è stato giocato: proposta modificabile alla conferma."""
    devices = set(devices or [])
    if devices & {"PC", "Win32"}:
        return "Game Pass"
    return "Xbox 360" if devices == {"Xbox360"} else "Xbox"


def sync_xbox():
    with httpx.Client(timeout=30) as client:
        titles = xbl_get(client, "achievements").get("titles", [])
        have = known("xbox")
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
                    j = xbl_get(client, f"achievements/title/{ext}" + (f"/{token}" if token else ""))
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
                sig, label = "100", "Tutto il gamerscore"
            upsert({"source": "xbox", "ext_id": ext, "title": t.get("name") or ext, "platform": xbox_platform(devices),
                    "image_url": t.get("displayImage"), "hours": None, "ach_done": done, "ach_total": total,
                    "first_played": min((a["date"] for a in achs if a["date"]), default=None), "last_played": day(last),
                    "signal": sig, "signal_label": label, "signal_date": sdate, "fingerprint": fp})
    return f"{len(titles)} titoli, {fetched} aggiornati"


# --- PSN (psnawp: NPSSO -> token che si rinnova; 1 richiesta ogni 3 s) ------------

PSN_PLATFORMS = ["PS5", "PS4", "PS3", "PSVITA", "PSPC"]
PSN_LABEL = {"PSVITA": "PS Vita", "PSPC": "PC"}


def psn_client():
    from psnawp_api import PSNAWP
    token = None
    if os.path.exists(PSN_TOKEN_FILE):
        with open(PSN_TOKEN_FILE) as f:
            token = json.load(f)
        if token.get("npsso_hash") != hash_npsso():
            token = None          # NPSSO cambiato nel .env: ripartire da quello
    psn = PSNAWP(PSN_NPSSO, token_response=token and token["token"])
    return psn


def hash_npsso() -> str:
    import hashlib
    return hashlib.sha256(PSN_NPSSO.encode()).hexdigest()[:16]


def save_psn_token(psn):
    tr = psn.authenticator.token_response
    if tr:
        tmp = PSN_TOKEN_FILE + ".tmp"
        with open(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
            json.dump({"npsso_hash": hash_npsso(), "token": dict(tr)}, f)
        os.replace(tmp, PSN_TOKEN_FILE)


def sync_psn():
    from psnawp_api.core.psnawp_exceptions import PSNAWPAuthenticationError, PSNAWPTooManyRequestsError
    from psnawp_api.models.trophies.trophy_constants import TrophyType
    try:
        psn = psn_client()
        me = psn.me()
        # ore di gioco (solo PS4/PS5), abbinate per titolo
        stats = {}
        for s in me.title_stats():
            if s.name:
                stats.setdefault(norm(s.name), s)
        have = known("psn")
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
                update_light("psn", ext, hours=hours)
                continue
            achs, plat_t = [], None
            if earned:
                from psnawp_api.models.trophies.trophy_constants import PlatformType
                for tr in me.trophies(ext, PlatformType(plat), include_progress=True, trophy_group_id="all"):
                    if tr.earned:
                        a = {"name": tr.trophy_name, "desc": tr.trophy_detail, "date": day(tr.earned_date_time)}
                        if tr.trophy_type == TrophyType.PLATINUM:
                            plat_t = a
                        elif tr.trophy_group_id in (None, "default"):   # niente DLC per l'indizio di fine
                            achs.append(a)
            fetched += 1
            sig, label, sdate = end_signal(achs, t.title_name or "", 0, plat_t)
            if sig in (None, "last") and total and earned >= total:
                sig, label, sdate = "100", "Tutti i trofei", day(t.last_updated_datetime)
            upsert({"source": "psn", "ext_id": ext, "title": t.title_name or ext, "platform": PSN_LABEL.get(plat, plat),
                    "image_url": t.title_icon_url, "hours": hours, "ach_done": earned, "ach_total": total,
                    "first_played": day(st.first_played_date_time) if st else None,
                    "last_played": day(st.last_played_date_time) if st else day(t.last_updated_datetime),
                    "signal": sig, "signal_label": label, "signal_date": sdate, "fingerprint": fp})
            save_psn_token(psn)
        save_psn_token(psn)
        return f"{len(titles)} giochi con trofei, {fetched} aggiornati"
    except PSNAWPAuthenticationError as e:
        raise RuntimeError("PSN: NPSSO scaduto o non valido, va rigenerato (vedi README)") from e
    except PSNAWPTooManyRequestsError as e:
        raise RateLimited("PSN: troppe richieste") from e


# --- orchestrazione ------------------------------------------------------------

SYNC = {"steam": sync_steam, "xbox": sync_xbox, "psn": sync_psn}


def run(source: str) -> bool:
    """Esegue una sincronizzazione se non è già in corso. Ritorna False se era già in corso."""
    lock = _locks[source]
    if not lock.acquire(blocking=False):
        return False
    try:
        set_state(source, "running", "in corso…")
        msg = SYNC[source]()
        set_state(source, "ok", msg, ok=True)
    except RateLimited as e:
        set_state(source, "partial", f"{e}: riprende tra un'ora")
    except Exception as e:   # noqa: BLE001 - l'errore va mostrato nell'app, non deve fermare lo scheduler
        log.exception("sync %s", source)
        set_state(source, "error", str(e)[:500])
    finally:
        lock.release()
    return True


def run_async(source: str):
    threading.Thread(target=run, args=(source,), daemon=True).start()


def scheduler():
    """Ogni ora: rifà le fonti configurate rimaste a metà (limite API) e una volta al giorno tutte."""
    time.sleep(60)
    while True:
        with db() as con:
            state = {r["source"]: dict(r) for r in con.execute("SELECT * FROM sync_state")}
        for src, ok in configured().items():
            if not ok:
                continue
            st = state.get(src) or {}
            last_ok = st.get("last_ok")
            stale = not last_ok or (datetime.now() - datetime.fromisoformat(last_ok)).total_seconds() > 20 * 3600
            if st.get("status") in ("partial",) or stale:
                run(src)
        time.sleep(3600)


def start_scheduler():
    with db() as con:   # un riavvio a metà sincronizzazione lascia "running": non è più vero
        con.execute("UPDATE sync_state SET status='error', message='interrotta dal riavvio' WHERE status='running'")
    threading.Thread(target=scheduler, daemon=True).start()
