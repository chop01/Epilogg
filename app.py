"""Roll Credits — registro dei giochi completati: SQLite + API JSON + pagina unica (static/index.html).

Più utenti: ognuno ha registro, candidati e credenziali delle piattaforme suoi. Il primo utente (creato dalla
pagina al primo avvio) è admin e crea gli altri. Sessione con cookie; /api/stats accetta anche un token
(per il widget di Homepage).

Copertine e metadati da IGDB (serve un'app Twitch: IGDB_CLIENT_ID / IGDB_CLIENT_SECRET).
Le copertine vengono scaricate in /data/covers, così la pagina non dipende da servizi esterni.
"""
import csv
import io
import os
import sqlite3
import threading
import time
from difflib import SequenceMatcher
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

import connectors
from store import COVERS, DATA, assign_orphans, check_pw, db, hash_pw, init_db, norm, now, sha

STATIC = os.path.join(os.path.dirname(__file__), "static")
IGDB_ID = os.environ.get("IGDB_CLIENT_ID", "")
IGDB_SECRET = os.environ.get("IGDB_CLIENT_SECRET", "")


# --- IGDB ---------------------------------------------------------------------

_token = {"value": None, "exp": 0.0}


def igdb_enabled() -> bool:
    return bool(IGDB_ID and IGDB_SECRET)


def igdb_query(endpoint: str, body: str):
    if not igdb_enabled():
        raise HTTPException(503, "igdb_off")
    if not _token["value"] or time.time() > _token["exp"] - 300:
        r = httpx.post("https://id.twitch.tv/oauth2/token", params={
            "client_id": IGDB_ID, "client_secret": IGDB_SECRET, "grant_type": "client_credentials"}, timeout=15)
        r.raise_for_status()
        j = r.json()
        _token.update(value=j["access_token"], exp=time.time() + j["expires_in"])
    r = httpx.post(f"https://api.igdb.com/v4/{endpoint}", content=body, timeout=15, headers={
        "Client-ID": IGDB_ID, "Authorization": f"Bearer {_token['value']}"})
    if r.status_code == 429:  # limite 4 richieste/s
        time.sleep(1)
        return igdb_query(endpoint, body)
    r.raise_for_status()
    return r.json()


IGDB_FIELDS = "fields name,first_release_date,cover.image_id,platforms.abbreviation,parent_game,total_rating_count;"


def igdb_search(q: str, limit: int = 10, contains: bool = False):
    """Ricerca testuale di IGDB più una per nome (esatto, o "contiene" se contains=True): la ricerca testuale
    non trova titoli corti con caratteri accentati ("Öoo" -> solo "Ooo" e "oOo: Ascension")."""
    q = q.replace('"', "").strip()
    by_name = f'*"{q}"*' if contains else f'"{q}"'
    rows = igdb_query("games", f'{IGDB_FIELDS} where name ~ {by_name} & version_parent = null; limit {limit};')
    rows += igdb_query("games", f'search "{q}"; {IGDB_FIELDS} where version_parent = null; limit {limit};')
    out, seen = [], set()
    for g in rows:
        if g["id"] in seen:
            continue
        seen.add(g["id"])
        ts = g.get("first_release_date")
        out.append({
            "id": g["id"],
            "name": g["name"],
            "year": datetime.fromtimestamp(ts, timezone.utc).year if ts else None,
            "cover_id": (g.get("cover") or {}).get("image_id"),
            "platforms": [p.get("abbreviation") for p in g.get("platforms", []) if p.get("abbreviation")],
            "dlc": bool(g.get("parent_game")),
            "pop": g.get("total_rating_count") or 0,
        })
    return out


def download_cover(image_id: str) -> str:
    fname = f"{image_id}.jpg"
    path = os.path.join(COVERS, fname)
    if not os.path.exists(path):
        r = httpx.get(f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg", timeout=30)
        r.raise_for_status()
        with open(path + ".tmp", "wb") as f:
            f.write(r.content)
        os.replace(path + ".tmp", path)
    return fname


def apply_match(game_id: int, m: dict, manual: bool = False, score: float | None = None):
    cover = download_cover(m["cover_id"]) if m.get("cover_id") else None
    with db() as con:
        con.execute("UPDATE games SET igdb_id=?, igdb_name=?, release_year=?, cover=?, match_score=?, match_manual=?,"
                    " updated_at=? WHERE id=?",
                    (m["id"], m["name"], m["year"], cover, 1.0 if manual else score, int(manual), now(), game_id))


# piattaforma dell'app -> abbreviazioni IGDB compatibili (bonus nell'abbinamento)
PLATFORM_HINT = {"Steam": {"PC"}, "PC": {"PC"}, "Game Pass": {"PC", "Series X|S", "XONE"},
                 "PS5": {"PS5"}, "PS4": {"PS4"}, "PS3": {"PS3"}, "PS2": {"PS2"}, "PS1": {"PS1"},
                 "GBA": {"GBA"}, "Switch": {"Switch", "NS2", "Switch 2"},
                 "Xbox": {"XBOX", "X360", "XONE", "Series X|S"}, "Xbox 360": {"X360"},
                 "PS Vita": {"Vita"}, "PSP": {"PSP"}}


def similarity(title: str, name: str) -> float:
    t = norm(title)
    nums = lambda x: {w for w in x.split() if w.isdigit()}
    # numeri diversi = altro capitolo della serie (Spider-Man vs Spider-Man 2)
    penalty = 0.75 if nums(t) != nums(norm(name)) and not (nums(t) <= nums(norm(name.split(":")[0]))) else 1.0
    best = 0.0
    for n in {norm(name), norm(name.split(":")[0]), norm(name.split(" - ")[0])}:
        best = max(best, SequenceMatcher(None, t, n).ratio(),
                   SequenceMatcher(None, t.replace(" ", ""), n.replace(" ", "")).ratio())
    return best * penalty


def score(title: str, platform: str, r: dict) -> float:
    s = similarity(title, r["name"])
    if PLATFORM_HINT.get(platform, set()) & set(r["platforms"]):
        s += 0.08
    s += 0.1 * min(r["pop"], 300) / 300   # a parità di nome, il gioco più noto
    if r["dlc"]:
        s -= 0.15
    if not r["cover_id"]:
        s -= 0.05
    return s


def find_match(title: str, platform: str):
    """Miglior risultato IGDB e la sua somiglianza col titolo (0-1).
    Se la ricerca diretta non convince (refusi), riprova togliendo una parola per volta."""
    # 50 risultati: per titoli molto portati ("Prince of Persia") i primi 10 sono tutti versioni anni '90
    cands = {r["id"]: r for r in igdb_search(title, 50)}
    best = max(cands.values(), key=lambda r: score(title, platform, r), default=None)
    words = title.split()
    if (not best or similarity(title, best["name"]) < 0.85) and len(words) >= 2:
        for i in range(min(len(words), 6)):
            time.sleep(0.25)   # IGDB: max 4 richieste al secondo
            for r in igdb_search(" ".join(words[:i] + words[i + 1:])):
                cands.setdefault(r["id"], r)
        best = max(cands.values(), key=lambda r: score(title, platform, r), default=None)
    if not best:
        return None, 0.0
    sim = similarity(title, best["name"])
    return (best, sim) if sim >= 0.6 else (None, sim)


# --- API ----------------------------------------------------------------------

class GameIn(BaseModel):
    title: str = Field(min_length=1, max_length=300)
    platform: str = Field(min_length=1, max_length=50)
    completed_on: date | None = None
    rating: float | None = Field(default=None, ge=0, le=10)
    notes: str | None = Field(default=None, max_length=5000)
    hours: float | None = Field(default=None, ge=0, le=100000)   # manuale: ha la precedenza sulle piattaforme

    @field_validator("rating")
    @classmethod
    def half_steps(cls, v):
        return None if v is None else round(v * 2) / 2


class MatchIn(BaseModel):
    igdb_id: int


@asynccontextmanager
async def lifespan(_app):
    init_db()
    connectors.start_scheduler()
    yield


app = FastAPI(title="Roll Credits", docs_url=None, redoc_url=None, lifespan=lifespan)
init_db()


# --- utenti e sessioni ------------------------------------------------------------

COOKIE = "rc_session"
SESSION_DAYS = 180
OPEN_PATHS = ("/api/auth/", "/api/stats")   # il resto di /api e /covers richiede la sessione
_fails: dict = {}   # ip -> [orari dei login sbagliati] (anti forza bruta, in memoria)


def session_user(request: Request):
    tok = request.cookies.get(COOKIE)
    if not tok:
        return None
    with db() as con:
        r = con.execute("SELECT u.* FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token=? AND s.expires_at > ?",
                        (sha(tok), now())).fetchone()
    return dict(r) if r else None


@app.middleware("http")
async def auth(request: Request, call_next):
    p = request.url.path
    if (p.startswith("/api/") or p.startswith("/covers/")) and not p.startswith(OPEN_PATHS):
        u = session_user(request)
        if not u:
            return JSONResponse({"detail": "auth_required"}, status_code=401)
        request.state.user = u
    return await call_next(request)


def uid(request: Request) -> int:
    return request.state.user["id"]


def need_admin(request: Request):
    if not request.state.user["is_admin"]:
        raise HTTPException(403, "admin_only")


def public_user(u: dict) -> dict:
    return {"id": u["id"], "username": u["username"], "is_admin": bool(u["is_admin"]), "lang": u["lang"],
            "has_token": bool(u.get("api_token"))}


def start_session(request: Request, response: Response, user_id: int):
    import secrets
    tok = secrets.token_urlsafe(32)
    exp = datetime.now() + timedelta(days=SESSION_DAYS)
    with db() as con:
        con.execute("DELETE FROM sessions WHERE expires_at < ?", (now(),))
        con.execute("INSERT INTO sessions (token, user_id, expires_at) VALUES (?,?,?)",
                    (sha(tok), user_id, exp.isoformat(timespec="seconds")))
    response.set_cookie(COOKIE, tok, max_age=SESSION_DAYS * 86400, httponly=True,
                        secure=request.url.scheme == "https", samesite="lax")   # via Traefik sempre https


LANGS = ("en", "it")


class Credentials(BaseModel):
    username: str = Field(min_length=1, max_length=40, pattern=r"^[\w.\-]+$")
    password: str = Field(min_length=1, max_length=200)


class NewUser(Credentials):
    password: str = Field(min_length=8, max_length=200)
    is_admin: bool = False
    lang: str = "en"


@app.get("/api/auth/state")
def auth_state(request: Request):
    with db() as con:
        setup = con.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0
    u = None if setup else session_user(request)
    return {"setup": setup, "user": public_user(u) if u else None}


@app.post("/api/auth/setup")
def setup(body: NewUser, request: Request, response: Response):
    """Primo avvio: crea l'admin, gli assegna i dati già presenti e le credenziali del .env."""
    with db() as con:
        if con.execute("SELECT COUNT(*) FROM users").fetchone()[0]:
            raise HTTPException(409, "setup_done")
        cur = con.execute("INSERT INTO users (username, pw_hash, is_admin, lang, created_at) VALUES (?,?,1,?,?)",
                          (body.username, hash_pw(body.password), body.lang if body.lang in LANGS else "en", now()))
        new = cur.lastrowid
        assign_orphans(con, new)
        for k, v in connectors.ENV_CREDS.items():
            if v:
                con.execute("INSERT OR IGNORE INTO credentials (user_id, key, value) VALUES (?,?,?)", (new, k, v))
    old = os.path.join(DATA, "psn_token.json")
    if os.path.exists(old):
        os.replace(old, connectors.psn_token_file(new))
    start_session(request, response, new)
    return {"ok": True}


@app.post("/api/auth/login")
def login(body: Credentials, request: Request, response: Response):
    ip = request.client.host if request.client else "?"
    recent = [t for t in _fails.get(ip, []) if t > time.time() - 900]
    if len(recent) >= 8:
        raise HTTPException(429, "too_many")
    with db() as con:
        u = con.execute("SELECT * FROM users WHERE username=?", (body.username,)).fetchone()
    if not u or not check_pw(body.password, u["pw_hash"]):
        _fails[ip] = recent + [time.time()]
        time.sleep(1)
        raise HTTPException(401, "bad_login")
    _fails.pop(ip, None)
    start_session(request, response, u["id"])
    return public_user(dict(u))


@app.post("/api/auth/logout")
def logout(request: Request, response: Response):
    tok = request.cookies.get(COOKIE)
    if tok:
        with db() as con:
            con.execute("DELETE FROM sessions WHERE token=?", (sha(tok),))
    response.delete_cookie(COOKIE)
    return {"ok": True}


class MeIn(BaseModel):
    lang: str


@app.put("/api/me")
def update_me(body: MeIn, request: Request):
    if body.lang not in LANGS:
        raise HTTPException(400, "bad_lang")
    with db() as con:
        con.execute("UPDATE users SET lang=? WHERE id=?", (body.lang, uid(request)))
    return {"ok": True}


class PasswordIn(BaseModel):
    old: str = ""
    new: str = Field(min_length=8, max_length=200)


@app.post("/api/me/password")
def change_password(body: PasswordIn, request: Request):
    if not check_pw(body.old, request.state.user["pw_hash"]):
        raise HTTPException(400, "bad_password")
    with db() as con:
        con.execute("UPDATE users SET pw_hash=? WHERE id=?", (hash_pw(body.new), uid(request)))
        # le altre sessioni (altri dispositivi) decadono
        con.execute("DELETE FROM sessions WHERE user_id=? AND token != ?", (uid(request), sha(request.cookies[COOKIE])))
    return {"ok": True}


@app.get("/api/me/credentials")
def get_credentials(request: Request):
    """Solo quali sono impostate: i valori non tornano mai alla pagina."""
    with db() as con:
        have = {r["key"] for r in con.execute("SELECT key FROM credentials WHERE user_id=?", (uid(request),))}
    return {k: k in have for k in connectors.CRED_KEYS} | {"env_steam_key": bool(connectors.ENV_STEAM_KEY)}


@app.put("/api/me/credentials")
def set_credentials(body: dict[str, str | None], request: Request):
    """Chiave con stringa = nuovo valore, null o "" = cancella, chiave assente = invariata."""
    with db() as con:
        for k, v in body.items():
            if k not in connectors.CRED_KEYS:
                raise HTTPException(400, "bad_key")
            v = (v or "").strip()
            if len(v) > 500:
                raise HTTPException(400, "too_long")
            if v:
                con.execute("INSERT INTO credentials (user_id, key, value) VALUES (?,?,?) ON CONFLICT(user_id, key)"
                            " DO UPDATE SET value=excluded.value", (uid(request), k, v))
            else:
                con.execute("DELETE FROM credentials WHERE user_id=? AND key=?", (uid(request), k))
    return get_credentials(request)


@app.post("/api/me/token")
def new_token(request: Request):
    """Token per Homepage (legge solo /api/stats). Mostrato una volta sola."""
    import secrets
    tok = "rc_" + secrets.token_urlsafe(24)
    with db() as con:
        con.execute("UPDATE users SET api_token=? WHERE id=?", (sha(tok), uid(request)))
    return {"token": tok}


@app.delete("/api/me/token")
def drop_token(request: Request):
    with db() as con:
        con.execute("UPDATE users SET api_token=NULL WHERE id=?", (uid(request),))
    return {"ok": True}


@app.get("/api/users")
def list_users(request: Request):
    need_admin(request)
    with db() as con:
        rows = con.execute("SELECT u.*, (SELECT COUNT(*) FROM games g WHERE g.user_id=u.id) games FROM users u"
                           " ORDER BY u.id").fetchall()
    return [public_user(dict(r)) | {"games": r["games"]} for r in rows]


@app.post("/api/users", status_code=201)
def add_user(body: NewUser, request: Request):
    need_admin(request)
    try:
        with db() as con:
            con.execute("INSERT INTO users (username, pw_hash, is_admin, lang, created_at) VALUES (?,?,?,?,?)",
                        (body.username, hash_pw(body.password), int(body.is_admin),
                         body.lang if body.lang in LANGS else "en", now()))
    except sqlite3.IntegrityError:
        raise HTTPException(409, "user_exists")
    return list_users(request)


class ResetIn(BaseModel):
    password: str = Field(min_length=8, max_length=200)


@app.post("/api/users/{user_id}/password")
def reset_password(user_id: int, body: ResetIn, request: Request):
    need_admin(request)
    with db() as con:
        con.execute("UPDATE users SET pw_hash=? WHERE id=?", (hash_pw(body.password), user_id))
        con.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
    return {"ok": True}


@app.delete("/api/users/{user_id}", status_code=204)
def delete_user(user_id: int, request: Request):
    """Cancella l'utente con registro, candidati e credenziali (la pagina chiede conferma)."""
    need_admin(request)
    if user_id == uid(request):
        raise HTTPException(400, "self_delete")
    with db() as con:
        con.execute("DELETE FROM users WHERE id=?", (user_id,))
    tf = connectors.psn_token_file(user_id)
    if os.path.exists(tf):
        os.remove(tf)


@app.get("/api/stats")
def stats(request: Request):
    """Numeri per Homepage: sessione o "Authorization: Bearer <token>" creato nelle impostazioni."""
    u = session_user(request)
    if not u:
        h = request.headers.get("authorization", "")
        if h.lower().startswith("bearer ") and len(h) > 10:
            with db() as con:
                r = con.execute("SELECT * FROM users WHERE api_token=?", (sha(h[7:].strip()),)).fetchone()
            u = dict(r) if r else None
    if not u:
        return JSONResponse({"detail": "auth_required"}, status_code=401)
    y = str(date.today().year)
    with db() as con:
        g = con.execute("SELECT COUNT(*) total, SUM(substr(completed_on,1,4)=?) year, AVG(rating) avg"
                        " FROM games WHERE user_id=?", (y, u["id"])).fetchone()
        review = con.execute("SELECT COUNT(*) FROM candidates WHERE user_id=? AND status='new'"
                             " AND signal IN ('story', 'platinum', '100')", (u["id"],)).fetchone()[0]
    return {"total": g["total"], "year": g["year"] or 0, "avg_rating": round(g["avg"], 1) if g["avg"] else None,
            "to_review": review}


def row(r: sqlite3.Row) -> dict:
    return dict(r)


def platform_stats(con, user_id: int) -> dict:
    """game_id -> dati del candidato collegato più adatto: stessa piattaforma del gioco, altrimenti quello con più
    ore (un gioco può essere collegato a Steam e Xbox insieme)."""
    best = {}
    for c in con.execute("SELECT c.game_id, c.source, c.platform, c.hours, c.ach_done, c.ach_total, g.platform gp"
                         " FROM candidates c JOIN games g ON g.id = c.game_id WHERE g.user_id=?", (user_id,)):
        key = (c["platform"].lower() == c["gp"].lower(), c["hours"] or 0, c["ach_total"] or 0)
        if c["game_id"] not in best or key > best[c["game_id"]][0]:
            best[c["game_id"]] = (key, c)
    return {gid: {"plat_source": c["source"], "plat_hours": c["hours"], "ach_done": c["ach_done"],
                  "ach_total": c["ach_total"] or None}
            for gid, (_, c) in best.items()}


def with_stats(r: sqlite3.Row, stats: dict) -> dict:
    g = dict(r)
    g.update(stats.get(g["id"], {"plat_source": None, "plat_hours": None, "ach_done": None, "ach_total": None}))
    g["manual_hours"] = g["hours"]
    g["hours"] = g["hours"] if g["hours"] is not None else g["plat_hours"]
    return g


def games_of(user_id: int):
    with db() as con:
        rows = con.execute("SELECT * FROM games WHERE user_id=? ORDER BY completed_on DESC NULLS LAST, id DESC",
                           (user_id,)).fetchall()
        stats = platform_stats(con, user_id)
    return [with_stats(r, stats) for r in rows]


@app.get("/api/games")
def list_games(request: Request):
    return games_of(uid(request))


@app.get("/api/config")
def config(request: Request):
    return {"igdb": igdb_enabled(), "user": public_user(request.state.user)}


def create_game(user_id: int, g: GameIn, source: str = "manual") -> int:
    with db() as con:
        cur = con.execute(
            "INSERT INTO games (user_id, title, platform, completed_on, rating, notes, hours, source, created_at,"
            " updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (user_id, g.title.strip(), g.platform.strip(), g.completed_on and g.completed_on.isoformat(),
             g.rating, g.notes, g.hours, source, now(), now()))
        gid = cur.lastrowid
    if igdb_enabled():   # in background: la risposta non aspetta IGDB, la copertina compare poco dopo
        threading.Thread(target=match_new_game, args=(user_id, gid, g.title, g.platform), daemon=True).start()
    return gid


def match_new_game(user_id: int, gid: int, title: str, platform: str):
    try:
        m, sim = find_match(title, platform)
        if m:
            apply_match(gid, m, score=sim)
            connectors.link_existing(user_id)   # il nome IGDB può collegare altri candidati
    except Exception:
        pass  # la copertina si può abbinare dopo dalla scheda


@app.post("/api/games", status_code=201)
def add_game(g: GameIn, request: Request):
    return get_game(create_game(uid(request), g), request)


@app.get("/api/games/{gid}")
def get_game(gid: int, request: Request):
    with db() as con:
        r = con.execute("SELECT * FROM games WHERE id=? AND user_id=?", (gid, uid(request))).fetchone()
        stats = platform_stats(con, uid(request))
    if not r:
        raise HTTPException(404, "not_found")
    return with_stats(r, stats)


@app.put("/api/games/{gid}")
def update_game(gid: int, g: GameIn, request: Request):
    get_game(gid, request)
    with db() as con:
        con.execute("UPDATE games SET title=?, platform=?, completed_on=?, rating=?, notes=?, hours=?, updated_at=?"
                    " WHERE id=?",
                    (g.title.strip(), g.platform.strip(),
                     g.completed_on and g.completed_on.isoformat(), g.rating, g.notes, g.hours, now(), gid))
    return get_game(gid, request)


@app.delete("/api/games/{gid}", status_code=204)
def delete_game(gid: int, request: Request):
    get_game(gid, request)
    with db() as con:
        con.execute("DELETE FROM games WHERE id=?", (gid,))


@app.get("/api/igdb/search")
def search(q: str, platform: str = ""):
    """Risultati ordinati come l'abbinamento automatico (titolo, piattaforma, popolarità), i primi 24."""
    return sorted(igdb_search(q, 50, contains=True), key=lambda r: score(q, platform, r), reverse=True)[:24]


@app.post("/api/games/{gid}/match")
def match(gid: int, m: MatchIn, request: Request):
    get_game(gid, request)
    rows = igdb_query("games", f"fields name,first_release_date,cover.image_id; where id = {m.igdb_id};")
    if not rows:
        raise HTTPException(404, "not_found")
    g = rows[0]
    ts = g.get("first_release_date")
    apply_match(gid, {"id": g["id"], "name": g["name"],
                      "year": datetime.fromtimestamp(ts, timezone.utc).year if ts else None,
                      "cover_id": (g.get("cover") or {}).get("image_id")}, manual=True)
    return get_game(gid, request)


@app.post("/api/covers/auto")
def auto_covers(request: Request, redo: bool = False):
    """Abbina a IGDB i giochi senza abbinamento (redo=true: rifà anche quelli automatici, mai quelli scelti a mano).
    Gli abbinamenti incerti (somiglianza < 0.85) sono da controllare nella scheda."""
    where = "match_manual = 0" if redo else "igdb_id IS NULL"
    with db() as con:
        todo = con.execute(f"SELECT id, title, platform FROM games WHERE user_id=? AND {where}", (uid(request),)).fetchall()
    done, missing = 0, []
    for r in todo:
        m, sim = find_match(r["title"], r["platform"])
        if m:
            apply_match(r["id"], m, score=round(sim, 3))
            done += 1
        else:
            missing.append(r["title"])
        time.sleep(0.25)
    with db() as con:
        doubtful = con.execute("SELECT COUNT(*) FROM games WHERE user_id=? AND match_manual = 0 AND match_score < 0.85",
                               (uid(request),)).fetchone()[0]
    return {"matched": done, "not_found": missing, "doubtful": doubtful}


# --- piattaforme (Steam, Xbox, PSN) -------------------------------------------------

@app.get("/api/sync")
def sync_status(request: Request):
    u = uid(request)
    with db() as con:
        state = {r["source"]: dict(r) for r in con.execute("SELECT * FROM sync_state WHERE user_id=?", (u,))}
        counts = {r["source"]: dict(r) for r in con.execute(
            "SELECT source, COUNT(*) total, SUM(status='new') new, SUM(status='new' AND signal IS NOT NULL) signals,"
            " SUM(status='new' AND signal IN ('story', 'platinum', '100')) strong"
            " FROM candidates WHERE user_id=? GROUP BY source", (u,))}
    return {src: {"configured": ok, **(state.get(src) or {}), **(counts.get(src) or {})}
            for src, ok in connectors.configured(u).items()}


@app.post("/api/sync/{source}", status_code=202)
def sync_now(source: str, request: Request):
    if source not in connectors.SYNC:
        raise HTTPException(404, "not_found")
    if not connectors.configured(uid(request))[source]:
        raise HTTPException(400, "not_configured")
    connectors.run_async(uid(request), source)
    return {"started": True}


@app.get("/api/candidates")
def list_candidates(request: Request, status: str = "new"):
    with db() as con:
        rows = con.execute("SELECT * FROM candidates WHERE user_id=? AND status=? "
                           "ORDER BY COALESCE(signal_date, last_played) DESC", (uid(request), status)).fetchall()
    return [row(r) for r in rows]


@app.post("/api/candidates/{cid}/confirm")
def confirm_candidate(cid: int, g: GameIn, request: Request):
    u = uid(request)
    with db() as con:
        c = con.execute("SELECT * FROM candidates WHERE id=? AND user_id=?", (cid, u)).fetchone()
    if not c:
        raise HTTPException(404, "not_found")
    gid = create_game(u, g, source=c["source"])
    with db() as con:
        con.execute("UPDATE candidates SET status='confirmed', game_id=?, updated_at=? WHERE id=?", (gid, now(), cid))
    connectors.link_existing(u)   # lo stesso gioco su un'altra piattaforma risulta già registrato
    return get_game(gid, request)


@app.post("/api/candidates/{cid}/{action}")
def candidate_status(cid: int, action: str, request: Request):
    status = {"dismiss": "dismissed", "restore": "new"}.get(action)
    if not status:
        raise HTTPException(404, "not_found")
    with db() as con:
        con.execute("UPDATE candidates SET status=?, updated_at=? WHERE id=? AND user_id=?",
                    (status, now(), cid, uid(request)))
    return {"ok": True}


@app.get("/api/export.csv")
def export_csv(request: Request):
    buf = io.StringIO()
    w = csv.writer(buf)
    cols = ["completed_on", "title", "platform", "rating", "hours", "ach_done", "ach_total", "notes", "igdb_name", "igdb_id", "release_year"]
    w.writerow(cols)
    for g in games_of(uid(request)):
        w.writerow([g[c] for c in cols])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename=roll-credits-{date.today()}.csv"})


app.mount("/covers", StaticFiles(directory=COVERS), name="covers")
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))
