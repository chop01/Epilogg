"""Registro dei giochi completati: SQLite + API JSON + pagina unica (static/index.html).

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
from datetime import date, datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

import connectors
from store import COVERS, db, init_db, norm, now

STATIC = os.path.join(os.path.dirname(__file__), "static")
IGDB_ID = os.environ.get("IGDB_CLIENT_ID", "")
IGDB_SECRET = os.environ.get("IGDB_CLIENT_SECRET", "")


# --- IGDB ---------------------------------------------------------------------

_token = {"value": None, "exp": 0.0}


def igdb_enabled() -> bool:
    return bool(IGDB_ID and IGDB_SECRET)


def igdb_query(endpoint: str, body: str):
    if not igdb_enabled():
        raise HTTPException(503, "IGDB non configurato (IGDB_CLIENT_ID / IGDB_CLIENT_SECRET nel .env)")
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


app = FastAPI(title="Giochi", docs_url=None, redoc_url=None, lifespan=lifespan)
init_db()


def row(r: sqlite3.Row) -> dict:
    return dict(r)


@app.get("/api/games")
def list_games():
    with db() as con:
        rows = con.execute("SELECT * FROM games ORDER BY completed_on DESC NULLS LAST, id DESC").fetchall()
    return [row(r) for r in rows]


@app.get("/api/config")
def config():
    return {"igdb": igdb_enabled()}


def create_game(g: GameIn, source: str = "manual", hours: float | None = None) -> int:
    with db() as con:
        cur = con.execute(
            "INSERT INTO games (title, platform, completed_on, rating, notes, hours, source, created_at,"
            " updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (g.title.strip(), g.platform.strip(), g.completed_on and g.completed_on.isoformat(),
             g.rating, g.notes, hours, source, now(), now()))
        gid = cur.lastrowid
    if igdb_enabled():   # in background: la risposta non aspetta IGDB, la copertina compare poco dopo
        threading.Thread(target=match_new_game, args=(gid, g.title, g.platform), daemon=True).start()
    return gid


def match_new_game(gid: int, title: str, platform: str):
    try:
        m, sim = find_match(title, platform)
        if m:
            apply_match(gid, m, score=sim)
            connectors.link_existing()   # il nome IGDB può collegare altri candidati
    except Exception:
        pass  # la copertina si può abbinare dopo dalla scheda


@app.post("/api/games", status_code=201)
def add_game(g: GameIn):
    return get_game(create_game(g))


@app.get("/api/games/{gid}")
def get_game(gid: int):
    with db() as con:
        r = con.execute("SELECT * FROM games WHERE id=?", (gid,)).fetchone()
    if not r:
        raise HTTPException(404)
    return row(r)


@app.put("/api/games/{gid}")
def update_game(gid: int, g: GameIn):
    get_game(gid)
    with db() as con:
        con.execute("UPDATE games SET title=?, platform=?, completed_on=?, rating=?, notes=?, updated_at=?"
                    " WHERE id=?",
                    (g.title.strip(), g.platform.strip(),
                     g.completed_on and g.completed_on.isoformat(), g.rating, g.notes, now(), gid))
    return get_game(gid)


@app.delete("/api/games/{gid}", status_code=204)
def delete_game(gid: int):
    get_game(gid)
    with db() as con:
        con.execute("DELETE FROM games WHERE id=?", (gid,))


@app.get("/api/igdb/search")
def search(q: str, platform: str = ""):
    """Risultati ordinati come l'abbinamento automatico (titolo, piattaforma, popolarità), i primi 24."""
    return sorted(igdb_search(q, 50, contains=True), key=lambda r: score(q, platform, r), reverse=True)[:24]


@app.post("/api/games/{gid}/match")
def match(gid: int, m: MatchIn):
    get_game(gid)
    rows = igdb_query("games", f"fields name,first_release_date,cover.image_id; where id = {m.igdb_id};")
    if not rows:
        raise HTTPException(404, "gioco IGDB non trovato")
    g = rows[0]
    ts = g.get("first_release_date")
    apply_match(gid, {"id": g["id"], "name": g["name"],
                      "year": datetime.fromtimestamp(ts, timezone.utc).year if ts else None,
                      "cover_id": (g.get("cover") or {}).get("image_id")}, manual=True)
    return get_game(gid)


@app.post("/api/covers/auto")
def auto_covers(redo: bool = False):
    """Abbina a IGDB i giochi senza abbinamento (redo=true: rifà anche quelli automatici, mai quelli scelti a mano).
    Gli abbinamenti incerti (somiglianza < 0.85) sono da controllare nella scheda."""
    where = "match_manual = 0" if redo else "igdb_id IS NULL"
    with db() as con:
        todo = con.execute(f"SELECT id, title, platform FROM games WHERE {where}").fetchall()
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
        doubtful = con.execute("SELECT COUNT(*) FROM games WHERE match_manual = 0 AND match_score < 0.85").fetchone()[0]
    return {"matched": done, "not_found": missing, "doubtful": doubtful}


# --- piattaforme (Steam, Xbox, PSN) -------------------------------------------------

@app.get("/api/sync")
def sync_status():
    with db() as con:
        state = {r["source"]: dict(r) for r in con.execute("SELECT * FROM sync_state")}
        counts = {r["source"]: dict(r) for r in con.execute(
            "SELECT source, COUNT(*) total, SUM(status='new') new, SUM(status='new' AND signal IS NOT NULL) signals,"
            " SUM(status='new' AND signal IN ('story', 'platinum', '100')) strong"
            " FROM candidates GROUP BY source")}
    return {src: {"configured": ok, **(state.get(src) or {}), **(counts.get(src) or {})}
            for src, ok in connectors.configured().items()}


@app.post("/api/sync/{source}", status_code=202)
def sync_now(source: str):
    if source not in connectors.SYNC:
        raise HTTPException(404)
    if not connectors.configured()[source]:
        raise HTTPException(400, f"{source}: credenziali mancanti nel .env")
    connectors.run_async(source)
    return {"started": True}


@app.get("/api/candidates")
def list_candidates(status: str = "new"):
    with db() as con:
        rows = con.execute("SELECT * FROM candidates WHERE status=? "
                           "ORDER BY COALESCE(signal_date, last_played) DESC", (status,)).fetchall()
    return [row(r) for r in rows]


@app.post("/api/candidates/{cid}/confirm")
def confirm_candidate(cid: int, g: GameIn):
    with db() as con:
        c = con.execute("SELECT * FROM candidates WHERE id=?", (cid,)).fetchone()
    if not c:
        raise HTTPException(404)
    gid = create_game(g, source=c["source"], hours=c["hours"])
    with db() as con:
        con.execute("UPDATE candidates SET status='confirmed', game_id=?, updated_at=? WHERE id=?", (gid, now(), cid))
    connectors.link_existing()   # lo stesso gioco su un'altra piattaforma risulta già registrato
    return get_game(gid)


@app.post("/api/candidates/{cid}/{action}")
def candidate_status(cid: int, action: str):
    status = {"dismiss": "dismissed", "restore": "new"}.get(action)
    if not status:
        raise HTTPException(404)
    with db() as con:
        con.execute("UPDATE candidates SET status=?, updated_at=? WHERE id=?", (status, now(), cid))
    return {"ok": True}


@app.get("/api/export.csv")
def export_csv():
    buf = io.StringIO()
    w = csv.writer(buf)
    cols = ["completed_on", "title", "platform", "rating", "hours", "notes", "igdb_name", "igdb_id", "release_year"]
    w.writerow(cols)
    for g in list_games():
        w.writerow([g[c] for c in cols])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename=giochi-{date.today()}.csv"})


app.mount("/covers", StaticFiles(directory=COVERS), name="covers")
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))
