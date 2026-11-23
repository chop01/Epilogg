"""Database SQLite condiviso da app, import e connettori."""
import os
import re
import sqlite3
import unicodedata
from contextlib import contextmanager
from datetime import datetime

DATA = os.environ.get("DATA_DIR", "/data")
DB = os.path.join(DATA, "games.db")
COVERS = os.path.join(DATA, "covers")

SCHEMA = """
CREATE TABLE IF NOT EXISTS games (
    id           INTEGER PRIMARY KEY,
    title        TEXT NOT NULL,
    platform     TEXT NOT NULL,
    emulated     INTEGER NOT NULL DEFAULT 0,
    completed_on TEXT,              -- YYYY-MM-DD
    rating       REAL,              -- 0-10, passi da 0.5
    hours        REAL,              -- ore giocate (dalle piattaforme, se note)
    notes        TEXT,
    igdb_id      INTEGER,
    igdb_name    TEXT,
    release_year INTEGER,
    cover        TEXT,              -- nome file in /data/covers
    match_score  REAL,              -- affidabilità dell'abbinamento automatico (1 = certo)
    match_manual INTEGER NOT NULL DEFAULT 0,  -- 1 = scelto a mano: l'abbinamento automatico non lo tocca
    source       TEXT NOT NULL DEFAULT 'manual',
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS games_completed ON games(completed_on);

-- Giochi trovati sulle piattaforme (Steam, Xbox, PSN): da confermare o scartare nell'app
CREATE TABLE IF NOT EXISTS candidates (
    id           INTEGER PRIMARY KEY,
    source       TEXT NOT NULL,     -- steam | xbox | psn
    ext_id       TEXT NOT NULL,     -- appid / titleId / npCommunicationId
    title        TEXT NOT NULL,
    platform     TEXT NOT NULL,     -- proposta per il registro (Steam, Game Pass, Xbox, PS5…)
    image_url    TEXT,
    hours        REAL,
    ach_done     INTEGER,
    ach_total    INTEGER,
    first_played TEXT,
    last_played  TEXT,
    signal       TEXT,              -- story | platinum | 100 | NULL (nessun indizio di fine)
    signal_label TEXT,              -- nome dell'achievement/trofeo usato come indizio
    signal_date  TEXT,              -- YYYY-MM-DD
    fingerprint  TEXT,              -- se non cambia, il dettaglio achievement non viene riscaricato
    status       TEXT NOT NULL DEFAULT 'new',   -- new | confirmed | dismissed
    game_id      INTEGER REFERENCES games(id) ON DELETE SET NULL,
    updated_at   TEXT NOT NULL,
    UNIQUE (source, ext_id)
);

CREATE TABLE IF NOT EXISTS sync_state (
    source   TEXT PRIMARY KEY,
    last_run TEXT,
    last_ok  TEXT,
    status   TEXT,                  -- ok | running | error | partial (limite API: riprende alla prossima ora)
    message  TEXT
);
"""

MIGRATIONS = {"games": (("match_score", "REAL"), ("match_manual", "INTEGER NOT NULL DEFAULT 0"), ("hours", "REAL"))}


@contextmanager
def db():
    con = sqlite3.connect(DB, timeout=30)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db():
    os.makedirs(COVERS, exist_ok=True)
    with db() as con:
        con.executescript(SCHEMA)
        for table, cols in MIGRATIONS.items():
            have = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
            for col, ddl in cols:
                if col not in have:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


def now():
    return datetime.now().isoformat(timespec="seconds")


ROMAN = {"ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6", "vii": "7", "viii": "8", "ix": "9", "x": "10"}


def norm(s: str) -> str:
    """Titolo normalizzato per confronti: minuscolo, senza accenti/punteggiatura/marchi, numeri romani in cifre."""
    s = re.sub("[\u2122\u00ae\u00a9]", "", s)   # marchi
    s = unicodedata.normalize("NFKD", s.replace("&", " and ")).encode("ascii", "ignore").decode()
    s = re.sub(r"['’´`]", "", s.lower())
    return " ".join(ROMAN.get(w, w) for w in re.sub(r"[^a-z0-9]+", " ", s).split())
