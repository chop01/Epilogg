"""SQLite database shared by the app, the importer and the connectors."""
import hashlib
import os
import re
import secrets
import sqlite3
import unicodedata
from contextlib import contextmanager
from datetime import datetime

DATA = os.environ.get("DATA_DIR", "/data")
DB = os.path.join(DATA, "games.db")
COVERS = os.path.join(DATA, "covers")

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id           INTEGER PRIMARY KEY,
    username     TEXT NOT NULL UNIQUE COLLATE NOCASE,
    pw_hash      TEXT NOT NULL,
    is_admin     INTEGER NOT NULL DEFAULT 0,
    lang         TEXT NOT NULL DEFAULT 'en',
    api_token    TEXT,              -- sha256 of the dashboard token (read-only /api/stats)
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    token        TEXT PRIMARY KEY,  -- sha256 of the cookie
    user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at   TEXT NOT NULL
);

-- Connector credentials, per user: steam_id, steam_key, xbl_key, psn_npsso
CREATE TABLE IF NOT EXISTS credentials (
    user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    key          TEXT NOT NULL,
    value        TEXT NOT NULL,
    PRIMARY KEY (user_id, key)
);

CREATE TABLE IF NOT EXISTS games (
    id           INTEGER PRIMARY KEY,
    user_id      INTEGER REFERENCES users(id) ON DELETE CASCADE,
    title        TEXT NOT NULL,
    platform     TEXT NOT NULL,
    emulated     INTEGER NOT NULL DEFAULT 0,
    completed_on TEXT,              -- YYYY-MM-DD
    rating       REAL,              -- 0-10, in 0.5 steps
    hours        REAL,              -- hours played, entered by hand (platform hours come from candidates)
    notes        TEXT,
    igdb_id      INTEGER,
    igdb_name    TEXT,
    release_year INTEGER,
    cover        TEXT,              -- file name in /data/covers
    match_score  REAL,              -- confidence of the automatic IGDB match (1 = certain)
    match_manual INTEGER NOT NULL DEFAULT 0,  -- 1 = picked by hand: automatic matching leaves it alone
    source       TEXT NOT NULL DEFAULT 'manual',
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS games_completed ON games(completed_on);
CREATE INDEX IF NOT EXISTS games_user ON games(user_id);

-- Games found on the platforms (Steam, Xbox, PSN): confirmed or dismissed in the app
CREATE TABLE IF NOT EXISTS candidates (
    id           INTEGER PRIMARY KEY,
    user_id      INTEGER REFERENCES users(id) ON DELETE CASCADE,
    source       TEXT NOT NULL,     -- steam | xbox | psn
    ext_id       TEXT NOT NULL,     -- appid / titleId / npCommunicationId
    title        TEXT NOT NULL,
    platform     TEXT NOT NULL,     -- suggested platform for the log (Steam, Game Pass, Xbox, PS5…)
    image_url    TEXT,
    hours        REAL,
    ach_done     INTEGER,
    ach_total    INTEGER,
    first_played TEXT,
    last_played  TEXT,
    signal       TEXT,              -- story | platinum | 100 | last | NULL (no ending hint)
    signal_label TEXT,              -- name of the achievement/trophy used as the hint
    signal_date  TEXT,              -- YYYY-MM-DD
    fingerprint  TEXT,              -- if unchanged, achievements are not downloaded again
    status       TEXT NOT NULL DEFAULT 'new',   -- new | confirmed | dismissed
    game_id      INTEGER REFERENCES games(id) ON DELETE SET NULL,
    updated_at   TEXT NOT NULL,
    UNIQUE (user_id, source, ext_id)
);

-- Nintendo play time per day (the Parental Controls app keeps only a few weeks): the history builds up here.
-- day "YYYY-MM-00" = monthly total, used only for months without daily rows
CREATE TABLE IF NOT EXISTS nintendo_play (
    user_id   INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id TEXT NOT NULL,
    day       TEXT NOT NULL,
    app_id    TEXT NOT NULL,
    seconds   INTEGER NOT NULL,
    PRIMARY KEY (user_id, device_id, day, app_id)
);

CREATE TABLE IF NOT EXISTS sync_state (
    user_id  INTEGER REFERENCES users(id) ON DELETE CASCADE,
    source   TEXT NOT NULL,
    last_run TEXT,
    last_ok  TEXT,
    status   TEXT,                  -- ok | running | error | partial (API rate limit: resumes next hour)
    message  TEXT,                  -- JSON {"k": text key, "p": params}, translated by the page
    alerted  TEXT,                  -- problem already notified on Telegram: error | stale | NULL
    PRIMARY KEY (user_id, source)
);
"""

MIGRATIONS = {"games": (("match_score", "REAL"), ("match_manual", "INTEGER NOT NULL DEFAULT 0"), ("hours", "REAL"),
                        ("user_id", "INTEGER REFERENCES users(id) ON DELETE CASCADE")),
              "sync_state": (("alerted", "TEXT"),)}
# tables rebuilt to add user_id to their key (multi-user migration)
REBUILD = ("candidates", "sync_state")


@contextmanager
def db():
    con = sqlite3.connect(DB, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db():
    os.makedirs(COVERS, exist_ok=True)
    con = sqlite3.connect(DB, timeout=30)   # no foreign_keys while rebuilding
    try:
        old = [t for t in REBUILD if con.execute("SELECT 1 FROM sqlite_master WHERE name=?", (t,)).fetchone()
               and "user_id" not in {r[1] for r in con.execute(f"PRAGMA table_info({t})")}]
        for t in old:
            con.execute(f"ALTER TABLE {t} RENAME TO {t}_old")
        con.executescript(SCHEMA.split("CREATE TABLE IF NOT EXISTS games")[0])   # users, sessions, credentials
        if con.execute("SELECT 1 FROM sqlite_master WHERE name='games'").fetchone():
            have = {r[1] for r in con.execute("PRAGMA table_info(games)")}
            if "user_id" not in have:
                con.execute("ALTER TABLE games ADD COLUMN user_id INTEGER REFERENCES users(id) ON DELETE CASCADE")
        con.executescript(SCHEMA)
        for t in old:   # existing rows have no user yet: the first admin gets them (see assign_orphans)
            cols = ", ".join(r[1] for r in con.execute(f"PRAGMA table_info({t}_old)"))
            con.execute(f"INSERT INTO {t} ({cols}) SELECT {cols} FROM {t}_old")
            con.execute(f"DROP TABLE {t}_old")
        con.commit()
    finally:
        con.close()
    with db() as con:
        for table, cols in MIGRATIONS.items():
            have = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
            for col, ddl in cols:
                if col not in have:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


def assign_orphans(con, uid: int):
    """Give rows from before multi-user (user_id NULL) to the first admin."""
    for t in ("games", "candidates", "sync_state"):
        con.execute(f"UPDATE {t} SET user_id=? WHERE user_id IS NULL", (uid,))


# --- passwords and tokens ---------------------------------------------------------

def hash_pw(pw: str) -> str:
    salt = secrets.token_bytes(16)
    h = hashlib.scrypt(pw.encode(), salt=salt, n=2 ** 14, r=8, p=1)
    return f"scrypt$16384$8$1${salt.hex()}${h.hex()}"


def check_pw(pw: str, stored: str) -> bool:
    try:
        _, n, r, p, salt, h = stored.split("$")
        got = hashlib.scrypt(pw.encode(), salt=bytes.fromhex(salt), n=int(n), r=int(r), p=int(p))
        return secrets.compare_digest(got.hex(), h)
    except ValueError:
        return False


def sha(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def now():
    return datetime.now().isoformat(timespec="seconds")


ROMAN = {"ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6", "vii": "7", "viii": "8", "ix": "9", "x": "10"}


def norm(s: str) -> str:
    """Title normalized for comparisons: lowercase, no accents/punctuation/trademarks, roman numerals as digits."""
    s = re.sub("[\u2122\u00ae\u00a9]", "", s)   # trademarks
    s = unicodedata.normalize("NFKD", s.replace("&", " and ")).encode("ascii", "ignore").decode()
    s = re.sub(r"['’´`]", "", s.lower())
    return " ".join(ROMAN.get(w, w) for w in re.sub(r"[^a-z0-9]+", " ", s).split())


if __name__ == "__main__":
    # Forgotten password: docker compose exec -it epilogg python store.py reset-password <username>
    import getpass
    import sys
    if len(sys.argv) != 3 or sys.argv[1] != "reset-password":
        sys.exit("usage: python store.py reset-password <username>")
    pw = getpass.getpass("New password (at least 8 characters): ")
    if len(pw) < 8:
        sys.exit("too short")
    with db() as con:
        n = con.execute("UPDATE users SET pw_hash=? WHERE username=?", (hash_pw(pw), sys.argv[2])).rowcount
        con.execute("DELETE FROM sessions WHERE user_id IN (SELECT id FROM users WHERE username=?)", (sys.argv[2],))
    print("done" if n else "user not found")
