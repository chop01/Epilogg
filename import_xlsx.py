"""Import una tantum del vecchio foglio Excel (colonne A-D: Data, Titolo, piattaforma, voto).

Uso: docker compose run --rm -v /mnt/games/giochi.xlsx:/import/giochi.xlsx:ro games \
        python /app/import_xlsx.py /import/giochi.xlsx [utente]   (default: il primo admin)
Rilanciabile: salta le righe già presenti (stesso titolo e stessa data).
"""
import sys
from datetime import datetime

import openpyxl

from store import db, init_db, now

# piattaforma nel foglio -> nome nell'app; "(emulato)" finisce nelle note
PLATFORMS = {"steam": "Steam", "pc": "PC", "gamepass": "Game Pass", "switch": "Switch",
             "ps5": "PS5", "ps4": "PS4", "ps3": "PS3", "ps2": "PS2", "psx": "PS1", "gba": "GBA"}


def platform(raw: str):
    raw = (raw or "").strip().lower()
    emulated = "emulat" in raw
    key = raw.split("(")[0].strip()
    return PLATFORMS.get(key, key.upper() if len(key) <= 4 else key.title()), emulated


def main(path, username=None):
    init_db()
    ws = openpyxl.load_workbook(path, data_only=True).worksheets[0]
    added = skipped = 0
    with db() as con:
        u = con.execute("SELECT id FROM users WHERE username=?" if username else
                        "SELECT id FROM users WHERE is_admin=1 ORDER BY id", (username,) if username else ()).fetchone()
        if not u:
            sys.exit("utente non trovato (al primo avvio crearlo dalla pagina)")
        uid = u["id"]
        for r in ws.iter_rows(min_row=2, max_col=4, values_only=True):
            when, title, plat, rating = r
            if not title:
                continue
            d = when.date().isoformat() if isinstance(when, datetime) else None
            p, emu = platform(plat)
            if con.execute("SELECT 1 FROM games WHERE user_id=? AND title=? AND completed_on IS ?",
                           (uid, title.strip(), d)).fetchone():
                skipped += 1
                continue
            con.execute("INSERT INTO games (user_id, title, platform, notes, completed_on, rating, source, created_at,"
                        " updated_at) VALUES (?,?,?,?,?,?, 'excel', ?, ?)",
                        (uid, title.strip(), p, "Emulato" if emu else None, d, rating, now(), now()))
            added += 1
    print(f"importati {added}, già presenti {skipped}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
