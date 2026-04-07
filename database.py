"""
database.py – SQLite-opsætning og queries.
"""

import sqlite3
from typing import Any


def init_db(db_path: str) -> sqlite3.Connection:
    """Opretter database og tabeller hvis de ikke eksisterer."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cyclists (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            wikidata_id  TEXT UNIQUE,
            name         TEXT,
            gender       TEXT,
            birthdate    TEXT,
            birthplace   TEXT,
            latitude     REAL,
            longitude    REAL,
            pcs_id       TEXT,
            weight_kg    REAL,
            height_m     REAL,
            total_wins    INTEGER DEFAULT 0,
            current_team  TEXT,
            wikipedia_url TEXT,
            fetched_at    TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            cyclist_id  INTEGER NOT NULL REFERENCES cyclists(id),
            date        TEXT,
            rank        INTEGER,
            race_name   TEXT,
            race_url    TEXT,
            race_class  TEXT,
            pcs_points  INTEGER,
            UNIQUE(cyclist_id, date, race_url)
        )
    """)
    conn.commit()
    return conn


def upsert_cyclist(conn: sqlite3.Connection, data: dict[str, Any]) -> None:
    """Indsætter eller opdaterer en rytter (INSERT OR REPLACE)."""
    conn.execute("""
        INSERT OR REPLACE INTO cyclists
            (wikidata_id, name, gender, birthdate, birthplace,
             latitude, longitude, pcs_id, weight_kg, height_m,
             total_wins, current_team, wikipedia_url, fetched_at)
        VALUES
            (:wikidata_id, :name, :gender, :birthdate, :birthplace,
             :latitude, :longitude, :pcs_id, :weight_kg, :height_m,
             :total_wins, :current_team, :wikipedia_url, :fetched_at)
    """, data)
    conn.commit()


def insert_results(conn: sqlite3.Connection, cyclist_id: int, results: list[dict]) -> None:
    """Indsætter top-3 resultater for en rytter (ignorerer dubletter)."""
    conn.executemany("""
        INSERT OR IGNORE INTO results
            (cyclist_id, date, rank, race_name, race_url, race_class, pcs_points)
        VALUES
            (:cyclist_id, :date, :rank, :race_name, :race_url, :race_class, :pcs_points)
    """, [{"cyclist_id": cyclist_id, **r} for r in results])
    conn.commit()


def get_cyclist_id(conn: sqlite3.Connection, pcs_id: str) -> int | None:
    """Returnerer cyclists.id for et givent pcs_id."""
    row = conn.execute("SELECT id FROM cyclists WHERE pcs_id = ?", (pcs_id,)).fetchone()
    return row["id"] if row else None


def get_all(conn: sqlite3.Connection) -> list[dict]:
    """Returnerer alle ryttere som liste af dicts."""
    return [dict(r) for r in conn.execute("SELECT * FROM cyclists ORDER BY name").fetchall()]


def get_all_with_results(conn: sqlite3.Connection) -> list[dict]:
    """Returnerer alle ryttere med deres resultater som nested liste."""
    cyclists = get_all(conn)
    cyclist_map = {c["id"]: c for c in cyclists}

    rows = conn.execute("""
        SELECT * FROM results ORDER BY cyclist_id, date DESC
    """).fetchall()

    for c in cyclists:
        c["results"] = []

    for row in rows:
        cid = row["cyclist_id"]
        if cid in cyclist_map:
            cyclist_map[cid]["results"].append(dict(row))

    return cyclists
