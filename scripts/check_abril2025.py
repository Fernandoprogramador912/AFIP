"""Diagnóstico: ¿los D.I. de ARCA_MOA_DESTINACION_IDS_EXTRA están en SQLite? ¿Hay algo de abril 2025?"""

from __future__ import annotations

import sqlite3

DB = "data/tasa_estadistica.db"
EXTRA = [
    "25001IC04068168A",
    "25001IC06005013F",
    "25001IC06008719V",
    "25001IC04239657D",
    "25001IC06004713L",
]


def main() -> int:
    conn = sqlite3.connect(DB)
    try:
        placeholders = ",".join("?" * len(EXTRA))
        rows = conn.execute(
            f"SELECT destinacion_id, fecha, numero FROM liquidaciones "
            f"WHERE destinacion_id IN ({placeholders}) ORDER BY destinacion_id",
            EXTRA,
        ).fetchall()
        print(f"Filas para los 5 D.I. extra en SQLite: {len(rows)}")
        for r in rows:
            print("  ", r)

        tot = conn.execute(
            "SELECT COUNT(*), MIN(fecha), MAX(fecha) "
            "FROM liquidaciones WHERE fecha BETWEEN '2025-04-01' AND '2025-04-30'"
        ).fetchone()
        print(f"Liquidaciones con fecha en abril 2025: count={tot[0]}, min={tot[1]}, max={tot[2]}")

        ultimos = conn.execute(
            "SELECT destinacion_id, fecha, numero FROM liquidaciones "
            "ORDER BY rowid DESC LIMIT 5"
        ).fetchall()
        print("Últimas 5 filas por orden de inserción:")
        for r in ultimos:
            print("  ", r)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
