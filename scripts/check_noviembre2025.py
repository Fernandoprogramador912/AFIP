from __future__ import annotations

import sqlite3
from pathlib import Path

DB = Path("data/tasa_estadistica.db")


def main() -> int:
    with sqlite3.connect(str(DB)) as conn:
        rows = conn.execute(
            """
            SELECT destinacion_id, fecha, numero, run_id
            FROM liquidaciones
            WHERE fecha LIKE '2025-11-%'
            ORDER BY fecha
            """
        ).fetchall()
        print(f"Liquidaciones con fecha NOVIEMBRE 2025: {len(rows)}")
        for r in rows:
            print("  ", r)

        print()
        dist = conn.execute(
            """
            SELECT substr(fecha,1,7) AS ym, COUNT(*)
            FROM liquidaciones
            WHERE fecha IS NOT NULL
            GROUP BY ym
            ORDER BY ym
            """
        ).fetchall()
        print("Distribucion por mes (solo filas con fecha):")
        for r in dist:
            print("  ", r)

        print()
        runs = conn.execute(
            """
            SELECT run_id, started_at, params_json
            FROM extraction_runs
            WHERE params_json LIKE '%2025-11%' OR params_json LIKE '%"mes":11%'
            ORDER BY started_at DESC
            LIMIT 10
            """
        ).fetchall()
        print(f"Runs de fetch que mencionan noviembre 2025: {len(runs)}")
        for r in runs:
            print("  ", r[0], r[1], r[2][:200])

        print()
        last_runs = conn.execute(
            """
            SELECT run_id, started_at, params_json
            FROM extraction_runs
            ORDER BY started_at DESC
            LIMIT 5
            """
        ).fetchall()
        print("Ultimos 5 runs (cualquiera):")
        for r in last_runs:
            print("  ", r[1], r[2][:180])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
