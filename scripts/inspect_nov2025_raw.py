from __future__ import annotations

import re
import sqlite3
from pathlib import Path

DB = Path("data/tasa_estadistica.db")


def _resumen(body: str) -> str:
    body = body or ""
    lower = body.lower()
    matches: list[str] = []

    m = re.search(r"<Codigo>(\d+)</Codigo>\s*<Descripcion>([^<]+)</Descripcion>", body)
    if m:
        matches.append(f"error={m.group(1)}: {m.group(2).strip()[:120]}")

    n_filas_simi = len(re.findall(r"<Djai\b", body))
    n_filas_det = len(re.findall(r"<DeclaracionDetallada\b|<DeclaracionImport\w*\b", body))
    n_filas_cualquiera = len(re.findall(r"<IdentificadorDestinacion>", body))
    if any([n_filas_simi, n_filas_det, n_filas_cualquiera]):
        matches.append(
            f"filas~ simi={n_filas_simi} det={n_filas_det} ids={n_filas_cualquiera}"
        )

    if "ejecucion exitosa" in lower:
        matches.append("ejecucion=OK")

    return " | ".join(matches) if matches else f"(body {len(body)} bytes)"


def main() -> int:
    run_ids = (
        "775b9833-3422-4c7f-b543-011e97b7b3a3",
        "18634f00-1535-47a4-ac54-632c1f60deb7",
    )
    with sqlite3.connect(str(DB)) as conn:
        for rid in run_ids:
            print("=" * 80)
            print(f"run_id={rid}")
            print("=" * 80)
            rows = conn.execute(
                """
                SELECT endpoint, length(body_text), body_text
                FROM raw_payloads
                WHERE run_id = ?
                ORDER BY id
                """,
                (rid,),
            ).fetchall()
            print(f"payloads guardados: {len(rows)}")
            for ep, n, body in rows:
                print(f"  [{ep}] ({n} bytes) -> {_resumen(body)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
