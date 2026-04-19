"""Despachos importación (identificador con IC) y monto tasa de estadística por período."""

from __future__ import annotations

import csv
import json
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from tasa_estadistica.config.settings import Settings, get_settings
from tasa_estadistica.export.recupero_destinacion_sql import (
    parse_destinacion_subcadenas,
    sql_destinacion_import_or,
)


def _codigos_sql_list(codigos: frozenset[str]) -> list[str]:
    return sorted(c.strip().upper() for c in codigos if c.strip())


def query_ic_tasa_rows(
    db_path: Path,
    desde: date,
    hasta: date,
    codigos_tasa: frozenset[str],
    *,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    """
    Una fila por destinación: despachos cuyo identificador coincide con el filtro de importación
    (subcadenas en `TASA_RECUPERO_DESTINACION_SUBCADENAS`, default IC)
    y monto del concepto de tasa (códigos configurados o es_tasa_estadistica).
    Si hubo varias extracciones, se toma la liquidación más reciente por `liquidaciones.id`.
    """
    settings = settings or get_settings()
    subcadenas = parse_destinacion_subcadenas(settings)
    cond_l2, p_dest_l2 = sql_destinacion_import_or("l2", subcadenas)
    cond_l, p_dest_l = sql_destinacion_import_or("l", subcadenas)
    codes = _codigos_sql_list(codigos_tasa)
    if not codes:
        codes = ["011", "TE"]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" * len(codes))
    # Subconsulta: última fila liquidaciones por destinación en el rango (mayor id = más reciente)
    sql = f"""
    SELECT
      l.destinacion_id,
      l.fecha,
      l.cuit,
      l.numero,
      c.codigo AS codigo_tasa,
      c.importe AS monto_tasa_estadistica,
      c.raw_json AS concepto_raw_json,
      c.match_reason
    FROM liquidaciones l
    JOIN conceptos_liquidacion c ON c.liquidacion_id = l.id
    WHERE l.id IN (
      SELECT MAX(l2.id)
      FROM liquidaciones l2
      WHERE l2.fecha >= ?
        AND l2.fecha <= ?
        AND {cond_l2}
      GROUP BY l2.destinacion_id
    )
    AND l.fecha >= ?
    AND l.fecha <= ?
    AND {cond_l}
    AND (
      c.es_tasa_estadistica = 1
      OR UPPER(TRIM(c.codigo)) IN ({placeholders})
    )
    ORDER BY l.fecha, l.destinacion_id
    """
    params: list[Any] = [
        desde.isoformat(),
        hasta.isoformat(),
        *p_dest_l2,
        desde.isoformat(),
        hasta.isoformat(),
        *p_dest_l,
        *codes,
    ]
    cur = conn.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    for r in rows:
        raw_s = r.pop("concepto_raw_json", None)
        imp_raw = r.get("monto_tasa_estadistica")
        try:
            if imp_raw is None:
                dec = Decimal("0")
            else:
                dec = Decimal(str(imp_raw).replace(",", ".").strip())
        except Exception:
            dec = Decimal("0")
        if dec == 0 and raw_s:
            try:
                d = json.loads(raw_s) if isinstance(raw_s, str) else raw_s
                mp = d.get("MontoPagado")
                if mp is not None and str(mp).strip() != "":
                    dec = Decimal(str(mp).replace(",", ".").strip())
            except Exception:
                pass
        r["monto_tasa_estadistica"] = dec
    return rows


def write_ic_tasa_csv(rows: list[dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "destinacion_id",
        "fecha",
        "cuit",
        "numero",
        "codigo_tasa",
        "monto_tasa_estadistica",
        "match_reason",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            out = {k: r.get(k) for k in fieldnames}
            v = out.get("monto_tasa_estadistica")
            if hasattr(v, "quantize"):
                out["monto_tasa_estadistica"] = str(v)
            w.writerow(out)
    return path


def total_monto(rows: list[dict[str, Any]]) -> Decimal:
    t = Decimal("0")
    for r in rows:
        m = r.get("monto_tasa_estadistica")
        if isinstance(m, Decimal):
            t += m
        elif m is not None:
            try:
                t += Decimal(str(m).replace(",", "."))
            except Exception:
                pass
    return t
