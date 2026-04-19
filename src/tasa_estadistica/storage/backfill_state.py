"""Estado de backfill mes a mes por CUIT (tabla `backfill_meses`).

El orquestador `backfill_runner` consulta y actualiza esta tabla para saber qué
meses ya están listos (`ok`/`sin_datos`), cuáles fallaron (`error`) y cuáles
nunca se intentaron (`pendiente`). Es la base para reanudar un backfill grande
sin volver a pegarle a AFIP por meses ya completos.

La tabla se crea idempotentemente acá y también desde `SqliteRepo._init_schema`,
para que tanto procesos web (que usan el repo) como CLI puedan trabajar sin
suposiciones de orden de inicialización.
"""

from __future__ import annotations

import sqlite3
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

ESTADOS_VALIDOS = ("pendiente", "en_proceso", "ok", "sin_datos", "error")
ESTADOS_TERMINALES = ("ok", "sin_datos")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backfill_meses (
            cuit TEXT NOT NULL,
            anio INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            estado TEXT NOT NULL CHECK (estado IN
                ('pendiente','en_proceso','ok','sin_datos','error')),
            run_id TEXT,
            n_declaraciones INTEGER,
            n_liquidaciones INTEGER,
            primer_intento TEXT,
            ultimo_intento TEXT,
            intentos INTEGER NOT NULL DEFAULT 0,
            ultimo_error TEXT,
            PRIMARY KEY (cuit, anio, mes)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_backfill_estado ON backfill_meses(cuit, estado)"
    )


def init_schema(db_path: str | Path) -> None:
    """Crea la tabla y el índice si faltan. Idempotente."""
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_table(conn)
        conn.commit()
    finally:
        conn.close()


@dataclass(frozen=True)
class MesBackfill:
    """Un mes concreto del backfill: `cuit + anio + mes`."""

    cuit: str
    anio: int
    mes: int

    @property
    def desde(self) -> date:
        return date(self.anio, self.mes, 1)

    @property
    def hasta(self) -> date:
        ult = monthrange(self.anio, self.mes)[1]
        return date(self.anio, self.mes, ult)

    @property
    def label(self) -> str:
        return f"{self.anio:04d}-{self.mes:02d}"


def iter_meses_rango(desde: date, hasta: date, cuit: str) -> Iterable[MesBackfill]:
    """Itera todos los meses (inclusive) entre `desde` y `hasta`."""
    if hasta < desde:
        return
    cur_a, cur_m = desde.year, desde.month
    end_a, end_m = hasta.year, hasta.month
    while (cur_a, cur_m) <= (end_a, end_m):
        yield MesBackfill(cuit=cuit, anio=cur_a, mes=cur_m)
        if cur_m == 12:
            cur_a += 1
            cur_m = 1
        else:
            cur_m += 1


def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def get_estado(db_path: str | Path, cuit: str, anio: int, mes: int) -> dict | None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_table(conn)
        row = conn.execute(
            "SELECT * FROM backfill_meses WHERE cuit=? AND anio=? AND mes=?",
            (cuit, anio, mes),
        ).fetchone()
        return _row_to_dict(row)
    finally:
        conn.close()


def listar_meses(
    db_path: str | Path,
    cuit: str,
    *,
    desde: date | None = None,
    hasta: date | None = None,
) -> list[dict]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_table(conn)
        sql = "SELECT * FROM backfill_meses WHERE cuit=?"
        params: list = [cuit]
        if desde is not None:
            sql += " AND (anio*100 + mes) >= ?"
            params.append(desde.year * 100 + desde.month)
        if hasta is not None:
            sql += " AND (anio*100 + mes) <= ?"
            params.append(hasta.year * 100 + hasta.month)
        sql += " ORDER BY anio, mes"
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def upsert_estado(
    db_path: str | Path,
    cuit: str,
    anio: int,
    mes: int,
    estado: str,
    *,
    run_id: str | None = None,
    n_declaraciones: int | None = None,
    n_liquidaciones: int | None = None,
    ultimo_error: str | None = None,
    incrementar_intentos: bool = True,
) -> dict:
    """Crea o actualiza la fila del mes con el estado dado.

    - `primer_intento` se setea sólo en el primer upsert.
    - `ultimo_intento` se actualiza siempre que `incrementar_intentos=True`.
    - `intentos` aumenta en 1 si `incrementar_intentos=True`.
    - Pasar `ultimo_error=None` y `estado != 'error'` limpia el error previo.
    """
    if estado not in ESTADOS_VALIDOS:
        raise ValueError(f"estado inválido: {estado!r} (válidos: {ESTADOS_VALIDOS})")
    cuit_n = (cuit or "").strip()
    if not cuit_n:
        raise ValueError("cuit vacío")
    if not (1 <= mes <= 12):
        raise ValueError("mes debe estar entre 1 y 12")
    if anio < 1900 or anio > 9999:
        raise ValueError("anio fuera de rango razonable")

    ahora = _utc_now_iso()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_table(conn)
        existente = conn.execute(
            "SELECT * FROM backfill_meses WHERE cuit=? AND anio=? AND mes=?",
            (cuit_n, anio, mes),
        ).fetchone()
        if existente is None:
            conn.execute(
                """
                INSERT INTO backfill_meses(
                    cuit, anio, mes, estado, run_id,
                    n_declaraciones, n_liquidaciones,
                    primer_intento, ultimo_intento, intentos, ultimo_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cuit_n,
                    anio,
                    mes,
                    estado,
                    run_id,
                    n_declaraciones,
                    n_liquidaciones,
                    ahora if incrementar_intentos else None,
                    ahora if incrementar_intentos else None,
                    1 if incrementar_intentos else 0,
                    ultimo_error if estado == "error" else None,
                ),
            )
        else:
            nuevos_intentos = int(existente["intentos"] or 0) + (
                1 if incrementar_intentos else 0
            )
            primer_intento = existente["primer_intento"] or (
                ahora if incrementar_intentos else None
            )
            ultimo_intento = (
                ahora if incrementar_intentos else existente["ultimo_intento"]
            )
            ultimo_error_db = ultimo_error if estado == "error" else None
            conn.execute(
                """
                UPDATE backfill_meses SET
                    estado = ?,
                    run_id = COALESCE(?, run_id),
                    n_declaraciones = COALESCE(?, n_declaraciones),
                    n_liquidaciones = COALESCE(?, n_liquidaciones),
                    primer_intento = ?,
                    ultimo_intento = ?,
                    intentos = ?,
                    ultimo_error = ?
                WHERE cuit=? AND anio=? AND mes=?
                """,
                (
                    estado,
                    run_id,
                    n_declaraciones,
                    n_liquidaciones,
                    primer_intento,
                    ultimo_intento,
                    nuevos_intentos,
                    ultimo_error_db,
                    cuit_n,
                    anio,
                    mes,
                ),
            )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM backfill_meses WHERE cuit=? AND anio=? AND mes=?",
            (cuit_n, anio, mes),
        ).fetchone()
        return _row_to_dict(row) or {}
    finally:
        conn.close()


def meses_pendientes(
    db_path: str | Path,
    cuit: str,
    desde: date,
    hasta: date,
    *,
    reintentar_errores: bool = False,
    forzar: bool = False,
) -> list[MesBackfill]:
    """Devuelve los meses del rango que todavía hay que procesar.

    - Por defecto: meses sin fila o con estado `pendiente`/`en_proceso`.
    - Con `reintentar_errores=True`: incluye además meses en `error`.
    - Con `forzar=True`: incluye TODOS los meses del rango (ignora estado).
    """
    cuit_n = (cuit or "").strip()
    if not cuit_n:
        return []
    existentes = {
        (int(r["anio"]), int(r["mes"])): r
        for r in listar_meses(db_path, cuit_n, desde=desde, hasta=hasta)
    }
    todos = list(iter_meses_rango(desde, hasta, cuit_n))
    if forzar:
        return todos
    out: list[MesBackfill] = []
    for m in todos:
        fila = existentes.get((m.anio, m.mes))
        if fila is None:
            out.append(m)
            continue
        est = (fila.get("estado") or "").strip()
        if est in ESTADOS_TERMINALES:
            continue
        if est == "error" and not reintentar_errores:
            continue
        out.append(m)
    return out


def resumen(
    db_path: str | Path,
    cuit: str,
    desde: date,
    hasta: date,
) -> dict[str, int]:
    """Cuenta cuántos meses hay en cada estado dentro del rango."""
    out: dict[str, int] = {e: 0 for e in ESTADOS_VALIDOS}
    out["fuera_de_rango"] = 0
    out["sin_fila"] = 0
    filas = {
        (int(r["anio"]), int(r["mes"])): r
        for r in listar_meses(db_path, cuit, desde=desde, hasta=hasta)
    }
    for m in iter_meses_rango(desde, hasta, cuit):
        f = filas.get((m.anio, m.mes))
        if f is None:
            out["sin_fila"] += 1
        else:
            est = (f.get("estado") or "").strip()
            if est in out:
                out[est] += 1
    return out
