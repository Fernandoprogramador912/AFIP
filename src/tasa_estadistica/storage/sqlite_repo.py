"""Persistencia SQLite: corridas, liquidaciones, conceptos y payloads crudos."""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from tasa_estadistica.domain.tasa_estadistica_mapper import TasaEstadisticaMapper
from tasa_estadistica.model.schemas import Liquidacion, RunParams

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError


@dataclass
class StoredRun:
    run_id: str
    started_at: str


class SqliteRepo:
    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS extraction_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                params_json TEXT NOT NULL,
                modo TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS raw_payloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                body_text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES extraction_runs(run_id)
            );
            CREATE TABLE IF NOT EXISTS liquidaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                cuit TEXT NOT NULL,
                id_externo TEXT NOT NULL,
                numero TEXT,
                fecha TEXT,
                destinacion_id TEXT,
                raw_json TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES extraction_runs(run_id)
            );
            CREATE TABLE IF NOT EXISTS conceptos_liquidacion (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                liquidacion_id INTEGER NOT NULL,
                codigo TEXT,
                descripcion TEXT,
                importe TEXT NOT NULL,
                moneda TEXT,
                raw_json TEXT NOT NULL,
                es_tasa_estadistica INTEGER NOT NULL,
                match_score REAL,
                match_reason TEXT,
                FOREIGN KEY (run_id) REFERENCES extraction_runs(run_id),
                FOREIGN KEY (liquidacion_id) REFERENCES liquidaciones(id)
            );
            CREATE INDEX IF NOT EXISTS idx_liq_run ON liquidaciones(run_id);
            CREATE INDEX IF NOT EXISTS idx_liq_fecha ON liquidaciones(fecha);
            CREATE INDEX IF NOT EXISTS idx_con_run ON conceptos_liquidacion(run_id);
            CREATE TABLE IF NOT EXISTS recupero_overrides (
                destinacion_id TEXT PRIMARY KEY,
                fob TEXT,
                flete TEXT,
                seguro TEXT,
                nota TEXT,
                actualizado_at TEXT NOT NULL
            );
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
            );
            CREATE INDEX IF NOT EXISTS idx_backfill_estado
                ON backfill_meses(cuit, estado);
            """
        )
        self._conn.commit()

    def start_run(self, params: RunParams) -> StoredRun:
        run_id = str(uuid.uuid4())
        started = _utc_now_iso()
        self._conn.execute(
            "INSERT INTO extraction_runs (run_id, started_at, params_json, modo) VALUES (?,?,?,?)",
            (run_id, started, params.model_dump_json(), params.modo),
        )
        self._conn.commit()
        return StoredRun(run_id=run_id, started_at=started)

    def save_raw_payload(self, run_id: str, endpoint: str, body: str) -> None:
        self._conn.execute(
            "INSERT INTO raw_payloads (run_id, endpoint, body_text, created_at) VALUES (?,?,?,?)",
            (run_id, endpoint, body, _utc_now_iso()),
        )
        self._conn.commit()

    def save_liquidaciones(
        self,
        run_id: str,
        liquidaciones: list[Liquidacion],
        mapper: TasaEstadisticaMapper,
    ) -> None:
        cur = self._conn.cursor()
        for liq in liquidaciones:
            row_matches = mapper.match_liquidacion(liq)
            cur.execute(
                """
                INSERT INTO liquidaciones
                (run_id, cuit, id_externo, numero, fecha, destinacion_id, raw_json)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    liq.cuit,
                    liq.id_externo,
                    liq.numero,
                    liq.fecha.isoformat() if liq.fecha else None,
                    liq.destinacion_id,
                    json.dumps(liq.model_dump(mode="json"), default=_json_default),
                ),
            )
            lid = int(cur.lastrowid)
            for conc, m in zip(liq.conceptos, row_matches, strict=True):
                cur.execute(
                    """
                    INSERT INTO conceptos_liquidacion
                    (run_id, liquidacion_id, codigo, descripcion, importe, moneda, raw_json,
                     es_tasa_estadistica, match_score, match_reason)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        run_id,
                        lid,
                        conc.codigo,
                        conc.descripcion,
                        str(conc.importe),
                        conc.moneda,
                        json.dumps(conc.raw, default=_json_default),
                        1 if m.matched else 0,
                        m.score,
                        m.reason,
                    ),
                )
        self._conn.commit()

    def iter_conceptos_for_export(self) -> list[sqlite3.Row]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT r.run_id, r.started_at, r.params_json, r.modo,
                   l.cuit, l.id_externo, l.numero, l.fecha, l.destinacion_id,
                   c.codigo, c.descripcion, c.importe, c.moneda,
                   c.es_tasa_estadistica, c.match_score, c.match_reason
            FROM conceptos_liquidacion c
            JOIN liquidaciones l ON l.id = c.liquidacion_id
            JOIN extraction_runs r ON r.run_id = c.run_id
            ORDER BY r.started_at DESC, l.id, c.id
            """
        )
        return list(cur.fetchall())

    def last_run_summary(self) -> dict[str, Any] | None:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT run_id, started_at, params_json, modo FROM extraction_runs "
            "ORDER BY started_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return None
        return dict(row)
