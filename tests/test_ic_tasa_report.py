"""Reporte despachos IC + monto tasa."""

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from tasa_estadistica.report.ic_tasa_report import (
    query_ic_tasa_rows,
    total_monto,
    write_ic_tasa_csv,
)


@pytest.fixture()
def db_ic(tmp_path: Path) -> Path:
    p = tmp_path / "t.db"
    import sqlite3

    conn = sqlite3.connect(p)
    conn.executescript(
        """
        CREATE TABLE extraction_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            params_json TEXT NOT NULL,
            modo TEXT NOT NULL
        );
        CREATE TABLE liquidaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            cuit TEXT NOT NULL,
            id_externo TEXT NOT NULL,
            numero TEXT,
            fecha TEXT,
            destinacion_id TEXT,
            raw_json TEXT NOT NULL
        );
        CREATE TABLE conceptos_liquidacion (
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
            match_reason TEXT
        );
        INSERT INTO extraction_runs VALUES ('r1', '2025-01-01', '{}', 'live');
        INSERT INTO liquidaciones (
          run_id, cuit, id_externo, numero, fecha, destinacion_id, raw_json
        )
        VALUES (
          'r1', '301', 'x', '25001IC00000001A', '2025-06-15', '25001IC00000001A', '{}'
        );
        INSERT INTO conceptos_liquidacion (
          run_id, liquidacion_id, codigo, descripcion, importe, moneda,
          raw_json, es_tasa_estadistica, match_score, match_reason
        )
        VALUES (
          'r1', 1, '011', 'tasa', '100.50', 'ARS', '{}', 1, 1.0, 'código_exacto:011'
        );
        """
    )
    conn.commit()
    conn.close()
    return p


def test_query_ic_tasa_filters_ic(db_ic: Path) -> None:
    rows = query_ic_tasa_rows(db_ic, date(2025, 1, 1), date(2025, 12, 31), frozenset({"011", "TE"}))
    assert len(rows) == 1
    assert rows[0]["destinacion_id"] == "25001IC00000001A"
    assert rows[0]["monto_tasa_estadistica"] == Decimal("100.50")


def test_total_monto(db_ic: Path) -> None:
    rows = query_ic_tasa_rows(db_ic, date(2025, 1, 1), date(2025, 12, 31), frozenset({"011"}))
    assert total_monto(rows) == Decimal("100.50")


def test_write_csv(db_ic: Path, tmp_path: Path) -> None:
    rows = query_ic_tasa_rows(db_ic, date(2025, 1, 1), date(2025, 12, 31), frozenset({"011"}))
    out = tmp_path / "o.csv"
    write_ic_tasa_csv(rows, out)
    assert out.read_text(encoding="utf-8-sig").splitlines()[0].startswith("destinacion_id")
