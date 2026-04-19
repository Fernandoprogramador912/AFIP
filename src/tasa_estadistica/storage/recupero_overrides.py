"""Overrides manuales FOB/FLETE/SEGURO por `destinacion_id`.

Cuando AFIP no devuelve montos (p. ej. declaraciones en estado CANC con carátula
vacía), permitimos cargar el valor a mano desde el panel y tiene precedencia sobre
el JSON MOA en el pipeline de Recupero V2. La tabla vive en el mismo SQLite que las
liquidaciones; se inicializa idempotentemente por `SqliteRepo._init_schema` y acá
tenemos una función compatible para procesos que no usan el repo.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recupero_overrides (
            destinacion_id TEXT PRIMARY KEY,
            fob TEXT,
            flete TEXT,
            seguro TEXT,
            nota TEXT,
            actualizado_at TEXT NOT NULL
        )
        """
    )


def init_schema(db_path: str | Path) -> None:
    """Crea la tabla si falta. Idempotente y seguro en procesos separados del `SqliteRepo`."""
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_table(conn)
        conn.commit()
    finally:
        conn.close()


def _norm_optional_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def upsert_override(
    db_path: str | Path,
    destinacion_id: str,
    *,
    fob: str | None,
    flete: str | None,
    seguro: str | None,
    nota: str | None = None,
) -> dict[str, Any]:
    """Inserta o actualiza un override manual para el `destinacion_id`."""
    did = (destinacion_id or "").strip()
    if not did:
        raise ValueError("destinacion_id vacío")
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO recupero_overrides(
                destinacion_id, fob, flete, seguro, nota, actualizado_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(destinacion_id) DO UPDATE SET
                fob = excluded.fob,
                flete = excluded.flete,
                seguro = excluded.seguro,
                nota = excluded.nota,
                actualizado_at = excluded.actualizado_at
            """,
            (
                did,
                _norm_optional_str(fob),
                _norm_optional_str(flete),
                _norm_optional_str(seguro),
                _norm_optional_str(nota),
                _utc_now_iso(),
            ),
        )
        conn.commit()
        return get_override(db_path, did) or {}
    finally:
        conn.close()


def delete_override(db_path: str | Path, destinacion_id: str) -> bool:
    did = (destinacion_id or "").strip()
    if not did:
        return False
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_table(conn)
        cur = conn.execute(
            "DELETE FROM recupero_overrides WHERE destinacion_id = ?",
            (did,),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_override(db_path: str | Path, destinacion_id: str) -> dict[str, Any] | None:
    did = (destinacion_id or "").strip()
    if not did:
        return None
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_table(conn)
        row = conn.execute(
            "SELECT * FROM recupero_overrides WHERE destinacion_id = ?",
            (did,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_overrides_map(db_path: str | Path) -> dict[str, dict[str, str]]:
    """Todos los overrides como dict `{destinacion_id: {fob, flete, seguro, nota}}`."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_table(conn)
        out: dict[str, dict[str, str]] = {}
        for row in conn.execute("SELECT * FROM recupero_overrides"):
            did = str(row["destinacion_id"] or "").strip()
            if not did:
                continue
            out[did] = {
                "fob": row["fob"] or "",
                "flete": row["flete"] or "",
                "seguro": row["seguro"] or "",
                "nota": row["nota"] or "",
                "actualizado_at": row["actualizado_at"] or "",
            }
        return out
    finally:
        conn.close()
