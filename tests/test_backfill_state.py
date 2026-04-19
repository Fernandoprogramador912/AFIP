"""Estado de backfill mes a mes: CRUD + meses pendientes + idempotencia."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from tasa_estadistica.storage.backfill_state import (
    ESTADOS_VALIDOS,
    MesBackfill,
    get_estado,
    init_schema,
    iter_meses_rango,
    listar_meses,
    meses_pendientes,
    resumen,
    upsert_estado,
)


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "back.db"
    init_schema(p)
    return p


def test_iter_meses_rango_inclusivo() -> None:
    meses = list(iter_meses_rango(date(2025, 11, 15), date(2026, 2, 1), "30709572438"))
    assert [(m.anio, m.mes) for m in meses] == [
        (2025, 11),
        (2025, 12),
        (2026, 1),
        (2026, 2),
    ]


def test_iter_meses_rango_un_solo_mes() -> None:
    meses = list(iter_meses_rango(date(2025, 4, 1), date(2025, 4, 30), "X"))
    assert len(meses) == 1
    assert meses[0].label == "2025-04"
    assert meses[0].desde == date(2025, 4, 1)
    assert meses[0].hasta == date(2025, 4, 30)


def test_iter_meses_rango_invertido_devuelve_vacio() -> None:
    assert list(iter_meses_rango(date(2025, 6, 1), date(2025, 5, 1), "X")) == []


def test_init_schema_idempotente(tmp_path: Path) -> None:
    p = tmp_path / "back.db"
    init_schema(p)
    init_schema(p)  # no debe explotar
    assert p.exists()


def test_upsert_crea_y_luego_actualiza(tmp_path: Path) -> None:
    p = _db(tmp_path)
    f1 = upsert_estado(p, "30709572438", 2025, 4, "en_proceso")
    assert f1["estado"] == "en_proceso"
    assert f1["intentos"] == 1
    assert f1["primer_intento"] is not None
    assert f1["ultimo_intento"] == f1["primer_intento"]

    f2 = upsert_estado(
        p,
        "30709572438",
        2025,
        4,
        "ok",
        run_id="run-x",
        n_declaraciones=3,
        n_liquidaciones=5,
    )
    assert f2["estado"] == "ok"
    assert f2["intentos"] == 2
    assert f2["run_id"] == "run-x"
    assert f2["n_declaraciones"] == 3
    assert f2["n_liquidaciones"] == 5
    assert f2["primer_intento"] == f1["primer_intento"]


def test_upsert_error_guarda_mensaje(tmp_path: Path) -> None:
    p = _db(tmp_path)
    f = upsert_estado(p, "X", 2024, 2, "error", ultimo_error="6013 saturado")
    assert f["estado"] == "error"
    assert f["ultimo_error"] == "6013 saturado"

    # Si después pasa a OK, el ultimo_error debe limpiarse.
    f2 = upsert_estado(p, "X", 2024, 2, "ok")
    assert f2["estado"] == "ok"
    assert f2["ultimo_error"] is None


def test_upsert_estado_invalido_falla(tmp_path: Path) -> None:
    p = _db(tmp_path)
    with pytest.raises(ValueError):
        upsert_estado(p, "X", 2025, 4, "INEXISTENTE")
    with pytest.raises(ValueError):
        upsert_estado(p, "", 2025, 4, "ok")
    with pytest.raises(ValueError):
        upsert_estado(p, "X", 2025, 13, "ok")


def test_get_estado_inexistente_devuelve_none(tmp_path: Path) -> None:
    p = _db(tmp_path)
    assert get_estado(p, "X", 2024, 1) is None


def test_listar_meses_filtra_por_rango(tmp_path: Path) -> None:
    p = _db(tmp_path)
    upsert_estado(p, "X", 2024, 12, "ok")
    upsert_estado(p, "X", 2025, 1, "error")
    upsert_estado(p, "X", 2025, 6, "ok")
    upsert_estado(p, "X", 2026, 1, "pendiente")

    todos = listar_meses(p, "X")
    assert len(todos) == 4
    en_2025 = listar_meses(p, "X", desde=date(2025, 1, 1), hasta=date(2025, 12, 31))
    assert [(r["anio"], r["mes"]) for r in en_2025] == [(2025, 1), (2025, 6)]


def test_meses_pendientes_default_skipea_terminales(tmp_path: Path) -> None:
    p = _db(tmp_path)
    cuit = "30709572438"
    upsert_estado(p, cuit, 2025, 1, "ok")
    upsert_estado(p, cuit, 2025, 2, "sin_datos")
    upsert_estado(p, cuit, 2025, 3, "error", ultimo_error="boom")
    upsert_estado(p, cuit, 2025, 4, "pendiente")

    pend = meses_pendientes(p, cuit, date(2025, 1, 1), date(2025, 5, 31))
    labels = [m.label for m in pend]
    # ene, feb terminales -> skip; mar es error pero no se reintenta por default;
    # abril pendiente y mayo no existe -> ambos en la lista.
    assert labels == ["2025-04", "2025-05"]


def test_meses_pendientes_reintentar_errores(tmp_path: Path) -> None:
    p = _db(tmp_path)
    upsert_estado(p, "X", 2025, 3, "error", ultimo_error="boom")
    upsert_estado(p, "X", 2025, 4, "pendiente")
    pend = meses_pendientes(
        p, "X", date(2025, 3, 1), date(2025, 4, 30), reintentar_errores=True
    )
    assert [m.label for m in pend] == ["2025-03", "2025-04"]


def test_meses_pendientes_forzar_devuelve_todo(tmp_path: Path) -> None:
    p = _db(tmp_path)
    upsert_estado(p, "X", 2025, 1, "ok")
    upsert_estado(p, "X", 2025, 2, "sin_datos")
    pend = meses_pendientes(p, "X", date(2025, 1, 1), date(2025, 3, 31), forzar=True)
    assert [m.label for m in pend] == ["2025-01", "2025-02", "2025-03"]


def test_resumen_cuenta_por_estado(tmp_path: Path) -> None:
    p = _db(tmp_path)
    upsert_estado(p, "X", 2025, 1, "ok")
    upsert_estado(p, "X", 2025, 2, "sin_datos")
    upsert_estado(p, "X", 2025, 3, "error", ultimo_error="x")
    r = resumen(p, "X", date(2025, 1, 1), date(2025, 5, 31))
    assert r["ok"] == 1
    assert r["sin_datos"] == 1
    assert r["error"] == 1
    assert r["sin_fila"] == 2  # abril y mayo
    assert all(e in r for e in ESTADOS_VALIDOS)


def test_mesbackfill_dataclass_fields() -> None:
    m = MesBackfill(cuit="X", anio=2025, mes=4)
    assert m.label == "2025-04"
    assert m.desde == date(2025, 4, 1)
    assert m.hasta == date(2025, 4, 30)
