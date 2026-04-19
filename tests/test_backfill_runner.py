"""`run_backfill`: idempotencia, error sigue procesando, sin_datos correcto, callback."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from tasa_estadistica.backfill_runner import run_backfill, run_backfill_un_mes
from tasa_estadistica.config.settings import Settings
from tasa_estadistica.storage.backfill_state import (
    init_schema,
    listar_meses,
    upsert_estado,
)


def _settings(tmp_path: Path) -> Settings:
    db = tmp_path / "back.db"
    init_schema(db)
    return Settings(arca_sqlite_path=db, arca_cuit="30709572438")


def _fake_ok(n: int) -> Any:
    """Factory: devuelve un fetch_fn que responde OK con `n` liquidaciones."""

    def _f(**kw: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "run_id": f"run-{kw['desde'].isoformat()}",
            "n_liquidaciones": n,
            "meta": {"declaraciones_encontradas": max(n, 1)},
        }

    return _f


def _fake_sin_datos() -> Any:
    def _f(**kw: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "run_id": f"run-{kw['desde'].isoformat()}",
            "n_liquidaciones": 0,
            "meta": {"declaraciones_encontradas": 0},
        }

    return _f


def _fake_error(msg: str = "AFIP 6013 saturado") -> Any:
    def _f(**kw: Any) -> dict[str, Any]:
        return {"ok": False, "error": msg}

    return _f


def test_run_backfill_secuencial_3_meses(tmp_path: Path) -> None:
    s = _settings(tmp_path)
    res = run_backfill(
        desde=date(2025, 1, 1),
        hasta=date(2025, 3, 31),
        cuit=s.arca_cuit,
        settings=s,
        fetch_fn=_fake_ok(2),
    )
    assert res["procesados"] == 3
    assert res["ok"] == 3
    assert res["sin_datos"] == 0
    assert res["error"] == 0
    filas = listar_meses(s.arca_sqlite_path, s.arca_cuit)
    assert {(f["anio"], f["mes"], f["estado"]) for f in filas} == {
        (2025, 1, "ok"),
        (2025, 2, "ok"),
        (2025, 3, "ok"),
    }


def test_run_backfill_idempotente_skipea_ok(tmp_path: Path) -> None:
    s = _settings(tmp_path)
    upsert_estado(s.arca_sqlite_path, s.arca_cuit, 2025, 1, "ok", run_id="prev")

    llamadas: list[str] = []

    def _spy(**kw: Any) -> dict[str, Any]:
        llamadas.append(kw["desde"].isoformat())
        return {
            "ok": True,
            "run_id": "x",
            "n_liquidaciones": 1,
            "meta": {"declaraciones_encontradas": 1},
        }

    res = run_backfill(
        desde=date(2025, 1, 1),
        hasta=date(2025, 2, 28),
        cuit=s.arca_cuit,
        settings=s,
        fetch_fn=_spy,
    )
    # Sólo se debió procesar febrero.
    assert llamadas == ["2025-02-01"]
    assert res["procesados"] == 1
    assert res["ok"] == 1


def test_run_backfill_error_no_aborta_resto(tmp_path: Path) -> None:
    s = _settings(tmp_path)

    def _alterno(**kw: Any) -> dict[str, Any]:
        if kw["desde"].month == 2:
            return {"ok": False, "error": "boom"}
        return {
            "ok": True,
            "run_id": f"run-{kw['desde'].month}",
            "n_liquidaciones": 1,
            "meta": {"declaraciones_encontradas": 1},
        }

    res = run_backfill(
        desde=date(2025, 1, 1),
        hasta=date(2025, 3, 31),
        cuit=s.arca_cuit,
        settings=s,
        fetch_fn=_alterno,
    )
    assert res["procesados"] == 3
    assert res["ok"] == 2
    assert res["error"] == 1
    filas = {f["mes"]: f for f in listar_meses(s.arca_sqlite_path, s.arca_cuit)}
    assert filas[1]["estado"] == "ok"
    assert filas[2]["estado"] == "error"
    assert "boom" in (filas[2]["ultimo_error"] or "")
    assert filas[3]["estado"] == "ok"


def test_run_backfill_sin_datos_marca_correcto(tmp_path: Path) -> None:
    s = _settings(tmp_path)
    res = run_backfill(
        desde=date(2025, 4, 1),
        hasta=date(2025, 4, 30),
        cuit=s.arca_cuit,
        settings=s,
        fetch_fn=_fake_sin_datos(),
    )
    assert res["sin_datos"] == 1
    f = listar_meses(s.arca_sqlite_path, s.arca_cuit)[0]
    assert f["estado"] == "sin_datos"
    assert f["n_declaraciones"] == 0
    assert f["n_liquidaciones"] == 0


def test_run_backfill_reintentar_errores(tmp_path: Path) -> None:
    s = _settings(tmp_path)
    upsert_estado(s.arca_sqlite_path, s.arca_cuit, 2025, 1, "error", ultimo_error="prev")
    upsert_estado(s.arca_sqlite_path, s.arca_cuit, 2025, 2, "ok")

    llamadas: list[str] = []

    def _spy(**kw: Any) -> dict[str, Any]:
        llamadas.append(kw["desde"].isoformat())
        return {
            "ok": True,
            "run_id": "x",
            "n_liquidaciones": 1,
            "meta": {"declaraciones_encontradas": 1},
        }

    res = run_backfill(
        desde=date(2025, 1, 1),
        hasta=date(2025, 2, 28),
        cuit=s.arca_cuit,
        settings=s,
        fetch_fn=_spy,
        reintentar_errores=True,
    )
    # Sólo enero (estaba en error) — febrero ya estaba ok.
    assert llamadas == ["2025-01-01"]
    assert res["ok"] == 1
    f_ene = next(f for f in listar_meses(s.arca_sqlite_path, s.arca_cuit) if f["mes"] == 1)
    assert f_ene["estado"] == "ok"
    assert f_ene["ultimo_error"] is None


def test_run_backfill_forzar_reprocesa_todo(tmp_path: Path) -> None:
    s = _settings(tmp_path)
    upsert_estado(s.arca_sqlite_path, s.arca_cuit, 2025, 1, "ok")
    upsert_estado(s.arca_sqlite_path, s.arca_cuit, 2025, 2, "sin_datos")

    res = run_backfill(
        desde=date(2025, 1, 1),
        hasta=date(2025, 2, 28),
        cuit=s.arca_cuit,
        settings=s,
        fetch_fn=_fake_ok(5),
        forzar=True,
    )
    assert res["procesados"] == 2
    assert res["ok"] == 2


def test_run_backfill_excepcion_python_no_aborta(tmp_path: Path) -> None:
    s = _settings(tmp_path)

    def _explota(**kw: Any) -> dict[str, Any]:
        if kw["desde"].month == 1:
            raise RuntimeError("kaboom")
        return {
            "ok": True,
            "run_id": "x",
            "n_liquidaciones": 1,
            "meta": {"declaraciones_encontradas": 1},
        }

    res = run_backfill(
        desde=date(2025, 1, 1),
        hasta=date(2025, 2, 28),
        cuit=s.arca_cuit,
        settings=s,
        fetch_fn=_explota,
    )
    assert res["procesados"] == 2
    assert res["error"] == 1
    assert res["ok"] == 1
    f = next(f for f in listar_meses(s.arca_sqlite_path, s.arca_cuit) if f["mes"] == 1)
    assert "kaboom" in (f["ultimo_error"] or "")


def test_run_backfill_progreso_callback(tmp_path: Path) -> None:
    s = _settings(tmp_path)
    eventos: list[dict[str, Any]] = []

    res = run_backfill(
        desde=date(2025, 1, 1),
        hasta=date(2025, 2, 28),
        cuit=s.arca_cuit,
        settings=s,
        fetch_fn=_fake_ok(1),
        on_progress=eventos.append,
    )
    fases = [e["fase"] for e in eventos]
    assert fases[0] == "inicio"
    assert fases[-1] == "fin"
    assert "mes_inicio" in fases
    assert "mes_fin" in fases
    assert res["procesados"] == 2


def test_run_backfill_un_mes_atajo(tmp_path: Path) -> None:
    s = _settings(tmp_path)
    upsert_estado(s.arca_sqlite_path, s.arca_cuit, 2025, 4, "error", ultimo_error="prev")

    res = run_backfill_un_mes(
        cuit=s.arca_cuit,
        anio=2025,
        mes=4,
        settings=s,
        fetch_fn=_fake_ok(1),
    )
    assert res["procesados"] == 1
    assert res["ok"] == 1
    f = next(f for f in listar_meses(s.arca_sqlite_path, s.arca_cuit) if f["mes"] == 4)
    assert f["estado"] == "ok"


def test_run_backfill_callback_que_explota_no_rompe_runner(tmp_path: Path) -> None:
    s = _settings(tmp_path)

    def _bad(_ev: dict[str, Any]) -> None:
        raise RuntimeError("bad cb")

    res = run_backfill(
        desde=date(2025, 1, 1),
        hasta=date(2025, 1, 31),
        cuit=s.arca_cuit,
        settings=s,
        fetch_fn=_fake_ok(1),
        on_progress=_bad,
    )
    assert res["procesados"] == 1
    assert res["ok"] == 1
