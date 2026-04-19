"""Fetch y backfill en segundo plano para el panel (polling de estado)."""

from __future__ import annotations

import threading
import uuid
from datetime import date
from typing import Any

from tasa_estadistica.backfill_runner import run_backfill
from tasa_estadistica.config.settings import Settings
from tasa_estadistica.fetch_runner import execute_fetch

_lock = threading.Lock()
_jobs: dict[str, dict[str, Any]] = {}
_running: bool = False
_backfill_jobs: dict[str, dict[str, Any]] = {}
_backfill_running: bool = False


def _set_running(running: bool) -> None:
    global _running
    _running = running


def _set_backfill_running(running: bool) -> None:
    global _backfill_running
    _backfill_running = running


def is_fetch_running() -> bool:
    return _running


def is_backfill_running() -> bool:
    return _backfill_running


def start_fetch_job(
    *,
    desde: date,
    hasta: date,
    cuit: str,
    settings: Settings,
) -> str | None:
    """
    Lanza `execute_fetch` en un thread. Devuelve job_id o None si ya hay un fetch en curso.
    """
    job_id = str(uuid.uuid4())
    with _lock:
        if _running:
            return None
        _set_running(True)
        _jobs[job_id] = {
            "status": "running",
            "desde": desde.isoformat(),
            "hasta": hasta.isoformat(),
            "cuit": cuit,
            "progress": None,
            "error": None,
            "result": None,
        }

    def run() -> None:
        def on_progress(info: dict[str, Any]) -> None:
            with _lock:
                if job_id in _jobs:
                    _jobs[job_id]["progress"] = info

        try:
            out = execute_fetch(
                desde=desde,
                hasta=hasta,
                cuit=cuit,
                settings=settings,
                on_progress=on_progress,
            )
            with _lock:
                if job_id in _jobs:
                    if out.get("ok"):
                        _jobs[job_id]["status"] = "done"
                        _jobs[job_id]["result"] = {
                            "run_id": out.get("run_id"),
                            "n_liquidaciones": out.get("n_liquidaciones"),
                        }
                    else:
                        _jobs[job_id]["status"] = "error"
                        _jobs[job_id]["error"] = out.get("error", "Error desconocido")
        except Exception as exc:  # noqa: BLE001
            with _lock:
                if job_id in _jobs:
                    _jobs[job_id]["status"] = "error"
                    _jobs[job_id]["error"] = str(exc)
        finally:
            with _lock:
                _set_running(False)

    t = threading.Thread(target=run, name=f"tasa-fetch-{job_id[:8]}", daemon=True)
    t.start()
    return job_id


def get_job(job_id: str) -> dict[str, Any] | None:
    with _lock:
        j = _jobs.get(job_id)
        return dict(j) if j else None


def start_backfill_job(
    *,
    desde: date,
    hasta: date,
    cuit: str,
    settings: Settings,
    reintentar_errores: bool = False,
    forzar: bool = False,
    pausa_entre_meses_seg: float = 0.0,
) -> str | None:
    """
    Lanza `run_backfill` en un thread. Devuelve job_id o None si ya hay backfill en curso.

    El estado por mes vive en SQLite (`backfill_meses`); este job solo expone progreso
    en memoria para el polling de UI (mes actual, total, último resultado).
    """
    job_id = str(uuid.uuid4())
    with _lock:
        if _backfill_running:
            return None
        _set_backfill_running(True)
        _backfill_jobs[job_id] = {
            "status": "running",
            "desde": desde.isoformat(),
            "hasta": hasta.isoformat(),
            "cuit": cuit,
            "reintentar_errores": reintentar_errores,
            "forzar": forzar,
            "progress": None,
            "error": None,
            "result": None,
        }

    def run() -> None:
        def on_progress(info: dict[str, Any]) -> None:
            with _lock:
                if job_id in _backfill_jobs:
                    _backfill_jobs[job_id]["progress"] = info

        try:
            res = run_backfill(
                desde=desde,
                hasta=hasta,
                cuit=cuit,
                settings=settings,
                reintentar_errores=reintentar_errores,
                forzar=forzar,
                pausa_entre_meses_seg=pausa_entre_meses_seg,
                on_progress=on_progress,
            )
            with _lock:
                if job_id in _backfill_jobs:
                    _backfill_jobs[job_id]["status"] = "done"
                    _backfill_jobs[job_id]["result"] = {
                        "procesados": res["procesados"],
                        "ok": res["ok"],
                        "sin_datos": res["sin_datos"],
                        "error": res["error"],
                        "total_pendientes": res["total_pendientes"],
                    }
        except Exception as exc:  # noqa: BLE001
            with _lock:
                if job_id in _backfill_jobs:
                    _backfill_jobs[job_id]["status"] = "error"
                    _backfill_jobs[job_id]["error"] = str(exc)
        finally:
            with _lock:
                _set_backfill_running(False)

    t = threading.Thread(target=run, name=f"tasa-backfill-{job_id[:8]}", daemon=True)
    t.start()
    return job_id


def get_backfill_job(job_id: str) -> dict[str, Any] | None:
    with _lock:
        j = _backfill_jobs.get(job_id)
        return dict(j) if j else None
