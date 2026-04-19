"""Ejecución de extracción AFIP → SQLite (CLI y panel web)."""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from tasa_estadistica.arca.auth_ticket_store import wsaa_ticket_expired
from tasa_estadistica.arca.liquidaciones_client import FetchProgressFn, LiquidacionesClient
from tasa_estadistica.config.date_policy import validate_analysis_period
from tasa_estadistica.config.settings import Settings, get_tasa_mapper_from_settings
from tasa_estadistica.model.schemas import RunParams
from tasa_estadistica.storage.sqlite_repo import SqliteRepo

logger = logging.getLogger(__name__)


def _enrich_fetch_error_message(message: str) -> str:
    """Añade pista accionable ante errores típicos de TA vencido en MOA/SOAP."""
    low = message.lower()
    if "7008" in message or "token invalido" in low:
        return (
            f"{message}\n\n"
            "Sugerencia: MOA/AFIP suele devolver 7008 cuando el Ticket de Acceso (WSAA) "
            "venció o no coincide con el entorno. Ejecutá `tasa-arca auth` y volvé a descargar."
        )
    return message


def execute_fetch(
    *,
    desde: date,
    hasta: date,
    cuit: str,
    settings: Settings,
    on_progress: FetchProgressFn | None = None,
) -> dict[str, Any]:
    """
    Descarga liquidaciones en el rango y persiste en SQLite.
    En live requiere ticket WSAA y WSDL configurados (igual que `tasa-arca fetch`).

    Devuelve un dict: ok True con run_id / n_liquidaciones, o ok False con error.
    """
    try:
        validate_analysis_period(desde, hasta, settings)
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    settings.arca_data_dir.mkdir(parents=True, exist_ok=True)
    modo = settings.arca_mode.lower()
    mapper = get_tasa_mapper_from_settings(settings)

    repo: SqliteRepo | None = None
    try:
        repo = SqliteRepo(settings.arca_sqlite_path)
        params = RunParams(fecha_desde=desde, fecha_hasta=hasta, cuit=cuit, modo=modo)
        run = repo.start_run(params)

        ta_xml: bytes | None = None
        if modo == "live":
            if not settings.arca_ticket_path.is_file():
                return {
                    "ok": False,
                    "error": (
                        f"No hay ticket en {settings.arca_ticket_path}. "
                        "Ejecute: tasa-arca auth"
                    ),
                }
            ta_xml = settings.arca_ticket_path.read_bytes()
            if wsaa_ticket_expired(ta_xml):
                return {
                    "ok": False,
                    "error": (
                        f"El ticket WSAA en {settings.arca_ticket_path} está vencido "
                        "(expirationTime ya pasó). MOA responderá 7008 token inválido "
                        "hasta renovarlo. Ejecute: tasa-arca auth"
                    ),
                }

        client = LiquidacionesClient(settings)
        liqs, meta = client.fetch_liquidaciones(
            cuit, desde, hasta, ta_xml=ta_xml, on_progress=on_progress
        )
        repo.save_raw_payload(
            run.run_id,
            "liquidaciones/meta",
            json.dumps(meta, ensure_ascii=False, indent=2),
        )
        repo.save_liquidaciones(run.run_id, liqs, mapper)

        logger.info(
            "Extracción guardada run_id=%s liquidaciones=%s",
            run.run_id,
            len(liqs),
        )
        return {
            "ok": True,
            "run_id": run.run_id,
            "n_liquidaciones": len(liqs),
            "meta": meta,
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception("Fallo en fetch")
        return {"ok": False, "error": _enrich_fetch_error_message(str(exc))}
    finally:
        if repo is not None:
            repo.close()
