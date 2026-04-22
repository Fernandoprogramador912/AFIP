"""Ejecución de extracción AFIP → SQLite (CLI y panel web)."""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from tasa_estadistica.arca.liquidaciones_client import FetchProgressFn, LiquidacionesClient
from tasa_estadistica.arca.wsaa_client import WSAAClient
from tasa_estadistica.config.date_policy import validate_analysis_period
from tasa_estadistica.config.settings import Settings, get_tasa_mapper_from_settings
from tasa_estadistica.model.schemas import RunParams
from tasa_estadistica.storage.sqlite_repo import SqliteRepo

logger = logging.getLogger(__name__)


def _enrich_fetch_error_message(message: str) -> str:
    """Añade pista accionable ante errores típicos de TA vencido en MOA/SOAP."""
    low = message.lower()
    if "7008" in message or "token invalido" in low or "token inválido" in low:
        return (
            f"{message}\n\n"
            "Sugerencia: MOA/AFIP suele devolver 7008 cuando el Ticket de Acceso (WSAA) "
            "venció o no coincide con el entorno. Ejecutá `tasa-arca auth --force` para "
            "renovar el TA (el TA en disco puede parecer vigente pero no serlo para ese WS) "
            "y volvé a descargar."
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
            if not settings.arca_cert_path or not settings.arca_cert_path.is_file():
                return {
                    "ok": False,
                    "error": (
                        "ARCA_MODE=live requiere certificado (.p12). "
                        f"ARCA_CERT_PATH inválido o ausente: {settings.arca_cert_path}"
                    ),
                }
            try:
                wsaa = WSAAClient(settings)
                ta_xml = wsaa.ensure_ticket(settings.arca_ticket_path)
            except (OSError, ValueError, RuntimeError) as exc:
                return {
                    "ok": False,
                    "error": (
                        f"No se pudo obtener/reutilizar el ticket WSAA: {exc}\n"
                        "Revise ARCA_CERT_PATH, ARCA_CERT_PASSWORD y ARCA_WSAA_SERVICE."
                    ),
                }

        client = LiquidacionesClient(settings)
        liqs, meta = client.fetch_liquidaciones(
            cuit, desde, hasta, ta_xml=ta_xml, on_progress=on_progress
        )
        # Si ARCA_MOA_LOG_RAW_SOAP está activo, el meta trae los envelopes SOAP de los listados.
        # Los guardamos en payloads separados para que el meta principal siga siendo legible.
        raw_soap = meta.pop("raw_soap_listados", None) if isinstance(meta, dict) else None
        repo.save_raw_payload(
            run.run_id,
            "liquidaciones/meta",
            json.dumps(meta, ensure_ascii=False, indent=2),
        )
        if raw_soap:
            repo.save_raw_payload(
                run.run_id,
                "liquidaciones/raw_soap_listados",
                json.dumps(raw_soap, ensure_ascii=False, indent=2),
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
