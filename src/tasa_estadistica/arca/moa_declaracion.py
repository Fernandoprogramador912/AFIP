"""MOA wconsdeclaracion: listados SIMI/detallada, liquidaciones y detalle de conceptos."""

from __future__ import annotations

import json
import logging
import random
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import requests
from lxml import etree
from zeep import Client
from zeep.helpers import serialize_object
from zeep.plugins import HistoryPlugin
from zeep.transports import Transport

from tasa_estadistica.arca.auth_ticket_store import TicketAcceso, parse_ticket_xml
from tasa_estadistica.arca.soap_common import TokenSignPlugin, zeep_result_to_json
from tasa_estadistica.config.settings import Settings
from tasa_estadistica.model.schemas import ConceptoLiquidacion, Liquidacion

logger = logging.getLogger(__name__)

_AR = ZoneInfo("America/Argentina/Buenos_Aires")


def _day_start_utc_ar(d: date) -> datetime:
    return datetime.combine(d, dt_time.min, tzinfo=_AR)


def _day_end_utc_ar(d: date) -> datetime:
    return datetime.combine(d, dt_time(23, 59, 59), tzinfo=_AR)


def _moa_auth_dict(ta: TicketAcceso, cuit: str, settings: Settings) -> dict[str, Any]:
    d: dict[str, Any] = {
        "Token": ta.token,
        "Sign": ta.sign,
        "CuitEmpresaConectada": int(cuit),
    }
    ta_str = (settings.arca_moa_tipo_agente or "").strip()
    if ta_str:
        d["TipoAgente"] = ta_str
    rol = (settings.arca_moa_rol or "").strip()
    if rol:
        d["Rol"] = rol
    return d


def _list_errores(obj: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if obj is None:
        return out
    le = getattr(obj, "ListaErrores", None)
    if le is None and isinstance(obj, dict):
        le = obj.get("ListaErrores")
    if le is None:
        return out
    det = getattr(le, "DetalleError", None)
    if det is None and isinstance(le, dict):
        det = le.get("DetalleError")
    if det is None:
        return out
    if not isinstance(det, list):
        det = [det]
    for e in det:
        if e is None:
            continue
        cod = getattr(e, "Codigo", None) if not isinstance(e, dict) else e.get("Codigo")
        desc = getattr(e, "Descripcion", None) if not isinstance(e, dict) else e.get("Descripcion")
        # Código 0 = éxito en algunas respuestas AFIP (no tratar como error)
        if cod == 0:
            continue
        out.append({"codigo": cod, "descripcion": desc})
    return out


def _raise_if_moa_errors(result: Any, operacion: str) -> None:
    errs = _list_errores(result)
    if not errs:
        return
    msg = "; ".join(f"{e.get('codigo')}: {e.get('descripcion')}" for e in errs)
    raise RuntimeError(f"MOA {operacion}: {msg}")


def _has_error_6013(result: Any) -> bool:
    for e in _list_errores(result):
        c = e.get("codigo")
        if c == 6013 or str(c) == "6013":
            return True
    return False


def _iter_chunks_30_days(fecha_desde: date, fecha_hasta: date) -> list[tuple[date, date]]:
    """AFIP limita DetalladaListaDeclaraciones a 30 días calendario (error 10750)."""
    chunks: list[tuple[date, date]] = []
    cur = fecha_desde
    while cur <= fecha_hasta:
        chunk_end = min(cur + timedelta(days=29), fecha_hasta)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


def _lista_fuente_normalizada(settings: Settings) -> str:
    v = (settings.arca_moa_lista_fuente or "both").strip().lower()
    if v in ("detallada", "simi_djai", "both"):
        return v
    return "both"


def _acumular_6013(
    meta: dict[str, Any],
    resumen_variantes: list[dict[str, Any]],
    operacion: str,
) -> None:
    """Propaga reintentos 6013 de cada variante al `meta` final (log resumen)."""
    total = 0
    for r in resumen_variantes:
        val = r.get("reintentos_6013")
        if isinstance(val, int):
            total += val
    if total <= 0:
        return
    meta["reintentos_6013_total"] = int(meta.get("reintentos_6013_total", 0)) + total
    por_op = meta.setdefault("reintentos_6013_por_operacion", {})
    por_op[operacion] = int(por_op.get(operacion, 0)) + total


def _sleep_seconds_reintento_6013(settings: Settings, intento: int) -> float:
    """
    Intento 1 = primer reintento tras el primer 6013.

    - `fixed` (legacy): siempre `arca_moa_retry_6013_sleep_seconds`.
    - `exponential`: `base * 2**(intento-1)` con jitter ±20%, capado por
      `arca_moa_retry_6013_max_sleep_seconds`. Más ágil los 1-2 primeros intentos y
      más conservador los últimos sin sobrecargar AFIP.
    """
    base = max(1.0, float(settings.arca_moa_retry_6013_sleep_seconds))
    mode = (settings.arca_moa_retry_6013_backoff or "fixed").strip().lower()
    if mode != "exponential":
        return base
    raw = base * (2 ** max(0, intento - 1))
    cap = max(base, float(settings.arca_moa_retry_6013_max_sleep_seconds))
    capped = min(raw, cap)
    jitter = random.uniform(-0.2, 0.2)
    return max(1.0, capped * (1.0 + jitter))


def _retry_moa_call(
    settings: Settings,
    client: Client,
    call: Callable[[Client], Any],
) -> tuple[Any, int]:
    """call(client) -> rta. Reintenta ante 6013 (reutiliza el mismo Client; no recarga WSDL)."""
    retries_6013 = 0
    max_retries = max(1, int(settings.arca_moa_retry_6013_max_retries))
    while True:
        rta = call(client)
        if _has_error_6013(rta) and retries_6013 < max_retries:
            retries_6013 += 1
            sleep_s = _sleep_seconds_reintento_6013(settings, retries_6013)
            logger.warning(
                "MOA 6013, reintento %s/%s tras %.1fs (backoff=%s)",
                retries_6013,
                max_retries,
                sleep_s,
                (settings.arca_moa_retry_6013_backoff or "fixed").strip().lower(),
            )
            time.sleep(sleep_s)
            continue
        return rta, retries_6013


def _limpiar_params(p: dict[str, Any]) -> dict[str, Any]:
    """No enviar claves vacías al SOAP (algunos servicios las interpretan mal)."""
    return {k: v for k, v in p.items() if v is not None and v != ""}


_SOAP_MAX_PER_ENTRY_BYTES = 300_000  # ~300 KB por envelope, suficiente para un listado CANC


def _capture_soap(
    history: HistoryPlugin,
    operation: str,
    variante: str,
    meta: dict[str, Any],
) -> None:
    """Vuelca los últimos envelopes SOAP enviados/recibidos en ``meta['raw_soap_listados']``.

    Solo se invoca si ``arca_moa_log_raw_soap`` está activo. Captura ambos envelopes para
    poder reproducir el caso sin re-llamar a AFIP. Cada entrada se trunca a
    ``_SOAP_MAX_PER_ENTRY_BYTES`` para que no explote el JSON del meta.
    """
    try:
        sent = history.last_sent
        received = history.last_received
    except (IndexError, AttributeError):
        return
    if sent is None or received is None:
        return
    try:
        sent_xml = etree.tostring(
            sent["envelope"], pretty_print=True, encoding="unicode"
        )
        recv_xml = etree.tostring(
            received["envelope"], pretty_print=True, encoding="unicode"
        )
    except (TypeError, ValueError, etree.LxmlError):
        return
    raw = meta.setdefault("raw_soap_listados", [])
    raw.append(
        {
            "operacion": operation,
            "variante": variante,
            "sent_xml": sent_xml[:_SOAP_MAX_PER_ENTRY_BYTES],
            "received_xml": recv_xml[:_SOAP_MAX_PER_ENTRY_BYTES],
            "received_truncado": len(recv_xml) > _SOAP_MAX_PER_ENTRY_BYTES,
        }
    )


def _variantes_auto_simi(
    desde: datetime,
    hasta: datetime,
    cuit: str,
) -> list[tuple[str, dict[str, Any]]]:
    # AFIP rechaza CUIT con guiones (42075). CodigoEstadoDeclaracion="TODOS" también es
    # rechazado por longitud (42075); hay que pasar códigos válidos de 4 letras (OFIC/CANC/ANUL).
    b = {"FechaOficializacionDesde": desde, "FechaOficializacionHasta": hasta}
    return [
        ("cuit_11", {**b, "CuitImportadorExportador": cuit}),
        (
            "cuit_11_CANC",
            {**b, "CuitImportadorExportador": cuit, "CodigoEstadoDeclaracion": "CANC"},
        ),
        (
            "cuit_11_ANUL",
            {**b, "CuitImportadorExportador": cuit, "CodigoEstadoDeclaracion": "ANUL"},
        ),
    ]


def _variantes_auto_detallada(
    desde: datetime,
    hasta: datetime,
    cuit: str,
) -> list[tuple[str, dict[str, Any]]]:
    # AFIP rechaza CUIT con guiones y CodigoTipoOperacion=I/IC (42075). CodigoEstadoDeclaracion
    # "TODOS" también es rechazado (longitud inválida); válidos: OFIC/CANC/ANUL/SUSP.
    b = {"FechaOficializacionDesde": desde, "FechaOficializacionHasta": hasta}
    return [
        ("cuit_11", {**b, "CuitImportadorExportador": cuit}),
        (
            "cuit_11_CANC",
            {**b, "CuitImportadorExportador": cuit, "CodigoEstadoDeclaracion": "CANC"},
        ),
        (
            "cuit_11_ANUL",
            {**b, "CuitImportadorExportador": cuit, "CodigoEstadoDeclaracion": "ANUL"},
        ),
    ]


def _variantes_minimal_simi(
    desde: datetime,
    hasta: datetime,
    cuit: str,
) -> list[tuple[str, dict[str, Any]]]:
    """
    Listado SIMI: la variante solo-CUIT omite CANC/ANUL (AFIP devuelve solo OFIC por defecto).
    Agregamos `cuit_11_CANC` para capturar declaraciones canceladas (caso típico: D.I. de
    importación retenidas). "TODOS" fue eliminado: AFIP lo rechaza con 42075 por longitud.
    """
    b = {"FechaOficializacionDesde": desde, "FechaOficializacionHasta": hasta}
    return [
        ("cuit_11", {**b, "CuitImportadorExportador": cuit}),
        (
            "cuit_11_CANC",
            {**b, "CuitImportadorExportador": cuit, "CodigoEstadoDeclaracion": "CANC"},
        ),
    ]


def _variantes_minimal_detallada(
    desde: datetime,
    hasta: datetime,
    cuit: str,
) -> list[tuple[str, dict[str, Any]]]:
    """
    DetalladaListaDeclaraciones: la variante solo-CUIT trae OFIC por defecto. Agregamos
    `cuit_11_CANC` para que también aparezcan las canceladas. "TODOS" fue eliminado: AFIP
    lo rechaza con 42075 "longitud invalida" (códigos válidos son de 4 letras: OFIC/CANC/
    ANUL/SUSP).
    """
    b = {"FechaOficializacionDesde": desde, "FechaOficializacionHasta": hasta}
    return [
        ("cuit_11", {**b, "CuitImportadorExportador": cuit}),
        (
            "cuit_11_CANC",
            {**b, "CuitImportadorExportador": cuit, "CodigoEstadoDeclaracion": "CANC"},
        ),
    ]


def _pick_variantes_lista(
    settings: Settings,
    desde: datetime,
    hasta: datetime,
    cuit: str,
) -> tuple[list[tuple[str, dict[str, Any]]], list[tuple[str, dict[str, Any]]]]:
    mode = (settings.arca_moa_lista_variantes or "minimal").strip().lower()
    if mode == "minimal":
        return (
            _variantes_minimal_simi(desde, hasta, cuit),
            _variantes_minimal_detallada(desde, hasta, cuit),
        )
    return (
        _variantes_auto_simi(desde, hasta, cuit),
        _variantes_auto_detallada(desde, hasta, cuit),
    )


def _extract_simi_declaraciones(rta: Any) -> list[dict[str, Any]]:
    """SimiDjaiListaDeclaraciones → filas tipo grilla SIMI."""
    result = getattr(rta, "SimiDjaiListaDeclaracionesResult", None) or rta
    decls = getattr(result, "Declaraciones", None) if result is not None else None
    if decls is None and isinstance(result, dict):
        decls = result.get("Declaraciones")
    if decls is None:
        return []
    inner = getattr(decls, "DeclaracionDetallada", None)
    if inner is None and isinstance(decls, dict):
        inner = decls.get("DeclaracionDetallada")
    if inner is None:
        inner = getattr(decls, "Declaracion", None)
    if inner is None and isinstance(decls, dict):
        inner = decls.get("Declaracion")
    if inner is None:
        return []
    if not isinstance(inner, list):
        inner = [inner]
    out: list[dict[str, Any]] = []
    for d in inner:
        if d is None:
            continue
        row = _obj_to_dict(d)
        if "FechaOficializacionDeclaracion" not in row and row.get("FechaOficializacion"):
            row["FechaOficializacionDeclaracion"] = row["FechaOficializacion"]
        out.append(row)
    return out


def _id_listado_declaracion(row: dict[str, Any]) -> str:
    """Id de fila de listado: Declaracion o Destinacion (MOA usa ambos nombres)."""
    for k in ("IdentificadorDeclaracion", "IdentificadorDestinacion"):
        v = str(row.get(k) or "").strip()
        if v:
            return v
    return ""


def _normalizar_fila_declaracion(row: dict[str, Any]) -> dict[str, Any]:
    r = dict(row)
    iid = _id_listado_declaracion(r)
    if iid and not str(r.get("IdentificadorDeclaracion") or "").strip():
        r["IdentificadorDeclaracion"] = iid
    return r


def _valor_listado_vacio(v: Any) -> bool:
    if v is None:
        return True
    return str(v).strip() == ""


def _merge_declaraciones_por_id(
    dest: dict[str, dict[str, Any]],
    rows: list[dict[str, Any]],
) -> None:
    """
    Acumula por Identificador*; filas posteriores completan (p. ej. SIMI sobre Detallada).

    No pisamos un valor **ya informado** con cadena vacía / null: Detallada puede traer la
    fila en estado CANC sin montos y borraría FOB/FLETE/SEGURO que SIMI había enviado antes.
    """
    for row in rows:
        norm = _normalizar_fila_declaracion(row)
        iid = _id_listado_declaracion(norm)
        if not iid:
            continue
        prev = dest.get(iid, {})
        merged: dict[str, Any] = {**prev}
        for k, v in norm.items():
            if _valor_listado_vacio(v) and not _valor_listado_vacio(merged.get(k)):
                continue
            merged[k] = v
        dest[iid] = merged


def destinacion_ids_sin_caratula_en_sqlite(sqlite_path: Any) -> list[str]:
    """
    D.I. que ya existen en SQLite pero su `raw_json` no tiene `moa_detallada_caratula`.

    Filtramos filas con `destinacion_id` no vacío. `instr(..., 0) = 0` es un LIKE exacto
    sobre la clave JSON — SQLite no indexa JSON acá, pero el coste es aceptable (ejecuta
    una vez por fetch). No deduplica entre IMPORT/EXPORT; devolver duplicados molestaría
    al merge que sí chequea membership.
    """
    import sqlite3

    try:
        conn = sqlite3.connect(str(sqlite_path))
    except sqlite3.Error:
        return []
    try:
        cur = conn.execute(
            """
            SELECT DISTINCT destinacion_id
            FROM liquidaciones
            WHERE destinacion_id IS NOT NULL
              AND TRIM(destinacion_id) <> ''
              AND instr(COALESCE(raw_json, ''), '"moa_detallada_caratula"') = 0
            ORDER BY destinacion_id
            """
        )
        return [str(r[0]).strip() for r in cur.fetchall() if r[0] is not None]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def merge_declaraciones_huerfanas_sin_caratula(
    dest: dict[str, dict[str, Any]],
    settings: Settings,
) -> list[str]:
    """
    Inyecta D.I. que ya están en SQLite pero sin `moa_detallada_caratula` (igual que
    `merge_declaraciones_extra_desde_settings`, pero la fuente son las propias filas
    incompletas). Devuelve los ids añadidos en este paso.
    """
    if not bool(settings.arca_moa_autoinject_sin_caratula):
        return []
    ids = destinacion_ids_sin_caratula_en_sqlite(settings.arca_sqlite_path)
    if not ids:
        return []
    added: list[str] = []
    for did in ids:
        if len(did) < 5 or did in dest:
            continue
        dest[did] = {
            "IdentificadorDestinacion": did,
            "IdentificadorDeclaracion": did,
            "_fuente_inyeccion": "ARCA_MOA_AUTOINJECT_SIN_CARATULA",
        }
        added.append(did)
        logger.info("MOA: inyectado D.I. huérfano sin carátula: %s", did)
    return added


def _extract_cancelaciones_detalladas(rta: Any) -> list[dict[str, Any]]:
    """
    Aplana la respuesta de ConsultarCancelacionDetallada.

    El WSDL envuelve el resultado en ``ConsultarCancelacionDetalladaResult.Resultado`` →
    ``ArrayOfCancelacionDetallada``. Cada ``CancelacionDetallada`` trae:

    - ``DestinacionCancelada``: el IdentificadorDestinacion cancelado.
    - ``FechaOficializacion``: la fecha que DetalladaListaDeclaraciones NO nos devuelve
      para CANC (clave para poblar ``fecha`` en SQLite).
    - ``FechaCancelacion``: momento en que se dio la cancelación.
    - ``Estado`` / ``ModalidadCancelacion``: contexto para el panel.

    Devuelve ``[]`` si la respuesta vino sin cancelaciones o con estructura inesperada.
    """
    result = getattr(rta, "ConsultarCancelacionDetalladaResult", None)
    if result is None and isinstance(rta, dict):
        result = rta.get("ConsultarCancelacionDetalladaResult")
    if result is None:
        result = rta
    resultado = getattr(result, "Resultado", None) if result is not None else None
    if resultado is None and isinstance(result, dict):
        resultado = result.get("Resultado")
    if resultado is None:
        return []
    inner = getattr(resultado, "CancelacionDetallada", None)
    if inner is None and isinstance(resultado, dict):
        inner = resultado.get("CancelacionDetallada")
    if inner is None:
        return []
    if not isinstance(inner, list):
        inner = [inner]
    out: list[dict[str, Any]] = []
    for c in inner:
        if c is None:
            continue
        out.append(_obj_to_dict(c))
    return out


def _consultar_cancelacion_detallada(
    settings: Settings,
    client: Client,
    auth: dict[str, Any],
    cuit: str,
    *,
    id_declaracion: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Core SOAP: reusa ``client`` y ``auth`` ya armados. Devuelve (cancelaciones, r6013)."""
    input_params: dict[str, Any] = {"CuitImportadorExportador": cuit}
    if id_declaracion and id_declaracion.strip():
        input_params["IdentificadorDeclaracion"] = id_declaracion.strip()
    input_params = _limpiar_params(input_params)

    rta, r6013 = _retry_moa_call(
        settings,
        client,
        lambda c: c.service.ConsultarCancelacionDetallada(
            argWSAutenticacionEmpresa=auth,
            inputCancelacionDetallada=input_params,
        ),
    )
    _raise_if_moa_errors(rta, "ConsultarCancelacionDetallada")
    return _extract_cancelaciones_detalladas(rta), r6013


def fetch_moa_cancelaciones_detalladas(
    settings: Settings,
    cuit: str,
    ta_xml: bytes,
    *,
    id_declaracion: str | None = None,
) -> list[dict[str, Any]]:
    """
    Consulta ConsultarCancelacionDetallada para un CUIT (y opcionalmente un D.I.).

    Este método devuelve cancelaciones con su ``FechaOficializacion`` y ``FechaCancelacion``,
    info que ``DetalladaListaDeclaraciones`` **no** entrega para estado CANC (AFIP responde
    la lista pero omite las fechas). Se usa para:

    - Completar fecha en D.I. inyectados vía env (que entran al pipeline con fecha=None).
    - Descubrir D.I. cancelados que no aparecen en ningún listado (caso típico: D.I.
      retenidas y anuladas post-oficialización).

    Devuelve una lista de diccionarios (ya normalizados). Nunca lanza por lista vacía; sí
    propaga ``RuntimeError`` si AFIP devuelve errores de SOAP (p. ej. 7008 TA inválido).
    """
    _validar_config_moa_minima(settings)
    ta = parse_ticket_xml(ta_xml)
    client = _make_moa_client(settings, ta)
    auth = _moa_auth_dict(ta, cuit, settings)
    cancelaciones, r6013 = _consultar_cancelacion_detallada(
        settings, client, auth, cuit, id_declaracion=id_declaracion
    )
    if r6013 > 0:
        logger.info(
            "ConsultarCancelacionDetallada: %s cancelaciones (reintentos 6013=%s)",
            len(cancelaciones),
            r6013,
        )
    return cancelaciones


def _indexar_cancelaciones_por_dest(
    cancelaciones: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """
    Mapa {IdentificadorDestinacion: cancelacion}.

    Si un mismo D.I. aparece varias veces (ítems cancelados distintos), nos quedamos con
    el primero: sus fechas son globales al D.I., no dependen del ítem.
    """
    index: dict[str, dict[str, Any]] = {}
    for c in cancelaciones:
        dest = str(c.get("DestinacionCancelada") or "").strip()
        if not dest or dest in index:
            continue
        index[dest] = c
    return index


def _fecha_en_rango(
    fecha: date | None,
    desde: date,
    hasta: date,
) -> bool:
    if fecha is None:
        return False
    return desde <= fecha <= hasta


def _inyectar_fecha_desde_cancelacion(
    fila: dict[str, Any],
    cancelacion: dict[str, Any],
) -> None:
    """Copia FechaOficializacion/FechaCancelacion de la cancelación AFIP a una fila de listado."""
    for origen, destino in (
        ("FechaOficializacion", "FechaOficializacionDeclaracion"),
        ("FechaOficializacion", "FechaOficializacion"),
        ("FechaCancelacion", "FechaCancelacion"),
        ("Estado", "_estado_cancelacion"),
        ("ModalidadCancelacion", "_modalidad_cancelacion"),
    ):
        val = cancelacion.get(origen)
        if val is None:
            continue
        if fila.get(destino) in (None, ""):
            fila[destino] = val


def merge_declaraciones_desde_cancelaciones(
    dest: dict[str, dict[str, Any]],
    cancelaciones_por_dest: dict[str, dict[str, Any]],
    *,
    fecha_desde: date,
    fecha_hasta: date,
    incluir_fuera_de_rango: bool = False,
) -> tuple[list[str], int]:
    """
    Usa el índice de cancelaciones para:

    1. Completar FechaOficializacion en filas ya presentes (inyectadas por env / listado).
    2. Inyectar D.I. cancelados **no listados** cuya fecha caiga en el rango del fetch
       (o todos, si ``incluir_fuera_de_rango``). Así aparecen en el pipeline detalle.

    Devuelve (ids_inyectados, filas_con_fecha_completada).
    """
    added: list[str] = []
    completadas = 0
    ya_presentes_ids = set(dest.keys())

    for did, fila in list(dest.items()):
        cancelacion = cancelaciones_por_dest.get(did)
        if not cancelacion:
            continue
        antes = _parse_fecha_decl(fila)
        _inyectar_fecha_desde_cancelacion(fila, cancelacion)
        despues = _parse_fecha_decl(fila)
        if antes is None and despues is not None:
            completadas += 1

    for did, cancelacion in cancelaciones_por_dest.items():
        if did in ya_presentes_ids:
            continue
        fecha_ofic = _parse_fecha_val(cancelacion.get("FechaOficializacion"))
        if not incluir_fuera_de_rango and not _fecha_en_rango(
            fecha_ofic, fecha_desde, fecha_hasta
        ):
            continue
        fila: dict[str, Any] = {
            "IdentificadorDestinacion": did,
            "IdentificadorDeclaracion": did,
            "_fuente_inyeccion": "ConsultarCancelacionDetallada",
        }
        _inyectar_fecha_desde_cancelacion(fila, cancelacion)
        dest[did] = fila
        added.append(did)
        logger.info(
            "MOA: inyectado D.I. cancelado descubierto por AFIP: %s (FechaOficializacion=%s)",
            did,
            fecha_ofic.isoformat() if fecha_ofic else "—",
        )

    return added, completadas


def merge_declaraciones_extra_desde_settings(
    dest: dict[str, dict[str, Any]],
    settings: Settings,
) -> list[str]:
    """
    Inyecta D.I. conocidos (env) para forzar DetalladaLiquidaciones aunque no vengan en listados.
    Devuelve la lista de ids añadidos.
    """
    raw = (settings.arca_moa_destinacion_ids_extra or "").strip()
    if not raw:
        return []
    added: list[str] = []
    for part in raw.split(","):
        did = part.strip()
        if len(did) < 5:
            continue
        if did in dest:
            continue
        dest[did] = {
            "IdentificadorDestinacion": did,
            "IdentificadorDeclaracion": did,
            "_fuente_inyeccion": "ARCA_MOA_DESTINACION_IDS_EXTRA",
        }
        added.append(did)
        logger.info("MOA: inyectado D.I. desde ARCA_MOA_DESTINACION_IDS_EXTRA: %s", did)
    return added


def _ejecutar_variantes_simi(
    settings: Settings,
    auth: dict[str, Any],
    client: Client,
    variantes: list[tuple[str, dict[str, Any]]],
    *,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
    progress_scope: str = "",
    history: HistoryPlugin | None = None,
    meta_raw: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Devuelve (resumen por variante, filas únicas por IdentificadorDeclaracion)."""
    merged: dict[str, dict[str, Any]] = {}
    resumen: list[dict[str, Any]] = []
    ok_any = False
    errs: list[str] = []
    to = int(settings.arca_liquidaciones_timeout)
    # AFIP limita el mismo método MOA con el mismo CUIT a >=25s entre llamadas (error 6013);
    # variantes del listado son todas SimiDjaiListaDeclaraciones → respetar ese intervalo.
    sleep_entre_variantes = max(26.0, float(settings.arca_moa_chunk_sleep_seconds))
    for i, (label, parms) in enumerate(variantes):
        if i > 0:
            if on_progress:
                scope = f"{progress_scope} · " if progress_scope else ""
                on_progress(
                    {
                        "phase": "moa_listado_variante",
                        "current": i + 1,
                        "total": len(variantes),
                        "message": (
                            f"{scope}SIMI/DJAI: pausa {sleep_entre_variantes:.0f}s entre variantes "
                            f"(AFIP exige >=25s por método/CUIT)…"
                        ),
                    }
                )
            time.sleep(sleep_entre_variantes)
        parms = _limpiar_params(parms)
        if on_progress:
            scope = f"{progress_scope} · " if progress_scope else ""
            on_progress(
                {
                    "phase": "moa_listado_variante",
                    "current": i + 1,
                    "total": len(variantes),
                    "message": (
                        f"{scope}SIMI/DJAI «{label}» ({i + 1}/{len(variantes)}) — "
                        f"esperando AFIP (hasta ~{to}s)…"
                    ),
                }
            )
        try:
            rta, r6013 = _retry_moa_call(
                settings,
                client,
                lambda c, p=parms: c.service.SimiDjaiListaDeclaraciones(
                    argWSAutenticacionEmpresa=auth,
                    argSimiDjaiListaParams=p,
                ),
            )
            _raise_if_moa_errors(rta, f"SimiDjaiListaDeclaraciones.{label}")
            if history is not None and meta_raw is not None:
                _capture_soap(history, "SimiDjaiListaDeclaraciones", label, meta_raw)
            ok_any = True
            rows = _extract_simi_declaraciones(rta)
            _merge_declaraciones_por_id(merged, rows)
            resumen.append(
                {
                    "variante": label,
                    "n": len(rows),
                    "reintentos_6013": r6013,
                    "acumulado": len(merged),
                }
            )
        except RuntimeError as e:
            errs.append(f"{label}: {e}")
            logger.debug("MOA Simi variante %s omitida: %s", label, e)
            resumen.append({"variante": label, "error": str(e)})
            continue
    if not merged and errs and not ok_any:
        raise RuntimeError(
            "SimiDjaiListaDeclaraciones: todas las variantes fallaron. " + " | ".join(errs)
        )
    return resumen, list(merged.values())


def _ejecutar_variantes_detallada(
    settings: Settings,
    auth: dict[str, Any],
    client: Client,
    variantes: list[tuple[str, dict[str, Any]]],
    *,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
    progress_scope: str = "",
    history: HistoryPlugin | None = None,
    meta_raw: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    merged: dict[str, dict[str, Any]] = {}
    resumen: list[dict[str, Any]] = []
    ok_any = False
    errs: list[str] = []
    to = int(settings.arca_liquidaciones_timeout)
    # AFIP limita el mismo método MOA con el mismo CUIT a >=25s entre llamadas (error 6013);
    # variantes del listado son todas DetalladaListaDeclaraciones → respetar ese intervalo.
    sleep_entre_variantes = max(26.0, float(settings.arca_moa_chunk_sleep_seconds))
    for i, (label, parms) in enumerate(variantes):
        if i > 0:
            if on_progress:
                scope = f"{progress_scope} · " if progress_scope else ""
                on_progress(
                    {
                        "phase": "moa_listado_variante",
                        "current": i + 1,
                        "total": len(variantes),
                        "message": (
                            f"{scope}DetalladaLista: pausa {sleep_entre_variantes:.0f}s entre "
                            f"variantes (AFIP exige >=25s por método/CUIT)…"
                        ),
                    }
                )
            time.sleep(sleep_entre_variantes)
        parms = _limpiar_params(parms)
        if on_progress:
            scope = f"{progress_scope} · " if progress_scope else ""
            on_progress(
                {
                    "phase": "moa_listado_variante",
                    "current": i + 1,
                    "total": len(variantes),
                    "message": (
                        f"{scope}DetalladaLista «{label}» ({i + 1}/{len(variantes)}) — "
                        f"esperando AFIP (hasta ~{to}s)…"
                    ),
                }
            )
        try:
            rta, r6013 = _retry_moa_call(
                settings,
                client,
                lambda c, p=parms: c.service.DetalladaListaDeclaraciones(
                    argWSAutenticacionEmpresa=auth,
                    argDetalladasListaParams=p,
                ),
            )
            _raise_if_moa_errors(rta, f"DetalladaListaDeclaraciones.{label}")
            if history is not None and meta_raw is not None:
                _capture_soap(history, "DetalladaListaDeclaraciones", label, meta_raw)
            ok_any = True
            rows = _extract_declaraciones(rta)
            _merge_declaraciones_por_id(merged, rows)
            resumen.append(
                {
                    "variante": label,
                    "n": len(rows),
                    "reintentos_6013": r6013,
                    "acumulado": len(merged),
                }
            )
        except RuntimeError as e:
            errs.append(f"{label}: {e}")
            logger.debug("MOA Detallada variante %s omitida: %s", label, e)
            resumen.append({"variante": label, "error": str(e)})
            continue
    if not merged and errs and not ok_any:
        raise RuntimeError(
            "DetalladaListaDeclaraciones: todas las variantes fallaron. " + " | ".join(errs)
        )
    return resumen, list(merged.values())


def fetch_moa_declaracion_liquidaciones(
    settings: Settings,
    cuit: str,
    fecha_desde: date,
    fecha_hasta: date,
    ta_xml: bytes,
    *,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[Liquidacion], dict[str, Any]]:
    """
    Listado vía DetalladaListaDeclaraciones y/o SimiDjaiListaDeclaraciones (ARCA_MOA_LISTA_FUENTE).
    Luego DetalladaLiquidaciones + DetalladaLiquidacionesDetalle por declaración.
    SimiDjai se alinea con la grilla SIMI/DJAI en MOA web.
    """
    if not settings.arca_liquidaciones_wsdl:
        raise ValueError("ARCA_LIQUIDACIONES_WSDL requerido para MOA")
    if not (settings.arca_moa_tipo_agente or "").strip():
        raise ValueError(
            "Configure ARCA_MOA_TIPO_AGENTE en .env (MOA lo exige; sin él, error 7016). "
            "Valor según perfil MOA / documentación AFIP."
        )

    ta = parse_ticket_xml(ta_xml)
    log_raw_soap = bool(settings.arca_moa_log_raw_soap)
    history_plugin = HistoryPlugin() if log_raw_soap else None

    def _make_moa_client() -> Client:
        transport = Transport(
            session=requests.Session(),
            timeout=settings.arca_liquidaciones_timeout,
        )
        plugins: list[Any] = [TokenSignPlugin(ta.token, ta.sign, settings.arca_soap_auth_ns)]
        if history_plugin is not None:
            plugins.append(history_plugin)
        return Client(settings.arca_liquidaciones_wsdl, transport=transport, plugins=plugins)

    auth = _moa_auth_dict(ta, cuit, settings)
    # Un solo Client para listados + detalle: evita recargar el WSDL en cada llamada (muy lento).
    moa_client = _make_moa_client()

    meta: dict[str, Any] = {
        "modo": settings.arca_mode,
        "cuit": cuit,
        "fecha_desde": fecha_desde.isoformat(),
        "fecha_hasta": fecha_hasta.isoformat(),
        "moa": "wconsdeclaracion",
        "moa_lista_fuente": _lista_fuente_normalizada(settings),
        "moa_lista_variantes": (settings.arca_moa_lista_variantes or "minimal").strip().lower(),
        "chunks_30d": [],
        "operaciones": [],
        "reintentos_6013_total": 0,
        "reintentos_6013_por_operacion": {},
    }
    t_inicio = time.monotonic()
    fuente = meta["moa_lista_fuente"]

    declaraciones_por_id: dict[str, dict[str, Any]] = {}
    chunks = _iter_chunks_30_days(fecha_desde, fecha_hasta)
    for idx, (chunk_desde, chunk_hasta) in enumerate(chunks):
        if on_progress:
            on_progress(
                {
                    "phase": "moa_listado_30d",
                    "current": idx + 1,
                    "total": len(chunks),
                    "message": (
                        f"Buscando listados {chunk_desde.isoformat()} → {chunk_hasta.isoformat()} "
                        f"({idx + 1}/{len(chunks)} tramos de 30 días)"
                    ),
                }
            )
        if idx > 0:
            time.sleep(max(0.5, float(settings.arca_moa_chunk_sleep_seconds)))
        desde = _day_start_utc_ar(chunk_desde)
        hasta = _day_end_utc_ar(chunk_hasta)
        vs_simi, vs_det = _pick_variantes_lista(settings, desde, hasta, cuit)

        chunk_meta: dict[str, Any] = {
            "desde": chunk_desde.isoformat(),
            "hasta": chunk_hasta.isoformat(),
        }
        if fuente in ("detallada", "both"):
            chunk_scope = f"{chunk_desde.isoformat()} → {chunk_hasta.isoformat()}"
            res_det, rows_det = _ejecutar_variantes_detallada(
                settings,
                auth,
                moa_client,
                vs_det,
                on_progress=on_progress,
                progress_scope=chunk_scope,
                history=history_plugin,
                meta_raw=meta,
            )
            chunk_meta["detallada"] = {"variantes": res_det}
            _acumular_6013(meta, res_det, "DetalladaListaDeclaraciones")
            _merge_declaraciones_por_id(declaraciones_por_id, rows_det)
        if fuente in ("simi_djai", "both"):
            if fuente == "both":
                time.sleep(2.0)
            chunk_scope = f"{chunk_desde.isoformat()} → {chunk_hasta.isoformat()}"
            res_simi, rows_simi = _ejecutar_variantes_simi(
                settings,
                auth,
                moa_client,
                vs_simi,
                on_progress=on_progress,
                progress_scope=chunk_scope,
                history=history_plugin,
                meta_raw=meta,
            )
            chunk_meta["simi_djai"] = {"variantes": res_simi}
            _acumular_6013(meta, res_simi, "SimiDjaiListaDeclaraciones")
            _merge_declaraciones_por_id(declaraciones_por_id, rows_simi)
        meta["chunks_30d"].append(chunk_meta)

    extra_ids = merge_declaraciones_extra_desde_settings(declaraciones_por_id, settings)
    meta["destinacion_ids_extra_inyectados"] = extra_ids
    huerfanos_ids = merge_declaraciones_huerfanas_sin_caratula(declaraciones_por_id, settings)
    meta["destinacion_ids_huerfanos_reinjectados"] = huerfanos_ids

    # Fase 1: descubrir cancelaciones y completar FechaOficializacion.
    # DetalladaListaDeclaraciones NO devuelve las fechas en estado CANC, así que usamos
    # ConsultarCancelacionDetallada (que sí expone FechaOficializacion + FechaCancelacion).
    if bool(settings.arca_moa_descubrir_canceladas):
        if on_progress:
            on_progress(
                {
                    "phase": "moa_cancelaciones",
                    "current": 0,
                    "total": 1,
                    "message": (
                        "Consultando cancelaciones detalladas (AFIP) para completar fechas y "
                        "descubrir D.I. en estado CANC…"
                    ),
                }
            )
        # ConsultarCancelacionDetallada también está sujeto al rate-limit 25s por método/CUIT;
        # respetamos la pausa usada entre variantes antes de pegarle.
        time.sleep(max(1.0, float(settings.arca_moa_chunk_sleep_seconds)))
        try:
            cancelaciones, r6013_canc = _consultar_cancelacion_detallada(
                settings, moa_client, auth, cuit
            )
            if r6013_canc:
                meta["reintentos_6013_total"] += r6013_canc
                meta["reintentos_6013_por_operacion"][
                    "ConsultarCancelacionDetallada"
                ] = (
                    meta["reintentos_6013_por_operacion"].get(
                        "ConsultarCancelacionDetallada", 0
                    )
                    + r6013_canc
                )
            index_canc = _indexar_cancelaciones_por_dest(cancelaciones)
            meta["cancelaciones_detalladas"] = {
                "n_total": len(cancelaciones),
                "n_destinaciones_unicas": len(index_canc),
            }
            cancel_ids, n_fechas_completadas = merge_declaraciones_desde_cancelaciones(
                declaraciones_por_id,
                index_canc,
                fecha_desde=fecha_desde,
                fecha_hasta=fecha_hasta,
                incluir_fuera_de_rango=bool(
                    settings.arca_moa_cancelaciones_incluir_fuera_de_rango
                ),
            )
            meta["cancelaciones_detalladas"]["destinacion_ids_inyectados"] = cancel_ids
            meta["cancelaciones_detalladas"]["fechas_completadas"] = n_fechas_completadas
            logger.info(
                "Cancelaciones detalladas: %s total, %s D.I. únicas, %s D.I. inyectados nuevos, "
                "%s fechas completadas",
                len(cancelaciones),
                len(index_canc),
                len(cancel_ids),
                n_fechas_completadas,
            )
        except RuntimeError as exc:
            # No abortamos el fetch: la cancelaciones son un complemento, no el core.
            logger.warning(
                "ConsultarCancelacionDetallada falló (sigo sin info de CANC): %s", exc
            )
            meta["cancelaciones_detalladas"] = {"error": str(exc)}

    declaraciones = list(declaraciones_por_id.values())
    meta["lista_declaraciones_agrupadas"] = len(declaraciones)
    liquidaciones_out: list[Liquidacion] = []

    n_decl_total = len(declaraciones)
    if on_progress and n_decl_total > 0:
        on_progress(
            {
                "phase": "moa_detalle",
                "current": 0,
                "total": n_decl_total,
                "message": f"Descargando liquidaciones de {n_decl_total} declaración(es)…",
            }
        )

    # AFIP limita cada método MOA a >=25s por CUIT; DetalladaLiquidaciones/DetalladaCaratula/
    # DetalladaLiquidacionesDetalle se repiten por declaración, así que dejamos una pausa.
    sleep_entre_decl = max(26.0, float(settings.arca_moa_chunk_sleep_seconds))

    for idx, decl in enumerate(declaraciones, start=1):
        decl = _normalizar_fila_declaracion(decl)
        id_decl = _id_listado_declaracion(decl)
        if not id_decl:
            continue
        id_dest = id_decl

        if idx > 1:
            if on_progress:
                on_progress(
                    {
                        "phase": "moa_detalle",
                        "current": idx,
                        "total": n_decl_total,
                        "message": (
                            f"Pausa {sleep_entre_decl:.0f}s antes de declaración {id_dest} "
                            f"(AFIP exige >=25s por método/CUIT)…"
                        ),
                    }
                )
            time.sleep(sleep_entre_decl)

        if on_progress and (
            idx == 1
            or idx == n_decl_total
            or idx % max(1, min(10, n_decl_total // 8)) == 0
        ):
            on_progress(
                {
                    "phase": "moa_detalle",
                    "current": idx,
                    "total": n_decl_total,
                    "message": f"Declaración {id_dest} ({idx}/{n_decl_total})",
                }
            )

        # Resiliencia: un D.I. inyectado inválido (p.ej. MOCK o con dígito verificador
        # incorrecto -> 21248/42075/20752) no debe abortar el batch completo; lo saltamos.
        try:
            liq_rta, _r6013_l = _retry_moa_call(
                settings,
                moa_client,
                lambda c: c.service.DetalladaLiquidaciones(
                    argWSAutenticacionEmpresa=auth,
                    argIdentificadorDestinacion=id_dest,
                ),
            )
            meta["reintentos_6013_total"] += _r6013_l
            meta["reintentos_6013_por_operacion"]["DetalladaLiquidaciones"] = (
                meta["reintentos_6013_por_operacion"].get("DetalladaLiquidaciones", 0)
                + _r6013_l
            )
            meta["operaciones"].append(
                {"declaracion": id_decl, "DetalladaLiquidaciones": zeep_result_to_json(liq_rta)}
            )
            _raise_if_moa_errors(liq_rta, f"DetalladaLiquidaciones({id_dest})")

            caratula_rta, _r6013_c = _retry_moa_call(
                settings,
                moa_client,
                lambda c, dest=id_dest: c.service.DetalladaCaratula(
                    argWSAutenticacionEmpresa=auth,
                    argIdentificadorDestinacion=dest,
                ),
            )
            meta["reintentos_6013_total"] += _r6013_c
            meta["reintentos_6013_por_operacion"]["DetalladaCaratula"] = (
                meta["reintentos_6013_por_operacion"].get("DetalladaCaratula", 0) + _r6013_c
            )
            meta["operaciones"].append(
                {"declaracion": id_decl, "DetalladaCaratula": zeep_result_to_json(caratula_rta)}
            )
            _raise_if_moa_errors(caratula_rta, f"DetalladaCaratula({id_dest})")
            caratula_json = zeep_result_to_json(caratula_rta)
            if caratula_json is None:
                caratula_json = {}

            liqs = _extract_liquidaciones_summary(liq_rta)
            fecha_base = _coerce_fecha_declaracion(decl, caratula_json, liqs)

            for liq in liqs:
                id_liq = (liq.get("IdentificadorLiquidacion") or "").strip()
                if not id_liq:
                    continue

                det_rta, _r6013_d = _retry_moa_call(
                    settings,
                    moa_client,
                    lambda c, dest=id_dest, liq=id_liq: c.service.DetalladaLiquidacionesDetalle(
                        argWSAutenticacionEmpresa=auth,
                        argDetalladaLiquidacionesDetalle={
                            "IdentificadorDestinacion": dest,
                            "IdentificadorLiquidacion": liq,
                        },
                    ),
                )
                meta["reintentos_6013_total"] += _r6013_d
                meta["reintentos_6013_por_operacion"]["DetalladaLiquidacionesDetalle"] = (
                    meta["reintentos_6013_por_operacion"].get(
                        "DetalladaLiquidacionesDetalle", 0
                    )
                    + _r6013_d
                )
                meta["operaciones"].append(
                    {
                        "declaracion": id_decl,
                        "liquidacion": id_liq,
                        "DetalladaLiquidacionesDetalle": zeep_result_to_json(det_rta),
                    }
                )
                _raise_if_moa_errors(
                    det_rta, f"DetalladaLiquidacionesDetalle({id_dest},{id_liq})"
                )

                conceptos = _conceptos_from_detalle(det_rta)
                det_json = zeep_result_to_json(det_rta)
                fecha_row = fecha_base
                if fecha_row is None and isinstance(det_json, dict):
                    fecha_row = _parse_fecha_decl(det_json)
                liquidaciones_out.append(
                    Liquidacion(
                        cuit=cuit,
                        id_externo=f"{id_dest}:{id_liq}",
                        numero=id_liq,
                        fecha=fecha_row,
                        destinacion_id=id_dest,
                        conceptos=conceptos,
                        raw={
                            "identificador_declaracion": id_decl,
                            "identificador_destinacion": id_dest,
                            "liquidacion_resumen": liq,
                            "declaracion_listado": decl,
                            "moa_detallada_caratula": caratula_json,
                            "moa_detallada_liquidaciones_detalle": det_json,
                        },
                    )
                )
        except RuntimeError as exc:
            logger.warning(
                "MOA: salteando D.I. %s por error de AFIP: %s", id_dest, exc
            )
            meta.setdefault("declaraciones_con_error", []).append(
                {"identificador_destinacion": id_dest, "error": str(exc)}
            )
            if on_progress:
                on_progress(
                    {
                        "phase": "moa_detalle",
                        "current": idx,
                        "total": n_decl_total,
                        "message": (
                            f"D.I. {id_dest} descartado ({str(exc)[:120]})"
                        ),
                    }
                )
            continue

    meta["declaraciones_encontradas"] = len(declaraciones)
    meta["liquidaciones_normalizadas"] = len(liquidaciones_out)
    meta["duracion_seg"] = round(time.monotonic() - t_inicio, 1)
    logger.info(
        "MOA fetch terminó en %.1fs: %s declaraciones, %s liquidaciones, %s reintentos 6013 (%s)",
        meta["duracion_seg"],
        meta["declaraciones_encontradas"],
        meta["liquidaciones_normalizadas"],
        meta.get("reintentos_6013_total", 0),
        meta.get("reintentos_6013_por_operacion") or "sin desglose",
    )
    return liquidaciones_out, meta


def _fetch_caratula_para_dest(
    settings: Settings,
    client: Client,
    auth: dict[str, Any],
    dest: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Núcleo de refetch de un D.I.: DetalladaCaratula + DetalladaLiquidaciones + detalle.

    Requiere un `Client` y `auth` ya armados (para no recargar el WSDL por D.I.).
    """
    caratula_rta, _ = _retry_moa_call(
        settings,
        client,
        lambda c: c.service.DetalladaCaratula(
            argWSAutenticacionEmpresa=auth,
            argIdentificadorDestinacion=dest,
        ),
    )
    _raise_if_moa_errors(caratula_rta, f"DetalladaCaratula({dest})")
    caratula_json = zeep_result_to_json(caratula_rta) or {}

    # DetalladaLiquidaciones: necesito los IdentificadorLiquidacion para pedir detalle.
    liq_rta, _ = _retry_moa_call(
        settings,
        client,
        lambda c: c.service.DetalladaLiquidaciones(
            argWSAutenticacionEmpresa=auth,
            argIdentificadorDestinacion=dest,
        ),
    )
    _raise_if_moa_errors(liq_rta, f"DetalladaLiquidaciones({dest})")
    liqs = _extract_liquidaciones_summary(liq_rta)

    detalles: list[dict[str, Any]] = []
    for liq in liqs:
        id_liq = (liq.get("IdentificadorLiquidacion") or "").strip()
        if not id_liq:
            continue
        det_rta, _ = _retry_moa_call(
            settings,
            client,
            lambda c, d=dest, lid=id_liq: c.service.DetalladaLiquidacionesDetalle(
                argWSAutenticacionEmpresa=auth,
                argDetalladaLiquidacionesDetalle={
                    "IdentificadorDestinacion": d,
                    "IdentificadorLiquidacion": lid,
                },
            ),
        )
        _raise_if_moa_errors(det_rta, f"DetalladaLiquidacionesDetalle({dest},{id_liq})")
        det_json = zeep_result_to_json(det_rta) or {}
        detalles.append({"identificador_liquidacion": id_liq, "detalle": det_json})

    return caratula_json, detalles


def _make_moa_client(settings: Settings, ta: TicketAcceso) -> Client:
    transport = Transport(
        session=requests.Session(),
        timeout=settings.arca_liquidaciones_timeout,
    )
    plugin = TokenSignPlugin(ta.token, ta.sign, settings.arca_soap_auth_ns)
    return Client(settings.arca_liquidaciones_wsdl, transport=transport, plugins=[plugin])


def _validar_config_moa_minima(settings: Settings) -> None:
    if not settings.arca_liquidaciones_wsdl:
        raise ValueError("ARCA_LIQUIDACIONES_WSDL requerido para MOA")
    if not (settings.arca_moa_tipo_agente or "").strip():
        raise ValueError("Configure ARCA_MOA_TIPO_AGENTE en .env (MOA exige este campo).")


def fetch_moa_caratula_unica(
    settings: Settings,
    cuit: str,
    id_destinacion: str,
    ta_xml: bytes,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Refetch acotado: DetalladaCaratula + DetalladaLiquidacionesDetalle para **un** D.I.

    Evita listar SIMI/Detallada (6-10 llamadas MOA → 1 o 2), útil cuando un despacho ya
    existe en SQLite pero le falta `moa_detallada_caratula` (p. ej. AFIP devolvió 6013
    solo en ese tramo). Devuelve `(caratula_json, detalles_json)` con una entrada por
    `IdentificadorLiquidacion` para que el caller haga merge in-place del raw_json.
    """
    _validar_config_moa_minima(settings)
    dest = (id_destinacion or "").strip()
    if not dest:
        raise ValueError("id_destinacion vacío")

    ta = parse_ticket_xml(ta_xml)
    client = _make_moa_client(settings, ta)
    auth = _moa_auth_dict(ta, cuit, settings)
    return _fetch_caratula_para_dest(settings, client, auth, dest)


def fetch_moa_caratulas_batch(
    settings: Settings,
    cuit: str,
    id_destinaciones: list[str],
    ta_xml: bytes,
    *,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    """
    Refetch en lote: ideal para completar todos los D.I. sin `moa_detallada_caratula`.

    Arma un único WSDL Client + auth, y respeta la pausa AFIP de >=25s entre D.I.
    (mismo método MOA + mismo CUIT → error 6013 si se invoca antes).

    Devuelve una lista con una entrada por D.I.:
    ``{"destinacion_id": "...", "ok": bool, "caratula": {...}, "detalles": [...], "error": str | None}``.
    Nunca levanta por un D.I. que falla: se captura y se reporta, para no abortar la corrida.
    """
    _validar_config_moa_minima(settings)

    ids = [d.strip() for d in id_destinaciones if (d or "").strip()]
    if not ids:
        return []

    ta = parse_ticket_xml(ta_xml)
    client = _make_moa_client(settings, ta)
    auth = _moa_auth_dict(ta, cuit, settings)

    # AFIP ratelimit por método/CUIT: los 3 métodos (DetalladaCaratula, DetalladaLiquidaciones,
    # DetalladaLiquidacionesDetalle) se repiten por D.I., así que dejamos la pausa estándar.
    sleep_entre_dest = max(26.0, float(settings.arca_moa_chunk_sleep_seconds))

    resultados: list[dict[str, Any]] = []
    total = len(ids)
    for idx, dest in enumerate(ids, start=1):
        if idx > 1:
            if on_progress:
                on_progress(
                    {
                        "phase": "moa_refetch_caratula",
                        "current": idx,
                        "total": total,
                        "message": (
                            f"Pausa {sleep_entre_dest:.0f}s antes de {dest} "
                            f"(AFIP exige >=25s por método/CUIT)…"
                        ),
                    }
                )
            time.sleep(sleep_entre_dest)
        if on_progress:
            on_progress(
                {
                    "phase": "moa_refetch_caratula",
                    "current": idx,
                    "total": total,
                    "message": f"Refetch carátula {dest} ({idx}/{total})",
                }
            )
        try:
            caratula, detalles = _fetch_caratula_para_dest(settings, client, auth, dest)
            resultados.append(
                {
                    "destinacion_id": dest,
                    "ok": True,
                    "caratula": caratula,
                    "detalles": detalles,
                    "error": None,
                }
            )
        except (RuntimeError, ValueError) as exc:
            logger.warning("Refetch carátula falló para %s: %s", dest, exc)
            resultados.append(
                {
                    "destinacion_id": dest,
                    "ok": False,
                    "caratula": None,
                    "detalles": [],
                    "error": str(exc),
                }
            )
    return resultados


def _extract_declaraciones(lista_rta: Any) -> list[dict[str, Any]]:
    result = getattr(lista_rta, "DetalladaListaDeclaracionesResult", None) or lista_rta
    decls = getattr(result, "Declaraciones", None) if result is not None else None
    if decls is None and isinstance(result, dict):
        decls = result.get("Declaraciones")
    if decls is None:
        return []
    inner = getattr(decls, "Declaracion", None)
    if inner is None and isinstance(decls, dict):
        inner = decls.get("Declaracion")
    if inner is None:
        return []
    if not isinstance(inner, list):
        inner = [inner]
    out: list[dict[str, Any]] = []
    for d in inner:
        if d is None:
            continue
        out.append(_obj_to_dict(d))
    return out


def _obj_to_dict(d: Any) -> dict[str, Any]:
    if isinstance(d, dict):
        return d
    try:
        so = serialize_object(d)
        if isinstance(so, dict):
            return so
    except Exception:
        pass
    z = zeep_result_to_json(d)
    return z if isinstance(z, dict) else {}


def _parse_fecha_val(fd: Any) -> date | None:
    if fd is None:
        return None
    if isinstance(fd, datetime):
        return fd.date()
    if isinstance(fd, date):
        return fd
    s = str(fd).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt).date()
        except ValueError:
            continue
    return None


def _parse_fecha_decl(decl: dict[str, Any]) -> date | None:
    """Fecha desde fila de listado o resumen MOA (varias claves posibles)."""
    for key in (
        "FechaOficializacionDeclaracion",
        "FechaOficializacion",
        "FechaLiquidacion",
        "FechaLiquidacionDestinacion",
        "Fecha",
    ):
        d = _parse_fecha_val(decl.get(key))
        if d:
            return d
    return None


def _coerce_fecha_declaracion(
    decl: dict[str, Any],
    caratula_json: Any,
    liqs: list[dict[str, Any]],
) -> date | None:
    """
    Declaraciones inyectadas desde env no traen fechas en `decl`; AFIP sí las devuelve
    en carátula o en el resumen de liquidaciones. Sin fecha, SQLite guarda NULL y el panel
    filtra por año y «pierde» esas filas.
    """
    d = _parse_fecha_decl(decl)
    if d:
        return d
    cj = caratula_json
    if isinstance(cj, str):
        try:
            cj = json.loads(cj)
        except json.JSONDecodeError:
            cj = None
    if isinstance(cj, dict):
        for sub in (
            cj,
            cj.get("Declaracion"),
            cj.get("CaratulaDeclaracion"),
            cj.get("DeclaracionDetallada"),
        ):
            if isinstance(sub, dict):
                d = _parse_fecha_decl(sub)
                if d:
                    return d
    for liq in liqs:
        if isinstance(liq, dict):
            d = _parse_fecha_decl(liq)
            if d:
                return d
    return None


def _extract_liquidaciones_summary(liq_rta: Any) -> list[dict[str, Any]]:
    result = getattr(liq_rta, "DetalladaLiquidacionesResult", None) or liq_rta
    bucket = getattr(result, "LiquidacionesDestinacion", None) if result is not None else None
    if bucket is None and isinstance(result, dict):
        bucket = result.get("LiquidacionesDestinacion")
    if bucket is None:
        return []
    inner = getattr(bucket, "Liquidacion", None)
    if inner is None and isinstance(bucket, dict):
        inner = bucket.get("Liquidacion")
    if inner is None:
        return []
    if not isinstance(inner, list):
        inner = [inner]
    return [_obj_to_dict(x) for x in inner if x is not None]


def _conceptos_from_detalle(det_rta: Any) -> list[ConceptoLiquidacion]:
    result = getattr(det_rta, "DetalladaLiquidacionesDetalleResult", None) or det_rta
    ld = getattr(result, "LiquidacionDetalle", None) if result is not None else None
    if ld is None and isinstance(result, dict):
        ld = result.get("LiquidacionDetalle")
    if ld is None:
        return []
    inner = getattr(ld, "ConceptoLiquidacion", None)
    if inner is None and isinstance(ld, dict):
        inner = ld.get("ConceptoLiquidacion")
    if inner is None:
        return []
    if not isinstance(inner, list):
        inner = [inner]
    out: list[ConceptoLiquidacion] = []
    for c in inner:
        if c is None:
            continue
        cd = _obj_to_dict(c)
        cod = str(cd.get("CodigoConcepto") or "").strip()
        monto = cd.get("MontoLiquidado")
        try:
            imp = Decimal(str(monto)) if monto is not None else Decimal("0")
        except Exception:
            imp = Decimal("0")
        if imp == 0 and cd.get("MontoPagado") is not None:
            try:
                imp = Decimal(str(cd.get("MontoPagado")))
            except Exception:
                pass
        desc_parts = [
            str(cd.get("TipoObligacionDescripcion") or "").strip(),
            str(cd.get("TipoObligacion") or "").strip(),
        ]
        descripcion = " — ".join(p for p in desc_parts if p) or cod
        out.append(
            ConceptoLiquidacion(
                codigo=cod,
                descripcion=descripcion,
                importe=imp,
                moneda="ARS",
                raw=cd,
            )
        )
    return out
