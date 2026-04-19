"""Cliente de liquidaciones y conceptos: mock local o SOAP (zeep) en homologación/producción."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable

import requests
from zeep import Client
from zeep.transports import Transport

from tasa_estadistica.arca.auth_ticket_store import TicketAcceso, parse_ticket_xml
from tasa_estadistica.arca.moa_declaracion import _iter_chunks_30_days
from tasa_estadistica.arca.soap_common import TokenSignPlugin, zeep_result_to_json
from tasa_estadistica.config.settings import Settings
from tasa_estadistica.model.schemas import ConceptoLiquidacion, Liquidacion

logger = logging.getLogger(__name__)

FetchProgressFn = Callable[[dict[str, Any]], None] | None


def _mock_liquidaciones(cuit: str, fecha_desde: date, fecha_hasta: date) -> list[Liquidacion]:
    """Datos sintéticos para desarrollo y tests sin red."""
    liq = Liquidacion(
        cuit=cuit,
        id_externo="MOCK-LQ-001",
        numero="MOCK-LQ-001",
        fecha=fecha_desde,
        destinacion_id="MOCK-DES-1",
        conceptos=[
            ConceptoLiquidacion(
                codigo="TE",
                descripcion="TASA ESTADISTICA",
                importe=Decimal("1250.50"),
                moneda="ARS",
                raw={"fuente": "mock"},
            ),
            ConceptoLiquidacion(
                codigo="OT",
                descripcion="OTROS CONCEPTOS",
                importe=Decimal("100.00"),
                moneda="ARS",
                raw={"fuente": "mock"},
            ),
        ],
        raw={"fuente": "mock"},
    )
    return [liq]


def _parse_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _to_decimal(val: Any) -> Decimal:
    if val is None:
        return Decimal("0")
    if isinstance(val, Decimal):
        return val
    s = str(val).replace(",", ".").strip()
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")


def _normalize_from_dicts(
    cuit: str,
    items: list[dict[str, Any]],
    concept_key: str = "conceptos",
) -> list[Liquidacion]:
    out: list[Liquidacion] = []
    for row in items:
        concepts_raw = row.get(concept_key) or row.get("Conceptos") or []
        conceptos: list[ConceptoLiquidacion] = []
        if isinstance(concepts_raw, dict):
            concepts_raw = [concepts_raw]
        for c in concepts_raw:
            if not isinstance(c, dict):
                continue
            conceptos.append(
                ConceptoLiquidacion(
                    codigo=str(c.get("codigo") or c.get("Codigo") or "").strip(),
                    descripcion=str(c.get("descripcion") or c.get("Descripcion") or "").strip(),
                    importe=_to_decimal(c.get("importe") or c.get("Importe")),
                    moneda=str(c.get("moneda") or c.get("Moneda") or "ARS"),
                    raw=c,
                )
            )
        out.append(
            Liquidacion(
                cuit=cuit,
                id_externo=str(row.get("id_externo") or row.get("Id") or row.get("numero") or ""),
                numero=str(row.get("numero") or row.get("Numero") or ""),
                fecha=_parse_date(row.get("fecha") or row.get("Fecha")),
                destinacion_id=(
                    str(row.get("destinacion_id") or row.get("DestinacionId") or "").strip() or None
                ),
                conceptos=conceptos,
                raw=row,
            )
        )
    return out


def _dedupe_liquidaciones(items: list[Liquidacion]) -> list[Liquidacion]:
    """Evita duplicados al unir tramos de fetch (misma clave operativa)."""
    seen: set[tuple[str, str, str, str]] = set()
    out: list[Liquidacion] = []
    for liq in items:
        key = (
            liq.cuit,
            str(liq.id_externo or ""),
            liq.fecha.isoformat() if liq.fecha else "",
            str(liq.numero or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(liq)
    return out


class LiquidacionesClient:
    def __init__(self, settings: Settings) -> None:
        self._s = settings

    def fetch_liquidaciones(
        self,
        cuit: str,
        fecha_desde: date,
        fecha_hasta: date,
        ta_xml: bytes | None = None,
        *,
        on_progress: FetchProgressFn = None,
    ) -> tuple[list[Liquidacion], dict[str, Any]]:
        """
        Devuelve liquidaciones normalizadas y metadatos de trazabilidad.
        En modo live requiere ARCA_LIQUIDACIONES_WSDL y ARCA_LIQUIDACIONES_METHOD.
        Rangos largos se dividen en tramos de 30 días (SOAP y MOA); `on_progress` informa avance.
        """
        meta: dict[str, Any] = {
            "modo": self._s.arca_mode,
            "cuit": cuit,
            "fecha_desde": fecha_desde.isoformat(),
            "fecha_hasta": fecha_hasta.isoformat(),
        }
        if self._s.arca_mode.lower() == "mock":
            if on_progress:
                on_progress(
                    {
                        "phase": "mock",
                        "current": 1,
                        "total": 1,
                        "message": "Modo mock (sin red)",
                    }
                )
            data = _mock_liquidaciones(cuit, fecha_desde, fecha_hasta)
            meta["payload_resumen"] = "mock"
            return data, meta

        if not self._s.arca_liquidaciones_wsdl:
            raise ValueError(
                "En modo live debe configurarse ARCA_LIQUIDACIONES_WSDL "
                "(URL del WSDL homologación).",
            )
        if not ta_xml:
            raise ValueError("Modo live requiere ticket WSAA (ta_xml). Ejecute: tasa-arca auth")
        wsdl = (self._s.arca_liquidaciones_wsdl or "").lower()
        if "wconsdeclaracion" in wsdl:
            from tasa_estadistica.arca.moa_declaracion import fetch_moa_declaracion_liquidaciones

            moa_liqs, meta = fetch_moa_declaracion_liquidaciones(
                self._s,
                cuit,
                fecha_desde,
                fecha_hasta,
                ta_xml,
                on_progress=on_progress,
            )
            comp = (self._s.arca_liquidaciones_complemento_wsdl or "").strip()
            if not comp:
                return moa_liqs, meta
            if "wconsdeclaracion" in comp.lower():
                logger.warning(
                    "ARCA_LIQUIDACIONES_COMPLEMENTO_WSDL no debe ser MOA (wconsdeclaracion); "
                    "se omite la fusión con consultarLiquidaciones."
                )
                return moa_liqs, meta
            if on_progress:
                on_progress(
                    {
                        "phase": "complemento_zeep",
                        "current": 1,
                        "total": 1,
                        "message": (
                            "Fusionando con WS complementario (consultarLiquidaciones por CUIT/fechas)…"
                        ),
                    }
                )
            ta = parse_ticket_xml(ta_xml)
            alt = self._s.model_copy(update={"arca_liquidaciones_wsdl": comp})
            client2 = LiquidacionesClient(alt)
            zeep_meta: dict[str, Any] = {
                "modo": self._s.arca_mode,
                "cuit": cuit,
                "fecha_desde": fecha_desde.isoformat(),
                "fecha_hasta": fecha_hasta.isoformat(),
                "fuente": "complemento_zeep",
            }
            zeep_liqs, zeep_meta_out = client2._fetch_zeep_chunked(
                cuit, fecha_desde, fecha_hasta, ta, zeep_meta, on_progress=on_progress
            )
            merged = _dedupe_liquidaciones(moa_liqs + zeep_liqs)
            meta["complemento_zeep"] = zeep_meta_out
            meta["n_liquidaciones_moa"] = len(moa_liqs)
            meta["n_liquidaciones_complemento"] = len(zeep_liqs)
            meta["n_liquidaciones_merged"] = len(merged)
            return merged, meta
        ta = parse_ticket_xml(ta_xml)
        return self._fetch_zeep_chunked(
            cuit, fecha_desde, fecha_hasta, ta, meta, on_progress=on_progress
        )

    def _fetch_zeep_chunked(
        self,
        cuit: str,
        fecha_desde: date,
        fecha_hasta: date,
        ta: TicketAcceso,
        meta: dict[str, Any],
        *,
        on_progress: FetchProgressFn = None,
    ) -> tuple[list[Liquidacion], dict[str, Any]]:
        """
        AFIP suele limitar rangos largos; se divide en tramos de 30 días (mismo criterio que MOA).
        Un solo tramo si el rango cabe en 30 días calendario.
        """
        chunks = _iter_chunks_30_days(fecha_desde, fecha_hasta)
        if len(chunks) == 1:
            if on_progress:
                on_progress(
                    {
                        "phase": "zeep_chunk",
                        "current": 1,
                        "total": 1,
                        "message": f"Consultando liquidaciones {fecha_desde.isoformat()} → {fecha_hasta.isoformat()}",
                    }
                )
            return self._fetch_zeep(cuit, fecha_desde, fecha_hasta, ta, meta)

        all_norm: list[Liquidacion] = []
        chunk_info: list[dict[str, Any]] = []
        sleep_s = max(0.5, min(3.0, float(self._s.arca_moa_chunk_sleep_seconds) * 0.25))
        for i, (d, h) in enumerate(chunks):
            if on_progress:
                on_progress(
                    {
                        "phase": "zeep_chunk",
                        "current": i + 1,
                        "total": len(chunks),
                        "message": f"Tramo SOAP {d.isoformat()} → {h.isoformat()} ({i + 1}/{len(chunks)})",
                    }
                )
            if i > 0:
                time.sleep(sleep_s)
            sub_meta = {**meta, "chunk_index": i + 1, "chunk_total": len(chunks)}
            part, sub_meta = self._fetch_zeep(cuit, d, h, ta, sub_meta)
            chunk_info.append(
                {
                    "desde": d.isoformat(),
                    "hasta": h.isoformat(),
                    "n_liquidaciones": len(part),
                }
            )
            all_norm.extend(part)
        meta["zeep_chunks"] = chunk_info
        meta["soap_method"] = sub_meta.get("soap_method", meta.get("soap_method"))
        meta["respuesta_json"] = json.dumps(
            {"resumen": "varios_tramos", "chunks": chunk_info},
            ensure_ascii=False,
        )
        return _dedupe_liquidaciones(all_norm), meta

    def _fetch_zeep(
        self,
        cuit: str,
        fecha_desde: date,
        fecha_hasta: date,
        ta: TicketAcceso,
        meta: dict[str, Any],
    ) -> tuple[list[Liquidacion], dict[str, Any]]:
        method = self._s.arca_liquidaciones_method or "consultarLiquidaciones"
        header_ns = self._s.arca_soap_auth_ns
        transport = Transport(
            session=requests.Session(),
            timeout=self._s.arca_liquidaciones_timeout,
        )
        plugin = TokenSignPlugin(ta.token, ta.sign, header_ns)
        client = Client(
            self._s.arca_liquidaciones_wsdl,
            transport=transport,
            plugins=[plugin],
        )

        service = client.service
        fn = getattr(service, method, None)
        if fn is None:
            fn = getattr(client.service, method, None)
        if fn is None:
            raise AttributeError(
                f"No se encontró la operación SOAP '{method}'. "
                "Ajuste ARCA_LIQUIDACIONES_METHOD y ARCA_LIQUIDACIONES_SERVICE según el WSDL.",
            )

        raw = self._invoke_variants(fn, cuit, fecha_desde, fecha_hasta)

        meta["soap_method"] = method
        meta["respuesta_json"] = zeep_result_to_json(raw)

        items = self._extract_items_from_zeep(raw)
        normalized = _normalize_from_dicts(cuit, items)
        return normalized, meta

    def _invoke_variants(self, fn: Any, cuit: str, fecha_desde: date, fecha_hasta: date) -> Any:
        """Intenta firmas comunes de WS AFIP/ARCA sin acoplar a un WSDL concreto."""
        attempts: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = [
            (
                "kwargs_cuit_fechas",
                (),
                {"cuit": cuit, "fechaDesde": fecha_desde, "fechaHasta": fecha_hasta},
            ),
            (
                "kwargs_Cuit",
                (),
                {"Cuit": cuit, "FechaDesde": fecha_desde, "FechaHasta": fecha_hasta},
            ),
            ("args_pos", (cuit, fecha_desde, fecha_hasta), {}),
        ]
        last_err: Exception | None = None
        for _name, args, kw in attempts:
            try:
                if args:
                    return fn(*args)
                return fn(**kw)
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue
        raise RuntimeError(f"No se pudo invocar la operación SOAP: {last_err}") from last_err

    def _extract_items_from_zeep(self, raw: Any) -> list[dict[str, Any]]:
        """Intenta extraer lista de dicts desde la respuesta zeep."""
        if raw is None:
            return []
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        if isinstance(raw, dict):
            for k in ("liquidaciones", "Liquidaciones", "items", "datos", "return"):
                v = raw.get(k)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
            return [raw]
        d = getattr(raw, "__dict__", None)
        if isinstance(d, dict):
            for k in ("liquidaciones", "Liquidaciones", "items"):
                v = d.get(k)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
        return [{"_raw": zeep_result_to_json(raw)}]


def fetch_liquidaciones_from_json_payload(
    cuit: str,
    payload: str | bytes,
) -> list[Liquidacion]:
    """Parsea JSON guardado en auditoría (respuesta cruda) a liquidaciones normalizadas."""
    data = json.loads(payload)
    if isinstance(data, dict) and "items" in data:
        data = data["items"]
    if not isinstance(data, list):
        raise ValueError("JSON esperado: lista de liquidaciones")
    return _normalize_from_dicts(cuit, data)
