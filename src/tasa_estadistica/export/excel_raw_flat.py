"""Extrae campos útiles del JSON guardado en SQLite (liquidación + concepto AFIP)."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any


def _raw_json_to_str(raw: Any) -> str:
    """Columna SQLite TEXT o bytes → str para json.loads."""
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def _safe_json(s: str | None) -> dict[str, Any]:
    if not s:
        return {}
    try:
        out = json.loads(s)
        return out if isinstance(out, dict) else {}
    except json.JSONDecodeError:
        return {}


# Nombres habituales en MOA (listado, SIMI, carátula); en web AFIP «VENDEDOR» / carátula SOAP.
_PROVEEDOR_DECL_KEYS: tuple[str, ...] = (
    "Vendedor",
    "NombreProveedorDestinatario",
    "DenominacionProveedorDestinatario",
    "DenominacionVendedor",
    "NombreVendedor",
    "RazonSocialVendedor",
    "DenominacionVendedorExterior",
    "DenominacionProveedorExterior",
    "RazonSocialProveedorExterior",
    "NombreProveedorExterior",
    "ProveedorExterior",
    "DenominacionProveedor",
    "NombreProveedor",
    "RazonSocialProveedor",
    "Proveedor",
    "DenominacionRemitenteComercialExterior",
    "RazonSocialRemitenteComercialExterior",
    "NombreRemitenteComercialExterior",
    "DenominacionRemitente",
    "RazonSocialRemitente",
    "NombreRemitente",
    "RemitenteComercialExterior",
)


def _walk_dicts_for_proveedor_scan(d: dict[str, Any], max_depth: int = 8) -> list[dict[str, Any]]:
    """
    Recorre dicts (p. ej. carátula MOA) buscando claves de proveedor/remitente.
    Omite conceptos de liquidación para no confundir descripciones con razón social.
    """
    out: list[dict[str, Any]] = []

    def visit(node: Any, depth: int) -> None:
        if depth > max_depth or not isinstance(node, dict):
            return
        out.append(node)
        for k, v in node.items():
            if k in ("conceptos", "ConceptoLiquidacion"):
                continue
            if isinstance(v, dict):
                visit(v, depth + 1)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        visit(item, depth + 1)

    visit(d, 0)
    return out


def extract_proveedor_from_liquidacion_raw_json(raw_json: str | bytes | None) -> str:
    """
    Proveedor exterior desde JSON guardado: listado SIMI/Detallada y/o carátula MOA.
    Incluye «Vendedor» y «NombreProveedorDestinatario» (carátula MOA).
    Orden: listado, luego carátula.
    """
    d = _safe_json(_raw_json_to_str(raw_json))
    inner = d.get("raw") or {}
    decl = inner.get("declaracion_listado")
    if not isinstance(decl, dict):
        decl = d.get("declaracion_listado")
    if isinstance(decl, dict):
        s = _proveedor_desde_declaracion_listado(decl)
        if s:
            return s
    car = inner.get("moa_detallada_caratula")
    if isinstance(car, dict):
        for bag in _walk_dicts_for_proveedor_scan(car):
            s = _proveedor_desde_declaracion_listado(bag)
            if s:
                return s
    return ""


def _proveedor_desde_declaracion_listado(decl: dict[str, Any]) -> str:
    for k in _PROVEEDOR_DECL_KEYS:
        for cand in (k, k.upper(), k.lower()):
            v = decl.get(cand)
            if v is not None and str(v).strip():
                return str(v).strip()
    for key, v in decl.items():
        if not isinstance(key, str):
            continue
        lk = key.lower()
        if ("proveedor" in lk or "vendedor" in lk) and str(v).strip():
            return str(v).strip()
    return ""


def flatten_liquidacion_raw_json(raw_json: str | bytes | None) -> dict[str, Any]:
    """Desde `liquidaciones.raw_json` (model_dump de Liquidación)."""
    raw_s = _raw_json_to_str(raw_json)
    d = _safe_json(raw_s)
    inner = d.get("raw") or {}
    lr = inner.get("liquidacion_resumen") or {}
    if not isinstance(lr, dict):
        lr = {}

    def pick(obj: dict[str, Any], *keys: str) -> str:
        for k in keys:
            v = obj.get(k)
            if v is not None and str(v).strip() != "":
                return str(v)
        return ""

    return {
        "proveedor_extraccion": extract_proveedor_from_liquidacion_raw_json(raw_s),
        "identificador_declaracion": pick(inner, "identificador_declaracion"),
        "identificador_destinacion": pick(inner, "identificador_destinacion"),
        "liq_IdentificadorLiquidacion": pick(lr, "IdentificadorLiquidacion"),
        "liq_CodigoMotivo": pick(lr, "CodigoMotivo"),
        "liq_CodigoEstado": pick(lr, "CodigoEstado"),
        "liq_CodigoMoneda": pick(lr, "CodigoMoneda"),
        "liq_Cotizacion": pick(lr, "Cotizacion"),
        "liq_TotalGarantizado": pick(lr, "TotalGarantizado"),
        "liq_TotalPagado": pick(lr, "TotalPagado"),
        "liq_TotalLiquidado": pick(lr, "TotalLiquidado"),
    }


def flatten_concepto_raw_json(raw_json: str | None) -> dict[str, Any]:
    """JSON de concepto (detalle AFIP por línea de obligación)."""
    cr = _safe_json(raw_json)

    def pick(*keys: str) -> str:
        for k in keys:
            v = cr.get(k)
            if v is not None and str(v).strip() != "":
                return str(v)
        return ""

    return {
        "co_CodigoConcepto": pick("CodigoConcepto"),
        "co_MontoPagado": pick("MontoPagado"),
        "co_MontoLiquidado": pick("MontoLiquidado"),
        "co_MontoGarantizado": pick("MontoGarantizado"),
        "co_TipoObligacion": pick("TipoObligacion"),
        "co_TipoObligacionDescripcion": pick("TipoObligacionDescripcion"),
        "co_CodigoMotivoGarantia": pick("CodigoMotivoGarantia"),
    }


def importe_concepto_efectivo(importe_db: str | None, raw_json: str | None) -> Decimal:
    """Importe mostrado: BD; si 0, MontoPagado del raw AFIP."""
    try:
        dec = Decimal(str(importe_db or "0").replace(",", ".").strip())
    except Exception:
        dec = Decimal("0")
    if dec != 0:
        return dec
    cr = _safe_json(raw_json)
    mp = cr.get("MontoPagado")
    if mp is None or str(mp).strip() == "":
        return dec
    try:
        return Decimal(str(mp).replace(",", ".").strip())
    except Exception:
        return dec


LIQ_FLAT_KEYS = list(flatten_liquidacion_raw_json("{}").keys())
CO_FLAT_KEYS = list(flatten_concepto_raw_json("{}").keys())


def _flatten_dicts_for_scan(*roots: dict[str, Any]) -> list[dict[str, Any]]:
    """Diccionarios planos a inspeccionar (incluye anidados de primer nivel)."""
    out: list[dict[str, Any]] = []
    for r in roots:
        if not r:
            continue
        out.append(r)
        for v in r.values():
            if isinstance(v, dict):
                out.append(v)
    return out


def _walk_dicts_for_recupero_scan(d: dict[str, Any], max_depth: int = 6) -> list[dict[str, Any]]:
    """
    Recorre el JSON de liquidación (dicts y listas de dicts) para encontrar claves AFIP
    (FOB, FLETE, etc.) aunque vengan anidadas en SIMI / detallada.
    No entra en `conceptos` (líneas de obligación) para evitar falsos positivos.
    """
    out: list[dict[str, Any]] = []

    def visit(node: Any, depth: int) -> None:
        if depth > max_depth or not isinstance(node, dict):
            return
        out.append(node)
        for k, v in node.items():
            if k == "conceptos":
                continue
            if isinstance(v, dict):
                visit(v, depth + 1)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        visit(item, depth + 1)

    visit(d, 0)
    return out


def _pick_first_key(
    bags: list[dict[str, Any]], candidates: tuple[str, ...]
) -> str:
    """Primer valor no vacío usando claves exactas (variantes de mayúsculas)."""
    for bag in bags:
        for cand in candidates:
            for k in (cand, cand.upper(), cand.lower()):
                if k not in bag:
                    continue
                v = bag[k]
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    return s
    return ""


def _pick_by_key_substrings(
    bags: list[dict[str, Any]],
    must_have: tuple[str, ...],
    must_not_have: tuple[str, ...] = (),
) -> str:
    """Primera clave cuyo nombre contiene todas las subcadenas (sin acentos en la búsqueda)."""

    def norm_key(k: str) -> str:
        return "".join(c.lower() for c in k if not c.isspace())

    must = tuple(norm_key(m) for m in must_have)
    bad = tuple(norm_key(m) for m in must_not_have)

    for bag in bags:
        for k, v in bag.items():
            if not isinstance(k, str):
                continue
            nk = norm_key(k)
            if not all(m in nk for m in must):
                continue
            if any(b in nk for b in bad):
                continue
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
    return ""


def extract_recupero_valores_extra(raw_json: str | bytes | None) -> dict[str, str]:
    """
    Valores opcionales para columnas Recupero V2 (ALICUOTA, BASE, FOB, FLETE, SEGURO…)
    desde `liquidaciones.raw_json` (MOA/AFIP).

    **CIF DOCUMENTAL** no se rellena aquí: en el modelo Excel es FOB+FLETE+SEGURO; eso se calcula
    en `recupero_v2_data_rows` para mantener la misma lógica.

    Si el fetch solo trajo liquidaciones sin datos de declaración, muchos campos seguirán vacíos.
    """
    out: dict[str, str] = {
        "alicuota": "",
        "base_reconstr": "",
        "fob": "",
        "flete": "",
        "seguro": "",
    }
    d = _safe_json(_raw_json_to_str(raw_json))
    if not isinstance(d, dict):
        return out

    # Profundidad amplia: carátula MOA puede venir con wrappers (DetalladaCaratulaRta, etc.).
    bags = _walk_dicts_for_recupero_scan(d, max_depth=10)

    # Alicuota / porcentaje tasa (evitar columnas genéricas "tasa" sin alícuota)
    out["alicuota"] = _pick_first_key(
        bags,
        (
            "AlicuotaCobrada",
            "Alicuota",
            "AliquotaAplicada",
            "PorcentajeAlicuota",
            "PorcentajeTasaEstadistica",
            "PorcentajeTE",
            "ValorAlicuota",
        ),
    ) or _pick_by_key_substrings(bags, ("alicuota",), ("identificador", "codigo"))
    if not out["alicuota"]:
        out["alicuota"] = _pick_by_key_substrings(
            bags, ("porcentaje", "tasa"), ("codigo", "estado")
        )

    # Base imponible / reconstrucción (si AFIP lo envía)
    out["base_reconstr"] = _pick_first_key(
        bags,
        (
            "BaseImponibleTasaEstadistica",
            "BaseImponible",
            "BaseGravamen",
            "ValorBase",
            "BaseReconstruida",
        ),
    ) or _pick_by_key_substrings(bags, ("base", "imponible"), ("cif", "documental"))

    # FOB / FLETE / SEGURO (USD u otra moneda según AFIP; nombres SIMI / detallada / carátula MOA)
    out["fob"] = _pick_first_key(
        bags,
        (
            "MontoFobTotal",
            "MontoFOBTotal",
            "ValorFOB",
            "ValorFOBUSD",
            "ValorFobUsd",
            "ImporteFOB",
            "ImporteFOBUSD",
            "ValorFob",
            "MontoFob",
            "MontoFOB",
            "ImporteFob",
            "ValorMercaderiaFOB",
            "ValorMercaderiaFob",
            "ValorMercaderiaUSD",
            "ValorMercaderiaUsd",
            "ImporteMercaderiaUSD",
            "ValorMercaderia",
            "FOB",
        ),
    ) or _pick_by_key_substrings(bags, ("fob",), ("identificador", "codigo", "estado"))
    out["flete"] = _pick_first_key(
        bags,
        (
            "MontoFleteTotal",
            "ValorFlete",
            "ValorFleteUSD",
            "ImporteFlete",
            "ImporteFleteUSD",
            "MontoFlete",
            "ImporteFleteInternacional",
            "ValorFleteInternacional",
            "Flete",
        ),
    ) or _pick_by_key_substrings(bags, ("flete",), ("identificador", "codigo"))
    out["seguro"] = _pick_first_key(
        bags,
        (
            "MontoSeguroTotal",
            "ValorSeguro",
            "ValorSeguroUSD",
            "ImporteSeguro",
            "ImporteSeguroUSD",
            "MontoSeguro",
            "ImporteSeguroEstimado",
            "ValorSeguroEstimado",
            "Seguro",
        ),
    ) or _pick_by_key_substrings(bags, ("seguro",), ("identificador", "codigo"))

    return out
