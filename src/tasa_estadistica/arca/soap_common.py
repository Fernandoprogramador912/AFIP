"""Utilidades SOAP compartidas (zeep + header WSAA)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from zeep import Plugin
from zeep.helpers import serialize_object


class TokenSignPlugin(Plugin):
    """Inyecta Token y Sign en el Header SOAP (nombres y namespace configurables)."""

    def __init__(self, token: str, sign: str, header_ns: str) -> None:
        self._token = token
        self._sign = sign
        self._ns = header_ns

    def egress(self, envelope, http_headers, operation, binding_options):
        from lxml import etree

        soap_ns = "http://schemas.xmlsoap.org/soap/envelope/"
        header = envelope.find(f"{{{soap_ns}}}Header")
        if header is None:
            body = envelope.find(f"{{{soap_ns}}}Body")
            if body is None:
                return envelope, http_headers
            header = etree.Element(f"{{{soap_ns}}}Header")
            body.addprevious(header)
        t_el = etree.SubElement(header, f"{{{self._ns}}}Token")
        t_el.text = self._token
        s_el = etree.SubElement(header, f"{{{self._ns}}}Sign")
        s_el.text = self._sign
        return envelope, http_headers


def zeep_result_to_json(obj: Any) -> Any:
    """Convierte respuesta zeep a estructura JSON-friendly (sin Decimal/datetime crudos)."""
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        if obj == obj.to_integral():
            return int(obj)
        return float(obj)
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: zeep_result_to_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [zeep_result_to_json(x) for x in obj]
    if hasattr(obj, "__dict__"):
        d = {
            k: zeep_result_to_json(v)
            for k, v in obj.__dict__.items()
            if not k.startswith("_")
        }
        if d:
            return d
    # Zeep CompoundValue u otros tipos: __dict__ vacío; serialize_object sí recorre el esquema.
    try:
        so = serialize_object(obj)
        if so is not obj:
            return zeep_result_to_json(so)
    except Exception:
        pass
    return str(obj)
