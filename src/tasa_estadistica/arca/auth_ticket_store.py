"""Persistencia local del Ticket de Acceso (TA) devuelto por WSAA."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

_AR = ZoneInfo("America/Argentina/Buenos_Aires")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TicketAcceso:
    """Credenciales extraídas del XML de loginTicketResponse."""

    token: str
    sign: str
    generation_time: str | None = None
    expiration_time: str | None = None


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _find_text(root: ET.Element, name: str) -> str | None:
    for el in root.iter():
        if _local(el.tag).lower() == name.lower() and el.text:
            return el.text.strip()
    return None


def wsaa_ticket_expired(
    xml_bytes: bytes,
    *,
    leeway: timedelta = timedelta(seconds=0),
) -> bool:
    """
    True si `expirationTime` del TA ya pasó (respecto del reloj local, mismo huso que trae el XML).

    Si falta `expirationTime` o no se puede parsear, devuelve False (no frenar el flujo).
    """
    try:
        ta = parse_ticket_xml(xml_bytes)
        exp_s = ta.expiration_time
        if not exp_s:
            return False
        normalized = exp_s.replace("Z", "+00:00")
        exp = datetime.fromisoformat(normalized)
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=_AR)
        now = datetime.now(exp.tzinfo)
        return now >= exp - leeway
    except (ValueError, TypeError, ET.ParseError) as exc:
        logger.debug("No se pudo evaluar vencimiento del TA: %s", exc)
        return False


def parse_ticket_xml(xml_bytes: bytes) -> TicketAcceso:
    """Parsea TA/respuesta WSAA y devuelve token/sign."""
    root = ET.fromstring(xml_bytes)
    token = _find_text(root, "token")
    sign = _find_text(root, "sign")
    if not token or not sign:
        raise ValueError("XML de ticket inválido: faltan token/sign")
    gen = _find_text(root, "generationTime")
    exp = _find_text(root, "expirationTime")
    return TicketAcceso(
        token=token,
        sign=sign,
        generation_time=gen,
        expiration_time=exp,
    )


def load_ticket(path: Path) -> TicketAcceso:
    if not path.is_file():
        raise FileNotFoundError(f"No existe ticket en {path}")
    return parse_ticket_xml(path.read_bytes())


def save_ticket(path: Path, xml_bytes: bytes) -> TicketAcceso:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(xml_bytes)
    logger.info("Ticket guardado en %s", path)
    return parse_ticket_xml(xml_bytes)
