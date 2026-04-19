"""Cliente WSAA: TRA, firma CMS (PKCS#7) y loginCms."""

from __future__ import annotations

import base64
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import pkcs7, pkcs12

from tasa_estadistica.config.settings import Settings

logger = logging.getLogger(__name__)

AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _hash_alg(name: str) -> hashes.HashAlgorithm:
    n = name.lower().strip()
    if n == "sha1":
        return hashes.SHA1()
    if n == "sha256":
        return hashes.SHA256()
    raise ValueError("ARCA_WSAA_HASH debe ser sha1 o sha256")


def _load_p12_key_cert(path: Path, password: str):
    data = path.read_bytes()
    key, cert, others = pkcs12.load_key_and_certificates(
        data,
        password.encode("utf-8") if password else b"",
    )
    if key is None or cert is None:
        raise ValueError("El .p12 no contiene clave privada o certificado")
    return key, cert, others or ()


def _now_from_http_date_header(url: str, timeout: float = 15.0) -> datetime | None:
    """Hora según cabecera Date (RFC 7231) de una respuesta AFIP; alinea el TRA con el reloj del servidor."""
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        d = r.headers.get("Date")
        if not d:
            r = requests.get(url, timeout=timeout, allow_redirects=True, stream=True)
            try:
                d = r.headers.get("Date")
            finally:
                r.close()
        if not d:
            return None
        dt = parsedate_to_datetime(d)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(AR_TZ)
    except Exception as e:
        logger.debug("No Date desde %s: %s", url, e)
        return None


def resolve_tra_now(settings: Settings) -> datetime:
    """Instante para generationTime/expirationTime del TRA (según ARCA_WSAA_TIME_SOURCE)."""
    src = (settings.arca_wsaa_time_source or "auto").lower().strip()
    wsaa_url = str(settings.arca_wsaa_url).strip()

    if src == "local":
        n = datetime.now(AR_TZ)
        logger.info("WSAA TRA: fuente de hora=local (%s)", n.isoformat())
        return n

    urls_try = (wsaa_url, "https://www.afip.gob.ar/")
    if src == "http":
        for u in urls_try:
            n = _now_from_http_date_header(u)
            if n is not None:
                logger.info("WSAA TRA: fuente de hora=http Date %s -> %s", u, n.isoformat())
                return n
        raise RuntimeError(
            "ARCA_WSAA_TIME_SOURCE=http pero no se pudo leer la cabecera Date desde AFIP. "
            "Revisá red o probá ARCA_WSAA_TIME_SOURCE=auto."
        )

    # auto
    for u in urls_try:
        n = _now_from_http_date_header(u)
        if n is not None:
            logger.info("WSAA TRA: fuente de hora=auto (Date %s) -> %s", u, n.isoformat())
            return n
    n = datetime.now(AR_TZ)
    logger.warning(
        "WSAA TRA: sin cabecera Date HTTP; se usa hora local %s (si falla generationTime, corregí fecha del PC)",
        n.isoformat(),
    )
    return n


def build_tra_xml(service: str, ttl_seconds: int = 600, *, now: datetime | None = None) -> bytes:
    """Construye el XML loginTicketRequest (TRA)."""
    base = now if now is not None else datetime.now(AR_TZ)
    gen = base
    exp = base + timedelta(seconds=ttl_seconds)
    unique_id = random.randint(1, 99_999_999)

    def _fmt(d: datetime) -> str:
        # AFIP valida el TRA contra XSD: el offset debe ser -03:00, no -0300
        s = d.strftime("%Y-%m-%dT%H:%M:%S%z")
        if len(s) >= 5 and s[-5] in "+-":
            return s[:-2] + ":" + s[-2:]
        return s

    gen_s, exp_s = _fmt(gen), _fmt(exp)
    logger.info(
        "WSAA TRA tiempos (America/Argentina/Buenos_Aires): generationTime=%s expirationTime=%s",
        gen_s,
        exp_s,
    )
    tra = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<loginTicketRequest version="1.0">'
        "<header>"
        f"<uniqueId>{unique_id}</uniqueId>"
        f"<generationTime>{gen_s}</generationTime>"
        f"<expirationTime>{exp_s}</expirationTime>"
        "</header>"
        f"<service>{service}</service>"
        "</loginTicketRequest>"
    )
    return tra.encode("utf-8")


def sign_tra_cms(tra_xml: bytes, p12_path: Path, password: str, hash_name: str) -> bytes:
    """Firma el TRA y devuelve CMS en DER (binario)."""
    key, cert, _others = _load_p12_key_cert(p12_path, password)
    h = _hash_alg(hash_name)
    builder = pkcs7.PKCS7SignatureBuilder().set_data(tra_xml).add_signer(cert, key, h)
    # CMS con contenido embebido (sin detached), formato DER — compatible con loginCms
    return builder.sign(serialization.Encoding.DER, [])


def _local_tag(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _parse_login_cms_response(xml_text: str) -> bytes:
    root = ET.fromstring(xml_text.encode("utf-8"))
    for el in root.iter():
        if _local_tag(el.tag) == "faultstring" and el.text:
            raise RuntimeError(f"Fault WSAA: {el.text.strip()}")
    for el in root.iter():
        if _local_tag(el.tag) != "loginCmsReturn":
            continue
        text = (el.text or "").strip()
        if not text and len(el):
            # a veces el XML viene en subelementos
            text = "".join(el.itertext()).strip()
        if not text:
            continue
        if "loginTicketResponse" in text:
            if text.startswith("<?xml"):
                return text.encode("utf-8")
            # payload base64 que decodifica a XML
            try:
                dec = base64.b64decode(text, validate=False)
                if b"loginTicketResponse" in dec:
                    return dec
            except Exception:
                pass
            return text.encode("utf-8")
        try:
            return base64.b64decode(text, validate=False)
        except Exception:
            return text.encode("utf-8")
    for el in root.iter():
        if el.text and "loginTicketResponse" in el.text:
            return el.text.strip().encode("utf-8")
    raise ValueError("Respuesta WSAA inesperada (sin loginCmsReturn ni loginTicketResponse)")


class WSAAClient:
    def __init__(self, settings: Settings) -> None:
        self._s = settings

    def login_cms(self) -> bytes:
        """Obtiene XML loginTicketResponse desde WSAA."""
        if not self._s.arca_cert_path:
            raise ValueError("ARCA_CERT_PATH es obligatorio para WSAA en modo live")
        tra = build_tra_xml(self._s.arca_wsaa_service, now=resolve_tra_now(self._s))
        cms = sign_tra_cms(
            tra, self._s.arca_cert_path, self._s.arca_cert_password, self._s.arca_wsaa_hash
        )
        b64 = base64.b64encode(cms).decode("ascii")
        body = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:wsaa="http://wsaa.view.sua.dvadac.desein.afip.gov">
  <soapenv:Header/>
  <soapenv:Body>
    <wsaa:loginCms>
      <wsaa:in0>{b64}</wsaa:in0>
    </wsaa:loginCms>
  </soapenv:Body>
</soapenv:Envelope>"""
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": '""',
        }
        logger.info("POST WSAA %s", self._s.arca_wsaa_url)
        r = requests.post(
            self._s.arca_wsaa_url,
            data=body.encode("utf-8"),
            headers=headers,
            timeout=60,
        )
        if not r.ok:
            preview = (r.text or "")[:4000]
            logger.error("WSAA HTTP %s URL=%s cuerpo=%s", r.status_code, self._s.arca_wsaa_url, preview)
            hint = ""
            if "generationTime" in preview and "invalid" in preview.lower():
                hint = (
                    " AFIP rechazó la hora del TRA: revisá fecha/año del PC o dejá ARCA_WSAA_TIME_SOURCE=auto "
                    "(usa hora vía cabecera Date de AFIP). "
                )
            raise RuntimeError(
                f"WSAA respondió HTTP {r.status_code}.{hint}"
                "Si no es hora del sistema: revisá certificado, contraseña del .p12 y ARCA_WSAA_SERVICE. "
                f"Extracto respuesta AFIP:\n{preview[:2000]}"
            )
        ta_xml = _parse_login_cms_response(r.text)
        # validar que sea XML TA
        if b"loginTicketResponse" not in ta_xml and b"LoginTicketResponse" not in ta_xml:
            raise ValueError("Respuesta WSAA no contiene loginTicketResponse")
        return ta_xml

    def ensure_ticket(self, ticket_path: Path, max_age_seconds: int = 500) -> bytes:
        """Reutiliza ticket en disco si parece reciente; si no, renueva."""
        if ticket_path.is_file():
            try:
                age = time.time() - ticket_path.stat().st_mtime
                txt = ticket_path.read_text(encoding="utf-8", errors="replace")
                if age < max_age_seconds and "loginTicketResponse" in txt:
                    return ticket_path.read_bytes()
            except OSError:
                pass
        ta = self.login_cms()
        ticket_path.parent.mkdir(parents=True, exist_ok=True)
        ticket_path.write_bytes(ta)
        return ta
