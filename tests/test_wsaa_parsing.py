"""Tests de parseo de ticket WSAA y respuesta loginCms."""

from pathlib import Path

import pytest

from tasa_estadistica.arca.auth_ticket_store import parse_ticket_xml, wsaa_ticket_expired
from tasa_estadistica.arca.wsaa_client import _parse_login_cms_response, build_tra_xml

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_parse_ticket_xml_sample() -> None:
    xml = (FIXTURES / "ta_sample.xml").read_bytes()
    ta = parse_ticket_xml(xml)
    assert ta.token.startswith("PD9")
    assert ta.sign == "SUVOX1RPS0VO"


def test_parse_login_cms_soap_with_base64() -> None:
    inner = (
        b'<?xml version="1.0"?><loginTicketResponse>'
        b"<credentials><token>T</token><sign>S</sign></credentials></loginTicketResponse>"
    )
    import base64

    b64 = base64.b64encode(inner).decode("ascii")
    soap = f"""<?xml version="1.0"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <loginCmsResponse xmlns="http://wsaa.view.sua.dvadac.desein.afip.gov">
          <loginCmsReturn>{b64}</loginCmsReturn>
        </loginCmsResponse>
      </soap:Body>
    </soap:Envelope>"""
    out = _parse_login_cms_response(soap)
    assert b"loginTicketResponse" in out


def test_wsaa_ticket_expired_past() -> None:
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<loginTicketResponse version="1.0">
  <header>
    <generationTime>2019-01-01T00:00:00-03:00</generationTime>
    <expirationTime>2019-01-01T12:00:00-03:00</expirationTime>
  </header>
  <credentials>
    <token>UE9TVA==</token>
    <sign>U0lHTg==</sign>
  </credentials>
</loginTicketResponse>"""
    assert wsaa_ticket_expired(xml) is True


def test_wsaa_ticket_expired_future() -> None:
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<loginTicketResponse version="1.0">
  <header>
    <generationTime>2099-01-01T00:00:00-03:00</generationTime>
    <expirationTime>2099-12-31T23:59:59-03:00</expirationTime>
  </header>
  <credentials>
    <token>UE9TVA==</token>
    <sign>U0lHTg==</sign>
  </credentials>
</loginTicketResponse>"""
    assert wsaa_ticket_expired(xml) is False


def test_build_tra_xml_now_none_does_not_raise() -> None:
    """Regression: expirationTime must use base time when now is omitted."""
    out = build_tra_xml("wsaduanas", ttl_seconds=120, now=None)
    assert b"loginTicketRequest" in out
    assert b"<service>wsaduanas</service>" in out
    assert b"expirationTime" in out


def test_parse_login_cms_fault() -> None:
    soap = """<?xml version="1.0"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <soap:Fault>
          <faultstring>cn=wsaa error</faultstring>
        </soap:Fault>
      </soap:Body>
    </soap:Envelope>"""
    with pytest.raises(RuntimeError, match="Fault WSAA"):
        _parse_login_cms_response(soap)
