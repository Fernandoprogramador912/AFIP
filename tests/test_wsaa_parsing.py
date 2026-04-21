"""Tests de parseo de ticket WSAA y respuesta loginCms."""

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from tasa_estadistica.arca.auth_ticket_store import parse_ticket_xml, wsaa_ticket_expired
from tasa_estadistica.arca.wsaa_client import (
    WSAAClient,
    _parse_login_cms_response,
    build_tra_xml,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"
AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


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


def test_build_tra_xml_leeway_back_seconds_moves_generation_backward() -> None:
    """Con leeway_back_seconds=120, generationTime va 120s antes del `now` dado."""
    fixed_now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=AR_TZ)
    out_sin = build_tra_xml("wsaduanas", ttl_seconds=600, now=fixed_now)
    out_con = build_tra_xml(
        "wsaduanas", ttl_seconds=600, now=fixed_now, leeway_back_seconds=120
    )
    assert b"<generationTime>2026-06-01T12:00:00-03:00</generationTime>" in out_sin
    assert b"<generationTime>2026-06-01T11:58:00-03:00</generationTime>" in out_con
    assert b"<expirationTime>2026-06-01T12:10:00-03:00</expirationTime>" in out_con


def _valid_ta_xml(hours_ahead: int = 6) -> bytes:
    """TA sintético con expirationTime en el futuro (hours_ahead puede ser negativo)."""
    now = datetime.now(AR_TZ)
    exp = now + timedelta(hours=hours_ahead)
    exp_s = exp.strftime("%Y-%m-%dT%H:%M:%S%z")
    exp_s = exp_s[:-2] + ":" + exp_s[-2:]
    return (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<loginTicketResponse version="1.0">'
        b"<header>"
        + f"<expirationTime>{exp_s}</expirationTime>".encode("utf-8")
        + b"</header>"
        b"<credentials><token>T</token><sign>S</sign></credentials>"
        b"</loginTicketResponse>"
    )


def test_ensure_ticket_reuses_valid_ta(tmp_path, monkeypatch) -> None:
    """Si el archivo tiene expirationTime futuro, no se llama a login_cms."""
    ticket_path = tmp_path / "ta.xml"
    ticket_path.write_bytes(_valid_ta_xml(hours_ahead=6))

    called = {"n": 0}

    def _fail_login() -> bytes:
        called["n"] += 1
        raise AssertionError("login_cms no debería llamarse con TA vigente")

    client = WSAAClient.__new__(WSAAClient)
    client._s = object()  # no se usa por el fast-path
    monkeypatch.setattr(client, "login_cms", _fail_login)

    out = client.ensure_ticket(ticket_path)
    assert b"loginTicketResponse" in out
    assert called["n"] == 0


def test_ensure_ticket_renews_when_expired(tmp_path, monkeypatch) -> None:
    """Si el TA en disco venció, se llama a login_cms y se reescribe el archivo."""
    ticket_path = tmp_path / "ta.xml"
    ticket_path.write_bytes(_valid_ta_xml(hours_ahead=-1))

    renewed = _valid_ta_xml(hours_ahead=12)

    def _fake_login() -> bytes:
        return renewed

    client = WSAAClient.__new__(WSAAClient)
    client._s = object()
    monkeypatch.setattr(client, "login_cms", _fake_login)

    out = client.ensure_ticket(ticket_path)
    assert out == renewed
    assert ticket_path.read_bytes() == renewed


def test_wsaa_ticket_expired_leeway_zona_gris() -> None:
    """Con TA que vence en 3 min y leeway=5 min, ya se considera vencido (para renovar antes)."""
    now = datetime.now(AR_TZ)
    exp = now + timedelta(minutes=3)
    exp_s = exp.strftime("%Y-%m-%dT%H:%M:%S%z")
    exp_s = exp_s[:-2] + ":" + exp_s[-2:]
    xml = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<loginTicketResponse version="1.0">'
        b"<header>"
        + f"<expirationTime>{exp_s}</expirationTime>".encode("utf-8")
        + b"</header>"
        b"<credentials><token>T</token><sign>S</sign></credentials>"
        b"</loginTicketResponse>"
    )
    assert wsaa_ticket_expired(xml, leeway=timedelta(minutes=5)) is True
    # sin leeway todavía no venció
    assert wsaa_ticket_expired(xml, leeway=timedelta(seconds=0)) is False


def test_parse_ticket_xml_extrae_service() -> None:
    """El service del TA (si viene) debe poder extraerse para diagnóstico."""
    xml = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<loginTicketResponse version="1.0">'
        b"<header>"
        b"<service>wsaduanas</service>"
        b"<expirationTime>2099-12-31T23:59:59-03:00</expirationTime>"
        b"</header>"
        b"<credentials><token>T</token><sign>S</sign></credentials>"
        b"</loginTicketResponse>"
    )
    ta = parse_ticket_xml(xml)
    assert ta.service == "wsaduanas"
