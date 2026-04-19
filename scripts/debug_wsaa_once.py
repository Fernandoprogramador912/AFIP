"""Una sola llamada WSAA e imprime status y cuerpo (diagnóstico)."""
from pathlib import Path

import base64
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import pkcs12, pkcs7
from dotenv import load_dotenv

load_dotenv()

from tasa_estadistica.arca.wsaa_client import build_tra_xml, resolve_tra_now
from tasa_estadistica.config.settings import get_settings


def main() -> None:
    s = get_settings()
    tra = build_tra_xml(s.arca_wsaa_service, now=resolve_tra_now(s))
    key, cert, _ = pkcs12.load_key_and_certificates(
        Path(s.arca_cert_path).read_bytes(),
        (s.arca_cert_password or "").encode(),
    )
    h = hashes.SHA256() if s.arca_wsaa_hash.lower() == "sha256" else hashes.SHA1()
    cms = (
        pkcs7.PKCS7SignatureBuilder()
        .set_data(tra)
        .add_signer(cert, key, h)
        .sign(serialization.Encoding.DER, [])
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
    r = requests.post(
        s.arca_wsaa_url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8", 'SOAPAction': '""'},
        timeout=60,
    )
    print("URL:", s.arca_wsaa_url)
    print("Service TRA:", s.arca_wsaa_service)
    print("HTTP:", r.status_code)
    print("--- body ---")
    print(r.text)


if __name__ == "__main__":
    main()
