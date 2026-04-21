"""Chequeo rápido del TA actual (hora sistema + vigencia)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from tasa_estadistica.arca.auth_ticket_store import parse_ticket_xml, wsaa_ticket_expired

AR = ZoneInfo("America/Argentina/Buenos_Aires")


def main() -> None:
    p = Path("data/ta.xml")
    if not p.is_file():
        print("No existe data/ta.xml")
        return
    raw = p.read_bytes()
    ta = parse_ticket_xml(raw)
    print(f"Hora sistema:   {datetime.now(AR).isoformat(timespec='seconds')}")
    print(f"mtime TA:       {datetime.fromtimestamp(p.stat().st_mtime, tz=AR).isoformat(timespec='seconds')}")
    print(f"generationTime: {ta.generation_time}")
    print(f"expirationTime: {ta.expiration_time}")
    print(f"expirado?       {wsaa_ticket_expired(raw)}")


if __name__ == "__main__":
    main()
