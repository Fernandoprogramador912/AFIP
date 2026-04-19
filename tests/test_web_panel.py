"""Panel web local (Starlette TemplateResponse requiere request primero)."""

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from tasa_estadistica.web.app import app


def test_panel_get_200() -> None:
    client = TestClient(app)
    r = client.get("/?desde=2025-05-01&hasta=2025-05-31")
    assert r.status_code == 200
    assert "Recupero" in r.text or "recupero" in r.text.lower()
    assert "T.E. TOTAL COBRADA" in r.text
    assert "A SOLICITAR ARS S/BASE" in r.text
    # Detalle: grilla CSS (columnas alineadas entre cabecera y datos)
    assert r.text.count("detalle-celda") >= 1
    assert "detalle-grid" in r.text and "grid-template-columns" in r.text
    assert "detalle-borde" in r.text and "detalle-scroll" in r.text
    assert "/export/recupero.xlsx?desde=2025-05-01&hasta=2025-05-31" in r.text
    assert "Descargar Excel" in r.text
    # Grilla: prefijos de moneda en texto plano (panel + Jinja)
    assert "$ " in r.text
    if "No hay liquidaciones que coincidan con el filtro de importación" not in r.text:
        assert "U$S " in r.text


def test_api_resumen_json() -> None:
    client = TestClient(app)
    r = client.get("/api/resumen?desde=2025-05-01&hasta=2025-05-31")
    assert r.status_code == 200
    data = r.json()
    assert "total_tasa_recupero_ars" in data
    assert "n_despachos_ic" in data
    assert "n_liquidaciones_en_rango" in data
    assert "destinacion_subcadenas" in data
    assert "cobertura_sqlite_global" in data


def test_api_resumen_422_si_desde_antes_min() -> None:
    client = TestClient(app)
    r = client.get("/api/resumen?desde=2018-01-01&hasta=2018-12-31")
    assert r.status_code == 422
    assert "detail" in r.json()


def test_api_recupero_422_si_hasta_futuro() -> None:
    from datetime import date, timedelta

    futuro = (date.today() + timedelta(days=10)).isoformat()
    client = TestClient(app)
    r = client.get(f"/api/recupero?desde=2020-01-01&hasta={futuro}")
    assert r.status_code == 422


def test_api_recupero_misma_consulta_que_panel() -> None:
    client = TestClient(app)
    r = client.get("/api/recupero?desde=2025-05-01&hasta=2025-05-31")
    assert r.status_code == 200
    data = r.json()
    assert "filas" in data and "columnas" in data
    assert "sqlite" in data and data["sqlite"]
    assert "T.E. TOTAL COBRADA" in data["columnas"]
    assert "filas_formateadas" not in data
    assert "recupero_avisos_fob" in data and isinstance(data["recupero_avisos_fob"], list)


def test_export_recupero_xlsx_200_y_magic_zip() -> None:
    client = TestClient(app)
    r = client.get("/export/recupero.xlsx?desde=2025-05-01&hasta=2025-05-31")
    assert r.status_code == 200
    assert r.headers.get("content-type", "").startswith(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert "attachment" in r.headers.get("content-disposition", "").lower()
    assert r.content[:2] == b"PK"


def test_export_recupero_xlsx_422_si_hasta_futuro() -> None:
    from datetime import date, timedelta

    futuro = (date.today() + timedelta(days=10)).isoformat()
    client = TestClient(app)
    r = client.get(f"/export/recupero.xlsx?desde=2020-01-01&hasta={futuro}")
    assert r.status_code == 422


def test_api_recupero_formatted_incluye_prefijos_moneda() -> None:
    client = TestClient(app)
    r = client.get("/api/recupero?desde=2025-05-01&hasta=2025-05-31&formatted=true")
    assert r.status_code == 200
    data = r.json()
    assert "filas_formateadas" in data
    ff = data["filas_formateadas"]
    if not ff:
        return
    row0 = ff[0]
    te = row0.get("T.E. TOTAL COBRADA", "")
    fob = row0.get("FOB", "")
    if te:
        assert te.startswith("$ ")
    if fob:
        assert fob.startswith("U$S ")
