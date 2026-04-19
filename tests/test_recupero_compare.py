"""Comparación Excel modelo vs SQLite."""

from datetime import date
from decimal import Decimal
from pathlib import Path

from openpyxl import Workbook

from tasa_estadistica.domain.tasa_estadistica_mapper import TasaEstadisticaMapper
from tasa_estadistica.export.recupero_compare import compare_excel_vs_sqlite
from tasa_estadistica.export.recupero_excel import RECUPERO_V2_HEADERS, recupero_resumen_local
from tasa_estadistica.model.schemas import ConceptoLiquidacion, Liquidacion, RunParams
from tasa_estadistica.storage.sqlite_repo import SqliteRepo


def _seed_demo_db(db: Path) -> None:
    repo = SqliteRepo(db)
    run = repo.start_run(
        RunParams(
            fecha_desde=date(2025, 5, 1),
            fecha_hasta=date(2025, 5, 31),
            cuit="20123456789",
            modo="mock",
        )
    )
    raw = {
        "identificador_destinacion": "25001IC00000001X",
        "liquidacion_resumen": {"Cotizacion": "1100"},
        "declaracion_listado": {
            "NombreProveedorExterior": "PROVEEDOR TEST SA",
            "ValorFOB": "100",
            "ValorFlete": "200",
            "ValorSeguro": "50",
        },
    }
    liq = Liquidacion(
        cuit="20123456789",
        id_externo="x",
        numero="x",
        fecha=date(2025, 5, 10),
        destinacion_id="25001IC00000001X",
        conceptos=[
            ConceptoLiquidacion(
                codigo="011",
                descripcion="TASA",
                importe=Decimal("100"),
                moneda="ARS",
                raw={"MontoPagado": "100"},
            ),
            ConceptoLiquidacion(
                codigo="061",
                descripcion="MONT MAX",
                importe=Decimal("50"),
                moneda="ARS",
                raw={"MontoPagado": "50"},
            ),
        ],
        raw=raw,
    )
    repo.save_liquidaciones(run.run_id, [liq], TasaEstadisticaMapper())
    repo.close()


def test_compare_excel_coincide_con_sqlite(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    _seed_demo_db(db)
    res = recupero_resumen_local(db, date(2025, 5, 1), date(2025, 5, 31))
    fila = res["filas"][0]

    wb = Workbook()
    ws = wb.active
    ws.title = "V2_Ejemplo"
    ws.append(list(RECUPERO_V2_HEADERS))
    ws.append(fila)
    xlsx = tmp_path / "modelo.xlsx"
    wb.save(xlsx)

    out = compare_excel_vs_sqlite(
        xlsx,
        db,
        date(2025, 5, 1),
        date(2025, 5, 31),
        data_start_row=2,
    )
    assert any(r["destinacion_id"] == "25001IC00000001X" and r["status"] == "ok" for r in out)


def test_compare_detecta_diferencia(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    _seed_demo_db(db)
    res = recupero_resumen_local(db, date(2025, 5, 1), date(2025, 5, 31))
    fila = list(res["filas"][0])
    fila[8] = 999  # T.E. TOTAL COBRADA

    wb = Workbook()
    ws = wb.active
    ws.title = "V2_Ejemplo"
    ws.append(list(RECUPERO_V2_HEADERS))
    ws.append(fila)
    xlsx = tmp_path / "modelo.xlsx"
    wb.save(xlsx)

    out = compare_excel_vs_sqlite(
        xlsx,
        db,
        date(2025, 5, 1),
        date(2025, 5, 31),
        data_start_row=2,
    )
    diff = next(r for r in out if r["destinacion_id"] == "25001IC00000001X")
    assert diff["status"] == "diff"
    cols = {d["columna"] for d in diff["diffs"]}
    assert "T.E. TOTAL COBRADA" in cols
