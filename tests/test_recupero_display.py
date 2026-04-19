"""Formato ARS / USD para el panel web."""

from datetime import date
from decimal import Decimal

from tasa_estadistica.web.recupero_display import (
    format_recupero_cell,
    format_recupero_cell_safe,
    format_total_ars_display,
    num_argentina_2dec,
)


def test_num_argentina_negativo() -> None:
    assert num_argentina_2dec(Decimal("-1234.56")) == "-1.234,56"


def test_format_total_ars() -> None:
    assert format_total_ars_display(Decimal("32784.33")) == "$ 32.784,33"


def test_format_usd_fob() -> None:
    assert format_recupero_cell("FOB", Decimal("20250.33")) == "U$S 20.250,33"


def test_format_ars_te() -> None:
    assert format_recupero_cell("T.E. TOTAL COBRADA", 1737.1) == "$ 1.737,10"


def test_format_alicuota_sin_prefijo() -> None:
    assert format_recupero_cell("ALICUOTA COBRADA", "2,5") == "2,50"


def test_format_oficializacion() -> None:
    assert format_recupero_cell("OFICIALIZACION", date(2025, 5, 14)) == "14/05/2025"


def test_format_vacio() -> None:
    assert format_recupero_cell("FOB", "") == ""
    assert format_recupero_cell("PROVEEDOR", None) == ""


def test_format_safe_no_rompe() -> None:
    """Valores imposibles no deben tirar el panel."""

    class Boom:
        def __str__(self) -> str:
            raise ValueError("x")

    assert format_recupero_cell_safe("PROVEEDOR", Boom()) == "—"
