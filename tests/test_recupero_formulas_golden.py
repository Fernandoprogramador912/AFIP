"""
Tests «golden» del modelo V2 (hoja V2_Ejemplo): entradas y salidas fijas.

No hay .xlsx del modelo versionado en el repo; los valores esperados se derivan de las
fórmulas documentadas en `recupero_formulas.py` (D1=0,005, F1=500) y se fijan acá para
detectar regresiones.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from tasa_estadistica.export.recupero_excel import RECUPERO_V2_HEADERS
from tasa_estadistica.export.recupero_formulas import build_recupero_v2_row_with_formulas


def _cell_eq(actual: Any, expected: Any) -> None:
    if actual == expected:
        return
    if isinstance(actual, date) and isinstance(expected, date):
        assert actual == expected
        return
    if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        assert Decimal(str(actual)).quantize(Decimal("1e-12")) == Decimal(
            str(expected)
        ).quantize(Decimal("1e-12"))
        return
    assert actual == expected


def _assert_row_25(actual: list[Any], expected: list[Any]) -> None:
    assert len(RECUPERO_V2_HEADERS) == 25
    assert len(actual) == 25, f"len={len(actual)}"
    assert len(expected) == 25, f"len(expected)={len(expected)}"
    for i, (a, e) in enumerate(zip(actual, expected, strict=True)):
        try:
            _cell_eq(a, e)
        except AssertionError as err:
            col = RECUPERO_V2_HEADERS[i]
            raise AssertionError(f"Columna {i} ({col!r}): actual={a!r} expected={e!r}") from err


def test_golden_caso_especial_cif_sin_alicuota_t_vacio_u_en_ars() -> None:
    """
    011+061, sin alícuota cobrada → J=N=CIF; O=0; P=Q=1.75; R=S=148.25; T=U=163075 ARS.
    TIENE MONT MAX = SI → método CIF / CASO ESPECIAL.
    """
    oficial = date(2025, 5, 10)
    row = build_recupero_v2_row_with_formulas(
        proveedor="PROVEEDOR TEST SA",
        destinacion_id="25001IC00000001X",
        oficial=oficial,
        tc_raw="1100",
        alicuota_cobrada_str="",
        te011=Decimal("100"),
        te061=Decimal("50"),
        te062=Decimal("0"),
        fob_str="100",
        flete_str="200",
        seguro_str="50",
    )
    # I=150; J=N=350; O=0; P=Q=min(350*0.005,500); R=S=max(150-1.75,0); T=U=R*1100
    expected: list[Any] = [
        "PROVEEDOR TEST SA",
        "25001IC00000001X",
        oficial,
        1100,
        "",
        100,
        50,
        0,
        150,
        350,
        100,
        200,
        50,
        350,
        0,
        1.75,
        1.75,
        148.25,
        148.25,
        163075.0,
        163075.0,
        "SI",
        "CIF",
        "CASO ESPECIAL",
        "25001IC00000001X.pdf",
    ]
    _assert_row_25(row, expected)


def test_golden_base_011_ok_sin_mont_max() -> None:
    """
    Solo 011, alícuota 0,1 → base reconstruida 1000; CIF 1000,5 → |O|=0,5 ≤ 1 → OK.
    """
    oficial = date(2024, 1, 15)
    row = build_recupero_v2_row_with_formulas(
        proveedor="ACME SA",
        destinacion_id="24IC00009999Z",
        oficial=oficial,
        tc_raw=Decimal("1100"),
        alicuota_cobrada_str="0.1",
        te011=Decimal("100"),
        te061=Decimal("0"),
        te062=Decimal("0"),
        fob_str="1000",
        flete_str="0.5",
        seguro_str="0",
    )
    # J=1000; N=1000.5; O=0.5; P=min(5,500)=5; R=95; T=104500; Q=5.0025; S=94.9975; U=104497.25
    expected: list[Any] = [
        "ACME SA",
        "24IC00009999Z",
        oficial,
        1100,
        "0.1",
        100,
        0,
        0,
        100,
        1000.0,
        1000,
        0.5,
        0,
        1000.5,
        0.5,
        5,
        5.0025,
        95,
        94.9975,
        104500.0,
        104497.25,
        "NO",
        "BASE 011",
        "OK",
        "24IC00009999Z.pdf",
    ]
    _assert_row_25(row, expected)


def test_golden_base_reconstr_desde_afip_prioridad_sobre_fallback() -> None:
    """Si AFIP envía base imponible explícita, J usa ese valor aunque E esté vacío."""
    row = build_recupero_v2_row_with_formulas(
        proveedor="X",
        destinacion_id="Y",
        oficial=None,
        tc_raw="100",
        alicuota_cobrada_str="",
        base_reconstr_str="5000",
        te011=Decimal("100"),
        te061=Decimal("0"),
        te062=Decimal("0"),
        fob_str="100",
        flete_str="",
        seguro_str="",
    )
    assert row[9] == 5000.0  # BASE RECONSTR. S/011


def test_golden_base_reconstr_fallback_cif_sin_alicuota() -> None:
    """
    Sin E ni base AFIP: si no hay MONT MAX y hay CIF (N), J = N (CIF documental).
    Equivale a F/E con E implícita = F/N → J = N.
    """
    oficial = date(2025, 6, 1)
    row = build_recupero_v2_row_with_formulas(
        proveedor="P",
        destinacion_id="DI",
        oficial=oficial,
        tc_raw="1100",
        alicuota_cobrada_str="",
        base_reconstr_str="",
        te011=Decimal("100"),
        te061=Decimal("0"),
        te062=Decimal("0"),
        fob_str="1000",
        flete_str="0.5",
        seguro_str="0",
    )
    expected: list[Any] = [
        "P",
        "DI",
        oficial,
        1100,
        "",
        100,
        0,
        0,
        100,
        1000.5,
        1000,
        0.5,
        0,
        1000.5,
        0,
        5.0025,
        5.0025,
        94.9975,
        94.9975,
        104497.25,
        104497.25,
        "NO",
        "BASE 011",
        "OK",
        "DI.pdf",
    ]
    _assert_row_25(row, expected)


def test_golden_revisar_sin_mont_max_dif_base_cif() -> None:
    """
    Sin MONT MAX, con alícuota y CIF lejos de la base → |O|>1 → REVISAR.

    FOB=10, flete/seguro vacíos (celdas en blanco). I=50, J=1000, N=10, O=-990.
    """
    row = build_recupero_v2_row_with_formulas(
        proveedor="X",
        destinacion_id="DI001",
        oficial=None,
        tc_raw="100",
        alicuota_cobrada_str="0.05",
        te011=Decimal("50"),
        te061=Decimal("0"),
        te062=Decimal("0"),
        fob_str="10",
        flete_str="",
        seguro_str="",
    )
    expected: list[Any] = [
        "X",
        "DI001",
        "",
        100,
        "0.05",
        50,
        0,
        0,
        50,
        1000.0,
        10,
        "",
        "",
        10,
        -990.0,
        5,
        0.05,
        45,
        49.95,
        4500.0,
        4995.0,
        "NO",
        "REVISAR",
        "REVISAR",
        "DI001.pdf",
    ]
    _assert_row_25(row, expected)


def test_montos_fob_flete_seguro_texto_formato_argentino() -> None:
    """Cadenas tipo «14.444,00» deben llenar K/L/M; antes _dec fallaba y las celdas quedaban vacías."""
    oficial = date(2025, 9, 18)
    row = build_recupero_v2_row_with_formulas(
        proveedor="P",
        destinacion_id="25001IC06008719V",
        oficial=oficial,
        tc_raw="1100",
        alicuota_cobrada_str="0.5",
        te011=Decimal("100"),
        te061=Decimal("0"),
        te062=Decimal("0"),
        fob_str="14.444,00",
        flete_str="1.158,14",
        seguro_str="78,01",
    )
    assert row[RECUPERO_V2_HEADERS.index("FOB")] == 14444
    assert row[RECUPERO_V2_HEADERS.index("FLETE")] == 1158.14
    assert row[RECUPERO_V2_HEADERS.index("SEGURO")] == 78.01
    assert row[RECUPERO_V2_HEADERS.index("CIF DOCUMENTAL")] == 15680.15
