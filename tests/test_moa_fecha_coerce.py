"""Fechas MOA cuando el listado no trae fila (inyección desde env)."""

from datetime import date

from tasa_estadistica.arca.moa_declaracion import _coerce_fecha_declaracion


def test_coerce_fecha_desde_caratula_si_decl_vacio() -> None:
    decl = {"IdentificadorDestinacion": "25001IC04068168A", "IdentificadorDeclaracion": "25001IC04068168A"}
    caratula = {"FechaOficializacionDeclaracion": "2025-04-10T00:00:00"}
    assert _coerce_fecha_declaracion(decl, caratula, []) == date(2025, 4, 10)


def test_coerce_fecha_desde_resumen_liquidacion() -> None:
    decl = {"IdentificadorDestinacion": "X"}
    liqs = [{"FechaLiquidacion": "2025-06-01"}]
    assert _coerce_fecha_declaracion(decl, {}, liqs) == date(2025, 6, 1)
