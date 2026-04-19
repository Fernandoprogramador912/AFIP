"""Dedup al unir tramos de fetch por rango largo."""

from datetime import date
from decimal import Decimal

from tasa_estadistica.arca.liquidaciones_client import _dedupe_liquidaciones
from tasa_estadistica.model.schemas import ConceptoLiquidacion, Liquidacion


def test_dedupe_misma_clave_operativa() -> None:
    c = ConceptoLiquidacion(
        codigo="TE",
        descripcion="TASA",
        importe=Decimal("1"),
        moneda="ARS",
        raw={},
    )
    a = Liquidacion(
        cuit="1",
        id_externo="X",
        numero="N",
        fecha=date(2025, 6, 1),
        destinacion_id=None,
        conceptos=[c],
        raw={},
    )
    b = Liquidacion(
        cuit="1",
        id_externo="X",
        numero="N",
        fecha=date(2025, 6, 1),
        destinacion_id=None,
        conceptos=[c],
        raw={},
    )
    out = _dedupe_liquidaciones([a, b])
    assert len(out) == 1


def test_dedupe_distintas_fechas() -> None:
    c = ConceptoLiquidacion(
        codigo="TE",
        descripcion="TASA",
        importe=Decimal("1"),
        moneda="ARS",
        raw={},
    )
    a = Liquidacion(
        cuit="1",
        id_externo="X",
        numero="N",
        fecha=date(2025, 6, 1),
        destinacion_id=None,
        conceptos=[c],
        raw={},
    )
    b = Liquidacion(
        cuit="1",
        id_externo="X",
        numero="N",
        fecha=date(2025, 6, 2),
        destinacion_id=None,
        conceptos=[c],
        raw={},
    )
    out = _dedupe_liquidaciones([a, b])
    assert len(out) == 2
