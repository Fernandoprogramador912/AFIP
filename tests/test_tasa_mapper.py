"""Reglas de mapeo tasa estadística."""

from decimal import Decimal

from tasa_estadistica.domain.tasa_estadistica_mapper import TasaEstadisticaMapper
from tasa_estadistica.model.schemas import ConceptoLiquidacion, Liquidacion


def test_match_codigo_exacto_te() -> None:
    m = TasaEstadisticaMapper()
    c = ConceptoLiquidacion(codigo="TE", descripcion="X", importe=Decimal("1"))
    r = m.match_concepto(c)
    assert r.matched and r.score == 1.0


def test_match_codigo_011_sidin_tasa() -> None:
    """011 verificado vs PDF liquidación (MontoPagado tasa estadística)."""
    m = TasaEstadisticaMapper()
    c = ConceptoLiquidacion(codigo="011", descripcion="A PAGAR — P", importe=Decimal("1737.10"))
    r = m.match_concepto(c)
    assert r.matched and r.score == 1.0


def test_match_descripcion_tasa_estadistica() -> None:
    m = TasaEstadisticaMapper()
    c = ConceptoLiquidacion(
        codigo="",
        descripcion="Tasa de estadística importación",
        importe=Decimal("10"),
    )
    r = m.match_concepto(c)
    assert r.matched
    assert r.score >= 0.65


def test_no_match() -> None:
    m = TasaEstadisticaMapper()
    c = ConceptoLiquidacion(
        codigo="IVA", descripcion="Impuesto al valor agregado", importe=Decimal("100")
    )
    r = m.match_concepto(c)
    assert not r.matched


def test_liquidacion_multiples_conceptos() -> None:
    m = TasaEstadisticaMapper()
    liq = Liquidacion(
        cuit="20123456789",
        id_externo="1",
        conceptos=[
            ConceptoLiquidacion(codigo="X", descripcion="Otro", importe=Decimal("1")),
            ConceptoLiquidacion(codigo="TE", descripcion="Tasa estadística", importe=Decimal("2")),
        ],
    )
    ms = m.match_liquidacion(liq)
    assert ms[0].matched is False
    assert ms[1].matched is True
