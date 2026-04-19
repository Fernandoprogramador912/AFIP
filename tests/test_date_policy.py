"""Política de fechas analíticas (desde 2019, orden, tope hoy)."""

from datetime import date, timedelta

import pytest

from tasa_estadistica.config.date_policy import (
    default_rango_mes_actual,
    validate_analysis_period,
)
from tasa_estadistica.config.settings import Settings


def _settings(**kwargs) -> Settings:
    return Settings(**kwargs)


def test_validate_rechaza_desde_menor_min() -> None:
    s = _settings()
    with pytest.raises(ValueError, match="desde"):
        validate_analysis_period(date(2018, 6, 1), date(2018, 6, 30), s)


def test_validate_rechaza_hasta_menor_min() -> None:
    s = _settings()
    with pytest.raises(ValueError, match="hasta"):
        validate_analysis_period(date(2019, 1, 1), date(2018, 12, 31), s)


def test_validate_rechaza_desde_mayor_hasta() -> None:
    s = _settings()
    with pytest.raises(ValueError, match="desde"):
        validate_analysis_period(date(2025, 6, 1), date(2025, 1, 1), s)


def test_validate_rechaza_hasta_futuro_si_max_hoy() -> None:
    s = _settings(tasa_analisis_hasta_max_hoy=True)
    futuro = date.today() + timedelta(days=1)
    with pytest.raises(ValueError, match="hoy"):
        validate_analysis_period(date(2020, 1, 1), futuro, s)


def test_validate_permite_futuro_si_max_hoy_false() -> None:
    s = _settings(tasa_analisis_hasta_max_hoy=False)
    futuro = date.today() + timedelta(days=30)
    validate_analysis_period(date(2020, 1, 1), futuro, s)


def test_validate_ok_rango_valido() -> None:
    s = _settings()
    validate_analysis_period(date(2019, 1, 1), date.today(), s)


def test_default_mes_actual_respeta_min() -> None:
    s = _settings(tasa_analisis_desde=date(2099, 1, 1))
    d, h = default_rango_mes_actual(s)
    assert d == date(2099, 1, 1)
    assert h >= d
