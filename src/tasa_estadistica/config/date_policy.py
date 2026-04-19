"""Reglas de período para consultas analíticas (panel, API, CLI)."""

from __future__ import annotations

import calendar
from datetime import date

from tasa_estadistica.config.settings import Settings


def validate_analysis_period(desde: date, hasta: date, settings: Settings) -> None:
    """
    Valida rango por fecha de liquidación.

    - desde <= hasta
    - ambas >= tasa_analisis_desde (default 2019-01-01)
    - opcionalmente hasta <= hoy (tasa_analisis_hasta_max_hoy)
    """
    if desde > hasta:
        raise ValueError("La fecha «desde» no puede ser posterior a «hasta».")
    min_d = settings.tasa_analisis_desde
    if desde < min_d:
        raise ValueError(
            f"La fecha «desde» no puede ser anterior a {min_d.isoformat()} "
            "(período analítico mínimo; configurable con TASA_ANALISIS_DESDE)."
        )
    if hasta < min_d:
        raise ValueError(
            f"La fecha «hasta» no puede ser anterior a {min_d.isoformat()} "
            "(período analítico mínimo)."
        )
    if settings.tasa_analisis_hasta_max_hoy and hasta > date.today():
        raise ValueError("La fecha «hasta» no puede ser posterior a hoy.")


def default_rango_mes_actual(settings: Settings) -> tuple[date, date]:
    """Primer y último día del mes calendario actual, recortado a la política de fechas."""
    today = date.today()
    ultimo = calendar.monthrange(today.year, today.month)[1]
    desde = date(today.year, today.month, 1)
    hasta = date(today.year, today.month, ultimo)
    min_d = settings.tasa_analisis_desde
    if hasta < min_d:
        u = calendar.monthrange(min_d.year, min_d.month)[1]
        return min_d, date(min_d.year, min_d.month, u)
    if desde < min_d:
        desde = min_d
    if settings.tasa_analisis_hasta_max_hoy and hasta > today:
        hasta = today
    if desde > hasta:
        return min_d, min_d
    return desde, hasta


def period_presets(settings: Settings) -> list[dict[str, str]]:
    """
    Atajos de período para enlaces GET (mes actual, año en curso, trimestre actual).
    Solo incluye rangos que pasan validate_analysis_period.
    """
    today = date.today()
    min_d = settings.tasa_analisis_desde
    out: list[dict[str, str]] = []

    def clip_h(h: date) -> date:
        if settings.tasa_analisis_hasta_max_hoy and h > today:
            return today
        return h

    def add(label: str, d: date, h: date) -> None:
        h = clip_h(h)
        try:
            validate_analysis_period(d, h, settings)
        except ValueError:
            return
        out.append({"label": label, "desde": d.isoformat(), "hasta": h.isoformat()})

    u_m = calendar.monthrange(today.year, today.month)[1]
    add(
        "Mes actual",
        date(today.year, today.month, 1),
        date(today.year, today.month, u_m),
    )

    add(
        "Año en curso",
        max(date(today.year, 1, 1), min_d),
        date(today.year, 12, 31),
    )

    q = (today.month - 1) // 3
    sm = q * 3 + 1
    em = sm + 2
    add(
        "Trimestre actual",
        date(today.year, sm, 1),
        date(today.year, em, calendar.monthrange(today.year, em)[1]),
    )

    # Año calendario anterior (ej. 2025 cuando estamos en 2026): útil para ver todo un año ya cerrado
    py = today.year - 1
    if py >= min_d.year:
        add(
            f"Año {py}",
            max(date(py, 1, 1), min_d),
            date(py, 12, 31),
        )

    return out
