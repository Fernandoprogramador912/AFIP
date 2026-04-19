"""Modelos internos estables para liquidaciones y conceptos."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, Field


class ConceptoLiquidacion(BaseModel):
    codigo: str = ""
    descripcion: str = ""
    importe: Decimal = Field(default=Decimal("0"))
    moneda: str = "ARS"
    raw: dict = Field(default_factory=dict)


class Liquidacion(BaseModel):
    """Una liquidación normalizada (origen WS o mock)."""

    cuit: str
    id_externo: str = Field(description="Identificador estable en el origen (número interno, etc.)")
    numero: str = ""
    fecha: date | None = None
    destinacion_id: str | None = None
    conceptos: list[ConceptoLiquidacion] = Field(default_factory=list)
    raw: dict = Field(default_factory=dict)


class RunParams(BaseModel):
    fecha_desde: date
    fecha_hasta: date
    cuit: str
    modo: str = "mock"
