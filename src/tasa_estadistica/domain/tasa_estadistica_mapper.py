"""Reglas para identificar la tasa de estadística en conceptos de liquidación."""

from __future__ import annotations

import re
from dataclasses import dataclass

from tasa_estadistica.model.schemas import ConceptoLiquidacion, Liquidacion


@dataclass(frozen=True)
class TasaEstadisticaMatch:
    matched: bool
    score: float
    reason: str


_CODE_HINTS = frozenset({"TE", "TASA", "EST", "ESTAD", "ESTADISTICA"})
_DESC_PATTERNS = (
    re.compile(r"tasa\s+de\s+estad", re.IGNORECASE),
    re.compile(r"tasa\s+estad", re.IGNORECASE),
    re.compile(r"estad[ií]stica", re.IGNORECASE),
)


# Códigos SIDIN/MOA frecuentes (validar con PDF o RG 632/99 Anexo VI): 011 = Tasa estadística
# en liquidaciones detalladas COMEX verificado por monto vs PDF.
_DEFAULT_CODIGOS_TASA = frozenset({"TE", "011"})


class TasaEstadisticaMapper:
    """Heurística versionable: códigos + texto + puntuación."""

    def __init__(
        self,
        codigos_exactos: frozenset[str] | None = None,
    ) -> None:
        self._codigos = codigos_exactos if codigos_exactos is not None else _DEFAULT_CODIGOS_TASA

    @property
    def codigos(self) -> frozenset[str]:
        return self._codigos

    def match_concepto(self, c: ConceptoLiquidacion) -> TasaEstadisticaMatch:
        code = (c.codigo or "").strip().upper()
        desc = (c.descripcion or "").strip()

        if code and code in self._codigos:
            return TasaEstadisticaMatch(True, 1.0, f"código_exacto:{code}")

        if code and any(h in code for h in _CODE_HINTS):
            return TasaEstadisticaMatch(True, 0.85, f"código_parcial:{code}")

        for pat in _DESC_PATTERNS:
            if pat.search(desc):
                return TasaEstadisticaMatch(True, 0.75, f"descripcion:{pat.pattern}")

        if "ESTADISTICA" in desc.upper() and "TASA" in desc.upper():
            return TasaEstadisticaMatch(True, 0.65, "descripcion:keywords_tasa_estadistica")

        return TasaEstadisticaMatch(False, 0.0, "sin_coincidencia")

    def match_liquidacion(self, liq: Liquidacion) -> list[TasaEstadisticaMatch]:
        return [self.match_concepto(c) for c in liq.conceptos]
