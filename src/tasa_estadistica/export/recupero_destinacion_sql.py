"""Filtro SQL por identificador de destinación (importación / Recupero V2)."""

from __future__ import annotations

from tasa_estadistica.config.settings import Settings


def parse_destinacion_subcadenas(settings: Settings) -> list[str]:
    """
    Subcadenas que deben aparecer en `destinacion_id` (mayúsculas).
    Default «IC» (modelo manual). Separadas por coma en TASA_RECUPERO_DESTINACION_SUBCADENAS.
    Tokens de 1 carácter se ignoran; «IC» siempre permitido con 2 letras.
    """
    raw = (settings.tasa_recupero_destinacion_subcadenas or "IC").strip()
    out: list[str] = []
    for p in raw.split(","):
        s = p.strip().upper()
        if not s:
            continue
        if len(s) == 1:
            continue
        out.append(s)
    return out or ["IC"]


def sql_destinacion_import_or(alias: str, subcadenas: list[str]) -> tuple[str, list[str]]:
    """Fragmento SQL `( … )` con OR de INSTR, y parámetros posicionales."""
    parts: list[str] = []
    params: list[str] = []
    for sub in subcadenas:
        parts.append(f"INSTR(UPPER(COALESCE({alias}.destinacion_id, '')), ?) > 0")
        params.append(sub)
    if not parts:
        parts.append(f"INSTR(UPPER(COALESCE({alias}.destinacion_id, '')), ?) > 0")
        params.append("IC")
    return "(" + " OR ".join(parts) + ")", params
