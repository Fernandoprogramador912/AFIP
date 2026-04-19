"""
Formato de montos para el panel web: USD vs ARS según columna recupero_V2.

- USD: prefijo «U$S » (símbolo $) + número AR (ej. U$S 20.250,33)
- ARS: prefijo «$ » + número AR (ej. $ 32.784,33)
- Alícuota / número sin moneda: solo número con 2 decimales estilo AR
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from tasa_estadistica.export.recupero_excel import RECUPERO_V2_HEADERS

# Columnas en USD (declaración / base CIF / excedentes en dólares según modelo)
_COL_USD: frozenset[str] = frozenset(
    {
        "BASE RECONSTR. S/011",
        "FOB",
        "FLETE",
        "SEGURO",
        "CIF DOCUMENTAL",
        "DIF. BASE VS CIF",
        "T.E. CORRECTA S/BASE",
        "T.E. CORRECTA S/CIF",
        "EXCESO USD S/BASE",
        "EXCESO USD S/CIF",
    }
)

# Pesos (liquidación / cotización / montos a solicitar en ARS)
_COL_ARS: frozenset[str] = frozenset(
    {
        "TC DESPACHO",
        "T.E. 011",
        "T.E. MONT MAX 061",
        "T.E. MONT MAX2 062",
        "T.E. TOTAL COBRADA",
        "A SOLICITAR ARS S/BASE",
        "A SOLICITAR ARS S/CIF",
    }
)

# Tasa / diferencia sin símbolo de moneda (solo formato numérico)
_COL_NUM: frozenset[str] = frozenset({"ALICUOTA COBRADA"})

_COL_TEXT: frozenset[str] = frozenset(
    {
        "PROVEEDOR",
        "D.I.",
        "TIENE MONT MAX",
        "METODO SUGERIDO",
        "ESTADO REVISION",
        "ARCHIVO FUENTE",
    }
)


def _to_decimal(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    s = str(v).strip()
    if not s:
        return None
    try:
        if s.count(",") == 1 and "." not in s:
            s = s.replace(",", ".")
        elif s.count(",") == 1 and s.count(".") >= 1:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
        return Decimal(s)
    except Exception:
        return None


def num_argentina_2dec(d: Decimal) -> str:
    """12.345,67 (sin prefijo). Maneja negativos."""
    neg = d < 0
    n = abs(d).quantize(Decimal("0.01"))
    s = f"{n:,.2f}"
    ar = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-" if neg else "") + ar


def format_recupero_cell(columna: str, value: Any) -> str:
    """
    Texto listo para mostrar (vacío → el template muestra —).

    Montos ARS/USD: cadena con prefijo «$ » / «U$S » (texto plano; sin Markup HTML).
    """
    col = (columna or "").strip().replace("\u00a0", " ")
    if col == "OFICIALIZACION":
        if value is None or value == "":
            return ""
        if isinstance(value, datetime):
            return value.date().strftime("%d/%m/%Y")
        if isinstance(value, date):
            return value.strftime("%d/%m/%Y")
        return str(value).strip()

    if col in _COL_TEXT:
        if value is None:
            return ""
        return str(value).strip()

    if col in _COL_NUM:
        dec = _to_decimal(value)
        if dec is None:
            return ""
        return num_argentina_2dec(dec)

    if col in _COL_USD:
        dec = _to_decimal(value)
        if dec is None:
            return ""
        return "U$S " + num_argentina_2dec(dec)

    if col in _COL_ARS:
        dec = _to_decimal(value)
        if dec is None:
            return ""
        return "$ " + num_argentina_2dec(dec)

    # Fallback: cualquier otra columna numérica
    dec = _to_decimal(value)
    if dec is not None:
        return num_argentina_2dec(dec)
    if value is None or value == "":
        return ""
    return str(value).strip()


def format_filas_recupero_grilla(filas: list[list[Any]]) -> list[dict[str, str]]:
    """Un dict por fila con celdas ya formateadas para Jinja2 (texto con $ / U$S)."""
    keys = list(RECUPERO_V2_HEADERS)
    out: list[dict[str, str]] = []
    for row in filas:
        d: dict[str, str] = {}
        for i, k in enumerate(keys):
            v = row[i] if i < len(row) else ""
            d[k] = format_recupero_cell_safe(k, v)
        out.append(d)
    return out


def format_total_ars_display(total: Decimal) -> str:
    """Total período en ARS: «$ 32.784,33»."""
    dec = total.quantize(Decimal("0.01"))
    return "$ " + num_argentina_2dec(dec)


def format_recupero_cell_safe(columna: str, value: Any) -> str:
    """Igual que `format_recupero_cell`, pero no rompe el panel si un valor es raro."""
    try:
        return format_recupero_cell(columna, value)
    except Exception:
        return "—"
