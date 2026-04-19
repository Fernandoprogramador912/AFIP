"""
Fórmulas de la hoja «V2_Ejemplo» del modelo Excel (Modelo_Excel_Recupero_Tasa_Estadistica.xlsx).

Parámetros globales (fila 1 del modelo):
  - $D$1 = alícuota «correcta» usada en T.E. CORRECTA (0,005 en el modelo)
  - $F$1 = tope USD (500 en el modelo)

Referencias de celdas por fila de datos (ej. fila 4):
  I = SUM(F:H), J = IF(AND(F>0,E>0), F/E, ""), N = SUM(K:M), O = IF(AND(J<>"",N<>""), N-J, ""),

  Completado en código cuando E está vacío (misma columna J):
  - base imponible desde AFIP (`base_reconstr` en JSON) si viene informada;
  - si hay CIF documental N=SUM(K:M)>0 y F>0: J = N (E implícita F/N → J=N), también con MONT MAX.
  P = IF(J="", "", MIN(J*$D$1, $F$1)), Q = IF(N="", "", MIN(N*$D$1, $F$1)),
  R = IF(P="", "", MAX(I-P, 0)), S = IF(Q="", "", MAX(I-Q, 0)),
  T = IF(R="", "", R*D), U = IF(S="", "", S*D),
  V = IF(SUM(G:H)>0, "SI", "NO"),
  W = IF(V="SI", "CIF", IF(ABS(O)<=1, "BASE 011", "REVISAR")),
  X = IF(V="SI", "CASO ESPECIAL", IF(ABS(O)<=1, "OK", "REVISAR")),
  Y = nombre de archivo (D.I. + .pdf)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any


def _dec(s: str | None) -> Decimal | None:
    """
    Convierte texto AFIP/MOA a Decimal.

    MOA a veces devuelve montos como cadena estilo AR (p. ej. «14.444,00»). El reemplazo
    ingenuo «coma → punto» deja «14.444.00» (inválido), la fila interpreta FOB=0 y
    `_cell_monto_component` vacía la celda — el panel y el Excel muestran FOB/FLETE/SEGURO
    en blanco aunque el JSON tenga el valor. Misma heurística que `recupero_display._to_decimal`.
    """
    if s is None:
        return None
    t = str(s).strip().replace("\xa0", " ")
    t = t.replace(" ", "")
    if not t:
        return None
    try:
        if t.count(",") == 1 and "." not in t:
            normalized = t.replace(",", ".")
        elif t.count(",") == 1 and t.count(".") >= 1:
            normalized = t.replace(".", "").replace(",", ".")
        else:
            normalized = t.replace(",", "")
        return Decimal(normalized)
    except Exception:
        return None


def _excel_num(d: Decimal) -> float | int:
    if d == d.to_integral():
        return int(d)
    return float(d)


def _cell_num(v: Decimal | None) -> float | int | str:
    if v is None:
        return ""
    return _excel_num(v)


def _cell_monto_component(val: Decimal, raw_str: str) -> float | int | str:
    """Vacío si no hay dato AFIP y el valor es 0 (como celda en blanco en el modelo)."""
    if _dec(raw_str) is None and val == 0:
        return ""
    return _excel_num(val)


@dataclass(frozen=True)
class RecuperoV2SheetParams:
    """Mismos valores que la fila 1 de V2_Ejemplo ($D$1 y $F$1)."""

    alicuota_correcta_d1: Decimal = Decimal("0.005")
    tope_usd_f1: Decimal = Decimal("500")


def build_recupero_v2_row_with_formulas(
    *,
    proveedor: str,
    destinacion_id: str,
    oficial: date | None,
    tc_raw: Any,
    alicuota_cobrada_str: str,
    base_reconstr_str: str = "",
    te011: Decimal,
    te061: Decimal,
    te062: Decimal,
    fob_str: str,
    flete_str: str,
    seguro_str: str,
    params: RecuperoV2SheetParams | None = None,
) -> list[Any]:
    """
    Devuelve una fila de 25 columnas alineada a RECUPERO_V2_HEADERS
    con las mismas fórmulas que el Excel.
    """
    p = params or RecuperoV2SheetParams()
    d1 = p.alicuota_correcta_d1
    f1 = p.tope_usd_f1

    tc = _dec(str(tc_raw)) if tc_raw not in (None, "") else None

    e = _dec(alicuota_cobrada_str)
    f = te011
    g = te061
    h = te062
    i_val = f + g + h

    k = _dec(fob_str) or Decimal("0")
    l_ = _dec(flete_str) or Decimal("0")
    m = _dec(seguro_str) or Decimal("0")
    n_val = k + l_ + m

    # J = BASE RECONSTR. S/011 — Excel: IF(AND(F>0,E>0), F/E, "")
    j_val: Decimal | None = None
    if f > 0 and e is not None and e > 0:
        j_val = f / e
    else:
        br = _dec(base_reconstr_str)
        if br is not None and br > 0:
            j_val = br
        elif f > 0 and n_val > 0:
            # Sin E en AFIP: E implícita = F/N → J = F/E = N (CIF), con o sin 061/062.
            j_val = n_val

    # O = DIF. BASE VS CIF
    o_val: Decimal | None
    if j_val is not None:
        o_val = n_val - j_val
    else:
        o_val = None

    # P = T.E. CORRECTA S/BASE
    p_val: Decimal | None
    if j_val is not None:
        p_val = min(j_val * d1, f1)
    else:
        p_val = None

    # Q = T.E. CORRECTA S/CIF (=IF(N="", "", MIN(N*$D$1, $F$1)) — N siempre numérico vía SUM)
    q_val = min(n_val * d1, f1)

    # R, S excesos (=IF(P="", "", MAX(I-P, 0)) y análogo con Q)
    r_val: Decimal | None = max(i_val - p_val, Decimal("0")) if p_val is not None else None
    s_val: Decimal | None = max(i_val - q_val, Decimal("0")) if q_val is not None else None

    # T, U en ARS (× TC columna D)
    t_val: Decimal | None
    if r_val is not None and tc is not None:
        t_val = r_val * tc
    else:
        t_val = None

    u_val: Decimal | None
    if s_val is not None and tc is not None:
        u_val = s_val * tc
    else:
        u_val = None

    # V = TIENE MONT MAX
    tiene_mont = "SI" if (g + h) > 0 else "NO"

    # W, X (ABS(O)<=1 como en Excel)
    if o_val is None:
        abs_o_ok = False
    else:
        abs_o_ok = abs(o_val) <= Decimal("1")

    if tiene_mont == "SI":
        metodo = "CIF"
        estado = "CASO ESPECIAL"
    elif abs_o_ok:
        metodo = "BASE 011"
        estado = "OK"
    else:
        metodo = "REVISAR"
        estado = "REVISAR"

    archivo = f"{destinacion_id}.pdf" if destinacion_id else ""

    return [
        proveedor or "",
        destinacion_id or "",
        oficial if oficial is not None else "",
        _cell_num(tc) if tc is not None else "",
        alicuota_cobrada_str or "",
        _excel_num(f),
        _excel_num(g),
        _excel_num(h),
        _excel_num(i_val),
        _cell_num(j_val),
        _cell_monto_component(k, fob_str),
        _cell_monto_component(l_, flete_str),
        _cell_monto_component(m, seguro_str),
        _excel_num(n_val),
        _cell_num(o_val),
        _cell_num(p_val),
        _cell_num(q_val),
        _cell_num(r_val),
        _cell_num(s_val),
        _cell_num(t_val),
        _cell_num(u_val),
        tiene_mont,
        metodo,
        estado,
        archivo,
    ]
