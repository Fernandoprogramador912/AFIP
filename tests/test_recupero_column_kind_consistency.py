"""recupero_V2: Excel number_format vs panel recupero_display (USD/ARS)."""

from tasa_estadistica.export.recupero_excel import (
    RECUPERO_V2_HEADERS,
    recupero_v2_excel_currency_format,
)
from tasa_estadistica.web.recupero_display import _COL_ARS, _COL_NUM, _COL_TEXT, _COL_USD


def test_excel_currency_kind_matches_display_sets() -> None:
    for i, name in enumerate(RECUPERO_V2_HEADERS):
        c = i + 1
        kind = recupero_v2_excel_currency_format(c)
        if name in _COL_USD:
            assert kind == "usd", (name, c, kind)
        elif name in _COL_ARS:
            assert kind in ("ars", "tc"), (name, c, kind)
        elif name in _COL_NUM:
            assert kind == "alic", (name, c, kind)
        elif name == "OFICIALIZACION":
            assert kind == "fecha", (name, c, kind)
        elif name in _COL_TEXT:
            assert kind is None, (name, c, kind)
