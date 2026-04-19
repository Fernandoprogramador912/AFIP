"""Inyección de D.I. extra en pipeline MOA."""

from tasa_estadistica.arca.moa_declaracion import merge_declaraciones_extra_desde_settings
from tasa_estadistica.config.settings import Settings


def test_extra_inyecta_solo_faltantes() -> None:
    dest: dict = {
        "25001IC06004713L": {"IdentificadorDeclaracion": "25001IC06004713L"},
    }
    s = Settings(
        arca_moa_destinacion_ids_extra=(
            "25001IC06004713L,25001IC04068168A, 25001IC06008719V"
        ),
    )
    added = merge_declaraciones_extra_desde_settings(dest, s)
    assert set(added) == {"25001IC04068168A", "25001IC06008719V"}
    assert dest["25001IC04068168A"]["_fuente_inyeccion"] == "ARCA_MOA_DESTINACION_IDS_EXTRA"


def test_extra_vacio_no_toca() -> None:
    dest = {}
    added = merge_declaraciones_extra_desde_settings(
        dest,
        Settings(arca_moa_destinacion_ids_extra=""),
    )
    assert added == []
    assert dest == {}
