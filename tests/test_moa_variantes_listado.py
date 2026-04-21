"""Variantes MOA de listado (SIMI/Detallada).

AFIP rechaza con 42075 ("longitud invalida"):
- CUIT con guiones (`cuit_guiones`).
- `CodigoTipoOperacion` = "I"/"IC".
- `CodigoEstadoDeclaracion` = "TODOS" (los válidos son de 4 letras: OFIC/CANC/ANUL/SUSP).

Las variantes minimal/auto deben usar CUIT de 11 dígitos y, cuando filtren por estado,
códigos válidos (CANC/ANUL).
"""

from datetime import datetime

from tasa_estadistica.arca.moa_declaracion import (
    _variantes_auto_detallada,
    _variantes_auto_simi,
    _variantes_minimal_detallada,
    _variantes_minimal_simi,
)


_DESDE = datetime(2025, 4, 1)
_HASTA = datetime(2025, 4, 30, 23, 59, 59)
_CUIT = "30709572438"


def _labels(variantes: list[tuple[str, dict]]) -> list[str]:
    return [label for label, _ in variantes]


def _parms_por_label(variantes: list[tuple[str, dict]]) -> dict[str, dict]:
    return {label: parms for label, parms in variantes}


def _assert_no_rechazadas_por_afip(labels: list[str]) -> None:
    for rechazada in ("cuit_guiones", "cuit_guiones_TODOS", "cuit_11_I", "cuit_11_IC", "cuit_11_TODOS"):
        assert rechazada not in labels, f"Variante {rechazada} no debería generarse (AFIP devuelve 42075)"


def test_minimal_detallada_excluye_rechazadas_y_suma_cuit_11_canc() -> None:
    variantes = _variantes_minimal_detallada(_DESDE, _HASTA, _CUIT)
    labels = _labels(variantes)
    _assert_no_rechazadas_por_afip(labels)
    assert "cuit_11" in labels
    assert "cuit_11_CANC" in labels


def test_minimal_simi_excluye_rechazadas_y_suma_cuit_11_canc() -> None:
    variantes = _variantes_minimal_simi(_DESDE, _HASTA, _CUIT)
    labels = _labels(variantes)
    _assert_no_rechazadas_por_afip(labels)
    assert "cuit_11" in labels
    assert "cuit_11_CANC" in labels


def test_auto_detallada_excluye_rechazadas_y_suma_estados_validos() -> None:
    variantes = _variantes_auto_detallada(_DESDE, _HASTA, _CUIT)
    labels = _labels(variantes)
    _assert_no_rechazadas_por_afip(labels)
    assert "cuit_11" in labels
    assert "cuit_11_CANC" in labels
    assert "cuit_11_ANUL" in labels


def test_auto_simi_excluye_rechazadas_y_suma_estados_validos() -> None:
    variantes = _variantes_auto_simi(_DESDE, _HASTA, _CUIT)
    labels = _labels(variantes)
    _assert_no_rechazadas_por_afip(labels)
    assert "cuit_11" in labels
    assert "cuit_11_CANC" in labels
    assert "cuit_11_ANUL" in labels


def test_variantes_mantienen_cuit_11_digitos() -> None:
    for fn in (
        _variantes_minimal_detallada,
        _variantes_minimal_simi,
        _variantes_auto_detallada,
        _variantes_auto_simi,
    ):
        for _, parms in fn(_DESDE, _HASTA, _CUIT):
            assert parms["CuitImportadorExportador"] == _CUIT, (
                f"{fn.__name__} devolvió CUIT distinto de 11 dígitos"
            )


def test_variantes_con_estado_usan_codigos_4_letras() -> None:
    """AFIP exige que CodigoEstadoDeclaracion sea un código de 4 letras válido."""
    validos = {"OFIC", "CANC", "ANUL", "SUSP"}
    for fn in (
        _variantes_minimal_detallada,
        _variantes_minimal_simi,
        _variantes_auto_detallada,
        _variantes_auto_simi,
    ):
        for _, parms in fn(_DESDE, _HASTA, _CUIT):
            estado = parms.get("CodigoEstadoDeclaracion")
            if estado is not None:
                assert estado in validos, (
                    f"{fn.__name__}: CodigoEstadoDeclaracion={estado!r} inválido "
                    f"(AFIP acepta solo {sorted(validos)})"
                )
