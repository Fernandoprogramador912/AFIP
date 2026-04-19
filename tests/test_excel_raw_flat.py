"""Extracción de campos desde raw JSON."""

import json

from tasa_estadistica.export.excel_raw_flat import (
    extract_proveedor_from_liquidacion_raw_json,
    extract_recupero_valores_extra,
)


def test_extract_proveedor_desde_declaracion_listado() -> None:
    raw = {
        "cuit": "1",
        "raw": {
            "declaracion_listado": {
                "DenominacionProveedorExterior": "ACME SUPPLIER CO LTD",
            },
        },
    }
    s = json.dumps(raw, ensure_ascii=False)
    assert extract_proveedor_from_liquidacion_raw_json(s) == "ACME SUPPLIER CO LTD"


def test_extract_proveedor_vacio_sin_listado() -> None:
    raw = {"cuit": "1", "raw": {"liquidacion_resumen": {"Cotizacion": "1"}}}
    assert extract_proveedor_from_liquidacion_raw_json(json.dumps(raw)) == ""


def test_extract_proveedor_desde_caratula_moa_anidada() -> None:
    """DetalladaCaratula suele traer remitente/proveedor en nodos anidados."""
    raw = {
        "cuit": "1",
        "raw": {
            "declaracion_listado": {},
            "moa_detallada_caratula": {
                "DetalladaCaratulaRta": {
                    "Caratula": {
                        "NombreProveedorExterior": "SUPPLIER CO LTD",
                    }
                }
            },
        },
    }
    assert (
        extract_proveedor_from_liquidacion_raw_json(json.dumps(raw, ensure_ascii=False))
        == "SUPPLIER CO LTD"
    )


def test_extract_proveedor_desde_vendedor_como_en_afip_web() -> None:
    """En MOA web el proveedor figura como columna VENDEDOR (carátula / detallada)."""
    raw = {
        "cuit": "1",
        "raw": {
            "declaracion_listado": {},
            "moa_detallada_caratula": {
                "Caratula": {
                    "Vendedor": "NINGHAI YONGZHENG ELECTRONICS CO.LTD",
                }
            },
        },
    }
    assert (
        extract_proveedor_from_liquidacion_raw_json(json.dumps(raw, ensure_ascii=False))
        == "NINGHAI YONGZHENG ELECTRONICS CO.LTD"
    )


def test_extract_recupero_extra_desde_declaracion_listado() -> None:
    raw = {
        "cuit": "1",
        "raw": {
            "declaracion_listado": {
                "AlicuotaCobrada": "2,5",
                "ValorFOB": "10000",
                "ValorFlete": "500",
                "ValorSeguro": "50",
                "ValorCIF": "12000",
            },
        },
    }
    ex = extract_recupero_valores_extra(json.dumps(raw, ensure_ascii=False))
    assert ex["alicuota"] == "2,5"
    assert ex["fob"] == "10000"
    assert ex["flete"] == "500"
    assert ex["seguro"] == "50"
    assert "cif_documental" not in ex


def test_extract_recupero_extra_vacio_sin_claves() -> None:
    assert extract_recupero_valores_extra("{}")["fob"] == ""


def test_extract_recupero_extra_desde_moa_caratula_monto_totales() -> None:
    """Carátula MOA producción: MontoFobTotal / MontoFleteTotal / MontoSeguroTotal bajo Caratula."""
    raw = {
        "cuit": "1",
        "raw": {
            "declaracion_listado": {},
            "moa_detallada_caratula": {
                "ListaErrores": {"DetalleError": [{"Codigo": 0}]},
                "Caratula": {
                    "MontoFobTotal": "14444",
                    "MontoFleteTotal": "1158.14",
                    "MontoSeguroTotal": "78.01",
                },
            },
        },
    }
    ex = extract_recupero_valores_extra(json.dumps(raw, ensure_ascii=False))
    assert ex["fob"] == "14444"
    assert ex["flete"] == "1158.14"
    assert ex["seguro"] == "78.01"


def test_extract_recupero_extra_desde_moa_detallada_liquidaciones_detalle() -> None:
    """FOB/FLETE/SEGURO en la respuesta DetalladaLiquidacionesDetalle (persistida en fetch MOA)."""
    raw = {
        "cuit": "1",
        "raw": {
            "declaracion_listado": {},
            "moa_detallada_liquidaciones_detalle": {
                "DetalladaLiquidacionesDetalleResult": {
                    "LiquidacionDetalle": {
                        "ValorFOB": "999",
                        "ValorFlete": "88",
                        "ValorSeguro": "7",
                    }
                }
            },
        },
    }
    ex = extract_recupero_valores_extra(json.dumps(raw, ensure_ascii=False))
    assert ex["fob"] == "999"
    assert ex["flete"] == "88"
    assert ex["seguro"] == "7"
