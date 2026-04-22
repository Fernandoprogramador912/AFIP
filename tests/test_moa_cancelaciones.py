"""Tests de ConsultarCancelacionDetallada (Fase 1: descubrir/completar D.I. CANC).

AFIP `DetalladaListaDeclaraciones` NO devuelve `FechaOficializacion` para estado CANC.
Usamos `ConsultarCancelacionDetallada` para:
1. Completar fecha en filas ya conocidas (listado/env).
2. Inyectar D.I. cancelados que el listado oculta, cuando caen en rango.
"""

from __future__ import annotations

from datetime import date

from tasa_estadistica.arca.moa_declaracion import (
    _extract_cancelaciones_detalladas,
    _indexar_cancelaciones_por_dest,
    _parse_fecha_decl,
    merge_declaraciones_desde_cancelaciones,
)


class _Fake:
    """Objeto con atributos arbitrarios para imitar la respuesta SOAP (serialize_object)."""

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


def test_extract_cancelaciones_desde_rta_tipo_objeto() -> None:
    """Una cancelación única llega como único elemento (no lista)."""
    c = _Fake(
        DestinacionCancelada="25001IC04068168A",
        FechaOficializacion="2025-04-15T00:00:00",
        FechaCancelacion="2025-04-20T12:00:00",
        Estado="CANC",
        ModalidadCancelacion="TOTAL",
    )
    rta = _Fake(
        ConsultarCancelacionDetalladaResult=_Fake(
            Resultado=_Fake(CancelacionDetallada=c)
        )
    )
    filas = _extract_cancelaciones_detalladas(rta)
    assert len(filas) == 1
    assert filas[0]["DestinacionCancelada"] == "25001IC04068168A"
    assert filas[0]["Estado"] == "CANC"


def test_extract_cancelaciones_desde_lista_y_dict_vacio() -> None:
    rta = _Fake(ConsultarCancelacionDetalladaResult=_Fake(Resultado=None))
    assert _extract_cancelaciones_detalladas(rta) == []

    rta_dict = {
        "ConsultarCancelacionDetalladaResult": {
            "Resultado": {
                "CancelacionDetallada": [
                    {"DestinacionCancelada": "25001IC04068168A", "Estado": "CANC"},
                    {"DestinacionCancelada": "25001IC99999999X", "Estado": "CANC"},
                ]
            }
        }
    }
    filas = _extract_cancelaciones_detalladas(rta_dict)
    assert len(filas) == 2


def test_indexar_cancelaciones_por_dest_deduplica() -> None:
    canc = [
        {"DestinacionCancelada": "25001IC04068168A", "ItemCancelado": 1},
        {"DestinacionCancelada": "25001IC04068168A", "ItemCancelado": 2},
        {"DestinacionCancelada": "25001IC00000001A"},
    ]
    idx = _indexar_cancelaciones_por_dest(canc)
    assert set(idx.keys()) == {"25001IC04068168A", "25001IC00000001A"}
    assert idx["25001IC04068168A"]["ItemCancelado"] == 1


def test_merge_completa_fecha_en_fila_ya_presente() -> None:
    """Un D.I. inyectado por env (sin fecha) recibe FechaOficializacion desde la cancelación."""
    dest = {
        "25001IC04068168A": {
            "IdentificadorDestinacion": "25001IC04068168A",
            "IdentificadorDeclaracion": "25001IC04068168A",
            "_fuente_inyeccion": "ARCA_MOA_DESTINACION_IDS_EXTRA",
        }
    }
    cancelaciones = {
        "25001IC04068168A": {
            "DestinacionCancelada": "25001IC04068168A",
            "FechaOficializacion": "2025-04-15T00:00:00",
            "FechaCancelacion": "2025-04-20T12:00:00",
            "Estado": "CANC",
        }
    }
    added, n_completadas = merge_declaraciones_desde_cancelaciones(
        dest,
        cancelaciones,
        fecha_desde=date(2025, 4, 1),
        fecha_hasta=date(2025, 4, 30),
    )
    assert added == []
    assert n_completadas == 1
    assert _parse_fecha_decl(dest["25001IC04068168A"]) == date(2025, 4, 15)
    assert dest["25001IC04068168A"]["_estado_cancelacion"] == "CANC"


def test_merge_inyecta_di_cancelada_no_listada_en_rango() -> None:
    """Un D.I. CANC que no aparece en el listado se inyecta si su fecha cae en rango."""
    dest: dict = {}
    cancelaciones = {
        "25001IC11111111A": {
            "DestinacionCancelada": "25001IC11111111A",
            "FechaOficializacion": "2025-11-10T00:00:00",
            "Estado": "CANC",
        }
    }
    added, _ = merge_declaraciones_desde_cancelaciones(
        dest,
        cancelaciones,
        fecha_desde=date(2025, 11, 1),
        fecha_hasta=date(2025, 11, 30),
    )
    assert added == ["25001IC11111111A"]
    assert "25001IC11111111A" in dest
    fila = dest["25001IC11111111A"]
    assert fila["_fuente_inyeccion"] == "ConsultarCancelacionDetallada"
    assert _parse_fecha_decl(fila) == date(2025, 11, 10)


def test_merge_descarta_fuera_de_rango_por_default() -> None:
    dest: dict = {}
    cancelaciones = {
        "25001IC22222222A": {
            "DestinacionCancelada": "25001IC22222222A",
            "FechaOficializacion": "2024-05-10T00:00:00",
            "Estado": "CANC",
        }
    }
    added, _ = merge_declaraciones_desde_cancelaciones(
        dest,
        cancelaciones,
        fecha_desde=date(2025, 1, 1),
        fecha_hasta=date(2025, 12, 31),
    )
    assert added == []
    assert dest == {}


def test_merge_respeta_incluir_fuera_de_rango() -> None:
    dest: dict = {}
    cancelaciones = {
        "25001IC22222222A": {
            "DestinacionCancelada": "25001IC22222222A",
            "FechaOficializacion": "2024-05-10T00:00:00",
            "Estado": "CANC",
        }
    }
    added, _ = merge_declaraciones_desde_cancelaciones(
        dest,
        cancelaciones,
        fecha_desde=date(2025, 1, 1),
        fecha_hasta=date(2025, 12, 31),
        incluir_fuera_de_rango=True,
    )
    assert added == ["25001IC22222222A"]


def test_merge_no_pisa_fecha_ya_informada() -> None:
    """Si la fila ya trae FechaOficializacionDeclaracion (p. ej. del listado SIMI), no la pisa."""
    dest = {
        "25001IC04068168A": {
            "IdentificadorDestinacion": "25001IC04068168A",
            "FechaOficializacionDeclaracion": "2025-05-01T00:00:00",
        }
    }
    cancelaciones = {
        "25001IC04068168A": {
            "DestinacionCancelada": "25001IC04068168A",
            "FechaOficializacion": "2025-04-15T00:00:00",
            "Estado": "CANC",
        }
    }
    added, n_completadas = merge_declaraciones_desde_cancelaciones(
        dest,
        cancelaciones,
        fecha_desde=date(2025, 1, 1),
        fecha_hasta=date(2025, 12, 31),
    )
    assert added == []
    assert n_completadas == 0
    assert _parse_fecha_decl(dest["25001IC04068168A"]) == date(2025, 5, 1)
