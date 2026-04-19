"""Orquestador de backfill mes a mes (2019 → hoy).

Itera la lista de meses pendientes para un CUIT y por cada uno llama a
`execute_fetch(YYYY-MM-01, YYYY-MM-último)`. El resultado se persiste en la
tabla `backfill_meses` (estado `ok`/`sin_datos`/`error`). Si un mes falla,
sigue con el siguiente; si lo volvés a correr, salta los meses ya `ok`.

Es secuencial a propósito (decisión: empresas chicas = robustez secuencial
alcanza; el motor MOA ya tiene backoff exponencial para 6013).
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Callable

from tasa_estadistica.config.settings import Settings
from tasa_estadistica.fetch_runner import execute_fetch
from tasa_estadistica.storage.backfill_state import (
    MesBackfill,
    init_schema,
    meses_pendientes,
    upsert_estado,
)

logger = logging.getLogger(__name__)

ProgresoFn = Callable[[dict[str, Any]], None] | None


def _emitir(on_progress: ProgresoFn, ev: dict[str, Any]) -> None:
    if on_progress is None:
        return
    try:
        on_progress(ev)
    except Exception:  # noqa: BLE001
        logger.exception("Callback de progreso falló (ignorado)")


def _meta_get_int(meta: dict[str, Any], *keys: str) -> int | None:
    """Devuelve el primer valor int presente entre `keys` (acepta 0 explícito)."""
    for k in keys:
        v = meta.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return None


def _clasificar_resultado(
    resultado: dict[str, Any],
) -> tuple[str, str | None]:
    """Mapea el `dict` de `execute_fetch` a `(estado, ultimo_error)`."""
    if not resultado.get("ok"):
        return "error", str(resultado.get("error") or "fetch fallido sin detalle")
    n_liq = int(resultado.get("n_liquidaciones") or 0)
    meta = resultado.get("meta") or {}
    n_decl = _meta_get_int(meta, "declaraciones_encontradas", "declaraciones") or 0
    if n_decl == 0 and n_liq == 0:
        return "sin_datos", None
    return "ok", None


def run_backfill(
    *,
    desde: date,
    hasta: date,
    cuit: str,
    settings: Settings,
    reintentar_errores: bool = False,
    forzar: bool = False,
    pausa_entre_meses_seg: float = 0.0,
    on_progress: ProgresoFn = None,
    fetch_fn: Callable[..., dict[str, Any]] = execute_fetch,
) -> dict[str, Any]:
    """Procesa secuencialmente todos los meses pendientes del rango.

    Devuelve un resumen con total/procesados/ok/sin_datos/error y el detalle por mes.

    `fetch_fn` es inyectable para testear sin AFIP (default: `execute_fetch` real).
    `pausa_entre_meses_seg` agrega un sleep entre meses (útil para no saturar AFIP
    en backfills muy largos; default 0).
    """
    init_schema(settings.arca_sqlite_path)
    cuit_n = (cuit or "").strip() or (settings.arca_cuit or "").strip()
    if not cuit_n:
        raise ValueError("CUIT vacío y no hay ARCA_CUIT en settings")

    pendientes = meses_pendientes(
        settings.arca_sqlite_path,
        cuit_n,
        desde,
        hasta,
        reintentar_errores=reintentar_errores,
        forzar=forzar,
    )
    total = len(pendientes)
    logger.info(
        "backfill cuit=%s rango=%s..%s pendientes=%s (reintentar_errores=%s, forzar=%s)",
        cuit_n,
        desde.isoformat(),
        hasta.isoformat(),
        total,
        reintentar_errores,
        forzar,
    )
    _emitir(
        on_progress,
        {
            "fase": "inicio",
            "cuit": cuit_n,
            "desde": desde.isoformat(),
            "hasta": hasta.isoformat(),
            "total": total,
        },
    )

    detalle: list[dict[str, Any]] = []
    contador = {"ok": 0, "sin_datos": 0, "error": 0}
    procesados = 0

    for mes in pendientes:
        procesados += 1
        _emitir(
            on_progress,
            {
                "fase": "mes_inicio",
                "mes": mes.label,
                "i": procesados,
                "total": total,
            },
        )
        upsert_estado(
            settings.arca_sqlite_path,
            cuit_n,
            mes.anio,
            mes.mes,
            "en_proceso",
            incrementar_intentos=True,
        )

        try:
            resultado = fetch_fn(
                desde=mes.desde,
                hasta=mes.hasta,
                cuit=cuit_n,
                settings=settings,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Excepción no controlada en mes %s", mes.label)
            resultado = {"ok": False, "error": f"excepcion: {exc}"}

        estado, ultimo_error = _clasificar_resultado(resultado)
        if resultado.get("ok"):
            n_liq = int(resultado.get("n_liquidaciones") or 0)
            meta = resultado.get("meta") or {}
            n_decl = _meta_get_int(meta, "declaraciones_encontradas", "declaraciones")
            if n_decl is None:
                n_decl = 0
        else:
            n_liq = None
            n_decl = None

        upsert_estado(
            settings.arca_sqlite_path,
            cuit_n,
            mes.anio,
            mes.mes,
            estado,
            run_id=resultado.get("run_id") if resultado.get("ok") else None,
            n_declaraciones=n_decl,
            n_liquidaciones=n_liq,
            ultimo_error=ultimo_error,
            incrementar_intentos=False,
        )

        contador[estado] = contador.get(estado, 0) + 1
        info_mes = {
            "mes": mes.label,
            "estado": estado,
            "n_declaraciones": n_decl,
            "n_liquidaciones": n_liq,
            "run_id": resultado.get("run_id") if resultado.get("ok") else None,
            "error": ultimo_error,
        }
        detalle.append(info_mes)

        logger.info(
            "backfill mes=%s estado=%s n_decl=%s n_liq=%s",
            mes.label,
            estado,
            n_decl,
            n_liq,
        )
        _emitir(
            on_progress,
            {
                "fase": "mes_fin",
                "mes": mes.label,
                "estado": estado,
                "n_declaraciones": n_decl,
                "n_liquidaciones": n_liq,
                "run_id": resultado.get("run_id") if resultado.get("ok") else None,
                "error": ultimo_error,
                "i": procesados,
                "total": total,
            },
        )

        if pausa_entre_meses_seg > 0 and procesados < total:
            time.sleep(pausa_entre_meses_seg)

    resumen = {
        "cuit": cuit_n,
        "desde": desde.isoformat(),
        "hasta": hasta.isoformat(),
        "total_pendientes": total,
        "procesados": procesados,
        "ok": contador.get("ok", 0),
        "sin_datos": contador.get("sin_datos", 0),
        "error": contador.get("error", 0),
        "detalle": detalle,
    }
    _emitir(on_progress, {"fase": "fin", **resumen})
    return resumen


def run_backfill_un_mes(
    *,
    cuit: str,
    anio: int,
    mes: int,
    settings: Settings,
    fetch_fn: Callable[..., dict[str, Any]] = execute_fetch,
    on_progress: ProgresoFn = None,
) -> dict[str, Any]:
    """Atajo para reintentar **un** mes puntual desde el panel."""
    m = MesBackfill(cuit=cuit, anio=anio, mes=mes)
    return run_backfill(
        desde=m.desde,
        hasta=m.hasta,
        cuit=cuit,
        settings=settings,
        forzar=True,
        fetch_fn=fetch_fn,
        on_progress=on_progress,
    )
