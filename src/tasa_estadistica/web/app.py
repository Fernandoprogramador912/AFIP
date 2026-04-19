"""FastAPI: panel local SQLite, recupero, export Excel y POST /api/fetch."""

from __future__ import annotations

import importlib.util
import shutil
import tempfile
from pathlib import Path

# Antes de importar `tasa_estadistica.*`: priorizar `src/` del repo (ver _repo_path.py).
_REPO_PATH = Path(__file__).resolve().parent.parent.parent / "tasa_estadistica" / "_repo_path.py"
if _REPO_PATH.is_file():
    _spec = importlib.util.spec_from_file_location(
        "tasa_estadistica__repo_path_bootstrap",
        _REPO_PATH,
    )
    if _spec and _spec.loader:
        _boot = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_boot)
        _boot.ensure_repo_src_on_path()

import html
import logging
import sqlite3
from contextlib import asynccontextmanager
from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from tasa_estadistica.config.date_policy import (
    default_rango_mes_actual,
    period_presets,
    validate_analysis_period,
)
from tasa_estadistica.config.settings import get_settings
from tasa_estadistica.export.recupero_excel import (
    RECUPERO_V2_HEADERS,
    build_recupero_v2_excel,
    recupero_resumen_local,
)
from tasa_estadistica.storage.recupero_overrides import (
    delete_override,
    get_overrides_map,
    init_schema as init_overrides_schema,
    upsert_override,
)
from tasa_estadistica.web.recupero_display import (
    format_filas_recupero_grilla,
    format_total_ars_display,
)

_HERE = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(_HERE / "templates"))
_log = logging.getLogger(__name__)


class FetchStartBody(BaseModel):
    """Inicia descarga de liquidaciones (mismo criterio que `tasa-arca fetch`)."""

    desde: date
    hasta: date
    cuit: str | None = None  # default ARCA_CUIT en .env


class BackfillStartBody(BaseModel):
    """Inicia backfill mes a mes (mismo motor que `tasa-arca backfill`)."""

    desde: date
    hasta: date
    cuit: str | None = None
    reintentar_errores: bool = False
    forzar: bool = False
    pausa_entre_meses_seg: float = 0.0


class BackfillReintentarMesBody(BaseModel):
    """Reintento puntual de un solo mes desde la grilla del panel."""

    cuit: str | None = None
    anio: int
    mes: int


class RecuperoOverrideBody(BaseModel):
    """Carga manual de FOB/FLETE/SEGURO cuando AFIP no los devuelve (p. ej. CANC)."""

    destinacion_id: str
    fob: str | None = None
    flete: str | None = None
    seguro: str | None = None
    nota: str | None = None


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """
    Log de diagnóstico: qué copia de `excel_raw_flat` carga el proceso
    (útil tras cambios o `pip install -e .`).
    """
    import tasa_estadistica.export.excel_raw_flat as erf
    import tasa_estadistica.web.recupero_display as rd

    _log.info("Panel: módulo excel_raw_flat cargado desde %s", erf.__file__)
    _log.info("Panel: recupero_display cargado desde %s", rd.__file__)
    yield


app = FastAPI(
    title="Tasa estadística — recupero (local)",
    description="Vista demo: totales desde la base SQLite cargada con tasa-arca fetch.",
    version="0.1.0",
    lifespan=_lifespan,
)


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    """Evita 404 en consola; el navegador pide /favicon.ico en HTTP."""
    return Response(status_code=204)


def _sqlite_liquidaciones_cobertura(db_path: Path) -> dict[str, Any] | None:
    """
    Rango global de fechas en `liquidaciones` (toda la BD), no el período del formulario.
    Sirve para explicar por qué un año “vacío” en pantalla solo tiene datos de un mes descargado.
    """
    if not db_path.is_file():
        return None
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            """
            SELECT MIN(fecha), MAX(fecha), COUNT(*)
            FROM liquidaciones
            WHERE fecha IS NOT NULL AND TRIM(fecha) != ''
            """
        ).fetchone()
        if not row or row[2] == 0:
            return None
        return {
            "min_fecha": row[0],
            "max_fecha": row[1],
            "total_filas": int(row[2]),
        }
    finally:
        conn.close()


def _filas_para_template(filas: list[list[Any]]) -> list[dict[str, Any]]:
    """Listado de dicts con claves por nombre de columna."""
    keys = list(RECUPERO_V2_HEADERS)
    out: list[dict[str, Any]] = []
    for row in filas:
        d: dict[str, Any] = {}
        for i, k in enumerate(keys):
            d[k] = row[i] if i < len(row) else ""
        out.append(d)
    return out


@app.get("/", response_class=HTMLResponse)
def panel(
    request: Request,
    desde: date | None = Query(None, description="Fecha liquidación desde"),
    hasta: date | None = Query(None, description="Fecha liquidación hasta"),
) -> HTMLResponse:
    try:
        s = get_settings()
        if desde is None or hasta is None:
            desde, hasta = default_rango_mes_actual(s)
        else:
            validate_analysis_period(desde, hasta, s)
        res = recupero_resumen_local(s.arca_sqlite_path, desde, hasta, s)
        total: Decimal = res["total_tasa_recupero_ars"]
        total_fmt = format_total_ars_display(total)
        cob = _sqlite_liquidaciones_cobertura(s.arca_sqlite_path)
        ctx = {
            "request": request,
            "desde": desde,
            "hasta": hasta,
            "n_despachos": res["n_despachos"],
            "n_liquidaciones_en_rango": res["n_liquidaciones_en_rango"],
            "n_destinaciones_distintas_en_rango": res["n_destinaciones_distintas_en_rango"],
            "destinacion_subcadenas": res["destinacion_subcadenas"],
            "total_ars": total,
            "total_ars_fmt": total_fmt,
            "filas": format_filas_recupero_grilla(res["filas"]),
            "columnas": list(RECUPERO_V2_HEADERS),
            "db_name": s.arca_sqlite_path.name,
            "sqlite_path": str(s.arca_sqlite_path.resolve()),
            "sqlite_cobertura": cob,
            "cuit_default": s.arca_cuit or "—",
            "fecha_min_iso": s.tasa_analisis_desde.isoformat(),
            "fecha_max_iso": date.today().isoformat() if s.tasa_analisis_hasta_max_hoy else "",
            "period_presets": period_presets(s),
            "panel_fetch_enabled": s.tasa_panel_fetch_enabled,
            "recupero_avisos_fob": res.get("recupero_avisos_fob") or [],
            "recupero_overrides": res.get("recupero_overrides") or {},
        }
        # Render en el try: errores de plantilla/filtro → except (evita 500 genérico sin mensaje).
        html_out = templates.get_template("index.html").render(ctx)
        return HTMLResponse(content=html_out)
    except ValueError as exc:
        msg = html.escape(str(exc), quote=False)
        err_html = (
            "<!DOCTYPE html><html lang=\"es\"><head><meta charset=\"utf-8\">"
            "<title>Período no válido</title></head>"
            "<body style=\"font-family:system-ui;padding:1.5rem\">"
            "<h1>Período no válido</h1>"
            f"<p style=\"color:#666\">{msg}</p>"
            "<p><a href=\"/\">Volver al panel</a></p>"
            "</body></html>"
        )
        return HTMLResponse(content=err_html, status_code=400)
    except Exception as exc:
        _log.exception("Error al renderizar el panel /")
        msg = html.escape(str(exc), quote=False)
        err_html = (
            "<!DOCTYPE html><html lang=\"es\"><head><meta charset=\"utf-8\">"
            "<title>Error panel recupero</title></head>"
            "<body style=\"font-family:system-ui;padding:1.5rem\">"
            "<h1>Error al cargar el panel</h1>"
            f"<p style=\"color:#666\">{msg}</p>"
            "<p>Revisá la consola donde corre <code>tasa-arca serve</code> "
            "(traceback completo).</p>"
            "</body></html>"
        )
        return HTMLResponse(content=err_html, status_code=500)


@app.get("/export/recupero.xlsx")
def export_recupero_xlsx(
    desde: date = Query(..., description="Fecha liquidación desde (YYYY-MM-DD)"),
    hasta: date = Query(..., description="Fecha liquidación hasta (YYYY-MM-DD)"),
) -> Response:
    """
    Igual que `tasa-arca export-recupero` (hoja recupero_V2); archivo temporal y respuesta binaria.
    """
    s = get_settings()
    try:
        validate_analysis_period(desde, hasta, s)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    tmp = tempfile.mkdtemp(prefix="tasa-recupero-")
    try:
        out = Path(tmp) / "recupero_tasa.xlsx"
        build_recupero_v2_excel(s.arca_sqlite_path, out, desde, hasta, s)
        body = out.read_bytes()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    fname = f"recupero_tasa_{desde.isoformat()}_{hasta.isoformat()}.xlsx"
    return Response(
        content=body,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/resumen")
def api_resumen(
    desde: date = Query(..., description="YYYY-MM-DD"),
    hasta: date = Query(..., description="YYYY-MM-DD"),
) -> JSONResponse:
    s = get_settings()
    try:
        validate_analysis_period(desde, hasta, s)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    res = recupero_resumen_local(s.arca_sqlite_path, desde, hasta, s)
    total = res["total_tasa_recupero_ars"]
    cob = _sqlite_liquidaciones_cobertura(s.arca_sqlite_path)
    return JSONResponse(
        {
            "desde": desde.isoformat(),
            "hasta": hasta.isoformat(),
            "n_despachos_ic": res["n_despachos"],
            "n_liquidaciones_en_rango": res["n_liquidaciones_en_rango"],
            "n_destinaciones_distintas_en_rango": res["n_destinaciones_distintas_en_rango"],
            "destinacion_subcadenas": res["destinacion_subcadenas"],
            "total_tasa_recupero_ars": str(total),
            "sqlite": str(s.arca_sqlite_path.resolve()),
            "cobertura_sqlite_global": cob,
        }
    )


@app.get("/api/recupero")
def api_recupero(
    desde: date = Query(..., description="YYYY-MM-DD (fecha liquidación)"),
    hasta: date = Query(..., description="YYYY-MM-DD inclusive"),
    formatted: bool = Query(
        False,
        description=(
            "Si es true, incluye `filas_formateadas` con prefijos $ / U$S como el HTML del panel. "
            "Por defecto `filas` trae valores crudos (números/strings) para integración."
        ),
    ),
) -> JSONResponse:
    """
    Misma consulta que la grilla del panel y que `tasa-arca export-recupero --desde --hasta`.

    - `filas`: valores crudos por columna (como en SQLite/export), adecuado para scripts.
    - La vista HTML en `/` aplica formato de moneda; opcionalmente pedí `formatted=true`.
    """
    s = get_settings()
    try:
        validate_analysis_period(desde, hasta, s)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    res = recupero_resumen_local(s.arca_sqlite_path, desde, hasta, s)
    payload: dict[str, Any] = {
        "desde": desde.isoformat(),
        "hasta": hasta.isoformat(),
        "sqlite": str(s.arca_sqlite_path.resolve()),
        "n_despachos": res["n_despachos"],
        "n_liquidaciones_en_rango": res["n_liquidaciones_en_rango"],
        "n_destinaciones_distintas_en_rango": res["n_destinaciones_distintas_en_rango"],
        "destinacion_subcadenas": res["destinacion_subcadenas"],
        "columnas": list(RECUPERO_V2_HEADERS),
        "filas": _filas_para_template(res["filas"]),
        "recupero_avisos_fob": res.get("recupero_avisos_fob") or [],
        "recupero_overrides": res.get("recupero_overrides") or {},
    }
    if formatted:
        payload["filas_formateadas"] = format_filas_recupero_grilla(res["filas"])
    return JSONResponse(jsonable_encoder(payload))


@app.post("/api/fetch")
def api_fetch_start(body: FetchStartBody) -> JSONResponse:
    """
    Encola una descarga en segundo plano (tramos de 30 días en SOAP/MOA).
    Hacé polling a GET /api/fetch/status/{job_id} hasta status done o error.
    """
    s = get_settings()
    if not s.tasa_panel_fetch_enabled:
        raise HTTPException(
            status_code=403,
            detail="Descarga desde panel deshabilitada (TASA_PANEL_FETCH_ENABLED).",
        )
    try:
        validate_analysis_period(body.desde, body.hasta, s)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    cuit = (body.cuit or s.arca_cuit or "").strip()
    if not cuit:
        raise HTTPException(
            status_code=422,
            detail="Indicá CUIT en el cuerpo o configurá ARCA_CUIT en .env",
        )
    from tasa_estadistica.web.fetch_jobs import start_fetch_job

    job_id = start_fetch_job(
        desde=body.desde, hasta=body.hasta, cuit=cuit, settings=s
    )
    if job_id is None:
        raise HTTPException(
            status_code=409,
            detail="Ya hay una descarga en curso. Esperá a que termine.",
        )
    return JSONResponse({"job_id": job_id, "status": "running"})


@app.get("/api/fetch/status/{job_id}")
def api_fetch_status(job_id: str) -> JSONResponse:
    from tasa_estadistica.web.fetch_jobs import get_job

    j = get_job(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job no encontrado")
    return JSONResponse(jsonable_encoder(j))


def _parse_mes_o_fecha_iso(s: str, *, fin_de_mes: bool) -> date:
    """Acepta `YYYY-MM` o `YYYY-MM-DD` desde el cliente web."""
    from calendar import monthrange
    from datetime import datetime as _dt

    txt = (s or "").strip()
    if not txt:
        raise ValueError("fecha vacía")
    for fmt in ("%Y-%m-%d",):
        try:
            return _dt.strptime(txt, fmt).date()
        except ValueError:
            pass
    try:
        d = _dt.strptime(txt, "%Y-%m").date()
    except ValueError as e:
        raise ValueError(f"fecha inválida (use YYYY-MM o YYYY-MM-DD): {s}") from e
    dia = monthrange(d.year, d.month)[1] if fin_de_mes else 1
    return date(d.year, d.month, dia)


@app.post("/api/backfill")
def api_backfill_start(body: BackfillStartBody) -> JSONResponse:
    """Inicia backfill mes a mes en background. Polling: GET /api/backfill/status/{job_id}."""
    s = get_settings()
    if not s.tasa_panel_fetch_enabled:
        raise HTTPException(
            status_code=403,
            detail="Descarga desde panel deshabilitada (TASA_PANEL_FETCH_ENABLED).",
        )
    try:
        validate_analysis_period(body.desde, body.hasta, s)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    cuit = (body.cuit or s.arca_cuit or "").strip()
    if not cuit:
        raise HTTPException(
            status_code=422,
            detail="Indicá CUIT en el cuerpo o configurá ARCA_CUIT en .env",
        )
    from tasa_estadistica.web.fetch_jobs import start_backfill_job

    job_id = start_backfill_job(
        desde=body.desde,
        hasta=body.hasta,
        cuit=cuit,
        settings=s,
        reintentar_errores=body.reintentar_errores,
        forzar=body.forzar,
        pausa_entre_meses_seg=float(body.pausa_entre_meses_seg or 0.0),
    )
    if job_id is None:
        raise HTTPException(
            status_code=409,
            detail="Ya hay un backfill en curso. Esperá a que termine.",
        )
    return JSONResponse({"job_id": job_id, "status": "running"})


@app.get("/api/backfill/status/{job_id}")
def api_backfill_status(job_id: str) -> JSONResponse:
    from tasa_estadistica.web.fetch_jobs import get_backfill_job

    j = get_backfill_job(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job no encontrado")
    return JSONResponse(jsonable_encoder(j))


@app.get("/api/backfill/meses")
def api_backfill_meses(
    cuit: str | None = Query(default=None),
    desde: str | None = Query(default=None, description="YYYY-MM o YYYY-MM-DD"),
    hasta: str | None = Query(default=None, description="YYYY-MM o YYYY-MM-DD"),
) -> JSONResponse:
    """Devuelve el estado por mes (para pintar la grilla del panel)."""
    from tasa_estadistica.storage.backfill_state import (
        init_schema as init_backfill_schema,
        listar_meses,
    )

    s = get_settings()
    init_backfill_schema(s.arca_sqlite_path)
    cuit_n = (cuit or s.arca_cuit or "").strip()
    if not cuit_n:
        raise HTTPException(
            status_code=422,
            detail="Indicá ?cuit=... o configurá ARCA_CUIT en .env",
        )
    try:
        d_desde = _parse_mes_o_fecha_iso(desde, fin_de_mes=False) if desde else None
        d_hasta = _parse_mes_o_fecha_iso(hasta, fin_de_mes=True) if hasta else None
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    filas = listar_meses(s.arca_sqlite_path, cuit_n, desde=d_desde, hasta=d_hasta)
    return JSONResponse(
        jsonable_encoder({"cuit": cuit_n, "meses": filas, "total": len(filas)})
    )


@app.post("/api/backfill/reintentar-mes")
def api_backfill_reintentar_mes(body: BackfillReintentarMesBody) -> JSONResponse:
    """Reintenta un único mes (atajo desde la grilla del panel)."""
    s = get_settings()
    if not s.tasa_panel_fetch_enabled:
        raise HTTPException(
            status_code=403,
            detail="Descarga desde panel deshabilitada (TASA_PANEL_FETCH_ENABLED).",
        )
    if not (1 <= int(body.mes) <= 12):
        raise HTTPException(status_code=422, detail="mes debe ser 1..12")
    cuit = (body.cuit or s.arca_cuit or "").strip()
    if not cuit:
        raise HTTPException(
            status_code=422, detail="Indicá CUIT o configurá ARCA_CUIT en .env"
        )
    from calendar import monthrange

    desde = date(body.anio, body.mes, 1)
    hasta = date(body.anio, body.mes, monthrange(body.anio, body.mes)[1])
    try:
        validate_analysis_period(desde, hasta, s)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    from tasa_estadistica.web.fetch_jobs import start_backfill_job

    job_id = start_backfill_job(
        desde=desde,
        hasta=hasta,
        cuit=cuit,
        settings=s,
        forzar=True,
    )
    if job_id is None:
        raise HTTPException(
            status_code=409,
            detail="Ya hay un backfill en curso. Esperá a que termine.",
        )
    return JSONResponse({"job_id": job_id, "status": "running"})


@app.get("/api/recupero/override")
def api_recupero_overrides_list() -> JSONResponse:
    """Devuelve todos los overrides manuales cargados (para inspección / debugging)."""
    s = get_settings()
    init_overrides_schema(s.arca_sqlite_path)
    return JSONResponse(jsonable_encoder(get_overrides_map(s.arca_sqlite_path)))


@app.post("/api/recupero/override")
def api_recupero_override_upsert(body: RecuperoOverrideBody) -> JSONResponse:
    """
    Alta/edición de override para un D.I. Los campos vacíos se guardan como NULL.

    Cuando todos los campos numéricos quedan vacíos la fila persiste (vale como nota),
    pero no altera la grilla: el pipeline solo usa override si FOB/FLETE/SEGURO tienen
    valor. Para eliminar por completo el registro usá DELETE.
    """
    s = get_settings()
    init_overrides_schema(s.arca_sqlite_path)
    did = (body.destinacion_id or "").strip()
    if not did:
        raise HTTPException(status_code=422, detail="destinacion_id vacío")
    try:
        saved = upsert_override(
            s.arca_sqlite_path,
            did,
            fob=body.fob,
            flete=body.flete,
            seguro=body.seguro,
            nota=body.nota,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    return JSONResponse(jsonable_encoder({"ok": True, "override": saved}))


@app.delete("/api/recupero/override/{destinacion_id}")
def api_recupero_override_delete(destinacion_id: str) -> JSONResponse:
    """Borra el override (la fila vuelve a depender del JSON AFIP)."""
    s = get_settings()
    init_overrides_schema(s.arca_sqlite_path)
    did = (destinacion_id or "").strip()
    if not did:
        raise HTTPException(status_code=422, detail="destinacion_id vacío")
    ok = delete_override(s.arca_sqlite_path, did)
    return JSONResponse({"ok": ok})


@app.post("/api/recupero/override/csv")
async def api_recupero_override_csv(
    archivo: UploadFile = File(..., description="CSV con destinacion_id;fob;flete;seguro;nota"),
) -> JSONResponse:
    """
    Bulk upsert de overrides desde un CSV `destinacion_id;fob;flete;seguro;nota`.

    - Separador: `;` (Excel por defecto en AR). Acepta también `,` si la primera fila lo usa.
    - La primera fila se interpreta como encabezado si contiene `destinacion_id`.
    - Columnas faltantes se toman como vacías y borran el valor previo del override.
    """
    import csv as _csv
    import io

    s = get_settings()
    init_overrides_schema(s.arca_sqlite_path)
    try:
        data = await archivo.read()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"No pude leer el archivo: {e}") from e
    if not data:
        raise HTTPException(status_code=422, detail="Archivo vacío")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data.decode("latin-1")

    sample = text.splitlines()[0] if text else ""
    delim = ";" if sample.count(";") >= sample.count(",") else ","
    reader = _csv.reader(io.StringIO(text), delimiter=delim)
    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=422, detail="CSV sin filas")

    # Detectar encabezado: si la primera fila contiene 'destinacion_id' o 'd.i.'.
    header: list[str] | None = None
    primera = [str(c or "").strip().lower() for c in rows[0]]
    if any(h in ("destinacion_id", "d.i.", "di") for h in primera):
        header = primera
        rows = rows[1:]

    def _idx(nombre: str, *alias: str, default: int | None = None) -> int | None:
        if not header:
            return default
        for candidato in (nombre, *alias):
            c = candidato.lower()
            if c in header:
                return header.index(c)
        return None

    i_dest = _idx("destinacion_id", "d.i.", "di", default=0)
    i_fob = _idx("fob", default=1)
    i_flete = _idx("flete", default=2)
    i_seguro = _idx("seguro", default=3)
    i_nota = _idx("nota", "observacion", default=4)

    def _get(row: list[str], i: int | None) -> str | None:
        if i is None or i < 0 or i >= len(row):
            return None
        v = (row[i] or "").strip()
        return v or None

    procesadas = 0
    errores: list[dict[str, Any]] = []
    for n, row in enumerate(rows, start=1):
        dest_id = _get(row, i_dest)
        if not dest_id:
            errores.append({"fila": n, "error": "destinacion_id vacío"})
            continue
        try:
            upsert_override(
                s.arca_sqlite_path,
                dest_id,
                fob=_get(row, i_fob),
                flete=_get(row, i_flete),
                seguro=_get(row, i_seguro),
                nota=_get(row, i_nota),
            )
            procesadas += 1
        except ValueError as e:
            errores.append({"fila": n, "error": str(e), "destinacion_id": dest_id})

    return JSONResponse(
        {
            "procesadas": procesadas,
            "errores": errores,
            "delimitador": delim,
            "tenia_encabezado": header is not None,
        }
    )
