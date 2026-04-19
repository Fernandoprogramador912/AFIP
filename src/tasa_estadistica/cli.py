"""CLI local: doctor (diagnóstico), auth (WSAA), fetch, export, rebuild."""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import platform
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

# Antes de importar `tasa_estadistica.*`: priorizar `src/` del repo (ver _repo_path.py).
_REPO_PATH = Path(__file__).resolve().parent / "_repo_path.py"
if _REPO_PATH.is_file():
    _spec = importlib.util.spec_from_file_location(
        "tasa_estadistica__repo_path_bootstrap",
        _REPO_PATH,
    )
    if _spec and _spec.loader:
        _boot = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_boot)
        _boot.ensure_repo_src_on_path()

from tasa_estadistica.arca.auth_ticket_store import parse_ticket_xml, wsaa_ticket_expired
from tasa_estadistica.arca.wsaa_client import WSAAClient
from tasa_estadistica.config.date_policy import validate_analysis_period
from tasa_estadistica.config.settings import get_settings, get_tasa_mapper_from_settings
from tasa_estadistica.export.excel_report import build_auditable_excel, reapply_mapper_sqlite
from tasa_estadistica.export.recupero_compare import compare_excel_vs_sqlite, format_compare_report
from tasa_estadistica.export.recupero_excel import build_recupero_v2_excel
from tasa_estadistica.backfill_runner import run_backfill
from tasa_estadistica.fetch_runner import execute_fetch
from tasa_estadistica.storage.backfill_state import (
    iter_meses_rango,
    listar_meses,
)
from tasa_estadistica.report.ic_tasa_report import (
    query_ic_tasa_rows,
    total_monto,
    write_ic_tasa_csv,
)

logger = logging.getLogger(__name__)


def _validate_period_cli(desde, hasta) -> int | None:
    """Devuelve código de salida 2 si el período no cumple la política analítica."""
    try:
        validate_analysis_period(desde, hasta, get_settings())
    except ValueError as e:
        logger.error("%s", e)
        return 2
    return None


def _parse_date(s: str):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Fecha inválida: {s}")


def _parse_mes_o_fecha(s: str, *, fin_de_mes: bool) -> date:
    """Acepta `YYYY-MM` o `YYYY-MM-DD`. Para `YYYY-MM`, fin_de_mes elige día 1 vs último día."""
    from calendar import monthrange

    txt = (s or "").strip()
    if not txt:
        raise argparse.ArgumentTypeError("Fecha vacía")
    try:
        return _parse_date(txt)
    except argparse.ArgumentTypeError:
        pass
    try:
        d = datetime.strptime(txt, "%Y-%m").date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Fecha inválida (use YYYY-MM o YYYY-MM-DD): {s}"
        ) from e
    dia = monthrange(d.year, d.month)[1] if fin_de_mes else 1
    return date(d.year, d.month, dia)


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _url_host(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return "—"
    try:
        netloc = urlparse(u).netloc
        return netloc if netloc else u[:80]
    except Exception:
        return u[:80]


def cmd_doctor(_args: argparse.Namespace) -> int:
    """
    Imprime diagnóstico local (rutas resueltas, WSAA, WSDL, TA) sin contraseñas ni TA completo.
    """
    s = get_settings()
    lines: list[str] = []
    lines.append(f"python: {sys.version.split()[0]} ({platform.system()})")
    lines.append(f"ARCA_MODE: {s.arca_mode}")
    lines.append(f"ARCA_CUIT: {(s.arca_cuit or '').strip() or '—'}")
    db = s.arca_sqlite_path.resolve()
    lines.append(f"SQLite: {db} exists={db.is_file()}")
    if db.is_file():
        try:
            conn = sqlite3.connect(str(db))
            row = conn.execute(
                """
                SELECT COUNT(*), MIN(fecha), MAX(fecha)
                FROM liquidaciones
                WHERE fecha IS NOT NULL AND TRIM(fecha) != ''
                """
            ).fetchone()
            conn.close()
            if row and row[0]:
                lines.append(f"  liquidaciones (con fecha): n={row[0]} min={row[1]} max={row[2]}")
            else:
                lines.append("  liquidaciones (con fecha): n=0")
        except sqlite3.Error as exc:
            lines.append(f"  (no se pudo leer SQLite: {exc})")
    ta_path = s.arca_ticket_path.resolve()
    lines.append(f"TA path: {ta_path} exists={ta_path.is_file()}")
    if ta_path.is_file():
        try:
            raw_ta = ta_path.read_bytes()
            ta = parse_ticket_xml(raw_ta)
            lines.append(f"  TA generationTime: {ta.generation_time or '—'}")
            lines.append(f"  TA expirationTime: {ta.expiration_time or '—'}")
            lines.append(f"  TA vencido (según expirationTime): {wsaa_ticket_expired(raw_ta)}")
        except (OSError, ValueError) as exc:
            lines.append(f"  (TA no parseable: {exc})")
    cert = s.arca_cert_path
    if cert is not None:
        cp = cert.resolve()
        lines.append(f"ARCA_CERT_PATH: {cp} exists={cp.is_file()}")
    else:
        lines.append("ARCA_CERT_PATH: —")
    lines.append(f"ARCA_WSAA_URL: {s.arca_wsaa_url}")
    lines.append(f"  host: {_url_host(s.arca_wsaa_url)}")
    lines.append(f"ARCA_WSAA_SERVICE (TRA): {s.arca_wsaa_service}")
    lines.append(
        f"ARCA_WSAA_HASH: {s.arca_wsaa_hash}  ARCA_WSAA_TIME_SOURCE: {s.arca_wsaa_time_source}"
    )
    w = (s.arca_liquidaciones_wsdl or "").strip()
    lines.append(f"ARCA_LIQUIDACIONES_WSDL: {w or '—'}")
    if w:
        lines.append(f"  host: {_url_host(w)}")
    lines.append(f"ARCA_LIQUIDACIONES_METHOD: {s.arca_liquidaciones_method}")
    comp = (s.arca_liquidaciones_complemento_wsdl or "").strip()
    lines.append(f"ARCA_LIQUIDACIONES_COMPLEMENTO_WSDL: {comp or '—'}")
    if comp:
        lines.append(f"  host: {_url_host(comp)}")
    lines.append(
        "MOA sleeps: "
        f"chunk={s.arca_moa_chunk_sleep_seconds}s "
        f"retry_6013={s.arca_moa_retry_6013_sleep_seconds}s"
    )
    lines.append(f"TASA_PANEL_FETCH_ENABLED: {s.tasa_panel_fetch_enabled}")
    print("\n".join(lines))
    return 0


def cmd_auth(_args: argparse.Namespace) -> int:
    s = get_settings()
    if s.arca_mode.lower() == "mock":
        logger.warning(
            "ARCA_MODE=mock: no se llama a WSAA. Use ARCA_MODE=live para obtener TA real."
        )
        return 0
    wsaa = WSAAClient(s)
    ta = wsaa.login_cms()
    s.arca_ticket_path.parent.mkdir(parents=True, exist_ok=True)
    s.arca_ticket_path.write_bytes(ta)
    logger.info("Ticket guardado en %s", s.arca_ticket_path)
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    s = get_settings()

    cuit = args.cuit or s.arca_cuit
    if not cuit:
        logger.error("Indique CUIT (--cuit) o ARCA_CUIT en .env")
        return 2

    desde = _parse_date(args.desde)
    hasta = _parse_date(args.hasta)
    bad = _validate_period_cli(desde, hasta)
    if bad is not None:
        return bad

    result = execute_fetch(desde=desde, hasta=hasta, cuit=cuit, settings=s)
    if not result.get("ok"):
        logger.error("%s", result.get("error", "Error en fetch"))
        return 2
    logger.info(
        "Extracción guardada run_id=%s liquidaciones=%s",
        result.get("run_id"),
        result.get("n_liquidaciones"),
    )
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    """Backfill mes a mes con estado persistente y reanudación automática."""
    s = get_settings()
    cuit = (args.cuit or s.arca_cuit or "").strip()
    if not cuit:
        logger.error("Indique --cuit o configure ARCA_CUIT en .env")
        return 2
    try:
        if getattr(args, "solo_mes", None):
            d = _parse_mes_o_fecha(args.solo_mes, fin_de_mes=False)
            desde = date(d.year, d.month, 1)
            hasta = _parse_mes_o_fecha(args.solo_mes, fin_de_mes=True)
        else:
            desde = _parse_mes_o_fecha(args.desde, fin_de_mes=False)
            hasta = _parse_mes_o_fecha(args.hasta, fin_de_mes=True)
    except argparse.ArgumentTypeError as e:
        logger.error("%s", e)
        return 2
    bad = _validate_period_cli(desde, hasta)
    if bad is not None:
        return bad

    pausa = float(getattr(args, "pausa", 0.0) or 0.0)
    logger.info(
        "Backfill cuit=%s rango=%s..%s reintentar_errores=%s forzar=%s",
        cuit,
        desde.isoformat(),
        hasta.isoformat(),
        args.reintentar_errores,
        args.forzar,
    )
    res = run_backfill(
        desde=desde,
        hasta=hasta,
        cuit=cuit,
        settings=s,
        reintentar_errores=bool(args.reintentar_errores),
        forzar=bool(args.forzar),
        pausa_entre_meses_seg=pausa,
    )
    logger.info(
        "Backfill terminado: procesados=%s ok=%s sin_datos=%s error=%s",
        res["procesados"],
        res["ok"],
        res["sin_datos"],
        res["error"],
    )
    if res["error"] > 0:
        for d_mes in res["detalle"]:
            if d_mes["estado"] == "error":
                logger.warning(
                    "  - %s ERROR: %s",
                    d_mes["mes"],
                    (d_mes.get("error") or "")[:200],
                )
        return 3 if res["ok"] == 0 and res["sin_datos"] == 0 else 0
    return 0


_GLIFOS_ESTADO = {
    "ok": "X",
    "sin_datos": "o",
    "error": "E",
    "pendiente": ".",
    "en_proceso": "~",
}


def cmd_backfill_status(args: argparse.Namespace) -> int:
    """Imprime una tabla compacta anio/mes con el estado del backfill (solo lectura)."""
    s = get_settings()
    cuit = (args.cuit or s.arca_cuit or "").strip()
    if not cuit:
        logger.error("Indique --cuit o configure ARCA_CUIT en .env")
        return 2
    try:
        desde = _parse_mes_o_fecha(args.desde, fin_de_mes=False)
        hasta = _parse_mes_o_fecha(args.hasta, fin_de_mes=True)
    except argparse.ArgumentTypeError as e:
        logger.error("%s", e)
        return 2

    filas = {
        (int(r["anio"]), int(r["mes"])): r
        for r in listar_meses(s.arca_sqlite_path, cuit, desde=desde, hasta=hasta)
    }
    meses_pedidos = list(iter_meses_rango(desde, hasta, cuit))
    if not meses_pedidos:
        print("(sin meses en el rango)")
        return 0
    rango_anios = sorted({m.anio for m in meses_pedidos})
    pedidos_set = {(m.anio, m.mes) for m in meses_pedidos}

    cabezal = "       " + " ".join(
        f"{n:>3}" for n in
        ["ene","feb","mar","abr","may","jun","jul","ago","sep","oct","nov","dic"]
    )
    print(f"CUIT {cuit}  -  desde {desde.strftime('%Y-%m')} hasta {hasta.strftime('%Y-%m')}")
    print(cabezal)
    contador = {"ok": 0, "sin_datos": 0, "error": 0, "pendiente": 0, "en_proceso": 0}
    for anio in rango_anios:
        celdas: list[str] = []
        for m in range(1, 13):
            if (anio, m) not in pedidos_set:
                celdas.append("  -")
                continue
            row = filas.get((anio, m))
            if row is None:
                glifo = "."
                contador["pendiente"] += 1
            else:
                est = (row.get("estado") or "").strip()
                glifo = _GLIFOS_ESTADO.get(est, "?")
                if est in contador:
                    contador[est] += 1
            celdas.append(f"  {glifo}")
        print(f"{anio:<6} {' '.join(celdas)}")
    print()
    print(
        "Leyenda: X=ok  o=sin_datos  E=error  .=pendiente  ~=en_proceso  -=fuera de rango"
    )
    total = sum(contador.values())
    print(
        f"Resumen: total={total}  ok={contador['ok']}  sin_datos={contador['sin_datos']}  "
        f"error={contador['error']}  pendiente={contador['pendiente']}  "
        f"en_proceso={contador['en_proceso']}"
    )
    return 0


def _parse_export_fechas(args: argparse.Namespace) -> tuple[date | None, date | None]:
    """--desde y --hasta juntos, o ninguno (filtro por fecha de liquidación en SQLite)."""
    desde = (getattr(args, "desde", None) or "").strip()
    hasta = (getattr(args, "hasta", None) or "").strip()
    if not desde and not hasta:
        return None, None
    if not desde or not hasta:
        raise ValueError(
            "Para filtrar el Excel use --desde y --hasta juntos (fecha de liquidación, YYYY-MM-DD)."
        )
    return _parse_date(desde), _parse_date(hasta)


def cmd_export(args: argparse.Namespace) -> int:
    try:
        fd, fh = _parse_export_fechas(args)
    except ValueError as e:
        logger.error("%s", e)
        return 2
    s = get_settings()
    if fd is not None and fh is not None:
        bad = _validate_period_cli(fd, fh)
        if bad is not None:
            return bad
    out = build_auditable_excel(
        s.arca_sqlite_path, s.arca_excel_output, fecha_desde=fd, fecha_hasta=fh
    )
    logger.info("Excel generado: %s", out)
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    """Panel web FastAPI en localhost (requiere dependencia opcional web)."""
    try:
        import uvicorn
    except ImportError:
        logger.error('Instale el extra web: pip install -e ".[web]"')
        return 2
    host = os.environ.get("TASA_ARCA_HOST", args.host)
    port = int(os.environ.get("TASA_ARCA_PORT", str(args.port)))
    reload = bool(getattr(args, "reload", False)) or (
        os.environ.get("TASA_ARCA_RELOAD", "").strip().lower() in ("1", "true", "yes")
    )
    logger.info("Abrí en el navegador: http://%s:%s/", host, port)
    if reload:
        logger.info(
            "Recarga automática activa (--reload o TASA_ARCA_RELOAD=1); "
            "reinicia manual si no ves cambios."
        )
    uvicorn.run(
        "tasa_estadistica.web.app:app",
        host=host,
        port=port,
        reload=reload,
    )
    return 0


def cmd_compare_recupero_excel(args: argparse.Namespace) -> int:
    """Compara el Excel modelo (hoja V2_Ejemplo) con SQLite para el mismo rango de fechas."""
    s = get_settings()
    excel = Path(args.excel)
    desde = _parse_date(args.desde)
    hasta = _parse_date(args.hasta)
    bad = _validate_period_cli(desde, hasta)
    if bad is not None:
        return bad
    sheet = (args.sheet or "").strip() or "V2_Ejemplo"
    start = int(args.data_start)
    try:
        results = compare_excel_vs_sqlite(
            excel,
            s.arca_sqlite_path,
            desde,
            hasta,
            sheet_name=sheet,
            data_start_row=start,
        )
    except FileNotFoundError as e:
        logger.error("%s", e)
        return 2
    except ValueError as e:
        logger.error("%s", e)
        return 2
    print(format_compare_report(results))
    any_diff = any(r.get("status") == "diff" for r in results)
    any_solo = any(r.get("status") == "solo_excel" for r in results)
    return 1 if (any_diff or any_solo) else 0


def cmd_export_recupero(args: argparse.Namespace) -> int:
    """Excel tipo modelo Recupero (hoja recupero_V2) desde SQLite."""
    s = get_settings()
    desde = _parse_date(args.desde)
    hasta = _parse_date(args.hasta)
    bad = _validate_period_cli(desde, hasta)
    if bad is not None:
        return bad
    out = Path(args.out) if args.out else s.arca_excel_output.parent / "recupero_tasa.xlsx"
    path = build_recupero_v2_excel(s.arca_sqlite_path, out, desde, hasta, s)
    csv_path = path.with_suffix(".csv")
    logger.info("Excel recupero modelo: %s", path.resolve())
    logger.info("CSV (mismos datos, abre en Cursor): %s", csv_path.resolve())
    return 0


def cmd_report_ic_tasa(args: argparse.Namespace) -> int:
    """Despachos con IC en el identificador y monto tasa por período (SQLite)."""
    s = get_settings()
    desde = _parse_date(args.desde)
    hasta = _parse_date(args.hasta)
    bad = _validate_period_cli(desde, hasta)
    if bad is not None:
        return bad
    m = get_tasa_mapper_from_settings(s)
    rows = query_ic_tasa_rows(s.arca_sqlite_path, desde, hasta, m.codigos, settings=s)
    tot = total_monto(rows)
    logger.info(
        "Período %s → %s: %s despachos IC con tasa, total ARS %s",
        desde,
        hasta,
        len(rows),
        tot,
    )
    for r in rows:
        logger.info(
            "  %s | fecha=%s | codigo=%s | monto=%s",
            r.get("destinacion_id"),
            r.get("fecha"),
            r.get("codigo_tasa"),
            r.get("monto_tasa_estadistica"),
        )
    out = Path(args.out) if args.out else s.arca_data_dir / "report_ic_tasa.csv"
    write_ic_tasa_csv(rows, out)
    logger.info("CSV: %s", out.resolve())
    return 0


def cmd_refetch_caratula(args: argparse.Namespace) -> int:
    """Refetch acotado: solo `DetalladaCaratula` + `DetalladaLiquidacionesDetalle` de un D.I.

    Actualiza in-place el `raw_json` de la fila más reciente del despacho en SQLite.
    Útil cuando AFIP devolvió 6013 justo en `DetalladaCaratula` y la fila quedó sin montos
    FOB/FLETE/SEGURO, pero el resto del fetch terminó OK.
    """
    from tasa_estadistica.arca.moa_declaracion import fetch_moa_caratula_unica

    s = get_settings()
    if s.arca_mode.lower() != "live":
        logger.error("ARCA_MODE=live requerido (actual: %s)", s.arca_mode)
        return 2
    ta_path = s.arca_ticket_path
    if not ta_path.is_file():
        logger.error("Ticket WSAA no encontrado en %s. Ejecute `tasa-arca auth`.", ta_path)
        return 2
    try:
        ta_xml = ta_path.read_bytes()
    except OSError as e:
        logger.error("No pude leer %s: %s", ta_path, e)
        return 2
    if wsaa_ticket_expired(ta_xml):
        logger.error("Ticket WSAA expirado; ejecute `tasa-arca auth`.")
        return 2

    dest = (args.destinacion or "").strip()
    if not dest:
        logger.error("Indique --destinacion <D.I.>")
        return 2

    cuit = (args.cuit or s.arca_cuit or "").strip()
    if not cuit:
        logger.error("Indique --cuit o configure ARCA_CUIT")
        return 2

    with sqlite3.connect(str(s.arca_sqlite_path)) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            """
            SELECT id, raw_json
            FROM liquidaciones
            WHERE destinacion_id = ?
            ORDER BY id DESC
            """,
            (dest,),
        ).fetchall()
        if not rows:
            logger.error("Sin filas en SQLite para destinacion_id=%s", dest)
            return 2

    logger.info("Refetch carátula MOA para %s…", dest)
    try:
        caratula, detalles = fetch_moa_caratula_unica(s, cuit, dest, ta_xml)
    except (RuntimeError, ValueError) as e:
        logger.error("Falló refetch: %s", e)
        return 2

    # Merge in-place: actualizamos SOLO los bloques MOA detallados, sin tocar el resto.
    with sqlite3.connect(str(s.arca_sqlite_path)) as c:
        c.row_factory = sqlite3.Row
        for r in rows:
            raw_s = r["raw_json"] or "{}"
            try:
                payload = json.loads(raw_s)
            except json.JSONDecodeError:
                continue
            inner = payload.setdefault("raw", {})
            if not isinstance(inner, dict):
                inner = {}
                payload["raw"] = inner
            inner["moa_detallada_caratula"] = caratula
            # Si hay 1 fila por `IdentificadorLiquidacion` en detalles y la fila tiene
            # liquidacion_resumen, elegimos el detalle que coincida; fallback = primer detalle.
            liq_res = inner.get("liquidacion_resumen") or {}
            id_liq_row = (
                str(liq_res.get("IdentificadorLiquidacion") or "").strip()
                if isinstance(liq_res, dict)
                else ""
            )
            det_elegido: dict | None = None
            for d in detalles:
                if d.get("identificador_liquidacion") == id_liq_row:
                    det_elegido = d.get("detalle") if isinstance(d, dict) else None
                    break
            if det_elegido is None and detalles:
                det_elegido = detalles[0].get("detalle")
            if det_elegido is not None:
                inner["moa_detallada_liquidaciones_detalle"] = det_elegido
            c.execute(
                "UPDATE liquidaciones SET raw_json = ? WHERE id = ?",
                (json.dumps(payload, ensure_ascii=False), r["id"]),
            )
        c.commit()
    logger.info(
        "OK: %s fila(s) actualizadas para %s (carátula: %s, detalles: %s)",
        len(rows),
        dest,
        "SI" if caratula else "NO",
        len(detalles),
    )
    return 0


def cmd_rebuild(args: argparse.Namespace) -> int:
    try:
        fd, fh = _parse_export_fechas(args)
    except ValueError as e:
        logger.error("%s", e)
        return 2
    s = get_settings()
    if fd is not None and fh is not None:
        bad = _validate_period_cli(fd, fh)
        if bad is not None:
            return bad
    m = get_tasa_mapper_from_settings(s)
    n = reapply_mapper_sqlite(s.arca_sqlite_path, m)
    logger.info("Reglas reaplicadas sobre %s filas de conceptos", n)
    out = build_auditable_excel(
        s.arca_sqlite_path, s.arca_excel_output, fecha_desde=fd, fecha_hasta=fh
    )
    logger.info("Excel regenerado: %s", out)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="tasa-arca", description="ARCA/AFIP — liquidaciones y tasa estadística (local)"
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Log debug")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "auth", help="Obtiene y guarda ticket WSAA (requiere ARCA_MODE=live y certificado)"
    )

    sub.add_parser(
        "doctor",
        help=(
            "Rutas SQLite/TA, WSAA y WSDL (sin contraseña). "
            "Ver docs/afip_runbook_diagnostico.md"
        ),
    )

    pf = sub.add_parser("fetch", help="Descarga liquidaciones/conceptos y persiste en SQLite")
    pf.add_argument("--desde", required=True, help="Fecha desde (YYYY-MM-DD)")
    pf.add_argument("--hasta", required=True, help="Fecha hasta (YYYY-MM-DD)")
    pf.add_argument("--cuit", default="", help="CUIT (default: ARCA_CUIT)")

    pex = sub.add_parser("export", help="Genera Excel auditable desde SQLite")
    pex.add_argument(
        "--desde",
        default="",
        help="Con --hasta: filtra por fecha_liquidacion en SQLite (YYYY-MM-DD)",
    )
    pex.add_argument(
        "--hasta",
        default="",
        help="Opcional con --desde: fin del rango inclusive (YYYY-MM-DD)",
    )

    prb = sub.add_parser("rebuild", help="Reaplica reglas de tasa sobre SQLite y regenera Excel")
    prb.add_argument("--desde", default="", help="Igual que export: filtro opcional de fechas")
    prb.add_argument("--hasta", default="", help="Igual que export: filtro opcional de fechas")

    prec = sub.add_parser(
        "export-recupero",
        help="Excel formato modelo Recupero (V2): D.I., TC, T.E. 011/061/062 desde SQLite",
    )
    prec.add_argument("--desde", required=True, help="Fecha desde liquidación (YYYY-MM-DD)")
    prec.add_argument("--hasta", required=True, help="Fecha hasta liquidación (YYYY-MM-DD)")
    prec.add_argument(
        "--out",
        default="",
        help="Salida .xlsx (default: out/recupero_tasa.xlsx junto al Excel de auditoría)",
    )

    pcmp = sub.add_parser(
        "compare-recupero-excel",
        help="Compara el Excel modelo (hoja V2_Ejemplo) con filas Recupero desde SQLite",
    )
    pcmp.add_argument(
        "--excel",
        required=True,
        help='Ruta al .xlsx (ej. "Modelo Excel - Recupero Tasa de Estadistica.xlsx")',
    )
    pcmp.add_argument("--desde", required=True, help="Fecha desde liquidación (YYYY-MM-DD)")
    pcmp.add_argument("--hasta", required=True, help="Fecha hasta liquidación (YYYY-MM-DD)")
    pcmp.add_argument(
        "--sheet",
        default="V2_Ejemplo",
        help="Nombre de hoja (default V2_Ejemplo)",
    )
    pcmp.add_argument(
        "--data-start",
        type=int,
        default=4,
        help="Primera fila de datos en el Excel (default 4; fila 3 = cabeceras)",
    )

    pr = sub.add_parser(
        "report-ic-tasa",
        help="Lista despachos importación (identificador con IC) y monto tasa (desde SQLite)",
    )
    pr.add_argument("--desde", required=True, help="Fecha desde liquidación (YYYY-MM-DD)")
    pr.add_argument("--hasta", required=True, help="Fecha hasta liquidación (YYYY-MM-DD)")
    pr.add_argument(
        "--out",
        default="",
        help="CSV de salida (default: ARCA_DATA_DIR/report_ic_tasa.csv)",
    )

    prfc = sub.add_parser(
        "refetch-caratula",
        help="Refetch acotado (solo carátula + detalle) de un D.I. para completar FOB/Flete/Seguro",
    )
    prfc.add_argument(
        "--destinacion",
        required=True,
        help="Identificador de destinación (D.I.) a refrescar",
    )
    prfc.add_argument("--cuit", default="", help="CUIT (default: ARCA_CUIT)")

    pbf = sub.add_parser(
        "backfill",
        help=(
            "Backfill mes a mes con estado persistente: idempotente, reanudable, "
            "skipea meses ya OK. Acepta --desde/--hasta como YYYY-MM o YYYY-MM-DD."
        ),
    )
    pbf.add_argument("--desde", default="", help="Mes/fecha desde (YYYY-MM o YYYY-MM-DD)")
    pbf.add_argument("--hasta", default="", help="Mes/fecha hasta (YYYY-MM o YYYY-MM-DD)")
    pbf.add_argument("--cuit", default="", help="CUIT (default: ARCA_CUIT)")
    pbf.add_argument(
        "--reintentar-errores",
        action="store_true",
        help="Reprocesa meses en estado 'error' (no toca 'ok' ni 'sin_datos').",
    )
    pbf.add_argument(
        "--forzar",
        action="store_true",
        help="Reprocesa TODO el rango ignorando el estado actual (uso excepcional).",
    )
    pbf.add_argument(
        "--solo-mes",
        default="",
        help="Atajo: procesa un único mes (YYYY-MM). Reemplaza --desde/--hasta.",
    )
    pbf.add_argument(
        "--pausa",
        type=float,
        default=0.0,
        help="Segundos a esperar entre meses (default 0; útil para no saturar AFIP).",
    )

    pbs = sub.add_parser(
        "backfill-status",
        help="Imprime tabla anio/mes con estado del backfill (sin tocar AFIP).",
    )
    pbs.add_argument("--desde", required=True, help="Mes/fecha desde (YYYY-MM o YYYY-MM-DD)")
    pbs.add_argument("--hasta", required=True, help="Mes/fecha hasta (YYYY-MM o YYYY-MM-DD)")
    pbs.add_argument("--cuit", default="", help="CUIT (default: ARCA_CUIT)")

    pserve = sub.add_parser(
        "serve",
        help="Panel web local (SQLite). Extra: pip install -e \".[web]\"",
    )
    pserve.add_argument(
        "--host",
        default="127.0.0.1",
        help="127.0.0.1 solo esta PC; use 0.0.0.0 para acceder desde la red local",
    )
    pserve.add_argument("--port", type=int, default=8000, help="Puerto HTTP (default 8000)")
    pserve.add_argument(
        "--reload",
        action="store_true",
        help="Recargar código al guardar (desarrollo). Equivale a TASA_ARCA_RELOAD=1.",
    )

    return p


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    argv = argv if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    if args.command == "auth":
        return cmd_auth(args)
    if args.command == "doctor":
        return cmd_doctor(args)
    if args.command == "fetch":
        return cmd_fetch(args)
    if args.command == "export":
        return cmd_export(args)
    if args.command == "rebuild":
        return cmd_rebuild(args)
    if args.command == "export-recupero":
        return cmd_export_recupero(args)
    if args.command == "compare-recupero-excel":
        return cmd_compare_recupero_excel(args)
    if args.command == "report-ic-tasa":
        return cmd_report_ic_tasa(args)
    if args.command == "serve":
        return cmd_serve(args)
    if args.command == "refetch-caratula":
        return cmd_refetch_caratula(args)
    if args.command == "backfill":
        return cmd_backfill(args)
    if args.command == "backfill-status":
        return cmd_backfill_status(args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
