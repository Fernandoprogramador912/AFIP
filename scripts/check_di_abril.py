"""Diagnóstico para 25001IC04068168A: raw_json, fecha y carátula."""

from __future__ import annotations

import json
import sqlite3

DB = "data/tasa_estadistica.db"
DI = "25001IC04068168A"


def _find_fecha_en_dict(d: dict, claves: list[str]) -> str | None:
    for k in claves:
        v = d.get(k)
        if v is not None and str(v).strip():
            return str(v)
    return None


def main() -> int:
    keys_fecha = [
        "FechaOficializacionDeclaracion",
        "FechaOficializacion",
        "FechaLiquidacion",
        "FechaLiquidacionDestinacion",
        "Fecha",
    ]
    with sqlite3.connect(DB) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            "SELECT id, fecha, destinacion_id, numero, raw_json FROM liquidaciones "
            "WHERE destinacion_id = ? ORDER BY id DESC",
            (DI,),
        ).fetchall()
        print(f"Filas para {DI}: {len(rows)}")
        for r in rows:
            raw = r["raw_json"] or "{}"
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                print(f"  id={r['id']} fecha={r['fecha']} raw_json inválido")
                continue
            inner = payload.get("raw") or {}
            cara = inner.get("moa_detallada_caratula")
            detalle = inner.get("moa_detallada_liquidaciones_detalle")
            listado = inner.get("declaracion_listado") or {}
            resumen = inner.get("liquidacion_resumen") or {}

            tiene_cara = bool(cara) and cara not in ({}, [], None)
            tiene_det = bool(detalle) and detalle not in ({}, [], None)

            print(f"\n  id={r['id']} fecha_col={r['fecha']} numero={r['numero']}")
            print(f"    carátula_presente={tiene_cara} detalle_presente={tiene_det}")
            if isinstance(cara, dict):
                # carátula puede envolver: CaratulaDeclaracion/Declaracion/DeclaracionDetallada
                for key in ("", "Declaracion", "CaratulaDeclaracion", "DeclaracionDetallada"):
                    sub = cara if key == "" else cara.get(key)
                    if isinstance(sub, dict):
                        f = _find_fecha_en_dict(sub, keys_fecha)
                        if f:
                            print(f"    fecha desde carátula[{key or '(root)'}]: {f}")
                            break
            if isinstance(listado, dict):
                f = _find_fecha_en_dict(listado, keys_fecha)
                if f:
                    print(f"    fecha desde listado: {f}")
            if isinstance(resumen, dict):
                f = _find_fecha_en_dict(resumen, keys_fecha)
                if f:
                    print(f"    fecha desde liquidacion_resumen: {f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
